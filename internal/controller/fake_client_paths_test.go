/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
)

// ─────────────────────────────────────────────────────────────────────────────
// Replication-lag scraping (deterministic, no cluster connectivity required)
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("LitestreamReplicaReconciler scrapeReplicationLag", func() {
	const ns = "default"
	ctx := context.Background()

	newTarget := func() *workloadTarget {
		return &workloadTarget{deployment: newFakeDeployment("lag-dep", ns)}
	}
	runningPod := func(name, ip string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"app": "lag-dep"}},
			Status: corev1.PodStatus{
				PodIP: ip,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:  litestreamSidecar,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				}},
			},
		}
	}

	It("returns error when listing pods fails", func() {
		r := buildFakeDBClient(nil, interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, opts ...client.ListOption) error {
				return fmt.Errorf("list failed")
			},
		})
		_, err := r.scrapeReplicationLag(ctx, newFakeDB("lag-db", ns, "lag-dep"), newTarget())
		Expect(err).To(MatchError(ContainSubstring("listing pods")))
	})

	It("returns error when no pod has a running Litestream sidecar", func() {
		noIP := runningPod("no-ip", "")
		notRunning := runningPod("not-running", "10.0.0.1")
		notRunning.Status.ContainerStatuses[0].State = corev1.ContainerState{}
		r := buildFakeDBClient([]client.Object{noIP, notRunning}, interceptor.Funcs{})
		_, err := r.scrapeReplicationLag(ctx, newFakeDB("lag-db", ns, "lag-dep"), newTarget())
		Expect(err).To(MatchError(ContainSubstring("no running pod with Litestream sidecar")))
	})

	It("propagates the fetch error for a running sidecar pod", func() {
		// A cancelled context makes the HTTP request fail immediately without
		// depending on network timing.
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		r := buildFakeDBClient([]client.Object{runningPod("sidecar", "127.0.0.1")}, interceptor.Funcs{})
		_, err := r.scrapeReplicationLag(cctx, newFakeDB("lag-db", ns, "lag-dep"), newTarget())
		Expect(err).To(HaveOccurred())
	})

	Describe("fetchLastSyncTime", func() {
		It("returns error when the request cannot be built", func() {
			r := buildFakeDBClient(nil, interceptor.Funcs{})
			_, err := r.fetchLastSyncTime(ctx, "bad\x7fhost")
			Expect(err).To(HaveOccurred())
		})

		// The metrics port is fixed at 9090, so these cases bind it on loopback.
		// If the port is unavailable on the host the case is skipped.
		serve := func(body string) {
			l, err := net.Listen("tcp", "127.0.0.1:9090")
			if err != nil {
				Skip("port 9090 unavailable: " + err.Error())
			}
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			srv.Listener = l
			srv.Start()
			DeferCleanup(srv.Close)
		}

		It("parses litestream_replica_last_sync_seconds from the metrics body", func() {
			serve("# HELP litestream_replica_last_sync_seconds x\n" +
				"litestream_replica_last_sync_seconds{db=\"a\"} notanumber\n" +
				"litestream_replica_last_sync_seconds{db=\"a\"} 1700000000.5\n")
			r := buildFakeDBClient(nil, interceptor.Funcs{})
			ts, err := r.fetchLastSyncTime(ctx, "127.0.0.1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ts.Unix()).To(Equal(int64(1700000000)))
			Expect(ts.Nanosecond()).To(BeNumerically("~", 5e8, 1e6))
		})

		It("returns error when the metric is absent", func() {
			serve("some_other_metric 1\n")
			r := buildFakeDBClient(nil, interceptor.Funcs{})
			_, err := r.fetchLastSyncTime(ctx, "127.0.0.1")
			Expect(err).To(MatchError(ContainSubstring("metric not found")))
		})
	})
})

// ─────────────────────────────────────────────────────────────────────────────
// LitestreamRestoreReconciler additional fake-client paths
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("LitestreamRestoreReconciler fake-client paths", func() {
	const (
		ns          = "default"
		srcDBName   = "path-src-db"
		srcDepName  = "path-src-dep"
		restoreName = "path-restore"
	)
	ctx := context.Background()
	restoreKey := reconcile.Request{NamespacedName: types.NamespacedName{Name: restoreName, Namespace: ns}}

	getRestore := func(r *LitestreamRestoreReconciler) *databasev1.LitestreamRestore {
		out := &databasev1.LitestreamRestore{}
		ExpectWithOffset(1, r.Get(ctx, types.NamespacedName{Name: restoreName, Namespace: ns}, out)).To(Succeed())
		return out
	}

	Describe("Reconcile", func() {
		It("adds the finalizer and requeues when it is missing", func() {
			restore := newFakeRestore(restoreName, ns, srcDBName, "")
			restore.Finalizers = nil
			r := buildFakeRestoreClient([]client.Object{restore}, interceptor.Funcs{})
			res, err := r.Reconcile(ctx, restoreKey)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).NotTo(Equal(reconcile.Result{}))
			Expect(getRestore(r).Finalizers).To(ContainElement(restoreFinalizerName))
		})

		It("returns error when adding the finalizer fails", func() {
			restore := newFakeRestore(restoreName, ns, srcDBName, "")
			restore.Finalizers = nil
			r := buildFakeRestoreClient([]client.Object{restore}, interceptor.Funcs{
				Update: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.UpdateOption) error {
					return fmt.Errorf("update failed")
				},
			})
			_, err := r.Reconcile(ctx, restoreKey)
			Expect(err).To(MatchError(ContainSubstring("update failed")))
		})

		It("returns error when Get(LitestreamReplica) returns a transient error", func() {
			restore := newFakeRestore(restoreName, ns, srcDBName, "")
			r := buildFakeRestoreClient([]client.Object{restore}, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, o client.Object, opts ...client.GetOption) error {
					if _, ok := o.(*databasev1.LitestreamReplica); ok {
						return fmt.Errorf("transient source error")
					}
					return c.Get(ctx, k, o, opts...)
				},
			})
			_, err := r.Reconcile(ctx, restoreKey)
			Expect(err).To(MatchError(ContainSubstring("transient source error")))
		})

		It("fails the restore when the source LitestreamReplica is not found", func() {
			restore := newFakeRestore(restoreName, ns, srcDBName, "")
			r := buildFakeRestoreClient([]client.Object{restore}, interceptor.Funcs{})
			_, err := r.Reconcile(ctx, restoreKey)
			Expect(err).NotTo(HaveOccurred())
			updated := getRestore(r)
			Expect(updated.Status.Phase).To(Equal(databasev1.RestorePhaseFailed))
			Expect(updated.Status.Message).To(ContainSubstring("not found"))
		})

		It("finalizes a deleting restore whose source LitestreamReplica is gone", func() {
			restore := newFakeRestore(restoreName, ns, srcDBName, databasev1.RestorePhasePending)
			now := metav1.Now()
			restore.DeletionTimestamp = &now
			r := buildFakeRestoreClient([]client.Object{restore}, interceptor.Funcs{})
			_, err := r.Reconcile(ctx, restoreKey)
			Expect(err).NotTo(HaveOccurred())
			// Removing the last finalizer lets the fake client delete the object.
			err = r.Get(ctx, types.NamespacedName{Name: restoreName, Namespace: ns}, &databasev1.LitestreamRestore{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("does nothing for a restore in a terminal phase", func() {
			db := newFakeDB(srcDBName, ns, srcDepName)
			restore := newFakeRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseCompleted)
			r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{})
			res, err := r.Reconcile(ctx, restoreKey)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(reconcile.Result{}))
		})

		It("fails the restore when the source has no S3 backup destination", func() {
			db := newFakeDB(srcDBName, ns, srcDepName)
			db.Spec.Backup.Enabled = false
			restore := newFakeRestore(restoreName, ns, srcDBName, databasev1.RestorePhasePending)
			r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{})
			_, err := r.Reconcile(ctx, restoreKey)
			Expect(err).NotTo(HaveOccurred())
			updated := getRestore(r)
			Expect(updated.Status.Phase).To(Equal(databasev1.RestorePhaseFailed))
			Expect(updated.Status.Message).To(ContainSubstring("does not have backup enabled"))
		})

		It("moves a legacy Validating ToPVC restore to Completed", func() {
			db := newFakeDB(srcDBName, ns, srcDepName)
			restore := newFakeRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseValidating)
			r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{})
			_, err := r.Reconcile(ctx, restoreKey)
			Expect(err).NotTo(HaveOccurred())
			Expect(getRestore(r).Status.Phase).To(Equal(databasev1.RestorePhaseCompleted))
		})

		It("moves a legacy Validating InPlace restore to Resuming", func() {
			db := newFakeDB(srcDBName, ns, srcDepName)
			restore := newFakeInPlaceRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseValidating)
			r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{})
			_, err := r.Reconcile(ctx, restoreKey)
			Expect(err).NotTo(HaveOccurred())
			Expect(getRestore(r).Status.Phase).To(Equal(databasev1.RestorePhaseResuming))
		})

		It("ignores an unknown phase", func() {
			db := newFakeDB(srcDBName, ns, srcDepName)
			restore := newFakeRestore(restoreName, ns, srcDBName, "Bogus")
			r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{})
			res, err := r.Reconcile(ctx, restoreKey)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(reconcile.Result{}))
		})
	})

	Describe("handleFinalization", func() {
		It("is a no-op when the finalizer is already gone", func() {
			restore := newFakeRestore(restoreName, ns, srcDBName, databasev1.RestorePhasePending)
			restore.Finalizers = nil
			r := buildFakeRestoreClient(nil, interceptor.Funcs{})
			res, err := r.handleFinalization(ctx, restore, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(reconcile.Result{}))
		})

		It("leaves the workload fenced when deleted mid-restore", func() {
			db := newFakeDB(srcDBName, ns, srcDepName)
			db.Annotations = map[string]string{pauseAnnotation: injectEnabled}
			restore := newFakeInPlaceRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseRestoring)
			r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{})
			_, err := r.handleFinalization(ctx, restore, db)
			Expect(err).NotTo(HaveOccurred())

			got := &databasev1.LitestreamReplica{}
			Expect(r.Get(ctx, types.NamespacedName{Name: srcDBName, Namespace: ns}, got)).To(Succeed())
			Expect(got.Annotations).To(HaveKeyWithValue(pauseAnnotation, injectEnabled))
		})

		It("resumes replication when deleted outside a mid-restore phase", func() {
			db := newFakeDB(srcDBName, ns, srcDepName)
			db.Annotations = map[string]string{pauseAnnotation: injectEnabled}
			restore := newFakeInPlaceRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseAcquiringLock)
			r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{})
			_, err := r.handleFinalization(ctx, restore, db)
			Expect(err).NotTo(HaveOccurred())

			got := &databasev1.LitestreamReplica{}
			Expect(r.Get(ctx, types.NamespacedName{Name: srcDBName, Namespace: ns}, got)).To(Succeed())
			Expect(got.Annotations).NotTo(HaveKey(pauseAnnotation))
		})

		It("still removes the finalizer when resuming replication fails", func() {
			db := newFakeDB(srcDBName, ns, srcDepName)
			db.Annotations = map[string]string{pauseAnnotation: injectEnabled}
			restore := newFakeInPlaceRestore(restoreName, ns, srcDBName, databasev1.RestorePhasePending)
			r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, o client.Object, p client.Patch, opts ...client.PatchOption) error {
					if _, ok := o.(*databasev1.LitestreamReplica); ok {
						return fmt.Errorf("patch failed")
					}
					return c.Patch(ctx, o, p, opts...)
				},
			})
			_, err := r.handleFinalization(ctx, restore, db)
			Expect(err).NotTo(HaveOccurred())
			Expect(restore.Finalizers).NotTo(ContainElement(restoreFinalizerName))
		})

		It("returns error when removing the finalizer fails", func() {
			restore := newFakeRestore(restoreName, ns, srcDBName, databasev1.RestorePhasePending)
			r := buildFakeRestoreClient([]client.Object{restore}, interceptor.Funcs{
				Update: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.UpdateOption) error {
					return fmt.Errorf("update failed")
				},
			})
			_, err := r.handleFinalization(ctx, restore, nil)
			Expect(err).To(MatchError(ContainSubstring("update failed")))
		})
	})

	Describe("reconcileAcquiringLock", func() {
		leaseName := "litestream-restore-" + srcDBName
		inPlace := func(objs ...client.Object) (*databasev1.LitestreamRestore, *databasev1.LitestreamReplica, []client.Object) {
			db := newFakeDB(srcDBName, ns, srcDepName)
			restore := newFakeInPlaceRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseAcquiringLock)
			all := append([]client.Object{db, newFakeDeploymentWithPVC(srcDepName, ns), restore}, objs...)
			return restore, db, all
		}

		It("skips locking for ToPVC restores", func() {
			db := newFakeDB(srcDBName, ns, srcDepName)
			restore := newFakeRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseAcquiringLock)
			r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{})
			_, err := r.reconcileAcquiringLock(ctx, restore, db)
			Expect(err).NotTo(HaveOccurred())
			Expect(getRestore(r).Status.Phase).To(Equal(databasev1.RestorePhaseFencing))
		})

		It("returns error when the lease Get fails with a non-NotFound error", func() {
			restore, db, objs := inPlace()
			r := buildFakeRestoreClient(objs, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, o client.Object, opts ...client.GetOption) error {
					if _, ok := o.(*coordinationv1.Lease); ok {
						return fmt.Errorf("lease get failed")
					}
					return c.Get(ctx, k, o, opts...)
				},
			})
			_, err := r.reconcileAcquiringLock(ctx, restore, db)
			Expect(err).To(MatchError(ContainSubstring("checking restore lock")))
		})

		It("requeues when the lease is created concurrently (AlreadyExists)", func() {
			restore, db, objs := inPlace()
			r := buildFakeRestoreClient(objs, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.CreateOption) error {
					if _, ok := o.(*coordinationv1.Lease); ok {
						return apierrors.NewAlreadyExists(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, leaseName)
					}
					return c.Create(ctx, o, opts...)
				},
			})
			res, err := r.reconcileAcquiringLock(ctx, restore, db)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(restoreRequeueInterval))
		})

		It("returns error when creating the lease fails", func() {
			restore, db, objs := inPlace()
			r := buildFakeRestoreClient(objs, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.CreateOption) error {
					if _, ok := o.(*coordinationv1.Lease); ok {
						return fmt.Errorf("lease create failed")
					}
					return c.Create(ctx, o, opts...)
				},
			})
			_, err := r.reconcileAcquiringLock(ctx, restore, db)
			Expect(err).To(MatchError(ContainSubstring("creating restore lock")))
		})

		It("waits when another restore holds the lease", func() {
			other := "other-restore"
			lease := &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: ns},
				Spec:       coordinationv1.LeaseSpec{HolderIdentity: &other},
			}
			restore, db, objs := inPlace(lease)
			r := buildFakeRestoreClient(objs, interceptor.Funcs{})
			res, err := r.reconcileAcquiringLock(ctx, restore, db)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(30 * time.Second))
			updated := getRestore(r)
			Expect(updated.Status.Phase).To(Equal(databasev1.RestorePhaseAcquiringLock))
			Expect(updated.Status.Message).To(ContainSubstring(other))
		})

		It("waits when the lease has no holder identity", func() {
			lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: ns}}
			restore, db, objs := inPlace(lease)
			r := buildFakeRestoreClient(objs, interceptor.Funcs{})
			_, err := r.reconcileAcquiringLock(ctx, restore, db)
			Expect(err).NotTo(HaveOccurred())
			Expect(getRestore(r).Status.Message).To(ContainSubstring("<unknown>"))
		})

		It("acquires the lease, pauses replication and moves to Fencing", func() {
			restore, db, objs := inPlace()
			r := buildFakeRestoreClient(objs, interceptor.Funcs{})
			_, err := r.reconcileAcquiringLock(ctx, restore, db)
			Expect(err).NotTo(HaveOccurred())
			Expect(getRestore(r).Status.Phase).To(Equal(databasev1.RestorePhaseFencing))
			Expect(r.Get(ctx, types.NamespacedName{Name: leaseName, Namespace: ns}, &coordinationv1.Lease{})).To(Succeed())
		})
	})

	Describe("restoreJobPodLogs", func() {
		It("returns empty when no KubeClient is configured", func() {
			r := buildFakeRestoreClient(nil, interceptor.Funcs{})
			Expect(r.restoreJobPodLogs(ctx, ns, restoreName)).To(BeEmpty())
		})

		It("returns empty when the Pod list fails", func() {
			r := buildFakeRestoreClient(nil, interceptor.Funcs{})
			kc := k8sfake.NewSimpleClientset()
			kc.PrependReactor("list", "pods", func(_ clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, fmt.Errorf("list failed")
			})
			r.KubeClient = kc
			Expect(r.restoreJobPodLogs(ctx, ns, restoreName)).To(BeEmpty())
		})

		It("returns empty when no pod matches", func() {
			r := buildFakeRestoreClient(nil, interceptor.Funcs{})
			r.KubeClient = k8sfake.NewSimpleClientset()
			Expect(r.restoreJobPodLogs(ctx, ns, restoreName)).To(BeEmpty())
		})

		It("returns the pod logs when a matching pod exists", func() {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "restore-pod", Namespace: ns, Labels: map[string]string{restoreLabelKey: restoreName},
			}}
			r := buildFakeRestoreClient(nil, interceptor.Funcs{})
			r.KubeClient = k8sfake.NewSimpleClientset(pod)
			// The fake clientset serves a fixed log body for any pod.
			Expect(r.restoreJobPodLogs(ctx, ns, restoreName)).To(Equal("fake logs"))
		})
	})
})
