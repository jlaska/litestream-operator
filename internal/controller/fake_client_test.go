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

// Package controller contains fake-client error-injection tests.
// These tests use controller-runtime's fake client with WithInterceptorFuncs to
// exercise error paths (non-NotFound Get failures, Patch failures, etc.) that
// the live envtest environment cannot inject without API server cooperation.
package controller

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
)

// newFakeDB creates a minimal LitestreamReplica for fake client tests.
func newFakeDB(name, namespace, targetDep string) *databasev1.LitestreamReplica {
	return &databasev1.LitestreamReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Finalizers: []string{replicaFinalizerName},
		},
		Spec: databasev1.LitestreamReplicaSpec{
			DatabaseName:     "app.db",
			DatabasePath:     "/data",
			TargetDeployment: targetDep,
			Backup: databasev1.BackupSpec{
				Enabled: true,
				Destination: databasev1.BackupDestination{
					S3: &databasev1.S3Destination{Bucket: "b", SecretRef: "s"},
				},
			},
		},
	}
}

// newFakeDeploymentWithPVC returns a Deployment with a PVC volume mount at /data for InPlace restore tests.
func newFakeDeploymentWithPVC(name, namespace string) *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "app",
						Image: "busybox",
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/data"},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "restore-pvc",
							},
						},
					}},
				},
			},
		},
	}
}

// newFakeInPlaceRestore returns a LitestreamRestore using InPlace mode for fake client tests.
func newFakeInPlaceRestore(name, namespace, sourceRef, phase string) *databasev1.LitestreamRestore {
	r := &databasev1.LitestreamRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Finalizers: []string{restoreFinalizerName},
		},
		Spec: databasev1.LitestreamRestoreSpec{
			SourceRef: databasev1.RestoreSourceRef{Name: sourceRef},
		},
	}
	r.Status.Phase = phase
	return r
}

// newFakeDeployment returns a minimal single-replica Deployment for fake client tests.
func newFakeDeployment(name, namespace string) *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
			},
		},
	}
}

// newFakeRestore returns a minimal LitestreamRestore for fake client tests.
// Uses ToPVC mode so tests don't need a real Deployment for PVC resolution.
func newFakeRestore(name, namespace, sourceRef, phase string) *databasev1.LitestreamRestore {
	r := &databasev1.LitestreamRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Finalizers: []string{restoreFinalizerName},
		},
		Spec: databasev1.LitestreamRestoreSpec{
			SourceRef: databasev1.RestoreSourceRef{Name: sourceRef},
			Mode:      databasev1.RestoreModeToPVC,
			Target: &databasev1.RestoreTarget{
				PVC:  "restore-pvc",
				Path: "/data/app.db",
			},
		},
	}
	r.Status.Phase = phase
	r.Status.ResolvedPVC = "restore-pvc"
	r.Status.ResolvedPath = "/data/app.db"
	return r
}

// buildFakeDBClient creates a fake client loaded with the given objects and interceptors,
// then returns a LitestreamReplicaReconciler backed by it.
func buildFakeDBClient(objs []client.Object, funcs interceptor.Funcs) *LitestreamReplicaReconciler {
	b := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(funcs)
	if len(objs) > 0 {
		b = b.WithObjects(objs...)
		// Register CRs as status-subresource objects so Status().Patch works correctly.
		var statusObjs []client.Object
		for _, o := range objs {
			switch o.(type) {
			case *databasev1.LitestreamReplica, *databasev1.LitestreamRestore:
				statusObjs = append(statusObjs, o)
			}
		}
		if len(statusObjs) > 0 {
			b = b.WithStatusSubresource(statusObjs...)
		}
	}
	fc := b.Build()
	return &LitestreamReplicaReconciler{
		Client:   fc,
		Scheme:   scheme.Scheme,
		Recorder: record.NewFakeRecorder(10),
	}
}

// buildFakeRestoreClient creates a fake client loaded with the given objects and interceptors,
// then returns a LitestreamRestoreReconciler backed by it.
func buildFakeRestoreClient(objs []client.Object, funcs interceptor.Funcs) *LitestreamRestoreReconciler {
	b := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(funcs)
	if len(objs) > 0 {
		b = b.WithObjects(objs...)
		var statusObjs []client.Object
		for _, o := range objs {
			switch o.(type) {
			case *databasev1.LitestreamReplica, *databasev1.LitestreamRestore:
				statusObjs = append(statusObjs, o)
			}
		}
		if len(statusObjs) > 0 {
			b = b.WithStatusSubresource(statusObjs...)
		}
	}
	fc := b.Build()
	return &LitestreamRestoreReconciler{
		Client:    fc,
		APIReader: fc,
		Scheme:    scheme.Scheme,
		Recorder:  record.NewFakeRecorder(10),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// LitestreamReplicaReconciler error injection
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("LitestreamReplicaReconciler error injection", func() {
	const (
		ns      = "default"
		dbName  = "fake-db"
		depName = "fake-dep"
	)
	ctx := context.Background()
	key := reconcile.Request{NamespacedName: types.NamespacedName{Name: dbName, Namespace: ns}}

	It("Reconcile returns error when Get(LitestreamReplica) returns transient error", func() {
		r := buildFakeDBClient(nil, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, o client.Object, opts ...client.GetOption) error {
				if _, ok := o.(*databasev1.LitestreamReplica); ok {
					return fmt.Errorf("transient API error")
				}
				return c.Get(ctx, k, o, opts...)
			},
		})
		_, err := r.Reconcile(ctx, key)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("transient"))
	})

	It("reconcileLitestreamConfig returns error when CreateOrUpdate Create fails", func() {
		db := newFakeDB(dbName, ns, depName)
		// ConfigMap does not exist so controllerutil.CreateOrUpdate calls Create.
		r := buildFakeDBClient([]client.Object{db}, interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.CreateOption) error {
				if _, ok := o.(*corev1.ConfigMap); ok {
					return fmt.Errorf("patch failed")
				}
				return c.Create(ctx, o, opts...)
			},
		})
		err := r.reconcileLitestreamConfig(ctx, db)
		Expect(err).To(HaveOccurred())
	})

	It("updateStatus returns error when Status().Patch fails", func() {
		// DB targets a Deployment that does not exist so updateStatus takes the
		// WorkloadNotFound path and calls Status().Patch immediately.
		db := newFakeDB(dbName, ns, "nonexistent-dep")
		dep := newFakeDeployment(depName, ns)
		r := buildFakeDBClient([]client.Object{db, dep}, interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, o client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
				return fmt.Errorf("status patch injected error")
			},
		})
		err := r.updateStatus(ctx, db)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("status patch injected error"))
	})
})

// ─────────────────────────────────────────────────────────────────────────────
// LitestreamRestoreReconciler error injection
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("LitestreamRestoreReconciler error injection", func() {
	const (
		ns          = "default"
		srcDBName   = "fake-src-db"
		srcDepName  = "fake-src-dep"
		restoreName = "fake-restore"
	)
	ctx := context.Background()
	restoreKey := reconcile.Request{NamespacedName: types.NamespacedName{Name: restoreName, Namespace: ns}}

	It("Reconcile returns error when Get(LitestreamRestore) returns transient error", func() {
		r := buildFakeRestoreClient(nil, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, o client.Object, opts ...client.GetOption) error {
				if _, ok := o.(*databasev1.LitestreamRestore); ok {
					return fmt.Errorf("transient API error")
				}
				return c.Get(ctx, k, o, opts...)
			},
		})
		_, err := r.Reconcile(ctx, restoreKey)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("transient"))
	})

	It("reconcileAcquiringLock returns error when pauseReplication Patch fails (InPlace)", func() {
		db := newFakeDB(srcDBName, ns, srcDepName)
		dep := newFakeDeploymentWithPVC(srcDepName, ns)
		restore := newFakeInPlaceRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseAcquiringLock)
		replicas := int32(1)
		restore.Status.OriginalReplicas = &replicas

		r := buildFakeRestoreClient([]client.Object{db, dep, restore}, interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, o client.Object, p client.Patch, opts ...client.PatchOption) error {
				if _, ok := o.(*databasev1.LitestreamReplica); ok {
					return fmt.Errorf("patch failed")
				}
				return c.Patch(ctx, o, p, opts...)
			},
		})
		_, err := r.Reconcile(ctx, restoreKey)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("setting pause annotation on LitestreamReplica"))
	})

	It("reconcileFencing returns error when Get(ConfigMap) fails", func() {
		db := newFakeDB(srcDBName, ns, srcDepName)
		// Pause annotation must be set so reconcileFencing skips the re-set-annotation branch.
		db.Annotations = map[string]string{pauseAnnotation: injectEnabled}

		restore := newFakeRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseFencing)
		replicas := int32(1)
		restore.Status.OriginalReplicas = &replicas

		r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{
			// Return a non-NotFound error for ConfigMap so reconcileFencing fails.
			Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, o client.Object, opts ...client.GetOption) error {
				if _, ok := o.(*corev1.ConfigMap); ok {
					return fmt.Errorf("configmap get error")
				}
				return c.Get(ctx, k, o, opts...)
			},
		})
		_, err := r.Reconcile(ctx, restoreKey)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("getting litestream ConfigMap"))
	})

	It("reconcileFencing returns error when Create(restore Job) fails with non-AlreadyExists error", func() {
		db := newFakeDB(srcDBName, ns, srcDepName)
		db.Annotations = map[string]string{pauseAnnotation: injectEnabled}
		dep := newFakeDeploymentWithPVC(srcDepName, ns)
		// Set deployment replicas to 0 so fencing considers pods terminated.
		zero := int32(0)
		dep.Spec.Replicas = &zero
		dep.Status.Replicas = 0
		replicas := int32(1)
		restore := newFakeRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseFencing)
		restore.Status.OriginalReplicas = &replicas
		// Source DB's litestream ConfigMap with paused content.
		sourceCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: srcDBName + "-litestream", Namespace: ns},
			Data:       map[string]string{"litestream.yml": "dbs: []\n"},
		}
		// Pre-built restore litestream ConfigMap so reconcileRestoreConfig succeeds.
		restoreCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName + "-litestream", Namespace: ns},
			Data:       map[string]string{"litestream.yml": "addr: \":9090\"\ndbs:\n  - path: /data/app.db\n"},
		}

		r := buildFakeRestoreClient([]client.Object{db, dep, restore, sourceCM, restoreCM}, interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.CreateOption) error {
				if _, ok := o.(*batchv1.Job); ok {
					return fmt.Errorf("quota exceeded")
				}
				return c.Create(ctx, o, opts...)
			},
		})
		_, err := r.Reconcile(ctx, restoreKey)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("creating restore Job"))
	})

	It("reconcileRestoring returns error when Get(restore Job) returns transient error", func() {
		db := newFakeDB(srcDBName, ns, srcDepName)
		restore := newFakeRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseRestoring)
		restore.Status.JobName = restoreName + "-restore"
		replicas := int32(1)
		restore.Status.OriginalReplicas = &replicas

		r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, o client.Object, opts ...client.GetOption) error {
				if _, ok := o.(*batchv1.Job); ok {
					return fmt.Errorf("transient")
				}
				return c.Get(ctx, k, o, opts...)
			},
		})
		_, err := r.Reconcile(ctx, restoreKey)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("getting restore Job"))
	})

	It("reconcileResuming returns error when resumeReplication re-fetch of LitestreamReplica fails", func() {
		db := newFakeDB(srcDBName, ns, srcDepName)
		// Pause annotation must be set so resumeReplication proceeds to the re-fetch.
		db.Annotations = map[string]string{pauseAnnotation: injectEnabled}

		// OriginalReplicas=0 skips the workload scale-back block so we reach
		// resumeReplication directly without needing a Deployment in the fake client.
		zero := int32(0)
		restore := newFakeRestore(restoreName, ns, srcDBName, databasev1.RestorePhaseResuming)
		restore.Status.OriginalReplicas = &zero

		// Allow the first LitestreamReplica Get (Reconcile's sourceDB lookup) to succeed.
		// The second Get (resumeReplication's re-fetch) must fail.
		dbGetCount := 0
		r := buildFakeRestoreClient([]client.Object{db, restore}, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, o client.Object, opts ...client.GetOption) error {
				if _, ok := o.(*databasev1.LitestreamReplica); ok {
					dbGetCount++
					if dbGetCount > 1 {
						return fmt.Errorf("transient re-fetch error")
					}
				}
				return c.Get(ctx, k, o, opts...)
			},
		})
		_, err := r.Reconcile(ctx, restoreKey)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("removing pause annotation"))
	})
})
