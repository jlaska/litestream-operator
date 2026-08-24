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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
)

var _ = Describe("LitestreamReplica Controller", func() {
	const (
		resourceName   = "test-litestreamreplica"
		deploymentName = "test-app"
		namespaceName  = "default"
		databaseName   = "myapp.db"
		databasePath   = "/data"
		appLabel       = "app"
	)

	ctx := context.Background()
	namespacedName := types.NamespacedName{Name: resourceName, Namespace: namespaceName}
	deploymentKey := types.NamespacedName{Name: deploymentName, Namespace: namespaceName}
	cmKey := types.NamespacedName{Name: resourceName + "-litestream", Namespace: namespaceName}

	BeforeEach(func() {
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: namespaceName},
			Spec: appsv1.DeploymentSpec{
				Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{appLabel: deploymentName},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{appLabel: deploymentName}},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: namespaceName},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     databaseName,
				DatabasePath:     databasePath,
				TargetDeployment: deploymentName,
				Backup:           databasev1.BackupSpec{Enabled: false},
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())
	})

	AfterEach(func() {
		// Delete ConfigMaps explicitly — envtest does not GC owned objects.
		for _, name := range []string{resourceName + "-litestream", resourceName + "-bootstrap-sql"} {
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespaceName}, cm); err == nil {
				_ = k8sClient.Delete(ctx, cm)
			}
		}
		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, namespacedName, db); err == nil {
			Expect(k8sClient.Delete(ctx, db)).To(Succeed())
			// Wait for the finalizer to complete and the CR to be fully removed.
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, namespacedName, db))
			}).Should(BeTrue())
		}
		dep := &appsv1.Deployment{}
		if err := k8sClient.Get(ctx, deploymentKey, dep); err == nil {
			Expect(k8sClient.Delete(ctx, dep)).To(Succeed())
		}
	})

	It("should create the Litestream ConfigMap", func() {
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			g.Expect(cm.Data).To(HaveKey("litestream.yml"))
			g.Expect(cm.Data["litestream.yml"]).To(ContainSubstring(databasePath + "/" + databaseName))
		}).Should(Succeed())
	})

	It("should annotate the target Deployment's pod template", func() {
		Eventually(func(g Gomega) {
			dep := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(ctx, deploymentKey, dep)).To(Succeed())
			g.Expect(dep.Spec.Template.Annotations).To(HaveKeyWithValue(injectAnnotation, "true"))
			g.Expect(dep.Spec.Template.Annotations).To(HaveKey(configAnnotation))
			g.Expect(dep.Spec.Template.Labels).To(HaveKeyWithValue(injectAnnotation, "true"))
		}).Should(Succeed())
	})

	It("should set TargetReady condition after annotation", func() {
		Eventually(func(g Gomega) {
			db := &databasev1.LitestreamReplica{}
			g.Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
			cond := meta.FindStatusCondition(db.Status.Conditions, databasev1.ConditionTargetReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		}).Should(Succeed())
	})

	It("should set ReplicationHealthy condition to False when backup is disabled", func() {
		Eventually(func(g Gomega) {
			db := &databasev1.LitestreamReplica{}
			g.Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
			cond := meta.FindStatusCondition(db.Status.Conditions, databasev1.ConditionReplicationHealthy)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(cond.Reason).To(Equal("BackupDisabled"))
		}).Should(Succeed())
	})

	It("should set ReplicationHealthy to False when no Litestream pods exist yet", func() {
		// Wait for initial reconcile, then enable backup.
		Eventually(func(g Gomega) {
			db := &databasev1.LitestreamReplica{}
			g.Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
			g.Expect(db.Status.Conditions).NotTo(BeEmpty())
		}).Should(Succeed())

		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
		db.Spec.Backup = databasev1.BackupSpec{
			Enabled: true,
			Destination: databasev1.BackupDestination{
				S3: &databasev1.S3Destination{Bucket: "test", SecretRef: "creds"},
			},
		}
		Expect(k8sClient.Update(ctx, db)).To(Succeed())

		Eventually(func(g Gomega) {
			db := &databasev1.LitestreamReplica{}
			g.Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
			cond := meta.FindStatusCondition(db.Status.Conditions, databasev1.ConditionReplicationHealthy)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(cond.Reason).To(Equal("SidecarUnhealthy"))
		}).Should(Succeed())
	})

	It("should requeue after the status sync interval", func() {
		// Call Reconcile directly to inspect the return value — the manager itself
		// does not expose its RequeueAfter, so this tests the contract directly.
		// Eventually retries on resourceVersion conflicts: the background manager
		// may race on CreateOrUpdate for the Litestream ConfigMap. After the
		// manager's reconcile settles it won't run again for statusSyncInterval,
		// so the retry gets a clean window.
		r := &LitestreamReplicaReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(1),
		}
		Eventually(func(g Gomega) {
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: namespacedName})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.RequeueAfter).To(Equal(statusSyncInterval))
		}).Should(Succeed())
	})

	Context("replication pause", func() {
		// setPauseAnnotation is retry-safe: re-fetches on conflict before setting the
		// pause annotation, avoiding 409 races with the background reconciler.
		setPauseAnnotation := func(value string) {
			Eventually(func() error {
				db := &databasev1.LitestreamReplica{}
				if err := k8sClient.Get(ctx, namespacedName, db); err != nil {
					return err
				}
				if db.Annotations == nil {
					db.Annotations = map[string]string{}
				}
				db.Annotations[databasev1.AnnotationPause] = value
				return k8sClient.Update(ctx, db)
			}).Should(Succeed())
		}
		removePauseAnnotation := func() {
			Eventually(func() error {
				db := &databasev1.LitestreamReplica{}
				if err := k8sClient.Get(ctx, namespacedName, db); err != nil {
					return err
				}
				delete(db.Annotations, databasev1.AnnotationPause)
				return k8sClient.Update(ctx, db)
			}).Should(Succeed())
		}

		It("produces empty dbs config when pause annotation is set", func() {
			// Wait for the manager to create the initial ConfigMap.
			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			}).Should(Succeed())

			setPauseAnnotation("true")

			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
				g.Expect(cm.Data["litestream.yml"]).To(Equal("dbs: []\n"))
			}).Should(Succeed())
		})

		It("produces normal config when pause annotation is absent", func() {
			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
				g.Expect(cm.Data["litestream.yml"]).To(ContainSubstring(databasePath + "/" + databaseName))
				g.Expect(cm.Data["litestream.yml"]).NotTo(Equal("dbs: []\n"))
			}).Should(Succeed())
		})

		It("reverts config to normal after pause annotation is removed", func() {
			// Wait for initial ConfigMap.
			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			}).Should(Succeed())

			setPauseAnnotation("true")

			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
				g.Expect(cm.Data["litestream.yml"]).To(Equal("dbs: []\n"))
			}).Should(Succeed())

			removePauseAnnotation()

			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
				g.Expect(cm.Data["litestream.yml"]).To(ContainSubstring(databasePath + "/" + databaseName))
			}).Should(Succeed())
		})

		It("sets ReplicationPaused condition to True when pause annotation is set", func() {
			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			}).Should(Succeed())

			setPauseAnnotation("true")

			Eventually(func(g Gomega) {
				db := &databasev1.LitestreamReplica{}
				g.Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
				cond := meta.FindStatusCondition(db.Status.Conditions, databasev1.ConditionReplicationPaused)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
				g.Expect(cond.Reason).To(Equal("PauseAnnotationSet"))
			}).Should(Succeed())
		})

		It("sets ReplicationPaused condition to False when pause annotation is absent", func() {
			Eventually(func(g Gomega) {
				db := &databasev1.LitestreamReplica{}
				g.Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
				cond := meta.FindStatusCondition(db.Status.Conditions, databasev1.ConditionReplicationPaused)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(cond.Reason).To(Equal("ReplicationActive"))
			}).Should(Succeed())
		})

		It("sets phase to Paused when pause annotation is set", func() {
			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			}).Should(Succeed())

			setPauseAnnotation("true")

			Eventually(func(g Gomega) {
				db := &databasev1.LitestreamReplica{}
				g.Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
				g.Expect(db.Status.Phase).To(Equal(databasev1.PhasePaused))
			}).Should(Succeed())
		})

		It("reverts phase from Paused when pause annotation is removed", func() {
			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			}).Should(Succeed())

			setPauseAnnotation("true")
			Eventually(func(g Gomega) {
				db := &databasev1.LitestreamReplica{}
				g.Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
				g.Expect(db.Status.Phase).To(Equal(databasev1.PhasePaused))
			}).Should(Succeed())

			removePauseAnnotation()

			Eventually(func(g Gomega) {
				db := &databasev1.LitestreamReplica{}
				g.Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
				g.Expect(db.Status.Phase).NotTo(Equal(databasev1.PhasePaused))
			}).Should(Succeed())
		})
	})

	Context("bootstrap SQL management", func() {
		const bootstrapSQL = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);"

		// setBootstrapSQL is a retry-safe helper that updates spec.Bootstrap.SQL on the test
		// LitestreamReplica, re-fetching on conflict to avoid 409 races with the background
		// reconciler that may patch status between the Get and Update.
		setBootstrapSQL := func(sql string) {
			Eventually(func() error {
				db := &databasev1.LitestreamReplica{}
				if err := k8sClient.Get(ctx, namespacedName, db); err != nil {
					return err
				}
				db.Spec.Bootstrap.SQL = sql
				return k8sClient.Update(ctx, db)
			}).Should(Succeed())
		}

		It("creates the bootstrap-sql ConfigMap when Bootstrap.SQL is set", func() {
			setBootstrapSQL(bootstrapSQL)

			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{
					Name: resourceName + "-bootstrap-sql", Namespace: namespaceName,
				}, cm)).To(Succeed())
				g.Expect(cm.Data["bootstrap.sql"]).To(Equal(bootstrapSQL))
			}).Should(Succeed())
		})

		It("updates the ConfigMap when Bootstrap.SQL changes", func() {
			setBootstrapSQL(bootstrapSQL)

			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{
					Name: resourceName + "-bootstrap-sql", Namespace: namespaceName,
				}, cm)).To(Succeed())
				g.Expect(cm.Data["bootstrap.sql"]).To(Equal(bootstrapSQL))
			}).Should(Succeed())

			// Change the SQL (retry-safe).
			newSQL := bootstrapSQL + "\nCREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY);"
			setBootstrapSQL(newSQL)

			Eventually(func(g Gomega) {
				cm := &corev1.ConfigMap{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{
					Name: resourceName + "-bootstrap-sql", Namespace: namespaceName,
				}, cm)).To(Succeed())
				g.Expect(cm.Data["bootstrap.sql"]).To(Equal(newSQL))
			}).Should(Succeed())
		})

		It("sets BootstrapApplied condition to True when ConfigMap is ready", func() {
			setBootstrapSQL(bootstrapSQL)

			Eventually(func(g Gomega) {
				db := &databasev1.LitestreamReplica{}
				g.Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
				cond := meta.FindStatusCondition(db.Status.Conditions, databasev1.ConditionBootstrapApplied)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
				g.Expect(cond.Reason).To(Equal("ConfigMapReady"))
			}).Should(Succeed())
		})
	})
})

// ─────────────────────────────────────────────────────────────────────────────
// Pure-function and helper tests — no envtest required for most.
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("isFailureReason", func() {
	DescribeTable("classifies container waiting reasons correctly",
		func(reason string, expected bool) {
			Expect(isFailureReason(reason)).To(Equal(expected))
		},
		Entry("CrashLoopBackOff → true", "CrashLoopBackOff", true),
		Entry("OOMKilled → true", "OOMKilled", true),
		Entry("Error → true", "Error", true),
		Entry("ImagePullBackOff → true", "ImagePullBackOff", true),
		Entry("ErrImagePull → true", "ErrImagePull", true),
		Entry("ContainerCreating → false", "ContainerCreating", false),
		Entry("PodInitializing → false", "PodInitializing", false),
		Entry("Running → false", "Running", false),
		Entry("empty string → false", "", false),
	)
})

var _ = Describe("buildLitestreamConfig", func() {
	var reconciler *LitestreamReplicaReconciler

	BeforeEach(func() {
		reconciler = &LitestreamReplicaReconciler{}
	})

	newDB := func(path, name string, backup databasev1.BackupSpec) *databasev1.LitestreamReplica {
		return &databasev1.LitestreamReplica{
			Spec: databasev1.LitestreamReplicaSpec{
				DatabasePath: path,
				DatabaseName: name,
				Backup:       backup,
			},
		}
	}

	It("produces minimal config when backup is disabled", func() {
		db := newDB("/data", "app.db", databasev1.BackupSpec{Enabled: false})
		cfg := reconciler.buildLitestreamConfig(db)
		Expect(cfg).To(ContainSubstring("dbs:"))
		Expect(cfg).To(ContainSubstring("/data/app.db"))
		Expect(cfg).NotTo(ContainSubstring("replica:"))
		Expect(cfg).NotTo(ContainSubstring("type: s3"))
	})

	It("includes full S3 replica block when backup is enabled", func() {
		db := newDB("/data", "app.db", databasev1.BackupSpec{
			Enabled: true,
			Destination: databasev1.BackupDestination{
				S3: &databasev1.S3Destination{Bucket: "mybucket", SecretRef: "creds"},
			},
		})
		cfg := reconciler.buildLitestreamConfig(db)
		Expect(cfg).To(ContainSubstring("type: s3"))
		Expect(cfg).To(ContainSubstring("bucket: mybucket"))
		Expect(cfg).NotTo(ContainSubstring("endpoint:"))        // no endpoint for AWS S3
		Expect(cfg).NotTo(ContainSubstring("force-path-style")) // no force-path-style without endpoint
	})

	It("auto-prefixes endpoint with http:// when no scheme present", func() {
		db := newDB("/data", "app.db", databasev1.BackupSpec{
			Enabled: true,
			Destination: databasev1.BackupDestination{
				S3: &databasev1.S3Destination{
					Endpoint:  "minio.homelab:9000",
					Bucket:    "b",
					SecretRef: "s",
				},
			},
		})
		cfg := reconciler.buildLitestreamConfig(db)
		Expect(cfg).To(ContainSubstring("endpoint: http://minio.homelab:9000"))
		Expect(cfg).To(ContainSubstring("force-path-style: true"))
	})

	It("preserves https:// scheme and does not double-prefix", func() {
		db := newDB("/data", "app.db", databasev1.BackupSpec{
			Enabled: true,
			Destination: databasev1.BackupDestination{
				S3: &databasev1.S3Destination{
					Endpoint:  "https://s3.example.com",
					Bucket:    "b",
					SecretRef: "s",
				},
			},
		})
		cfg := reconciler.buildLitestreamConfig(db)
		Expect(cfg).To(ContainSubstring("endpoint: https://s3.example.com"))
		Expect(cfg).NotTo(ContainSubstring("http://https://"))
	})

	It("preserves http:// scheme and does not double-prefix", func() {
		db := newDB("/data", "app.db", databasev1.BackupSpec{
			Enabled: true,
			Destination: databasev1.BackupDestination{
				S3: &databasev1.S3Destination{
					Endpoint:  "http://minio:9000",
					Bucket:    "b",
					SecretRef: "s",
				},
			},
		})
		cfg := reconciler.buildLitestreamConfig(db)
		Expect(cfg).To(ContainSubstring("endpoint: http://minio:9000"))
		Expect(cfg).NotTo(ContainSubstring("http://http://"))
	})

	It("includes path when set, stripping trailing slash", func() {
		db := newDB("/data", "app.db", databasev1.BackupSpec{
			Enabled: true,
			Destination: databasev1.BackupDestination{
				S3: &databasev1.S3Destination{
					Bucket:    "b",
					Path:      "myapp/",
					SecretRef: "s",
				},
			},
		})
		cfg := reconciler.buildLitestreamConfig(db)
		// Trailing slash must be stripped: Litestream 0.5.x appends "/L{N}/" to the
		// configured path, so a trailing slash produces "myapp//L0/" which MinIO
		// rejects as XMinioInvalidObjectName.
		Expect(cfg).To(ContainSubstring("path: myapp"))
		Expect(cfg).NotTo(ContainSubstring("path: myapp/"))
	})

	It("includes retention when set", func() {
		db := newDB("/data", "app.db", databasev1.BackupSpec{
			Enabled: true,
			Destination: databasev1.BackupDestination{
				S3: &databasev1.S3Destination{Bucket: "b", SecretRef: "s"},
			},
			Retention: databasev1.RetentionPolicy{Duration: "168h"},
		})
		cfg := reconciler.buildLitestreamConfig(db)
		Expect(cfg).To(ContainSubstring("retention: 168h"))
	})

	It("always includes addr: \":9090\" for Prometheus metrics", func() {
		db := newDB("/data", "app.db", databasev1.BackupSpec{Enabled: false})
		cfg := reconciler.buildLitestreamConfig(db)
		Expect(cfg).To(ContainSubstring(`addr: ":9090"`))
	})

	It("includes sync-interval when SyncInterval is set", func() {
		db := newDB("/data", "app.db", databasev1.BackupSpec{
			Enabled: true,
			Destination: databasev1.BackupDestination{
				S3: &databasev1.S3Destination{Bucket: "b", SecretRef: "s"},
			},
			SyncInterval: "500ms",
		})
		cfg := reconciler.buildLitestreamConfig(db)
		Expect(cfg).To(ContainSubstring("sync-interval: 500ms"))
	})

	It("omits sync-interval when SyncInterval is not set", func() {
		db := newDB("/data", "app.db", databasev1.BackupSpec{
			Enabled: true,
			Destination: databasev1.BackupDestination{
				S3: &databasev1.S3Destination{Bucket: "b", SecretRef: "s"},
			},
		})
		cfg := reconciler.buildLitestreamConfig(db)
		Expect(cfg).NotTo(ContainSubstring("sync-interval"))
	})
})

func createTestPod(ctx context.Context, name, namespace, appLabel string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{"app": appLabel}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	return pod
}

func patchContainerStatuses(ctx context.Context, pod *corev1.Pod, statuses []corev1.ContainerStatus) {
	patch := client.MergeFrom(pod.DeepCopy())
	pod.Status.ContainerStatuses = statuses
	Expect(k8sClient.Status().Patch(ctx, pod, patch)).To(Succeed())
}

func patchInitContainerStatuses(ctx context.Context, pod *corev1.Pod, statuses []corev1.ContainerStatus) {
	patch := client.MergeFrom(pod.DeepCopy())
	pod.Status.InitContainerStatuses = statuses
	Expect(k8sClient.Status().Patch(ctx, pod, patch)).To(Succeed())
}

var _ = Describe("litestreamContainerState and archiveCheckState", func() {
	// These tests use envtest to create Pods with manually-patched container statuses.
	const (
		lsTestNamespace = "default"
		lsDepName       = "ls-state-app"
		lsDBName        = "ls-state-db"
		lsSecretRef     = "ls-creds"
	)

	ctx := context.Background()

	var (
		deployment        *appsv1.Deployment
		litestreamReplica *databasev1.LitestreamReplica
		reconciler        *LitestreamReplicaReconciler
	)

	BeforeEach(func() {
		reconciler = &LitestreamReplicaReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(10),
		}

		replicas := int32(1)
		deployment = &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: lsDepName, Namespace: lsTestNamespace},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": lsDepName},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": lsDepName}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, deployment)).To(Succeed())

		litestreamReplica = &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: lsDBName, Namespace: lsTestNamespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "app.db",
				DatabasePath:     "/data",
				TargetDeployment: lsDepName,
				Backup: databasev1.BackupSpec{
					Enabled: true,
					Destination: databasev1.BackupDestination{
						S3: &databasev1.S3Destination{Bucket: "b", SecretRef: lsSecretRef},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, litestreamReplica)).To(Succeed())
	})

	AfterEach(func() {
		_ = k8sClient.Delete(ctx, litestreamReplica)
		_ = k8sClient.Delete(ctx, deployment)
		// Clean up any pods created during tests.
		podList := &corev1.PodList{}
		_ = k8sClient.List(ctx, podList,
			client.InNamespace(lsTestNamespace),
			client.MatchingLabels{"app": lsDepName})
		for i := range podList.Items {
			_ = k8sClient.Delete(ctx, &podList.Items[i])
		}
	})

	Describe("litestreamContainerState", func() {
		It("returns healthy=false when no pods exist", func() {
			wt := &workloadTarget{deployment: deployment}
			healthy, msg := reconciler.litestreamContainerState(ctx, litestreamReplica, wt)
			Expect(healthy).To(BeFalse())
			Expect(msg).To(ContainSubstring("no pods with Litestream sidecar found"))
		})

		It("returns healthy=true when sidecar is Running", func() {
			pod := createTestPod(ctx, "ls-running-pod", lsTestNamespace, lsDepName)
			patchContainerStatuses(ctx, pod, []corev1.ContainerStatus{{
				Name:  "litestream",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
			}})
			wt := &workloadTarget{deployment: deployment}
			healthy, msg := reconciler.litestreamContainerState(ctx, litestreamReplica, wt)
			Expect(healthy).To(BeTrue())
			Expect(msg).To(ContainSubstring("running in 1 pod"))
		})

		It("returns healthy=false when sidecar is in CrashLoopBackOff", func() {
			pod := createTestPod(ctx, "ls-crashloop-pod", lsTestNamespace, lsDepName)
			patchContainerStatuses(ctx, pod, []corev1.ContainerStatus{{
				Name:  "litestream",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			}})
			wt := &workloadTarget{deployment: deployment}
			healthy, msg := reconciler.litestreamContainerState(ctx, litestreamReplica, wt)
			Expect(healthy).To(BeFalse())
			Expect(msg).To(ContainSubstring("unhealthy"))
		})

		It("returns healthy=false when sidecar terminated with non-zero exit", func() {
			pod := createTestPod(ctx, "ls-terminated-pod", lsTestNamespace, lsDepName)
			patchContainerStatuses(ctx, pod, []corev1.ContainerStatus{{
				Name:  "litestream",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}},
			}})
			wt := &workloadTarget{deployment: deployment}
			healthy, msg := reconciler.litestreamContainerState(ctx, litestreamReplica, wt)
			Expect(healthy).To(BeFalse())
			Expect(msg).To(ContainSubstring("unhealthy"))
		})
	})

	Describe("clearSkipArchiveCheck", func() {
		It("clears skip-archive-check annotation when sidecar is healthy", func() {
			// Simulate the post-restore state: skip-archive-check is set on the
			// LitestreamReplica (placed there by the restore controller) and a healthy
			// sidecar pod is present (meaning litestream has started and created the state dir).
			Eventually(func() error {
				db := &databasev1.LitestreamReplica{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: lsDBName, Namespace: lsTestNamespace}, db); err != nil {
					return err
				}
				if db.Annotations == nil {
					db.Annotations = map[string]string{}
				}
				db.Annotations[databasev1.AnnotationSkipArchiveCheck] = "true"
				return k8sClient.Update(ctx, db)
			}).Should(Succeed())

			// Create a pod with a running litestream sidecar.
			pod := createTestPod(ctx, "skip-archive-clear-pod", lsTestNamespace, lsDepName)
			patchContainerStatuses(ctx, pod, []corev1.ContainerStatus{{
				Name:  "litestream",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
			}})

			db := &databasev1.LitestreamReplica{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lsDBName, Namespace: lsTestNamespace}, db)).To(Succeed())
			Expect(reconciler.updateStatus(ctx, db)).To(Succeed())

			// The annotation must be cleared after the sidecar is confirmed healthy.
			updated := &databasev1.LitestreamReplica{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lsDBName, Namespace: lsTestNamespace}, updated)).To(Succeed())
			Expect(updated.Annotations[databasev1.AnnotationSkipArchiveCheck]).NotTo(Equal("true"),
				"skip-archive-check must be cleared once litestream sidecar is healthy")
		})

		It("does not clear skip-archive-check when sidecar is not yet healthy", func() {
			// skip-archive-check is set but no healthy pod exists yet.
			Eventually(func() error {
				db := &databasev1.LitestreamReplica{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: lsDBName, Namespace: lsTestNamespace}, db); err != nil {
					return err
				}
				if db.Annotations == nil {
					db.Annotations = map[string]string{}
				}
				db.Annotations[databasev1.AnnotationSkipArchiveCheck] = "true"
				return k8sClient.Update(ctx, db)
			}).Should(Succeed())

			// No pods — sidecar is not healthy.
			db := &databasev1.LitestreamReplica{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lsDBName, Namespace: lsTestNamespace}, db)).To(Succeed())
			Expect(reconciler.updateStatus(ctx, db)).To(Succeed())

			updated := &databasev1.LitestreamReplica{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lsDBName, Namespace: lsTestNamespace}, updated)).To(Succeed())
			Expect(updated.Annotations[databasev1.AnnotationSkipArchiveCheck]).To(Equal("true"),
				"skip-archive-check must not be cleared while sidecar is not running")
		})
	})

	Describe("archiveCheckState", func() {
		It("returns (false, passed) when backup is disabled", func() {
			litestreamReplica.Spec.Backup.Enabled = false
			wt := &workloadTarget{deployment: deployment}
			failed, msg := reconciler.archiveCheckState(ctx, litestreamReplica, wt)
			Expect(failed).To(BeFalse())
			Expect(msg).To(Equal("backup not enabled"))
		})

		It("returns (false, passed) when no pods exist", func() {
			wt := &workloadTarget{deployment: deployment}
			failed, msg := reconciler.archiveCheckState(ctx, litestreamReplica, wt)
			Expect(failed).To(BeFalse())
			Expect(msg).To(Equal("archive check passed"))
		})

		It("returns (true, failed) when archive-check init container exited non-zero", func() {
			pod := createTestPod(ctx, "archive-check-fail-pod", lsTestNamespace, lsDepName)
			patchInitContainerStatuses(ctx, pod, []corev1.ContainerStatus{{
				Name:  "litestream-archive-check",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}},
			}})
			wt := &workloadTarget{deployment: deployment}
			failed, msg := reconciler.archiveCheckState(ctx, litestreamReplica, wt)
			Expect(failed).To(BeTrue())
			Expect(msg).To(ContainSubstring("archive check failed"))
		})

		It("returns (false, passed) when archive-check init container exited zero", func() {
			pod := createTestPod(ctx, "archive-check-pass-pod", lsTestNamespace, lsDepName)
			patchInitContainerStatuses(ctx, pod, []corev1.ContainerStatus{{
				Name:  "litestream-archive-check",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
			}})
			wt := &workloadTarget{deployment: deployment}
			failed, msg := reconciler.archiveCheckState(ctx, litestreamReplica, wt)
			Expect(failed).To(BeFalse())
			Expect(msg).To(Equal("archive check passed"))
		})
	})
})

// ─────────────────────────────────────────────────────────────────────────────
// StatefulSet support tests
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("LitestreamReplica Controller with StatefulSet target", func() {
	const (
		stsResourceName = "sts-litestreamreplica"
		stsName         = "sts-app"
		namespaceName   = "default"
		databaseName    = "myapp.db"
		databasePath    = "/data"
		appLabel        = "app"
	)

	ctx := context.Background()
	namespacedName := types.NamespacedName{Name: stsResourceName, Namespace: namespaceName}
	stsKey := types.NamespacedName{Name: stsName, Namespace: namespaceName}
	cmKey := types.NamespacedName{Name: stsResourceName + "-litestream", Namespace: namespaceName}

	BeforeEach(func() {
		replicas := int32(1)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: stsName, Namespace: namespaceName},
			Spec: appsv1.StatefulSetSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{appLabel: stsName},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{appLabel: stsName}},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())

		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: stsResourceName, Namespace: namespaceName},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:      databaseName,
				DatabasePath:      databasePath,
				TargetStatefulSet: stsName,
				Backup:            databasev1.BackupSpec{Enabled: false},
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())
	})

	AfterEach(func() {
		for _, name := range []string{stsResourceName + "-litestream", stsResourceName + "-bootstrap-sql"} {
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespaceName}, cm); err == nil {
				_ = k8sClient.Delete(ctx, cm)
			}
		}
		stsDBKey := types.NamespacedName{Name: stsResourceName, Namespace: namespaceName}
		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, stsDBKey, db); err == nil {
			Expect(k8sClient.Delete(ctx, db)).To(Succeed())
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, stsDBKey, db))
			}).Should(BeTrue())
		}
		sts := &appsv1.StatefulSet{}
		if err := k8sClient.Get(ctx, stsKey, sts); err == nil {
			Expect(k8sClient.Delete(ctx, sts)).To(Succeed())
		}
	})

	It("creates the Litestream ConfigMap for a StatefulSet target", func() {
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			g.Expect(cm.Data).To(HaveKey("litestream.yml"))
			g.Expect(cm.Data["litestream.yml"]).To(ContainSubstring(databasePath + "/" + databaseName))
		}).Should(Succeed())
	})

	It("annotates the target StatefulSet pod template", func() {
		Eventually(func(g Gomega) {
			sts := &appsv1.StatefulSet{}
			g.Expect(k8sClient.Get(ctx, stsKey, sts)).To(Succeed())
			g.Expect(sts.Spec.Template.Annotations).To(HaveKeyWithValue(injectAnnotation, "true"))
			g.Expect(sts.Spec.Template.Annotations).To(HaveKey(configAnnotation))
			g.Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue(injectAnnotation, "true"))
		}).Should(Succeed())
	})

	It("sets TargetReady condition after annotating StatefulSet", func() {
		Eventually(func(g Gomega) {
			db := &databasev1.LitestreamReplica{}
			g.Expect(k8sClient.Get(ctx, namespacedName, db)).To(Succeed())
			cond := meta.FindStatusCondition(db.Status.Conditions, databasev1.ConditionTargetReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		}).Should(Succeed())
	})
})

var _ = Describe("workloadTarget helpers", func() {
	It("reports correct typeName for Deployment", func() {
		wt := &workloadTarget{deployment: &appsv1.Deployment{}}
		Expect(wt.typeName()).To(Equal("Deployment"))
	})

	It("reports correct typeName for StatefulSet", func() {
		wt := &workloadTarget{statefulSet: &appsv1.StatefulSet{}}
		Expect(wt.typeName()).To(Equal("StatefulSet"))
	})

	It("desiredReplicas defaults to 1 when spec.Replicas is nil (Deployment)", func() {
		wt := &workloadTarget{deployment: &appsv1.Deployment{}}
		Expect(wt.desiredReplicas()).To(Equal(int32(1)))
	})

	It("desiredReplicas defaults to 1 when spec.Replicas is nil (StatefulSet)", func() {
		wt := &workloadTarget{statefulSet: &appsv1.StatefulSet{}}
		Expect(wt.desiredReplicas()).To(Equal(int32(1)))
	})

	It("desiredReplicas reflects explicit replica count (Deployment)", func() {
		replicas := int32(3)
		wt := &workloadTarget{deployment: &appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{Replicas: &replicas},
		}}
		Expect(wt.desiredReplicas()).To(Equal(int32(3)))
	})

	It("desiredReplicas reflects explicit replica count (StatefulSet)", func() {
		replicas := int32(2)
		wt := &workloadTarget{statefulSet: &appsv1.StatefulSet{
			Spec: appsv1.StatefulSetSpec{Replicas: &replicas},
		}}
		Expect(wt.desiredReplicas()).To(Equal(int32(2)))
	})
})

// ─────────────────────────────────────────────────────────────────────────────
// patchWorkloadPodTemplate edge-case tests — nil Labels on pod template.
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("patchWorkloadPodTemplate nil-Labels paths", func() {
	const patchNS = "default"
	ctx := context.Background()

	newR := func() *LitestreamReplicaReconciler {
		return &LitestreamReplicaReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(10),
		}
	}

	It("initialises nil Labels on a Deployment pod template", func() {
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "patch-dep-nil-labels", Namespace: patchNS},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "patch-dep-nil-labels"},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "patch-dep-nil-labels"},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, dep) }()

		// Clear Labels to nil after creation so the guard branch fires.
		dep.Spec.Template.Labels = nil
		wt := &workloadTarget{deployment: dep}
		Expect(newR().patchWorkloadPodTemplate(ctx, wt,
			map[string]string{"ann-key": "ann-val"},
			map[string]string{"label-key": "label-val"},
		)).To(Succeed())

		updated := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(dep), updated)).To(Succeed())
		Expect(updated.Spec.Template.Labels).To(HaveKeyWithValue("label-key", "label-val"))
	})

	It("initialises nil Labels on a StatefulSet pod template", func() {
		ss := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "patch-ss-nil-labels", Namespace: patchNS},
			Spec: appsv1.StatefulSetSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "patch-ss-nil-labels"},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "patch-ss-nil-labels"},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, ss)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, ss) }()

		ss.Spec.Template.Labels = nil
		wt := &workloadTarget{statefulSet: ss}
		Expect(newR().patchWorkloadPodTemplate(ctx, wt,
			map[string]string{"ann-key": "ann-val"},
			map[string]string{"label-key": "label-val"},
		)).To(Succeed())

		updated := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(ss), updated)).To(Succeed())
		Expect(updated.Spec.Template.Labels).To(HaveKeyWithValue("label-key", "label-val"))
	})
})

// ─────────────────────────────────────────────────────────────────────────────
// reconcileBootstrapConfig edge-case tests — use k8sClient but call the method
// directly to cover paths the background manager exercises only asynchronously.
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("LitestreamReplica Controller reconcileBootstrapConfig edge cases", func() {
	const (
		hashDBName = "hash-edge-db"
		hashNS     = "default"
		someSQL    = "CREATE TABLE t (id INTEGER PRIMARY KEY);"
	)
	ctx := context.Background()
	dbKey := types.NamespacedName{Name: hashDBName, Namespace: hashNS}

	newReconciler := func() *LitestreamReplicaReconciler {
		return &LitestreamReplicaReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(10),
		}
	}

	AfterEach(func() {
		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, dbKey, db); err == nil {
			_ = k8sClient.Delete(ctx, db)
		}
		for _, name := range []string{hashDBName + "-litestream", hashDBName + "-bootstrap-sql"} {
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: hashNS}, cm); err == nil {
				_ = k8sClient.Delete(ctx, cm)
			}
		}
	})

	It("is a no-op when Bootstrap.SQL is empty", func() {
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: hashDBName, Namespace: hashNS},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "app.db",
				DatabasePath:     "/data",
				TargetDeployment: "nonexistent",
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		r := newReconciler()
		Expect(r.reconcileBootstrapConfig(ctx, db)).To(Succeed())
	})

	It("creates bootstrap-sql ConfigMap when Bootstrap.SQL is set", func() {
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: hashDBName, Namespace: hashNS},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "app.db",
				DatabasePath:     "/data",
				TargetDeployment: "nonexistent",
				Bootstrap:        databasev1.BootstrapSpec{SQL: someSQL},
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		r := newReconciler()
		Eventually(func() error {
			return r.reconcileBootstrapConfig(ctx, db)
		}).Should(Succeed())

		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: hashDBName + "-bootstrap-sql", Namespace: hashNS}, cm)).To(Succeed())
		Expect(cm.Data["bootstrap.sql"]).To(Equal(someSQL))
	})
})

// ─────────────────────────────────────────────────────────────────────────────
// updateStatus direct unit tests — direct reconciler calls, no background manager.
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("LitestreamReplica Controller updateStatus direct tests", func() {
	const (
		usNamespace = "default"
	)
	ctx := context.Background()

	newR := func() *LitestreamReplicaReconciler {
		return &LitestreamReplicaReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(10),
		}
	}

	It("sets PhaseError and WorkloadNotFound condition when target Deployment does not exist", func() {
		const dbName = "us-no-dep-db"
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: dbName, Namespace: usNamespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "app.db",
				DatabasePath:     "/data",
				TargetDeployment: "nonexistent-dep-xyz",
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, db) }()

		r := newR()
		Expect(r.updateStatus(ctx, db)).To(Succeed())

		updated := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: dbName, Namespace: usNamespace}, updated)).To(Succeed())
		Expect(updated.Status.Phase).To(Equal(databasev1.PhaseError))
		Expect(updated.Status.Ready).To(BeFalse())
		cond := meta.FindStatusCondition(updated.Status.Conditions, databasev1.ConditionSidecarReady)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("WorkloadNotFound"))
	})

	It("sets PhaseError and ReplicaCountExceeded condition when target Deployment has replicas > 1", func() {
		const dbName = "us-multi-rep-db"
		const depName = "us-multi-rep-dep"
		replicas := int32(2)
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: depName, Namespace: usNamespace},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": depName}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": depName}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, dep) }()

		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: dbName, Namespace: usNamespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "app.db",
				DatabasePath:     "/data",
				TargetDeployment: depName,
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, db) }()
		defer func() {
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: dbName + "-litestream", Namespace: usNamespace}, cm); err == nil {
				_ = k8sClient.Delete(ctx, cm)
			}
		}()

		r := newR()
		Expect(r.updateStatus(ctx, db)).To(Succeed())

		updated := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: dbName, Namespace: usNamespace}, updated)).To(Succeed())
		Expect(updated.Status.Phase).To(Equal(databasev1.PhaseError))
		Expect(updated.Status.Ready).To(BeFalse())
		cond := meta.FindStatusCondition(updated.Status.Conditions, databasev1.ConditionReplicaCountExceeded)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
	})
})

// ─────────────────────────────────────────────────────────────────────────────
// workloadTarget pure-unit tests — no envtest required.
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("workloadTarget pure-unit tests", func() {
	It("desiredReplicas returns 1 for a Deployment with nil spec.Replicas", func() {
		wt := &workloadTarget{deployment: &appsv1.Deployment{}}
		Expect(wt.desiredReplicas()).To(Equal(int32(1)))
	})

	It("desiredReplicas returns 1 for a StatefulSet with nil spec.Replicas", func() {
		wt := &workloadTarget{statefulSet: &appsv1.StatefulSet{}}
		Expect(wt.desiredReplicas()).To(Equal(int32(1)))
	})

	It("desiredReplicas returns the configured value for a Deployment", func() {
		r := int32(3)
		wt := &workloadTarget{deployment: &appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{Replicas: &r},
		}}
		Expect(wt.desiredReplicas()).To(Equal(int32(3)))
	})

	It("desiredReplicas returns the configured value for a StatefulSet", func() {
		r := int32(2)
		wt := &workloadTarget{statefulSet: &appsv1.StatefulSet{
			Spec: appsv1.StatefulSetSpec{Replicas: &r},
		}}
		Expect(wt.desiredReplicas()).To(Equal(int32(2)))
	})

	It("name returns StatefulSet name when set", func() {
		wt := &workloadTarget{statefulSet: &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "my-sts"},
		}}
		Expect(wt.name()).To(Equal("my-sts"))
	})

	It("typeName returns StatefulSet for StatefulSet workloads", func() {
		wt := &workloadTarget{statefulSet: &appsv1.StatefulSet{}}
		Expect(wt.typeName()).To(Equal("StatefulSet"))
	})

	It("typeName returns Deployment for Deployment workloads", func() {
		wt := &workloadTarget{deployment: &appsv1.Deployment{}}
		Expect(wt.typeName()).To(Equal("Deployment"))
	})
})
