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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
)

var _ = Describe("LitestreamRestore Controller", func() {
	const (
		restoreName   = "test-restore"
		sourceDBName  = "source-db"
		sourceDepName = "myapp"
		targetPVC     = "restore-pvc"
		targetPath    = "/data/myapp.db"
		secretRef     = "s3-creds"
		bucketName    = "my-backups"
		namespaceName = "default"
	)

	ctx := context.Background()
	restoreKey := types.NamespacedName{Name: restoreName, Namespace: namespaceName}
	sourceDBKey := types.NamespacedName{Name: sourceDBName, Namespace: namespaceName}
	sourceDepKey := types.NamespacedName{Name: sourceDepName, Namespace: namespaceName}
	sourceConfigMapKey := types.NamespacedName{Name: sourceDBName + "-litestream", Namespace: namespaceName}

	newRestoreReconciler := func() *LitestreamRestoreReconciler {
		return &LitestreamRestoreReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(10),
		}
	}

	newSourceDB := func() *databasev1.LitestreamReplica {
		return &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: sourceDBName, Namespace: namespaceName},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "myapp.db",
				DatabasePath:     "/data",
				TargetDeployment: sourceDepName,
				Backup: databasev1.BackupSpec{
					Enabled: true,
					Destination: databasev1.BackupDestination{
						S3: &databasev1.S3Destination{
							Bucket:    bucketName,
							Path:      "myapp/",
							SecretRef: secretRef,
						},
					},
				},
			},
		}
	}

	newRestore := func() *databasev1.LitestreamRestore {
		return &databasev1.LitestreamRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespaceName},
			Spec: databasev1.LitestreamRestoreSpec{
				SourceRef:  sourceDBName,
				TargetPVC:  targetPVC,
				TargetPath: targetPath,
			},
		}
	}

	// positionAtScalingDown pre-positions the given restore at ScalingDown phase with
	// the Deployment status.replicas=0, so the next reconcile creates the restore Job.
	positionAtScalingDown := func(rKey types.NamespacedName) {
		replicas := int32(1)
		r := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, rKey, r)).To(Succeed())
		patch := client.MergeFrom(r.DeepCopy())
		r.Status.Phase = databasev1.RestorePhaseScalingDown
		r.Status.OriginalReplicas = &replicas
		Expect(k8sClient.Status().Patch(ctx, r, patch)).To(Succeed())

		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, sourceDepKey, dep)).To(Succeed())
		depStatusPatch := client.MergeFrom(dep.DeepCopy())
		dep.Status.Replicas = 0
		Expect(k8sClient.Status().Patch(ctx, dep, depStatusPatch)).To(Succeed())
	}

	BeforeEach(func() {
		// Create the target Deployment if not present.
		dep := &appsv1.Deployment{}
		if err := k8sClient.Get(ctx, sourceDepKey, dep); err != nil && errors.IsNotFound(err) {
			replicas := int32(1)
			dep = &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: sourceDepName, Namespace: namespaceName},
				Spec: appsv1.DeploymentSpec{
					Replicas: &replicas,
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": sourceDepName}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": sourceDepName}},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())
		}

		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, sourceDBKey, db); err != nil && errors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, newSourceDB())).To(Succeed())
		}

		// Wait for LitestreamReplicaReconciler to create the Litestream ConfigMap.
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, sourceConfigMapKey, cm)).To(Succeed())
		}).Should(Succeed())

		restore := &databasev1.LitestreamRestore{}
		if err := k8sClient.Get(ctx, restoreKey, restore); err != nil && errors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, newRestore())).To(Succeed())
		}
	})

	AfterEach(func() {
		restore := &databasev1.LitestreamRestore{}
		if err := k8sClient.Get(ctx, restoreKey, restore); err == nil {
			Expect(k8sClient.Delete(ctx, restore)).To(Succeed())
		}

		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, sourceDBKey, db); err == nil {
			Expect(k8sClient.Delete(ctx, db)).To(Succeed())
		}

		// Explicitly delete ConfigMaps — envtest does not GC owned objects.
		for _, cmName := range []string{sourceDBName + "-litestream", sourceDBName + "-init-sql"} {
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: cmName, Namespace: namespaceName}, cm); err == nil {
				_ = k8sClient.Delete(ctx, cm)
			}
		}

		dep := &appsv1.Deployment{}
		if err := k8sClient.Get(ctx, sourceDepKey, dep); err == nil {
			Expect(k8sClient.Delete(ctx, dep)).To(Succeed())
		}

		// Clean up the restore Job if it exists.
		job := &batchv1.Job{}
		if err := k8sClient.Get(ctx, types.NamespacedName{
			Name: restoreName + "-restore", Namespace: namespaceName,
		}, job); err == nil {
			Expect(k8sClient.Delete(ctx, job)).To(Succeed())
		}
	})

	It("creates a restore Job with correct args and env vars", func() {
		positionAtScalingDown(restoreKey)
		_, err := newRestoreReconciler().Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name:      restoreName + "-restore",
			Namespace: namespaceName,
		}, job)).To(Succeed())

		container := job.Spec.Template.Spec.Containers[0]
		Expect(container.Name).To(Equal("litestream-restore"))
		Expect(container.Image).To(ContainSubstring("litestream"))

		// Should use -config flag (endpoint comes from config file, not env var).
		Expect(container.Args).To(ContainElement("-config"))
		Expect(container.Args).To(ContainElement("/etc/litestream/litestream.yml"))
		// Should include -o <targetPath> and the db path from the source LitestreamReplica spec.
		Expect(container.Args).To(ContainElement("-o"))
		Expect(container.Args).To(ContainElement(targetPath))

		// Should inject S3 credential env vars from the secret.
		envNames := make([]string, len(container.Env))
		for i, e := range container.Env {
			envNames[i] = e.Name
		}
		Expect(envNames).To(ContainElements("LITESTREAM_ACCESS_KEY_ID", "LITESTREAM_SECRET_ACCESS_KEY"))

		// Credential env vars must reference the correct secret.
		for _, e := range container.Env {
			if e.Name == "LITESTREAM_ACCESS_KEY_ID" {
				Expect(e.ValueFrom.SecretKeyRef.Name).To(Equal(secretRef))
			}
		}
	})

	It("mounts the target PVC at the parent directory of TargetPath", func() {
		positionAtScalingDown(restoreKey)
		_, err := newRestoreReconciler().Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name:      restoreName + "-restore",
			Namespace: namespaceName,
		}, job)).To(Succeed())

		volumes := job.Spec.Template.Spec.Volumes
		// Two volumes: target PVC + litestream-config ConfigMap.
		Expect(volumes).To(HaveLen(2))
		var pvcVol, cmVol corev1.Volume
		for _, v := range volumes {
			if v.PersistentVolumeClaim != nil {
				pvcVol = v
			}
			if v.ConfigMap != nil {
				cmVol = v
			}
		}
		Expect(pvcVol.PersistentVolumeClaim.ClaimName).To(Equal(targetPVC))
		// The restore job must mount its OWN ConfigMap (restore.Name + "-litestream"),
		// NOT the source LitestreamReplica's ConfigMap — which is paused (dbs: []) at this point.
		Expect(cmVol.ConfigMap.Name).To(Equal(restoreName + "-litestream"))

		mounts := job.Spec.Template.Spec.Containers[0].VolumeMounts
		// Two mounts: target PVC at /data and litestream-config at /etc/litestream.
		Expect(mounts).To(HaveLen(2))
		mountPaths := make([]string, len(mounts))
		for i, m := range mounts {
			mountPaths[i] = m.MountPath
		}
		Expect(mountPaths).To(ContainElement("/data"))           // dirOf("/data/myapp.db")
		Expect(mountPaths).To(ContainElement("/etc/litestream")) // config file
	})

	It("includes -timestamp arg when PITR timestamp is set", func() {
		// Use a unique restore name to avoid sharing a Job with other specs.
		const pitrRestoreName = "pitr-restore"
		pitrKey := types.NamespacedName{Name: pitrRestoreName, Namespace: namespaceName}

		pitrRestore := &databasev1.LitestreamRestore{
			ObjectMeta: metav1.ObjectMeta{Name: pitrRestoreName, Namespace: namespaceName},
			Spec: databasev1.LitestreamRestoreSpec{
				SourceRef:  sourceDBName,
				TargetPVC:  targetPVC,
				TargetPath: targetPath,
				Timestamp:  "2026-06-17T10:00:00Z",
			},
		}
		Expect(k8sClient.Create(ctx, pitrRestore)).To(Succeed())
		defer func() {
			_ = k8sClient.Delete(ctx, pitrRestore)
			job := &batchv1.Job{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: pitrRestoreName + "-restore", Namespace: namespaceName,
			}, job); err == nil {
				_ = k8sClient.Delete(ctx, job)
			}
		}()

		positionAtScalingDown(pitrKey)
		_, err := newRestoreReconciler().Reconcile(ctx, reconcile.Request{NamespacedName: pitrKey})
		Expect(err).NotTo(HaveOccurred())

		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name:      pitrRestoreName + "-restore",
			Namespace: namespaceName,
		}, job)).To(Succeed())

		args := job.Spec.Template.Spec.Containers[0].Args
		Expect(args).To(ContainElements("-timestamp", "2026-06-17T10:00:00Z"))
	})

	It("sets status to Running after creating the Job", func() {
		positionAtScalingDown(restoreKey)
		_, err := newRestoreReconciler().Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseRunning))
		Expect(restore.Status.JobName).To(Equal(restoreName + "-restore"))
	})

	It("is idempotent — does not create a second Job on re-reconcile", func() {
		positionAtScalingDown(restoreKey)
		reconciler := newRestoreReconciler()

		// First reconcile: ScalingDown → Running (creates Job).
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Second reconcile in Running phase — Job already exists, should not error.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Still exactly one Job.
		jobList := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobList,
			client.InNamespace(namespaceName),
			client.MatchingLabels{"litestream.io/restore": restoreName},
		)).To(Succeed())
		Expect(jobList.Items).To(HaveLen(1))
	})

	It("fails immediately when the referenced LitestreamReplica has backup disabled", func() {
		// Create a separate restore referencing a DB with backup off.
		const badRestoreName = "bad-restore"
		noBackupDB := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: "no-backup-db", Namespace: namespaceName},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "other.db",
				DatabasePath:     "/data",
				TargetDeployment: "other-app",
				Backup:           databasev1.BackupSpec{Enabled: false},
			},
		}
		Expect(k8sClient.Create(ctx, noBackupDB)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, noBackupDB) }()

		badRestore := &databasev1.LitestreamRestore{
			ObjectMeta: metav1.ObjectMeta{Name: badRestoreName, Namespace: namespaceName},
			Spec: databasev1.LitestreamRestoreSpec{
				SourceRef:  "no-backup-db",
				TargetPVC:  targetPVC,
				TargetPath: targetPath,
			},
		}
		Expect(k8sClient.Create(ctx, badRestore)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, badRestore) }()

		_, err := newRestoreReconciler().Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: badRestoreName, Namespace: namespaceName},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: badRestoreName, Namespace: namespaceName}, badRestore)).To(Succeed())
		Expect(badRestore.Status.Phase).To(Equal(databasev1.RestorePhaseFailed))
	})
})

// ─────────────────────────────────────────────────────────────────────────────
// State machine tests — drive phase transitions with envtest.
// Each test creates its own isolated resources to avoid cross-test interference.
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("LitestreamRestore State Machine", func() {
	const (
		namespaceName = "default"
		targetPVC     = "sm-restore-pvc"
		targetPath    = "/data/myapp.db"
		secretRef     = "sm-s3-creds"
		bucketName    = "sm-backups"
		deployName    = "sm-app"
	)

	ctx := context.Background()

	// newStateMachineResources creates isolated resources for a state machine test:
	// a Deployment, a LitestreamReplica, the Litestream ConfigMap (initially empty dbs list),
	// and a LitestreamRestore CR. Returns keys for all three for cleanup.
	newStateMachineResources := func(suffix string, replicas int32) (
		dbKey types.NamespacedName,
		restoreKey types.NamespacedName,
		deployKey types.NamespacedName,
	) {
		dbName := "sm-db-" + suffix
		restoreName := "sm-restore-" + suffix
		depName := deployName + "-" + suffix

		dbKey = types.NamespacedName{Name: dbName, Namespace: namespaceName}
		restoreKey = types.NamespacedName{Name: restoreName, Namespace: namespaceName}
		deployKey = types.NamespacedName{Name: depName, Namespace: namespaceName}

		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: depName, Namespace: namespaceName},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": depName},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": depName}},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: dbName, Namespace: namespaceName},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "myapp.db",
				DatabasePath:     "/data",
				TargetDeployment: depName,
				Backup: databasev1.BackupSpec{
					Enabled: true,
					Destination: databasev1.BackupDestination{
						S3: &databasev1.S3Destination{
							Bucket:    bucketName,
							SecretRef: secretRef,
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		// Wait for LitestreamReplicaReconciler to create the Litestream ConfigMap.
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: dbName + "-litestream", Namespace: namespaceName,
			}, cm)).To(Succeed())
		}).Should(Succeed())

		restore := &databasev1.LitestreamRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespaceName},
			Spec: databasev1.LitestreamRestoreSpec{
				SourceRef:  dbName,
				TargetPVC:  targetPVC,
				TargetPath: targetPath,
			},
		}
		Expect(k8sClient.Create(ctx, restore)).To(Succeed())

		return dbKey, restoreKey, deployKey
	}

	cleanupResources := func(dbKey, restoreKey, deployKey types.NamespacedName) { //nolint:dupl
		restore := &databasev1.LitestreamRestore{}
		if err := k8sClient.Get(ctx, restoreKey, restore); err == nil {
			_ = k8sClient.Delete(ctx, restore)
		}
		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, dbKey, db); err == nil {
			_ = k8sClient.Delete(ctx, db)
		}
		// Explicitly delete ConfigMaps — envtest does not GC owned objects.
		for _, suffix := range []string{"-litestream", "-init-sql"} {
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: dbKey.Name + suffix, Namespace: namespaceName,
			}, cm); err == nil {
				_ = k8sClient.Delete(ctx, cm)
			}
		}
		dep := &appsv1.Deployment{}
		if err := k8sClient.Get(ctx, deployKey, dep); err == nil {
			_ = k8sClient.Delete(ctx, dep)
		}
		job := &batchv1.Job{}
		if err := k8sClient.Get(ctx, types.NamespacedName{
			Name: restoreKey.Name + "-restore", Namespace: namespaceName,
		}, job); err == nil {
			_ = k8sClient.Delete(ctx, job)
		}
	}

	newReconciler := func() *LitestreamRestoreReconciler {
		return &LitestreamRestoreReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(20),
		}
	}

	It("sets pause annotation on LitestreamReplica and transitions to Pausing", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("pause-pending", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// LitestreamReplica should now have the pause annotation.
		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, dbKey, db)).To(Succeed())
		Expect(db.Annotations[databasev1.AnnotationPause]).To(Equal("true"))

		// Restore should be in Pausing phase.
		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhasePausing))
	})

	It("records originalReplicas in status during Pending phase", func() {
		replicas := int32(3)
		dbKey, restoreKey, deployKey := newStateMachineResources("orig-replicas", replicas)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		_, err := newReconciler().Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.OriginalReplicas).NotTo(BeNil())
		Expect(*restore.Status.OriginalReplicas).To(Equal(replicas))
	})

	It("scales Deployment to 0 when ConfigMap reflects pause (Pausing → ScalingDown)", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("scale-down", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()

		// Pending → Pausing.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Simulate controller reconciling the LitestreamReplica and updating the ConfigMap.
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: dbKey.Name + "-litestream", Namespace: namespaceName,
		}, cm)).To(Succeed())
		cmPatch := client.MergeFrom(cm.DeepCopy())
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["litestream.yml"] = "dbs: []\n"
		Expect(k8sClient.Patch(ctx, cm, cmPatch)).To(Succeed())

		// Pausing → ScalingDown.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseScalingDown))

		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, deployKey, dep)).To(Succeed())
		Expect(dep.Spec.Replicas).NotTo(BeNil())
		Expect(*dep.Spec.Replicas).To(BeZero())
	})

	It("waits in ScalingDown while Deployment still has running replicas", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("wait-drain", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()

		// Drive to Pausing.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Update ConfigMap to reflect pause.
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: dbKey.Name + "-litestream", Namespace: namespaceName,
		}, cm)).To(Succeed())
		cmPatch := client.MergeFrom(cm.DeepCopy())
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["litestream.yml"] = "dbs: []\n"
		Expect(k8sClient.Patch(ctx, cm, cmPatch)).To(Succeed())

		// Drive to ScalingDown.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseScalingDown))

		// Simulate Deployment still draining (status.replicas = 1).
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, deployKey, dep)).To(Succeed())
		depStatusPatch := client.MergeFrom(dep.DeepCopy())
		dep.Status.Replicas = 1
		Expect(k8sClient.Status().Patch(ctx, dep, depStatusPatch)).To(Succeed())

		// Reconcile should still be in ScalingDown.
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(restoreRequeueInterval))

		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseScalingDown))
		// Job should NOT exist yet.
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: restoreKey.Name + "-restore", Namespace: namespaceName,
		}, job)).To(MatchError(ContainSubstring("not found")))
	})

	It("creates Job and transitions to Running once Deployment reaches 0 replicas", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("create-job", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()

		// Drive to Pausing.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Update ConfigMap.
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: dbKey.Name + "-litestream", Namespace: namespaceName,
		}, cm)).To(Succeed())
		cmPatch := client.MergeFrom(cm.DeepCopy())
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["litestream.yml"] = "dbs: []\n"
		Expect(k8sClient.Patch(ctx, cm, cmPatch)).To(Succeed())

		// Drive to ScalingDown.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Simulate Deployment fully scaled down (status.replicas = 0).
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, deployKey, dep)).To(Succeed())
		depStatusPatch := client.MergeFrom(dep.DeepCopy())
		dep.Status.Replicas = 0
		Expect(k8sClient.Status().Patch(ctx, dep, depStatusPatch)).To(Succeed())

		// ScalingDown → Running.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseRunning))

		// Job should exist.
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: restoreKey.Name + "-restore", Namespace: namespaceName,
		}, job)).To(Succeed())
	})

	It("transitions to Validating when restore Job succeeds", func() { //nolint:dupl
		dbKey, restoreKey, deployKey := newStateMachineResources("job-complete", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()

		// Drive to Running.
		driveToRunning(ctx, reconciler, dbKey, restoreKey, deployKey)

		// Simulate restore Job success.
		jobKey := types.NamespacedName{Name: restoreKey.Name + "-restore", Namespace: namespaceName}
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, jobKey, job)).To(Succeed())
		now := metav1.Now()
		jobStatusPatch := client.MergeFrom(job.DeepCopy())
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
		}
		Expect(k8sClient.Status().Patch(ctx, job, jobStatusPatch)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		// Running → Validating (integrity check phase inserted between Running and ScalingUp).
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseValidating))
	})

	It("transitions to ScalingUp when validation Job succeeds", func() { //nolint:dupl
		dbKey, restoreKey, deployKey := newStateMachineResources("validate-pass", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()
		driveToValidating(ctx, reconciler, dbKey, restoreKey, deployKey)

		// Simulate validation Job success.
		validateJobKey := types.NamespacedName{Name: restoreKey.Name + "-validate", Namespace: namespaceName}
		validateJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, validateJobKey, validateJob)).To(Succeed())
		now := metav1.Now()
		vpatch := client.MergeFrom(validateJob.DeepCopy())
		validateJob.Status.StartTime = &now
		validateJob.Status.CompletionTime = &now
		validateJob.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
		}
		Expect(k8sClient.Status().Patch(ctx, validateJob, vpatch)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseScalingUp))
	})

	It("transitions to Failed when validation Job fails (integrity check failure)", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("validate-fail", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()
		driveToValidating(ctx, reconciler, dbKey, restoreKey, deployKey)

		// Simulate validation Job failure (integrity check returned non-zero).
		// Kubernetes 1.31+ requires FailureTarget=True before Failed=True and startTime.
		validateJobKey := types.NamespacedName{Name: restoreKey.Name + "-validate", Namespace: namespaceName}
		validateJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, validateJobKey, validateJob)).To(Succeed())
		now := metav1.Now()
		vpatch := client.MergeFrom(validateJob.DeepCopy())
		validateJob.Status.StartTime = &now
		validateJob.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue,
				Message: "db integrity check failed", LastProbeTime: now, LastTransitionTime: now},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
				Message: "db integrity check failed", LastProbeTime: now, LastTransitionTime: now},
		}
		Expect(k8sClient.Status().Patch(ctx, validateJob, vpatch)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseFailed))
		Expect(restore.Status.Message).To(ContainSubstring("integrity check failed"))
	})

	It("scales Deployment back to originalReplicas and transitions to Complete", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("scale-up", 2)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()
		// Drive through Running → Validating → ScalingUp.
		driveToValidating(ctx, reconciler, dbKey, restoreKey, deployKey)
		simulateValidationSuccess(ctx, restoreKey)

		// Validating → ScalingUp.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// ScalingUp → Complete: retry until the background LitestreamReplica controller
		// updates the ConfigMap to the full config (after cache propagation of the pause
		// annotation removal). Each Reconcile is idempotent until the CM is updated.
		Eventually(func(g Gomega) {
			_, reconcileErr := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
			g.Expect(reconcileErr).NotTo(HaveOccurred())
			r := &databasev1.LitestreamRestore{}
			g.Expect(k8sClient.Get(ctx, restoreKey, r)).To(Succeed())
			g.Expect(r.Status.Phase).To(Equal(databasev1.RestorePhaseComplete))
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())

		// Deployment should be scaled back to originalReplicas.
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, deployKey, dep)).To(Succeed())
		Expect(dep.Spec.Replicas).NotTo(BeNil())
		Expect(*dep.Spec.Replicas).To(Equal(int32(2)))
	})

	It("removes pause annotation after successful restore", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("remove-pause", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()
		driveToValidating(ctx, reconciler, dbKey, restoreKey, deployKey)
		simulateValidationSuccess(ctx, restoreKey)

		// Two reconciles needed: first transitions Validating → ScalingUp (sets phase),
		// second calls reconcileScalingUp which removes the pause annotation before the
		// ConfigMap check (steps 1 and 2 run regardless of CM state).
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Pause annotation should be removed from LitestreamReplica.
		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, dbKey, db)).To(Succeed())
		Expect(db.Annotations[databasev1.AnnotationPause]).NotTo(Equal("true"))
	})

	It("sets skip-archive-check annotation on LitestreamReplica after successful restore", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("skip-archive-check-set", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()
		driveToValidating(ctx, reconciler, dbKey, restoreKey, deployKey)
		simulateValidationSuccess(ctx, restoreKey)

		// Two reconciles needed: first transitions Validating → ScalingUp (sets phase),
		// second calls reconcileScalingUp which sets skip-archive-check before the
		// ConfigMap check (step 1 runs regardless of CM state).
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// skip-archive-check must be set on the LitestreamReplica so that the
		// restored DB (which has no litestream state directory yet) doesn't trigger
		// a false-positive archive-check block on the next pod startup (issue #109).
		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, dbKey, db)).To(Succeed())
		Expect(db.Annotations[databasev1.AnnotationSkipArchiveCheck]).To(Equal("true"),
			"restore controller must set skip-archive-check to prevent false-positive archive-check on next pod start")
	})

	It("cleans up on Job failure: scales back up and removes pause annotation", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("fail-cleanup", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()
		driveToRunning(ctx, reconciler, dbKey, restoreKey, deployKey)

		// Simulate Job failure. Kubernetes 1.31+ requires FailureTarget before Failed,
		// plus startTime.
		jobKey := types.NamespacedName{Name: restoreKey.Name + "-restore", Namespace: namespaceName}
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, jobKey, job)).To(Succeed())
		now := metav1.Now()
		jobStatusPatch := client.MergeFrom(job.DeepCopy())
		job.Status.StartTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Message: "simulated failure", LastProbeTime: now, LastTransitionTime: now},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "simulated failure", LastProbeTime: now, LastTransitionTime: now},
		}
		Expect(k8sClient.Status().Patch(ctx, job, jobStatusPatch)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseFailed))

		// Pause annotation should be removed.
		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, dbKey, db)).To(Succeed())
		Expect(db.Annotations[databasev1.AnnotationPause]).NotTo(Equal("true"))

		// Deployment should be scaled back up.
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, deployKey, dep)).To(Succeed())
		Expect(dep.Spec.Replicas).NotTo(BeNil())
		Expect(*dep.Spec.Replicas).To(Equal(int32(1)))
	})

	It("is a no-op for Complete phase", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("noop-complete", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		// Force terminal phase directly.
		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		patch := client.MergeFrom(restore.DeepCopy())
		restore.Status.Phase = databasev1.RestorePhaseComplete
		Expect(k8sClient.Status().Patch(ctx, restore, patch)).To(Succeed())

		_, err := newReconciler().Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// LitestreamReplica should NOT have pause annotation.
		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, dbKey, db)).To(Succeed())
		Expect(db.Annotations[databasev1.AnnotationPause]).NotTo(Equal("true"))
	})

	It("is a no-op for Failed phase (terminal check covers both sides of ||)", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("noop-failed", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		patch := client.MergeFrom(restore.DeepCopy())
		restore.Status.Phase = databasev1.RestorePhaseFailed
		Expect(k8sClient.Status().Patch(ctx, restore, patch)).To(Succeed())

		_, err := newReconciler().Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Phase stays Failed — no further reconciliation.
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseFailed))
		_ = dbKey
		_ = deployKey
	})

	It("is a no-op for an unknown phase (default switch case)", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("noop-unknown-phase", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		patch := client.MergeFrom(restore.DeepCopy())
		restore.Status.Phase = "SomeUnrecognizedPhase"
		Expect(k8sClient.Status().Patch(ctx, restore, patch)).To(Succeed())

		// Reconcile hits the default case in the phase switch → no-op.
		_, err := newReconciler().Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())
		_ = dbKey
		_ = deployKey
	})

	It("reconcileScalingUp with OriginalReplicas=0 skips scale-up and completes", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("scale-up-zero-replicas", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()
		driveToValidating(ctx, reconciler, dbKey, restoreKey, deployKey)
		simulateValidationSuccess(ctx, restoreKey)

		// Validating → ScalingUp.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseScalingUp))

		// Set OriginalReplicas to 0 — app was already stopped before restore.
		zeroReplicas := int32(0)
		patch := client.MergeFrom(restore.DeepCopy())
		restore.Status.OriginalReplicas = &zeroReplicas
		Expect(k8sClient.Status().Patch(ctx, restore, patch)).To(Succeed())

		// ScalingUp with target=0 → skip scale-up, just resume and complete. Retry until
		// the LitestreamReplica controller updates the ConfigMap (cache propagation).
		Eventually(func(g Gomega) {
			_, reconcileErr := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
			g.Expect(reconcileErr).NotTo(HaveOccurred())
			r := &databasev1.LitestreamRestore{}
			g.Expect(k8sClient.Get(ctx, restoreKey, r)).To(Succeed())
			g.Expect(r.Status.Phase).To(Equal(databasev1.RestorePhaseComplete))
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		_ = dbKey
		_ = deployKey
	})

	It("handles nil spec.replicas as originalReplicas=1", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("nil-replicas", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		// Clear spec.replicas (nil → Kubernetes defaults to 1).
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, deployKey, dep)).To(Succeed())
		depPatch := client.MergeFrom(dep.DeepCopy())
		dep.Spec.Replicas = nil
		Expect(k8sClient.Patch(ctx, dep, depPatch)).To(Succeed())

		_, err := newReconciler().Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.OriginalReplicas).NotTo(BeNil())
		Expect(*restore.Status.OriginalReplicas).To(Equal(int32(1)))
	})

	It("proceeds gracefully when LitestreamReplica's targetDeployment does not exist", func() {
		// A missing Deployment is a valid case: the app may have been torn down
		// before the restore was requested. The controller skips scale-down and
		// proceeds to create the restore Job with originalReplicas=0.
		dbName := "sm-db-missing-dep"
		restoreName := "sm-restore-missing-dep"

		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: dbName, Namespace: namespaceName},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "myapp.db",
				DatabasePath:     "/data",
				TargetDeployment: "nonexistent-deployment",
				Backup: databasev1.BackupSpec{
					Enabled: true,
					Destination: databasev1.BackupDestination{
						S3: &databasev1.S3Destination{Bucket: "b", SecretRef: "s"},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())
		defer func() {
			_ = k8sClient.Delete(ctx, db)
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: dbName + "-litestream", Namespace: namespaceName}, cm); err == nil {
				_ = k8sClient.Delete(ctx, cm)
			}
		}()

		// Wait for LitestreamReplicaReconciler to create the Litestream ConfigMap.
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: dbName + "-litestream", Namespace: namespaceName,
			}, cm)).To(Succeed())
		}).Should(Succeed())

		restore := &databasev1.LitestreamRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespaceName},
			Spec: databasev1.LitestreamRestoreSpec{
				SourceRef:  dbName,
				TargetPVC:  targetPVC,
				TargetPath: targetPath,
			},
		}
		Expect(k8sClient.Create(ctx, restore)).To(Succeed())
		defer func() {
			_ = k8sClient.Delete(ctx, restore)
			job := &batchv1.Job{}
			_ = k8sClient.Delete(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: restoreName + "-restore", Namespace: namespaceName,
			}})
			_ = job.Name // suppress unused variable
		}()

		reconciler := newReconciler()

		// Pending → Pausing (deployment not found → originalReplicas=0)
		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: restoreName, Namespace: namespaceName},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: restoreName, Namespace: namespaceName}, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhasePausing))
		Expect(restore.Status.OriginalReplicas).NotTo(BeNil())
		Expect(*restore.Status.OriginalReplicas).To(Equal(int32(0)))

		// Pausing → ScalingDown (deployment not found → skip scale, go straight to ScalingDown)
		_, err = reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: restoreName, Namespace: namespaceName},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: restoreName, Namespace: namespaceName}, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseScalingDown))

		// ScalingDown → Running (deployment not found → treat as replicas=0, create Job)
		_, err = reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: restoreName, Namespace: namespaceName},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: restoreName, Namespace: namespaceName}, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseRunning))

		// Job should exist.
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: restoreName + "-restore", Namespace: namespaceName,
		}, job)).To(Succeed())
		_ = k8sClient.Delete(ctx, job)
	})

	// ── R1 ──────────────────────────────────────────────────────────────────
	It("reconcilePausing re-sets pause annotation if it was removed", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("pausing-re-pause", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()

		// Drive to Pausing.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhasePausing))

		// Remove the pause annotation from the LitestreamReplica (simulates a user mistake).
		// Use Eventually to handle any 409 conflict from the background reconciler.
		Eventually(func() error {
			db := &databasev1.LitestreamReplica{}
			if err := k8sClient.Get(ctx, dbKey, db); err != nil {
				return err
			}
			delete(db.Annotations, databasev1.AnnotationPause)
			return k8sClient.Update(ctx, db)
		}).Should(Succeed())

		// Verify annotation is gone.
		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, dbKey, db)).To(Succeed())
		Expect(db.Annotations[databasev1.AnnotationPause]).NotTo(Equal("true"))

		// Reconcile while in Pausing phase with annotation absent — controller re-sets it.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Annotation should be re-set.
		Expect(k8sClient.Get(ctx, dbKey, db)).To(Succeed())
		Expect(db.Annotations[databasev1.AnnotationPause]).To(Equal("true"))
		_ = deployKey
	})

	// ── R2 ──────────────────────────────────────────────────────────────────
	// Tests the full Pausing→ScalingDown transition: once both the pause annotation
	// is set AND the ConfigMap reflects dbs:[], the controller scales down and
	// transitions to ScalingDown. Exercises the ConfigMap-check path in reconcilePausing.
	It("reconcilePausing advances to ScalingDown once ConfigMap reflects the pause", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("pausing-cm-advances", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()

		// Pending → Pausing.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhasePausing))

		// The background LitestreamReplica controller will update the ConfigMap to "dbs: []\n".
		// Wait for it, then reconcile the restore — it should advance to ScalingDown.
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: dbKey.Name + "-litestream", Namespace: dbKey.Namespace,
			}, cm)).To(Succeed())
			g.Expect(cm.Data["litestream.yml"]).To(Equal("dbs: []\n"))
		}).Should(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseScalingDown))
		_ = deployKey
	})

	// ── R3 ──────────────────────────────────────────────────────────────────
	It("reconcileScalingDown requeues while deployment still has running pods", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("scaling-down-wait", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()

		// Pre-position at ScalingDown with deployment.Status.Replicas = 1 (still running).
		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		patch := client.MergeFrom(restore.DeepCopy())
		replicas := int32(1)
		restore.Status.Phase = databasev1.RestorePhaseScalingDown
		restore.Status.OriginalReplicas = &replicas
		Expect(k8sClient.Status().Patch(ctx, restore, patch)).To(Succeed())

		// Keep deployment.Status.Replicas at 1 (not yet scaled down).
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, deployKey, dep)).To(Succeed())
		depStatusPatch := client.MergeFrom(dep.DeepCopy())
		dep.Status.Replicas = 1
		Expect(k8sClient.Status().Patch(ctx, dep, depStatusPatch)).To(Succeed())

		// Reconcile — deployment still has pods, should requeue (stay in ScalingDown).
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeNumerically(">", 0))

		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseScalingDown))
		_ = dbKey
	})

	// ── R4 ──────────────────────────────────────────────────────────────────
	It("resumeReplication is a no-op when pause annotation is not set", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("resume-noop", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		r := newReconciler()

		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, dbKey, db)).To(Succeed())
		// No pause annotation — resumeReplication should be a no-op.
		Expect(r.resumeReplication(ctx, db)).To(Succeed())
		_ = restoreKey
		_ = deployKey
	})

	// ── R5 ──────────────────────────────────────────────────────────────────
	It("pauseReplication is a no-op when pause annotation is already set", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("pause-noop", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		r := newReconciler()

		// Use Eventually to handle any 409 conflict from the background reconciler.
		Eventually(func() error {
			db := &databasev1.LitestreamReplica{}
			if err := k8sClient.Get(ctx, dbKey, db); err != nil {
				return err
			}
			if db.Annotations == nil {
				db.Annotations = map[string]string{}
			}
			db.Annotations[databasev1.AnnotationPause] = "true"
			return k8sClient.Update(ctx, db)
		}).Should(Succeed())

		// Should return nil immediately without another patch.
		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, dbKey, db)).To(Succeed())
		Expect(r.pauseReplication(ctx, db)).To(Succeed())
		_ = restoreKey
		_ = deployKey
	})

	// ── R6 ──────────────────────────────────────────────────────────────────
	It("fails immediately when source LitestreamReplica has backup disabled", func() { //nolint:dupl
		const noBackupDB = "no-backup-db-r6"
		const noBackupRestore = "no-backup-restore-r6"

		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: noBackupDB, Namespace: namespaceName},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "app.db",
				DatabasePath:     "/data",
				TargetDeployment: "nonexistent",
				Backup:           databasev1.BackupSpec{Enabled: false},
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, db) }()

		// Wait for manager to create the ConfigMap.
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: noBackupDB + "-litestream", Namespace: namespaceName,
			}, cm)).To(Succeed())
		}).Should(Succeed())
		defer func() {
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: noBackupDB + "-litestream", Namespace: namespaceName}, cm); err == nil {
				_ = k8sClient.Delete(ctx, cm)
			}
		}()

		restore := &databasev1.LitestreamRestore{
			ObjectMeta: metav1.ObjectMeta{Name: noBackupRestore, Namespace: namespaceName},
			Spec: databasev1.LitestreamRestoreSpec{
				SourceRef:  noBackupDB,
				TargetPVC:  targetPVC,
				TargetPath: targetPath,
			},
		}
		Expect(k8sClient.Create(ctx, restore)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, restore) }()

		r := newReconciler()
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: noBackupRestore, Namespace: namespaceName},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: noBackupRestore, Namespace: namespaceName}, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseFailed))
		Expect(restore.Status.Message).To(ContainSubstring("backup enabled"))
	})

	// ── R-extra ──────────────────────────────────────────────────────────────────
	// reconcilePausing: workload deleted between Pending and Pausing (NotFound path).
	It("reconcilePausing proceeds to ScalingDown when target workload is deleted mid-flight", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("pausing-dep-deleted", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()

		// Pending → Pausing.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Wait for ConfigMap to be paused by the background LitestreamReplica controller.
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: dbKey.Name + "-litestream", Namespace: dbKey.Namespace,
			}, cm)).To(Succeed())
			g.Expect(cm.Data["litestream.yml"]).To(Equal("dbs: []\n"))
		}).Should(Succeed())

		// Delete the Deployment before reconciling Pausing — simulates a race where the
		// workload was already torn down.
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, deployKey, dep)).To(Succeed())
		Expect(k8sClient.Delete(ctx, dep)).To(Succeed())
		// Update cleanupResources won't fail — it skips missing resources.

		// Reconcile Pausing: workload not found → skip scale-down, go to ScalingDown.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseScalingDown))
	})

	// scaleWorkload: already at target replica count (early return, no patch).
	It("scaleWorkload is a no-op when workload is already at the target replica count", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("scale-noop", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		r := newReconciler()

		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, deployKey, dep)).To(Succeed())

		// Deployment already has spec.Replicas = 1.
		wt := &workloadTarget{deployment: dep}
		Expect(r.scaleWorkload(ctx, wt, 1)).To(Succeed())

		// Verify spec.Replicas is still 1 and no error.
		Expect(k8sClient.Get(ctx, deployKey, dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(1)))
		_ = dbKey
		_ = restoreKey
	})

	// Reconcile — backup.enabled=true but S3 destination is nil (second failRestore path).
	It("fails immediately when source LitestreamReplica has backup enabled but no S3 destination", func() { //nolint:dupl
		const noS3DB = "no-s3-db"
		const noS3Restore = "no-s3-restore"

		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: noS3DB, Namespace: namespaceName},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "app.db",
				DatabasePath:     "/data",
				TargetDeployment: "nonexistent",
				Backup: databasev1.BackupSpec{
					Enabled: true,
					// Destination.S3 is intentionally nil
				},
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, db) }()

		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: noS3DB + "-litestream", Namespace: namespaceName,
			}, cm)).To(Succeed())
		}).Should(Succeed())
		defer func() {
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: noS3DB + "-litestream", Namespace: namespaceName}, cm); err == nil {
				_ = k8sClient.Delete(ctx, cm)
			}
		}()

		restore := &databasev1.LitestreamRestore{
			ObjectMeta: metav1.ObjectMeta{Name: noS3Restore, Namespace: namespaceName},
			Spec: databasev1.LitestreamRestoreSpec{
				SourceRef:  noS3DB,
				TargetPVC:  targetPVC,
				TargetPath: targetPath,
			},
		}
		Expect(k8sClient.Create(ctx, restore)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, restore) }()

		r := newReconciler()
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: noS3Restore, Namespace: namespaceName},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: noS3Restore, Namespace: namespaceName}, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseFailed))
		Expect(restore.Status.Message).To(ContainSubstring("S3 destination"))
	})

	// reconcileScalingUp — OriginalReplicas is nil (edge case: uses default of 1).
	It("reconcileScalingUp scales back to 1 and completes when OriginalReplicas is nil", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("scaling-up-nil-replicas", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()
		driveToValidating(ctx, reconciler, dbKey, restoreKey, deployKey)
		simulateValidationSuccess(ctx, restoreKey)

		// Validating → ScalingUp.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Override OriginalReplicas to nil to exercise the default-to-1 path.
		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseScalingUp))

		patch := client.MergeFrom(restore.DeepCopy())
		restore.Status.OriginalReplicas = nil
		Expect(k8sClient.Status().Patch(ctx, restore, patch)).To(Succeed())

		// ScalingUp → Complete with nil OriginalReplicas (defaults to 1). Retry until
		// the LitestreamReplica controller updates the ConfigMap (cache propagation).
		Eventually(func(g Gomega) {
			_, reconcileErr := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
			g.Expect(reconcileErr).NotTo(HaveOccurred())
			r := &databasev1.LitestreamRestore{}
			g.Expect(k8sClient.Get(ctx, restoreKey, r)).To(Succeed())
			g.Expect(r.Status.Phase).To(Equal(databasev1.RestorePhaseComplete))
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		_ = dbKey
		_ = deployKey
	})

	// reconcileValidating idempotency — second call silently ignores AlreadyExists
	// when creating the validation Job.
	It("reconcileValidating is idempotent when validation Job already exists", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("validating-idempotent", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()
		// driveToValidating creates the validation Job on the second Validating reconcile.
		driveToValidating(ctx, reconciler, dbKey, restoreKey, deployKey)

		// Reconcile again in Validating — validation Job already exists (AlreadyExists silently ignored).
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		// Still Validating — Job not complete yet.
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseValidating))
		_ = dbKey
		_ = deployKey
	})

	// buildValidationJob — explicit InitImage from sourceDB spec.
	It("buildValidationJob uses sourceDB.spec.initImage when set", func() {
		const customInitImage = "my-org/sqlite3:v1.2.3"
		dbKey, restoreKey, deployKey := newStateMachineResources("custom-initimage-validate", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		// Update the LitestreamReplica to have a custom initImage.
		Eventually(func() error {
			db := &databasev1.LitestreamReplica{}
			if err := k8sClient.Get(ctx, dbKey, db); err != nil {
				return err
			}
			db.Spec.InitImage = customInitImage
			return k8sClient.Update(ctx, db)
		}).Should(Succeed())

		reconciler := newReconciler()
		driveToValidating(ctx, reconciler, dbKey, restoreKey, deployKey)

		// Check the validation Job uses the custom image.
		validateJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: restoreKey.Name + "-validate", Namespace: namespaceName,
		}, validateJob)).To(Succeed())
		Expect(validateJob.Spec.Template.Spec.Containers[0].Image).To(Equal(customInitImage))
		_ = deployKey
	})

	// buildRestoreJob / buildValidationJob — SecurityContext from spec.runAsUser/runAsGroup.
	Context("SecurityContext from spec.runAsUser / spec.runAsGroup", func() {
		var (
			reconciler *LitestreamRestoreReconciler
			sourceDB   *databasev1.LitestreamReplica
			restore    *databasev1.LitestreamRestore
		)

		BeforeEach(func() {
			reconciler = &LitestreamRestoreReconciler{Client: k8sClient}
			sourceDB = &databasev1.LitestreamReplica{
				Spec: databasev1.LitestreamReplicaSpec{
					DatabaseName: "app.db",
					DatabasePath: "/data",
					Backup: databasev1.BackupSpec{
						Enabled: true,
						Destination: databasev1.BackupDestination{
							S3: &databasev1.S3Destination{
								Bucket:    "bucket",
								SecretRef: "secret",
							},
						},
					},
				},
			}
			restore = &databasev1.LitestreamRestore{
				ObjectMeta: metav1.ObjectMeta{Name: "sec-ctx-test", Namespace: namespaceName},
				Spec: databasev1.LitestreamRestoreSpec{
					SourceRef:  sourceDB.Name,
					TargetPVC:  "test-pvc",
					TargetPath: "/data/app.db",
				},
			}
		})

		It("buildRestoreJob has no SecurityContext when RunAsUser/RunAsGroup omitted", func() {
			job := reconciler.buildRestoreJob(restore, sourceDB, "restore-job")
			Expect(job.Spec.Template.Spec.SecurityContext).To(BeNil())
		})

		It("buildRestoreJob sets PodSecurityContext when RunAsUser is set", func() {
			uid := int64(1000)
			restore.Spec.RunAsUser = &uid
			job := reconciler.buildRestoreJob(restore, sourceDB, "restore-job")
			Expect(job.Spec.Template.Spec.SecurityContext).NotTo(BeNil())
			Expect(job.Spec.Template.Spec.SecurityContext.RunAsUser).NotTo(BeNil())
			Expect(*job.Spec.Template.Spec.SecurityContext.RunAsUser).To(Equal(int64(1000)))
			Expect(job.Spec.Template.Spec.SecurityContext.RunAsGroup).To(BeNil())
		})

		It("buildRestoreJob sets both RunAsUser and RunAsGroup when both specified", func() {
			uid, gid := int64(1000), int64(2000)
			restore.Spec.RunAsUser = &uid
			restore.Spec.RunAsGroup = &gid
			job := reconciler.buildRestoreJob(restore, sourceDB, "restore-job")
			secCtx := job.Spec.Template.Spec.SecurityContext
			Expect(secCtx).NotTo(BeNil())
			Expect(*secCtx.RunAsUser).To(Equal(int64(1000)))
			Expect(*secCtx.RunAsGroup).To(Equal(int64(2000)))
		})

		It("buildValidationJob defaults to root SecurityContext when RunAsUser/RunAsGroup omitted", func() {
			job := reconciler.buildValidationJob(restore, sourceDB, "validate-job")
			secCtx := job.Spec.Template.Spec.SecurityContext
			Expect(secCtx).NotTo(BeNil())
			Expect(*secCtx.RunAsUser).To(Equal(int64(0)))
			Expect(*secCtx.RunAsGroup).To(Equal(int64(0)))
		})

		It("buildValidationJob uses restore's RunAsUser/RunAsGroup when set", func() {
			uid, gid := int64(1000), int64(1000)
			restore.Spec.RunAsUser = &uid
			restore.Spec.RunAsGroup = &gid
			job := reconciler.buildValidationJob(restore, sourceDB, "validate-job")
			secCtx := job.Spec.Template.Spec.SecurityContext
			Expect(secCtx).NotTo(BeNil())
			Expect(*secCtx.RunAsUser).To(Equal(int64(1000)))
			Expect(*secCtx.RunAsGroup).To(Equal(int64(1000)))
		})
	})

	// reconcileRunning — Job still running (no conditions set yet → requeue).
	It("requeues when restore Job is still running and has no completion conditions", func() {
		dbKey, restoreKey, deployKey := newStateMachineResources("job-still-running", 1)
		defer cleanupResources(dbKey, restoreKey, deployKey)

		reconciler := newReconciler()
		driveToRunning(ctx, reconciler, dbKey, restoreKey, deployKey)

		// Reconcile in Running phase — Job exists but has no conditions (still in-progress).
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())
		// Should requeue to check again.
		Expect(result.RequeueAfter).To(BeNumerically(">", 0))

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		// Phase stays at Running — job not yet complete.
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseRunning))
		_ = dbKey
		_ = deployKey
	})
})

// driveToRunning drives a restore through Pending → Pausing → ScalingDown → Running
// by simulating ConfigMap update and Deployment scale-down completion.
func driveToRunning(
	ctx context.Context,
	reconciler *LitestreamRestoreReconciler,
	dbKey, restoreKey, deployKey types.NamespacedName,
) {
	// Pending → Pausing.
	_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
	Expect(err).NotTo(HaveOccurred())

	// Simulate ConfigMap updated to dbs: [].
	cm := &corev1.ConfigMap{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{
		Name: dbKey.Name + "-litestream", Namespace: dbKey.Namespace,
	}, cm)).To(Succeed())
	cmPatch := client.MergeFrom(cm.DeepCopy())
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["litestream.yml"] = "dbs: []\n"
	Expect(k8sClient.Patch(ctx, cm, cmPatch)).To(Succeed())

	// Pausing → ScalingDown.
	_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
	Expect(err).NotTo(HaveOccurred())

	// Simulate Deployment fully scaled down.
	dep := &appsv1.Deployment{}
	Expect(k8sClient.Get(ctx, deployKey, dep)).To(Succeed())
	depStatusPatch := client.MergeFrom(dep.DeepCopy())
	dep.Status.Replicas = 0
	Expect(k8sClient.Status().Patch(ctx, dep, depStatusPatch)).To(Succeed())

	// ScalingDown → Running.
	_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
	Expect(err).NotTo(HaveOccurred())

	restore := &databasev1.LitestreamRestore{}
	Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
	Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseRunning))
}

// driveToValidating drives a restore through Running → Validating by simulating
// a successful restore Job and creating the validation Job.
func driveToValidating(
	ctx context.Context,
	reconciler *LitestreamRestoreReconciler,
	dbKey, restoreKey, deployKey types.NamespacedName,
) {
	driveToRunning(ctx, reconciler, dbKey, restoreKey, deployKey)

	// Simulate restore Job success.
	jobKey := types.NamespacedName{Name: restoreKey.Name + "-restore", Namespace: restoreKey.Namespace}
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, jobKey, job)).To(Succeed())
	now := metav1.Now()
	jobStatusPatch := client.MergeFrom(job.DeepCopy())
	job.Status.StartTime = &now
	job.Status.CompletionTime = &now
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
	}
	Expect(k8sClient.Status().Patch(ctx, job, jobStatusPatch)).To(Succeed())

	// Running → Validating (creates validation Job).
	_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
	Expect(err).NotTo(HaveOccurred())

	// First Validating reconcile creates the validation Job.
	_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
	Expect(err).NotTo(HaveOccurred())

	restore := &databasev1.LitestreamRestore{}
	Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
	Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseValidating))
}

// ─────────────────────────────────────────────────────────────────────────────
// StatefulSet target tests — drive the restore state machine against a StatefulSet.
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("LitestreamRestore State Machine with StatefulSet target", func() {
	const (
		namespaceName = "default"
		targetPVC     = "sm-sts-restore-pvc"
		targetPath    = "/data/myapp.db"
		secretRef     = "sm-sts-s3-creds"
		bucketName    = "sm-sts-backups"
	)

	ctx := context.Background()

	// newStateMachineResourcesSTS creates isolated resources targeting a StatefulSet.
	newStateMachineResourcesSTS := func(suffix string, replicas int32) (
		dbKey types.NamespacedName,
		restoreKey types.NamespacedName,
		stsKey types.NamespacedName,
	) {
		dbName := "sm-sts-db-" + suffix
		restoreName := "sm-sts-restore-" + suffix
		stsName := "sm-sts-app-" + suffix

		dbKey = types.NamespacedName{Name: dbName, Namespace: namespaceName}
		restoreKey = types.NamespacedName{Name: restoreName, Namespace: namespaceName}
		stsKey = types.NamespacedName{Name: stsName, Namespace: namespaceName}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: stsName, Namespace: namespaceName},
			Spec: appsv1.StatefulSetSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": stsName},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": stsName}},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())

		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: dbName, Namespace: namespaceName},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:      "myapp.db",
				DatabasePath:      "/data",
				TargetStatefulSet: stsName,
				Backup: databasev1.BackupSpec{
					Enabled: true,
					Destination: databasev1.BackupDestination{
						S3: &databasev1.S3Destination{
							Bucket:    bucketName,
							SecretRef: secretRef,
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		// Wait for LitestreamReplicaReconciler to create the Litestream ConfigMap.
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: dbName + "-litestream", Namespace: namespaceName,
			}, cm)).To(Succeed())
		}).Should(Succeed())

		restore := &databasev1.LitestreamRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespaceName},
			Spec: databasev1.LitestreamRestoreSpec{
				SourceRef:  dbName,
				TargetPVC:  targetPVC,
				TargetPath: targetPath,
			},
		}
		Expect(k8sClient.Create(ctx, restore)).To(Succeed())

		return dbKey, restoreKey, stsKey
	}

	cleanupSTS := func(dbKey, restoreKey, stsKey types.NamespacedName) { //nolint:dupl
		restore := &databasev1.LitestreamRestore{}
		if err := k8sClient.Get(ctx, restoreKey, restore); err == nil {
			_ = k8sClient.Delete(ctx, restore)
		}
		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, dbKey, db); err == nil {
			_ = k8sClient.Delete(ctx, db)
		}
		for _, suffix := range []string{"-litestream", "-init-sql"} {
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Name: dbKey.Name + suffix, Namespace: namespaceName,
			}, cm); err == nil {
				_ = k8sClient.Delete(ctx, cm)
			}
		}
		sts := &appsv1.StatefulSet{}
		if err := k8sClient.Get(ctx, stsKey, sts); err == nil {
			_ = k8sClient.Delete(ctx, sts)
		}
		job := &batchv1.Job{}
		if err := k8sClient.Get(ctx, types.NamespacedName{
			Name: restoreKey.Name + "-restore", Namespace: namespaceName,
		}, job); err == nil {
			_ = k8sClient.Delete(ctx, job)
		}
	}

	newReconciler := func() *LitestreamRestoreReconciler {
		return &LitestreamRestoreReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(20),
		}
	}

	It("sets pause annotation and transitions to Pausing (StatefulSet target)", func() {
		dbKey, restoreKey, stsKey := newStateMachineResourcesSTS("pause-pending-sts", 1)
		defer cleanupSTS(dbKey, restoreKey, stsKey)

		reconciler := newReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, dbKey, db)).To(Succeed())
		Expect(db.Annotations[databasev1.AnnotationPause]).To(Equal("true"))

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhasePausing))
	})

	It("scales StatefulSet to 0 and records originalReplicas", func() {
		dbKey, restoreKey, stsKey := newStateMachineResourcesSTS("scale-down-sts", 1)
		defer cleanupSTS(dbKey, restoreKey, stsKey)

		reconciler := newReconciler()

		// Pending → Pausing.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.OriginalReplicas).NotTo(BeNil())
		Expect(*restore.Status.OriginalReplicas).To(Equal(int32(1)))

		// Simulate ConfigMap updated to dbs: [].
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: dbKey.Name + "-litestream", Namespace: namespaceName,
		}, cm)).To(Succeed())
		cmPatch := client.MergeFrom(cm.DeepCopy())
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["litestream.yml"] = "dbs: []\n"
		Expect(k8sClient.Patch(ctx, cm, cmPatch)).To(Succeed())

		// Pausing → ScalingDown (scales StatefulSet to 0).
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, stsKey, sts)).To(Succeed())
		Expect(sts.Spec.Replicas).NotTo(BeNil())
		Expect(*sts.Spec.Replicas).To(BeZero())
	})

	It("scales StatefulSet back to originalReplicas after successful restore", func() {
		dbKey, restoreKey, stsKey := newStateMachineResourcesSTS("scale-up-sts", 1)
		defer cleanupSTS(dbKey, restoreKey, stsKey)

		reconciler := newReconciler()

		// Drive through Pending → Pausing.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Update ConfigMap to reflect pause.
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: dbKey.Name + "-litestream", Namespace: namespaceName,
		}, cm)).To(Succeed())
		cmPatch := client.MergeFrom(cm.DeepCopy())
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["litestream.yml"] = "dbs: []\n"
		Expect(k8sClient.Patch(ctx, cm, cmPatch)).To(Succeed())

		// Pausing → ScalingDown.
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		// Simulate StatefulSet fully scaled down (status.replicas = 0).
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, stsKey, sts)).To(Succeed())
		stsStatusPatch := client.MergeFrom(sts.DeepCopy())
		sts.Status.Replicas = 0
		Expect(k8sClient.Status().Patch(ctx, sts, stsStatusPatch)).To(Succeed())

		// ScalingDown → Running (creates restore Job).
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
		Expect(err).NotTo(HaveOccurred())

		restore := &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		Expect(restore.Status.Phase).To(Equal(databasev1.RestorePhaseRunning))

		// Transition through Validating → ScalingUp by faking job success.
		restore = &databasev1.LitestreamRestore{}
		Expect(k8sClient.Get(ctx, restoreKey, restore)).To(Succeed())
		patch := client.MergeFrom(restore.DeepCopy())
		restore.Status.Phase = databasev1.RestorePhaseScalingUp
		Expect(k8sClient.Status().Patch(ctx, restore, patch)).To(Succeed())

		// ScalingUp → Complete (scales StatefulSet back up to 1). Retry until the
		// LitestreamReplica controller updates the ConfigMap (cache propagation).
		Eventually(func(g Gomega) {
			_, reconcileErr := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
			g.Expect(reconcileErr).NotTo(HaveOccurred())
			r := &databasev1.LitestreamRestore{}
			g.Expect(k8sClient.Get(ctx, restoreKey, r)).To(Succeed())
			g.Expect(r.Status.Phase).To(Equal(databasev1.RestorePhaseComplete))
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())

		Expect(k8sClient.Get(ctx, stsKey, sts)).To(Succeed())
		Expect(sts.Spec.Replicas).NotTo(BeNil())
		Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
	})
})

// simulateValidationSuccess marks the validation Job as complete (integrity check passed).
func simulateValidationSuccess(ctx context.Context, restoreKey types.NamespacedName) {
	validateJobKey := types.NamespacedName{Name: restoreKey.Name + "-validate", Namespace: restoreKey.Namespace}
	validateJob := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, validateJobKey, validateJob)).To(Succeed())
	now := metav1.Now()
	vpatch := client.MergeFrom(validateJob.DeepCopy())
	validateJob.Status.StartTime = &now
	validateJob.Status.CompletionTime = &now
	validateJob.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
	}
	Expect(k8sClient.Status().Patch(ctx, validateJob, vpatch)).To(Succeed())
}
