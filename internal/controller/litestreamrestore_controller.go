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
	"io"
	"path/filepath"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
)

const (
	defaultLitestreamImage  = databasev1.LitestreamDefaultImage
	restoreRequeueInterval  = 5 * time.Second
	restoreLabelKey         = "litestream.io/restore"
	restoreTargetVolumeName = "target"
	restoreFinalizerName    = "litestream.io/restore-finalizer"
)

// LitestreamRestoreReconciler reconciles a LitestreamRestore object.
type LitestreamRestoreReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Recorder   record.EventRecorder
	KubeClient kubernetes.Interface
}

// +kubebuilder:rbac:groups=litestream.io,resources=litestreamrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=litestream.io,resources=litestreamrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=litestream.io,resources=litestreamrestores/finalizers,verbs=update
// +kubebuilder:rbac:groups=litestream.io,resources=litestreamreplicas,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

func (r *LitestreamRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	restore := &databasev1.LitestreamRestore{}
	if err := r.Get(ctx, req.NamespacedName, restore); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Add finalizer if not present (before any mutations).
	if !controllerutil.ContainsFinalizer(restore, restoreFinalizerName) {
		controllerutil.AddFinalizer(restore, restoreFinalizerName)
		if err := r.Update(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Look up the referenced LitestreamReplica to get backup config and credentials.
	sourceDB := &databasev1.LitestreamReplica{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: restore.Namespace,
		Name:      restore.Spec.SourceRef.Name,
	}, sourceDB); err != nil {
		if !errors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		// Source not found — if being deleted, handle finalization; otherwise fail.
		if !restore.DeletionTimestamp.IsZero() {
			return r.handleFinalization(ctx, restore, nil)
		}
		return ctrl.Result{}, r.failRestore(ctx, restore,
			fmt.Sprintf("LitestreamReplica %q not found: %v", restore.Spec.SourceRef.Name, err))
	}

	// Handle finalizer for safe cleanup during deletion.
	if !restore.DeletionTimestamp.IsZero() {
		return r.handleFinalization(ctx, restore, sourceDB)
	}

	// Terminal phases — nothing more to do.
	if restore.Status.Phase == databasev1.RestorePhaseCompleted ||
		restore.Status.Phase == databasev1.RestorePhaseFailed {
		return ctrl.Result{}, nil
	}

	log.Info("Reconciling LitestreamRestore", "phase", restore.Status.Phase, "source", restore.Spec.SourceRef.Name)

	if !sourceDB.Spec.Backup.Enabled || sourceDB.Spec.Backup.Destination.S3 == nil {
		return ctrl.Result{}, r.failRestore(ctx, restore,
			fmt.Sprintf("LitestreamReplica %q does not have backup enabled with an S3 destination", restore.Spec.SourceRef.Name))
	}

	switch restore.Status.Phase {
	case "", databasev1.RestorePhasePending:
		return r.reconcilePending(ctx, restore, sourceDB)
	case databasev1.RestorePhaseAcquiringLock:
		return r.reconcileAcquiringLock(ctx, restore, sourceDB)
	case databasev1.RestorePhaseFencing:
		return r.reconcileFencing(ctx, restore, sourceDB)
	case databasev1.RestorePhaseRestoring:
		return r.reconcileRestoring(ctx, restore, sourceDB)
	case databasev1.RestorePhaseValidating:
		// Integrity checking is now handled natively by litestream via -integrity-check.
		// This case handles CRs that were in-flight during upgrade; transition them forward.
		nextPhase := databasev1.RestorePhaseResuming
		if restore.Spec.Mode == databasev1.RestoreModeToPVC {
			nextPhase = databasev1.RestorePhaseCompleted
		}
		return ctrl.Result{RequeueAfter: restoreRequeueInterval}, r.setStatus(ctx, restore,
			nextPhase, restore.Status.JobName, "skipping legacy validation phase",
			restore.Status.OriginalReplicas, restore.Status.StartTime, nil)
	case databasev1.RestorePhaseResuming:
		return r.reconcileResuming(ctx, restore, sourceDB)
	default:
		return ctrl.Result{}, nil
	}
}

// handleFinalization handles safe cleanup when a LitestreamRestore is being deleted.
// If the restore was mid-operation (Fencing/Restoring), the workload is left
// fenced for safety — starting against uncertain data is worse than downtime.
func (r *LitestreamRestoreReconciler) handleFinalization(ctx context.Context, restore *databasev1.LitestreamRestore, sourceDB *databasev1.LitestreamReplica) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(restore, restoreFinalizerName) {
		return ctrl.Result{}, nil
	}

	log.Info("Handling finalization of LitestreamRestore", "phase", restore.Status.Phase)

	// Release the restore lock regardless of phase.
	if restore.Spec.Mode != databasev1.RestoreModeToPVC {
		r.releaseRestoreLock(ctx, restore)
	}

	phase := restore.Status.Phase
	if restore.Spec.Mode != databasev1.RestoreModeToPVC && sourceDB != nil {
		isMidRestore := phase == databasev1.RestorePhaseFencing ||
			phase == databasev1.RestorePhaseRestoring ||
			phase == databasev1.RestorePhaseValidating

		if isMidRestore {
			log.Info("Restore CR deleted mid-operation; leaving workload fenced for safety",
				"phase", phase)
			r.Recorder.Eventf(restore, corev1.EventTypeWarning, "DeletedMidRestore",
				"LitestreamRestore deleted during %s phase; workload left fenced — manual intervention required", phase)
		} else {
			if resumeErr := r.resumeReplication(ctx, sourceDB); resumeErr != nil {
				log.Error(resumeErr, "Failed to remove pause annotation during finalization")
			}
		}
	}

	controllerutil.RemoveFinalizer(restore, restoreFinalizerName)
	if err := r.Update(ctx, restore); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// resolveRestoreTarget resolves the target PVC and path for the restore.
// For InPlace mode, it derives them from the source LitestreamReplica's target workload.
// For ToPVC mode, it reads them from the restore spec.
func (r *LitestreamRestoreReconciler) resolveRestoreTarget(ctx context.Context, restore *databasev1.LitestreamRestore, sourceDB *databasev1.LitestreamReplica) (pvc, path string, err error) {
	if restore.Spec.Mode == databasev1.RestoreModeToPVC {
		if restore.Spec.Target == nil {
			return "", "", fmt.Errorf("mode is ToPVC but target is not specified")
		}
		return restore.Spec.Target.PVC, restore.Spec.Target.Path, nil
	}

	// InPlace mode: derive from source workload.
	wt, err := r.getTargetWorkloadForRestore(ctx, sourceDB)
	if err != nil {
		return "", "", fmt.Errorf("resolving InPlace target: cannot find workload for LitestreamReplica %q: %w", sourceDB.Name, err)
	}

	dbPath := sourceDB.Spec.DatabasePath
	dbFullPath := dbPath + "/" + sourceDB.Spec.DatabaseName

	var podSpec corev1.PodSpec
	if wt.deployment != nil {
		podSpec = wt.deployment.Spec.Template.Spec
	} else {
		podSpec = wt.statefulSet.Spec.Template.Spec
	}

	if len(podSpec.Containers) == 0 {
		return "", "", fmt.Errorf("target workload has no containers")
	}

	// Use explicit container selection when spec.container is set.
	var targetContainer *corev1.Container
	if sourceDB.Spec.Container != "" {
		for i := range podSpec.Containers {
			if podSpec.Containers[i].Name == sourceDB.Spec.Container {
				targetContainer = &podSpec.Containers[i]
				break
			}
		}
		if targetContainer == nil {
			return "", "", fmt.Errorf("container %q not found in workload pod spec", sourceDB.Spec.Container)
		}
	} else {
		targetContainer = &podSpec.Containers[0]
	}

	// Find best-matching volume mount (longest prefix).
	var volumeName string
	var bestLen int
	for _, vm := range targetContainer.VolumeMounts {
		if vm.MountPath == dbPath || strings.HasPrefix(dbPath, vm.MountPath+"/") {
			if len(vm.MountPath) > bestLen {
				volumeName = vm.Name
				bestLen = len(vm.MountPath)
			}
		}
	}
	if volumeName == "" {
		return "", "", fmt.Errorf("no volume mount in container %q covers database path %q", targetContainer.Name, dbPath)
	}

	for _, vol := range podSpec.Volumes {
		if vol.Name == volumeName && vol.PersistentVolumeClaim != nil {
			return vol.PersistentVolumeClaim.ClaimName, dbFullPath, nil
		}
	}

	return "", "", fmt.Errorf("volume %q backing database path %q is not a PersistentVolumeClaim", volumeName, dbPath)
}

// reconcilePending handles the initial phase.
// For InPlace: record original replicas, resolve target, set pause annotation, transition to Pausing.
// For ToPVC: resolve target, create restore job directly (no workload fencing needed).
func (r *LitestreamRestoreReconciler) reconcilePending(ctx context.Context, restore *databasev1.LitestreamRestore, sourceDB *databasev1.LitestreamReplica) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Resolve target PVC and path.
	targetPVC, targetPath, err := r.resolveRestoreTarget(ctx, restore, sourceDB)
	if err != nil {
		return ctrl.Result{}, r.failRestore(ctx, restore,
			fmt.Sprintf("resolving restore target: %v", err))
	}

	// Store resolved target in status for use in later phases.
	if restore.Status.ResolvedPVC == "" {
		patch := client.MergeFrom(restore.DeepCopy())
		restore.Status.ResolvedPVC = targetPVC
		restore.Status.ResolvedPath = targetPath
		if err := r.Status().Patch(ctx, restore, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("storing resolved target in status: %w", err)
		}
	}

	if restore.Spec.Mode == databasev1.RestoreModeToPVC {
		// ToPVC: skip workload fencing entirely — go straight to creating the restore job.
		if err := r.reconcileRestoreConfig(ctx, restore, sourceDB); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating restore litestream config: %w", err)
		}

		jobName := restore.Name + "-restore"
		newJob := r.buildRestoreJob(restore, sourceDB, jobName, targetPVC, targetPath)
		if err := controllerutil.SetControllerReference(restore, newJob, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, newJob); err != nil {
			if !errors.IsAlreadyExists(err) {
				return ctrl.Result{}, fmt.Errorf("creating restore Job: %w", err)
			}
		}

		log.Info("Created restore Job (ToPVC)", "job", jobName)
		r.Recorder.Eventf(restore, corev1.EventTypeNormal, "RestoreStarted",
			"Created restore Job %s (mode: ToPVC)", jobName)

		now := metav1.Now()
		return ctrl.Result{RequeueAfter: restoreRequeueInterval}, r.setStatus(ctx, restore,
			databasev1.RestorePhaseRestoring, jobName, "restore Job running",
			nil, &now, nil)
	}

	// InPlace mode: record original replicas and transition to AcquiringLock.
	originalReplicas := int32(0)
	wt, err := r.getTargetWorkloadForRestore(ctx, sourceDB)
	switch {
	case err == nil:
		originalReplicas = wt.desiredReplicas()
	case errors.IsNotFound(err):
		log.Info("target workload not found; scale-down will be skipped")
	default:
		return ctrl.Result{}, r.failRestore(ctx, restore,
			fmt.Sprintf("getting target workload for LitestreamReplica %q: %v", sourceDB.Name, err))
	}

	log.Info("Transitioning to AcquiringLock", "originalReplicas", originalReplicas)

	now := metav1.Now()
	return ctrl.Result{RequeueAfter: restoreRequeueInterval}, r.setStatus(ctx, restore,
		databasev1.RestorePhaseAcquiringLock, restore.Status.JobName, "acquiring restore lock",
		&originalReplicas, &now, nil)
}

// reconcileAcquiringLock acquires a coordination.k8s.io/v1 Lease to ensure only
// one InPlace restore runs per LitestreamReplica at a time. ToPVC restores skip
// locking since they don't fence the workload.
func (r *LitestreamRestoreReconciler) reconcileAcquiringLock(ctx context.Context, restore *databasev1.LitestreamRestore, sourceDB *databasev1.LitestreamReplica) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// ToPVC restores don't need locking.
	if restore.Spec.Mode == databasev1.RestoreModeToPVC {
		return ctrl.Result{RequeueAfter: restoreRequeueInterval}, r.setStatus(ctx, restore,
			databasev1.RestorePhaseFencing, restore.Status.JobName, "lock not required for ToPVC mode",
			restore.Status.OriginalReplicas, restore.Status.StartTime, nil)
	}

	leaseName := "litestream-restore-" + restore.Spec.SourceRef.Name

	lease := &coordinationv1.Lease{}
	err := r.Get(ctx, types.NamespacedName{Namespace: restore.Namespace, Name: leaseName}, lease)

	if err != nil && !errors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("checking restore lock: %w", err)
	}

	if errors.IsNotFound(err) {
		now := metav1.NewMicroTime(time.Now())
		leaseDuration := int32(3600)
		lease = &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      leaseName,
				Namespace: restore.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "litestream-operator",
					restoreLabelKey:                restore.Name,
				},
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &restore.Name,
				AcquireTime:          &now,
				LeaseDurationSeconds: &leaseDuration,
			},
		}
		if setErr := controllerutil.SetControllerReference(restore, lease, r.Scheme); setErr != nil {
			return ctrl.Result{}, setErr
		}
		if createErr := r.Create(ctx, lease); createErr != nil {
			if errors.IsAlreadyExists(createErr) {
				return ctrl.Result{RequeueAfter: restoreRequeueInterval}, nil
			}
			return ctrl.Result{}, fmt.Errorf("creating restore lock: %w", createErr)
		}

		log.Info("Acquired restore lock", "lease", leaseName)
	} else if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != restore.Name {
		holder := "<unknown>"
		if lease.Spec.HolderIdentity != nil {
			holder = *lease.Spec.HolderIdentity
		}
		log.Info("Restore lock held by another restore", "holder", holder)
		setCondition(&restore.Status.Conditions, databasev1.RestoreConditionLocked,
			metav1.ConditionFalse, "RestoreLocked",
			fmt.Sprintf("another restore %q holds the lock for LitestreamReplica %q", holder, restore.Spec.SourceRef.Name),
			restore.Generation, metav1.Now())

		return ctrl.Result{RequeueAfter: 30 * time.Second}, r.setStatus(ctx, restore,
			databasev1.RestorePhaseAcquiringLock, restore.Status.JobName,
			fmt.Sprintf("waiting for lock — active restore: %s", holder),
			restore.Status.OriginalReplicas, restore.Status.StartTime, nil)
	}

	// Lock acquired — pause replication and transition to Fencing.
	if err := r.pauseReplication(ctx, sourceDB); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting pause annotation on LitestreamReplica: %w", err)
	}

	r.Recorder.Eventf(restore, corev1.EventTypeNormal, "PausingReplication",
		"Pausing Litestream replication on LitestreamReplica %s", sourceDB.Name)

	setCondition(&restore.Status.Conditions, databasev1.RestoreConditionLocked,
		metav1.ConditionTrue, "LockAcquired", fmt.Sprintf("acquired lock %s", leaseName),
		restore.Generation, metav1.Now())

	return ctrl.Result{RequeueAfter: restoreRequeueInterval}, r.setStatus(ctx, restore,
		databasev1.RestorePhaseFencing, restore.Status.JobName, "lock acquired; fencing application",
		restore.Status.OriginalReplicas, restore.Status.StartTime, nil)
}

// reconcileFencing handles pausing replication, scaling the workload to 0, and
// waiting for pods to terminate before creating the restore Job.
func (r *LitestreamRestoreReconciler) reconcileFencing(ctx context.Context, restore *databasev1.LitestreamRestore, sourceDB *databasev1.LitestreamReplica) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Verify pause annotation is still set (defensive — could have been removed).
	if sourceDB.Annotations[pauseAnnotation] != injectEnabled {
		if err := r.pauseReplication(ctx, sourceDB); err != nil {
			return ctrl.Result{}, fmt.Errorf("re-setting pause annotation: %w", err)
		}
		return ctrl.Result{RequeueAfter: restoreRequeueInterval}, nil
	}

	// Verify ConfigMap reflects the pause (dbs: []).
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: sourceDB.Namespace,
		Name:      sourceDB.Name + "-litestream",
	}, cm); err != nil {
		return ctrl.Result{RequeueAfter: restoreRequeueInterval},
			fmt.Errorf("getting litestream ConfigMap: %w", err)
	}
	if cm.Data[litestreamConfigKey] != pausedConfig {
		log.Info("Waiting for ConfigMap to reflect pause")
		return ctrl.Result{RequeueAfter: restoreRequeueInterval}, nil
	}

	// Scale workload to 0. If it no longer exists, treat it as already scaled down.
	wt, err := r.getTargetWorkloadForRestore(ctx, sourceDB)
	if err != nil && !errors.IsNotFound(err) {
		return ctrl.Result{}, r.failRestore(ctx, restore,
			fmt.Sprintf("cannot find target workload: %v", err))
	}
	if err == nil {
		if scaleErr := r.scaleWorkload(ctx, wt, 0); scaleErr != nil {
			return ctrl.Result{}, fmt.Errorf("scaling workload to 0: %w", scaleErr)
		}
		if wt.runningReplicas() > 0 {
			log.Info("Scaled workload to 0, waiting for pods to terminate")
			r.Recorder.Eventf(restore, corev1.EventTypeNormal, "ApplicationFenced",
				"Scaled %s %s to 0 replicas", wt.typeName(), wt.name())
			return ctrl.Result{RequeueAfter: restoreRequeueInterval}, nil
		}
	} else {
		log.Info("target workload not found during Fencing; proceeding without scale-down")
	}

	// All pods terminated — set fenced condition.
	setCondition(&restore.Status.Conditions, databasev1.RestoreConditionApplicationFenced,
		metav1.ConditionTrue, "WorkloadScaledDown", "application is fenced",
		restore.Generation, metav1.Now())

	targetPVC := restore.Status.ResolvedPVC
	targetPath := restore.Status.ResolvedPath

	// Create a fresh litestream ConfigMap for the restore job.
	if err := r.reconcileRestoreConfig(ctx, restore, sourceDB); err != nil {
		return ctrl.Result{}, fmt.Errorf("creating restore litestream config: %w", err)
	}

	// Create the restore Job.
	jobName := restore.Name + "-restore"
	newJob := r.buildRestoreJob(restore, sourceDB, jobName, targetPVC, targetPath)
	if err := controllerutil.SetControllerReference(restore, newJob, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, newJob); err != nil {
		if !errors.IsAlreadyExists(err) {
			return ctrl.Result{}, fmt.Errorf("creating restore Job: %w", err)
		}
	}

	log.Info("Created restore Job", "job", jobName)
	r.Recorder.Eventf(restore, corev1.EventTypeNormal, "RestoreStarted",
		"Created restore Job %s", jobName)

	now := metav1.Now()
	return ctrl.Result{RequeueAfter: restoreRequeueInterval}, r.setStatus(ctx, restore,
		databasev1.RestorePhaseRestoring, jobName, "restore Job running",
		restore.Status.OriginalReplicas, &now, nil)
}

// reconcileRestoring watches the restore Job and transitions to Resuming (InPlace)
// or Completed (ToPVC) on success, or Failed on failure. Integrity checking is
// handled natively by litestream via the -integrity-check flag on the restore command.
func (r *LitestreamRestoreReconciler) reconcileRestoring(ctx context.Context, restore *databasev1.LitestreamRestore, sourceDB *databasev1.LitestreamReplica) (ctrl.Result, error) {
	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Namespace: restore.Namespace, Name: restore.Status.JobName}, job)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, r.failRestoreWithCleanup(ctx, restore, sourceDB, "restore Job not found")
		}
		return ctrl.Result{}, fmt.Errorf("getting restore Job: %w", err)
	}

	for _, cond := range job.Status.Conditions {
		switch cond.Type {
		case batchv1.JobComplete:
			if cond.Status == corev1.ConditionTrue {
				setCondition(&restore.Status.Conditions, databasev1.RestoreConditionRestoreSucceeded,
					metav1.ConditionTrue, "RestoreJobComplete", "restore Job completed with integrity check",
					restore.Generation, metav1.Now())

				if restore.Spec.Mode == databasev1.RestoreModeToPVC {
					r.Recorder.Event(restore, corev1.EventTypeNormal, "RestoreComplete",
						"Litestream restore completed successfully")
					now := metav1.Now()
					return ctrl.Result{}, r.setStatus(ctx, restore,
						databasev1.RestorePhaseCompleted, restore.Status.JobName, "restore completed successfully",
						restore.Status.OriginalReplicas, restore.Status.StartTime, &now)
				}

				r.Recorder.Event(restore, corev1.EventTypeNormal, "RestoreJobComplete",
					"Restore Job completed; resuming application")
				return ctrl.Result{RequeueAfter: restoreRequeueInterval}, r.setStatus(ctx, restore,
					databasev1.RestorePhaseResuming, restore.Status.JobName, "resuming application",
					restore.Status.OriginalReplicas, restore.Status.StartTime, nil)
			}
		case batchv1.JobFailed:
			if cond.Status == corev1.ConditionTrue {
				msg := "restore Job failed"
				if cond.Message != "" {
					msg = cond.Message
				}
				if podLogs := r.restoreJobPodLogs(ctx, restore.Namespace, restore.Name); podLogs != "" {
					msg = fmt.Sprintf("%s; litestream output: %s", msg, podLogs)
				}
				return ctrl.Result{}, r.failRestoreWithCleanup(ctx, restore, sourceDB, msg)
			}
		}
	}

	// Job still running.
	return ctrl.Result{RequeueAfter: restoreRequeueInterval}, nil
}

// reconcileResuming resumes replication, waits for the ConfigMap to reflect the
// full config, then scales the workload back up and transitions to Completed.
// Only used for InPlace mode.
func (r *LitestreamRestoreReconciler) reconcileResuming(ctx context.Context, restore *databasev1.LitestreamRestore, sourceDB *databasev1.LitestreamReplica) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if err := r.setSkipArchiveCheck(ctx, sourceDB, true); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting skip-archive-check on LitestreamReplica: %w", err)
	}

	if err := r.resumeReplication(ctx, sourceDB); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing pause annotation from LitestreamReplica: %w", err)
	}

	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: sourceDB.Namespace,
		Name:      sourceDB.Name + "-litestream",
	}, cm); err != nil {
		return ctrl.Result{RequeueAfter: restoreRequeueInterval},
			fmt.Errorf("getting litestream ConfigMap: %w", err)
	}
	if cm.Data[litestreamConfigKey] == pausedConfig {
		log.Info("Waiting for ConfigMap to reflect unpaused state before scaling up")
		return ctrl.Result{RequeueAfter: restoreRequeueInterval}, nil
	}

	target := int32(1)
	if restore.Status.OriginalReplicas != nil {
		target = *restore.Status.OriginalReplicas
	}
	if target > 0 {
		wt, err := r.getTargetWorkloadForRestore(ctx, sourceDB)
		if err != nil && !errors.IsNotFound(err) {
			log.Error(err, "Cannot find target workload during scale-up")
		} else if err == nil {
			if scaleErr := r.scaleWorkload(ctx, wt, target); scaleErr != nil {
				return ctrl.Result{}, fmt.Errorf("scaling workload back to %d: %w", target, scaleErr)
			}
			log.Info("Scaled workload back up", "replicas", target)
		}
	}

	// Release the restore lock before marking complete.
	if restore.Spec.Mode != databasev1.RestoreModeToPVC {
		r.releaseRestoreLock(ctx, restore)
	}

	setCondition(&restore.Status.Conditions, databasev1.RestoreConditionApplicationResumed,
		metav1.ConditionTrue, "WorkloadScaledUp", "application resumed",
		restore.Generation, metav1.Now())

	r.Recorder.Event(restore, corev1.EventTypeNormal, "ApplicationResumed",
		"Application workload resumed after restore")
	r.Recorder.Event(restore, corev1.EventTypeNormal, "RestoreComplete",
		"Litestream restore completed successfully")

	now := metav1.Now()
	return ctrl.Result{}, r.setStatus(ctx, restore,
		databasev1.RestorePhaseCompleted, restore.Status.JobName, "restore completed successfully",
		restore.Status.OriginalReplicas, restore.Status.StartTime, &now)
}

// failRestoreWithCleanup transitions to Failed. For InPlace mode, the workload
// is left FENCED (replicas=0) — availability loss is preferable to starting
// against uncertain data. The user must investigate and either re-attempt the
// restore or manually scale the workload back up.
func (r *LitestreamRestoreReconciler) failRestoreWithCleanup(ctx context.Context, restore *databasev1.LitestreamRestore, sourceDB *databasev1.LitestreamReplica, msg string) error {
	log := logf.FromContext(ctx)

	if restore.Spec.Mode != databasev1.RestoreModeToPVC {
		log.Info("Restore failed; leaving workload fenced for safety — manual intervention required")

		// Release the restore lock so another restore can be attempted.
		r.releaseRestoreLock(ctx, restore)

		// Remove pause annotation so replication config is restored,
		// but do NOT scale the workload back up.
		if resumeErr := r.resumeReplication(ctx, sourceDB); resumeErr != nil {
			log.Error(resumeErr, "Failed to remove pause annotation during cleanup")
		}

		msg = fmt.Sprintf("%s; APPLICATION IS FENCED (replicas=0) — manual intervention required to resume", msg)
	}

	r.Recorder.Event(restore, corev1.EventTypeWarning, "RestoreFailed", msg)
	now := metav1.Now()
	return r.setStatus(ctx, restore, databasev1.RestorePhaseFailed,
		restore.Status.JobName, msg, restore.Status.OriginalReplicas, restore.Status.StartTime, &now)
}

// pauseReplication sets the pause annotation on the LitestreamReplica CR.
func (r *LitestreamRestoreReconciler) pauseReplication(ctx context.Context, db *databasev1.LitestreamReplica) error {
	if db.Annotations[pauseAnnotation] == injectEnabled {
		return nil // already paused
	}
	patch := client.MergeFrom(db.DeepCopy())
	if db.Annotations == nil {
		db.Annotations = map[string]string{}
	}
	db.Annotations[pauseAnnotation] = injectEnabled
	return r.Patch(ctx, db, patch)
}

// resumeReplication removes the pause annotation from the LitestreamReplica CR.
func (r *LitestreamRestoreReconciler) resumeReplication(ctx context.Context, db *databasev1.LitestreamReplica) error {
	if db.Annotations[pauseAnnotation] != injectEnabled {
		return nil // not paused
	}
	latest := &databasev1.LitestreamReplica{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: db.Namespace, Name: db.Name}, latest); err != nil {
		return err
	}
	patch := client.MergeFrom(latest.DeepCopy())
	delete(latest.Annotations, pauseAnnotation)
	return r.Patch(ctx, latest, patch)
}

// setSkipArchiveCheck sets or removes the skip-archive-check annotation on a LitestreamReplica.
func (r *LitestreamRestoreReconciler) setSkipArchiveCheck(ctx context.Context, db *databasev1.LitestreamReplica, enable bool) error {
	latest := &databasev1.LitestreamReplica{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: db.Namespace, Name: db.Name}, latest); err != nil {
		return err
	}
	if enable {
		if latest.Annotations[databasev1.AnnotationSkipArchiveCheck] == injectEnabled {
			return nil
		}
	} else {
		if latest.Annotations[databasev1.AnnotationSkipArchiveCheck] != injectEnabled {
			return nil
		}
	}
	patch := client.MergeFrom(latest.DeepCopy())
	if latest.Annotations == nil {
		latest.Annotations = map[string]string{}
	}
	if enable {
		latest.Annotations[databasev1.AnnotationSkipArchiveCheck] = injectEnabled
	} else {
		delete(latest.Annotations, databasev1.AnnotationSkipArchiveCheck)
	}
	return r.Patch(ctx, latest, patch)
}

// releaseRestoreLock deletes the Lease used for restore concurrency control.
// Best-effort: errors are silently ignored.
func (r *LitestreamRestoreReconciler) releaseRestoreLock(ctx context.Context, restore *databasev1.LitestreamRestore) {
	leaseName := "litestream-restore-" + restore.Spec.SourceRef.Name
	lease := &coordinationv1.Lease{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: restore.Namespace,
		Name:      leaseName,
	}, lease); err != nil {
		return
	}
	if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity == restore.Name {
		_ = r.Delete(ctx, lease)
	}
}

// buildRestoreJob constructs the Job that runs `litestream restore`.
func (r *LitestreamRestoreReconciler) buildRestoreJob(
	restore *databasev1.LitestreamRestore,
	sourceDB *databasev1.LitestreamReplica,
	jobName, targetPVC, targetPath string,
) *batchv1.Job {
	s3 := sourceDB.Spec.Backup.Destination.S3

	image := restore.Spec.Image
	if image == "" {
		image = sourceDB.Spec.Image
	}
	if image == "" {
		image = defaultLitestreamImage
	}

	dbPathInConfig := sourceDB.Spec.DatabasePath + "/" + sourceDB.Spec.DatabaseName
	args := []string{
		"restore",
		"-config", "/etc/litestream/litestream.yml",
		"-integrity-check", "quick",
		"-o", targetPath,
	}
	if restore.Spec.Force {
		args = append(args, "-force")
	}
	if restore.Spec.Timestamp != "" {
		args = append(args, "-timestamp", restore.Spec.Timestamp)
	}
	args = append(args, dbPathInConfig)

	mountPath := filepath.Dir(targetPath)
	jobLabels := map[string]string{
		"app.kubernetes.io/managed-by": "litestream-operator",
		restoreLabelKey:                restore.Name,
	}

	restorePodSpec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyOnFailure,
		Containers: []corev1.Container{
			{
				Name:  "litestream-restore",
				Image: image,
				Args:  args,
				Env:   s3EnvVars(s3.SecretRef),
				VolumeMounts: []corev1.VolumeMount{
					{Name: restoreTargetVolumeName, MountPath: mountPath},
					{Name: "litestream-config", MountPath: "/etc/litestream", ReadOnly: true},
				},
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: "target",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: targetPVC,
					},
				},
			},
			{
				Name: "litestream-config",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: restore.Name + "-litestream",
						},
					},
				},
			},
		},
	}
	if restore.Spec.RunAsUser != nil || restore.Spec.RunAsGroup != nil {
		restorePodSpec.SecurityContext = &corev1.PodSecurityContext{
			RunAsUser:  restore.Spec.RunAsUser,
			RunAsGroup: restore.Spec.RunAsGroup,
		}
	}

	backoffLimit := int32(3)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: restore.Namespace,
			Labels:    jobLabels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: jobLabels},
				Spec:       restorePodSpec,
			},
		},
	}
	return job
}

// reconcileRestoreConfig creates a dedicated litestream ConfigMap for the restore job.
func (r *LitestreamRestoreReconciler) reconcileRestoreConfig(ctx context.Context, restore *databasev1.LitestreamRestore, sourceDB *databasev1.LitestreamReplica) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restore.Name + "-litestream",
			Namespace: restore.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Data = map[string]string{
			litestreamConfigKey: buildLitestreamConfigYAML(sourceDB),
		}
		return controllerutil.SetControllerReference(restore, cm, r.Scheme)
	})
	return err
}

// s3EnvVars returns S3 credential env vars sourced from the named Secret.
func s3EnvVars(secretRef string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name: "LITESTREAM_ACCESS_KEY_ID",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretRef},
					Key:                  "ACCESS_KEY_ID",
				},
			},
		},
		{
			Name: "LITESTREAM_SECRET_ACCESS_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretRef},
					Key:                  "SECRET_ACCESS_KEY",
				},
			},
		},
	}
}

// restoreJobPodLogs fetches the logs from the most recent pod of a restore Job
// (tail 30 lines) so the actual litestream error can be surfaced in status.message.
func (r *LitestreamRestoreReconciler) restoreJobPodLogs(ctx context.Context, namespace, restoreName string) string {
	if r.KubeClient == nil {
		return ""
	}
	podList, err := r.KubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: restoreLabelKey + "=" + restoreName,
	})
	if err != nil || len(podList.Items) == 0 {
		return ""
	}
	pod := podList.Items[0]
	tail := int64(30)
	req := r.KubeClient.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
		TailLines: &tail,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return ""
	}
	defer func() { _ = stream.Close() }()
	b, err := io.ReadAll(stream)
	if err != nil {
		return ""
	}
	return string(b)
}

func (r *LitestreamRestoreReconciler) failRestore(ctx context.Context, restore *databasev1.LitestreamRestore, msg string) error {
	r.Recorder.Event(restore, corev1.EventTypeWarning, "RestoreFailed", msg)
	now := metav1.Now()
	return r.setStatus(ctx, restore, databasev1.RestorePhaseFailed, "",
		msg, nil, &now, &now)
}

// setStatus writes the given phase and metadata to the restore's status subresource.
func (r *LitestreamRestoreReconciler) setStatus(
	ctx context.Context,
	restore *databasev1.LitestreamRestore,
	phase, jobName, message string,
	originalReplicas *int32,
	startTime *metav1.Time,
	completionTime *metav1.Time,
) error {
	patch := client.MergeFrom(restore.DeepCopy())
	restore.Status.Phase = phase
	if jobName != "" {
		restore.Status.JobName = jobName
	}
	restore.Status.Message = message
	if originalReplicas != nil && restore.Status.OriginalReplicas == nil {
		restore.Status.OriginalReplicas = originalReplicas
	}
	if startTime != nil && restore.Status.StartTime == nil {
		restore.Status.StartTime = startTime
	}
	if completionTime != nil {
		restore.Status.CompletionTime = completionTime
	}
	return r.Status().Patch(ctx, restore, patch)
}

// SetupWithManager sets up the controller with the Manager.
func (r *LitestreamRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&databasev1.LitestreamRestore{}).
		Owns(&batchv1.Job{}).
		Owns(&coordinationv1.Lease{}).
		Named("litestreamrestore").
		Complete(r)
}
