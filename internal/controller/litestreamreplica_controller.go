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

// Package controller contains the SQLite database controller implementation.
package controller

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
)

// statusSyncInterval is how often the controller re-checks backup health even
// when no resource change event has fired.
const statusSyncInterval = 2 * time.Minute

// Expose annotation keys as package-level aliases so tests can reference them
// without importing the API package directly.
const (
	injectAnnotation      = databasev1.AnnotationInject
	configAnnotation      = databasev1.AnnotationConfig
	pauseAnnotation       = databasev1.AnnotationPause
	skipArchiveAnnotation = databasev1.AnnotationSkipArchiveCheck
)

const (
	injectEnabled               = "true"
	pausedConfig                = "dbs: []\n"
	litestreamSidecar           = "litestream"
	litestreamConfigKey         = "litestream.yml"
	replicaFinalizerName        = "litestream.io/replica-finalizer"
	injectionSpecHashAnnotation = "litestream.io/injection-spec-hash"
)

// LitestreamReplicaReconciler reconciles a LitestreamReplica object
type LitestreamReplicaReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=litestream.io,resources=litestreamreplicas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=litestream.io,resources=litestreamreplicas/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=litestream.io,resources=litestreamreplicas/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

func (r *LitestreamReplicaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	db := &databasev1.LitestreamReplica{}
	if err := r.Get(ctx, req.NamespacedName, db); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion: clean up target workload annotations.
	if !db.DeletionTimestamp.IsZero() {
		return r.handleReplicaFinalization(ctx, db)
	}

	// Add finalizer if not present.
	if !controllerutil.ContainsFinalizer(db, replicaFinalizerName) {
		controllerutil.AddFinalizer(db, replicaFinalizerName)
		if err := r.Update(ctx, db); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	target := db.Spec.TargetDeployment
	if db.Spec.TargetStatefulSet != "" {
		target = db.Spec.TargetStatefulSet
	}
	log.Info("Reconciling LitestreamReplica", "target", target)

	if err := r.reconcileLitestreamConfig(ctx, db); err != nil {
		log.Error(err, "Failed to reconcile Litestream ConfigMap")
		return ctrl.Result{}, err
	}

	if err := r.reconcileBootstrapConfig(ctx, db); err != nil {
		log.Error(err, "Failed to reconcile bootstrap SQL ConfigMap")
		return ctrl.Result{}, err
	}

	if err := r.reconcileTargetAnnotation(ctx, db); err != nil {
		log.Error(err, "Failed to annotate target workload")
		return ctrl.Result{}, err
	}

	if err := r.updateStatus(ctx, db); err != nil {
		log.Error(err, "Failed to update LitestreamReplica status")
		return ctrl.Result{}, err
	}

	// Requeue periodically to refresh backup health even without change events.
	return ctrl.Result{RequeueAfter: statusSyncInterval}, nil
}

// reconcileLitestreamConfig creates or updates the ConfigMap holding litestream.yml.
// When the pause annotation is set, writes an empty dbs list so Litestream runs
// but replicates nothing — protecting the S3 backup chain during restores.
func (r *LitestreamReplicaReconciler) reconcileLitestreamConfig(ctx context.Context, db *databasev1.LitestreamReplica) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      db.Name + "-litestream",
			Namespace: db.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		var config string
		if db.Annotations[pauseAnnotation] == injectEnabled {
			config = pausedConfig
		} else {
			config = r.buildLitestreamConfig(db)
		}
		cm.Data = map[string]string{
			litestreamConfigKey: config,
		}
		return controllerutil.SetControllerReference(db, cm, r.Scheme)
	})

	return err
}

// buildLitestreamConfig renders the litestream.yml content for the given CR.
// Uses the singular `replica:` key (Litestream 0.5.x preferred form).
// This is a thin wrapper around the package-level buildLitestreamConfigYAML so
// the restore controller can call the same logic without a method receiver.
func (r *LitestreamReplicaReconciler) buildLitestreamConfig(db *databasev1.LitestreamReplica) string {
	return buildLitestreamConfigYAML(db)
}

// buildLitestreamConfigYAML is the package-level implementation shared by both
// the LitestreamReplica and LitestreamRestore controllers.
func buildLitestreamConfigYAML(db *databasev1.LitestreamReplica) string {
	dbPath := fmt.Sprintf("%s/%s", db.Spec.DatabasePath, db.Spec.DatabaseName)

	// Global settings: Prometheus metrics endpoint (upstream guide recommendation).
	cfg := "addr: \":9090\"\n"

	// Per-database config.
	cfg += fmt.Sprintf("dbs:\n  - path: %s\n", dbPath)

	if db.Spec.Backup.Enabled && db.Spec.Backup.Destination.S3 != nil {
		s3 := db.Spec.Backup.Destination.S3
		cfg += "    replica:\n"
		cfg += "      type: s3\n"
		if s3.Endpoint != "" {
			// Ensure the endpoint has a scheme. Litestream defaults to HTTPS
			// when no scheme is present, which breaks plain-HTTP S3-compatible
			// stores like MinIO without TLS. Preserve any existing scheme.
			endpoint := s3.Endpoint
			if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
				endpoint = "http://" + endpoint
			}
			cfg += fmt.Sprintf("      endpoint: %s\n", endpoint)
			// MinIO and other S3-compatible stores require path-style addressing.
			cfg += "      force-path-style: true\n"
		}
		cfg += fmt.Sprintf("      bucket: %s\n", s3.Bucket)
		if s3.Path != "" {
			// Litestream 0.5.x appends "/L{N}/" to the configured path when
			// constructing S3 object keys. A trailing slash produces "//L0/"
			// which MinIO rejects as XMinioInvalidObjectName.
			cfg += fmt.Sprintf("      path: %s\n", strings.TrimRight(s3.Path, "/"))
		}
		if db.Spec.Backup.Retention.Duration != "" {
			cfg += fmt.Sprintf("      retention: %s\n", db.Spec.Backup.Retention.Duration)
		}
		if db.Spec.Backup.SyncInterval != "" {
			cfg += fmt.Sprintf("      sync-interval: %s\n", db.Spec.Backup.SyncInterval)
		}
	}

	return cfg
}

// reconcileBootstrapConfig creates or updates the ConfigMap that holds bootstrap.sql.
// When Bootstrap.SQL is empty, any existing ConfigMap is left in place (owned
// resources are GC'd on CR deletion).
func (r *LitestreamReplicaReconciler) reconcileBootstrapConfig(ctx context.Context, db *databasev1.LitestreamReplica) error {
	if db.Spec.Bootstrap.SQL == "" {
		return nil
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      db.Name + "-bootstrap-sql",
			Namespace: db.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Data = map[string]string{
			"bootstrap.sql": db.Spec.Bootstrap.SQL,
		}
		return controllerutil.SetControllerReference(db, cm, r.Scheme)
	})
	if err != nil {
		return err
	}

	setCondition(&db.Status.Conditions, databasev1.ConditionBootstrapApplied,
		metav1.ConditionTrue, "ConfigMapReady",
		"bootstrap SQL ConfigMap ready",
		db.Generation, metav1.Now())

	return nil
}

// reconcileTargetAnnotation adds injection annotations to the target workload's
// pod template, triggering a rolling restart so new pods inherit them and the
// mutating webhook can inject the Litestream sidecar.
// If the target workload has more than one replica, the annotation is skipped
// and an event is emitted — Litestream requires exactly one writer.
func (r *LitestreamReplicaReconciler) reconcileTargetAnnotation(ctx context.Context, db *databasev1.LitestreamReplica) error {
	wt, err := r.getTargetWorkload(ctx, db)
	if err != nil {
		return err
	}

	// Litestream requires exactly one writer. Emit a warning event and skip
	// annotation; updateStatus will set phase=Error with ConditionReplicaCountExceeded.
	if wt.desiredReplicas() > 1 {
		r.Recorder.Eventf(db, corev1.EventTypeWarning, "ReplicaCountExceeded",
			"target %s %q has %d replicas; Litestream requires exactly 1 writer",
			wt.typeName(), wt.name(), wt.desiredReplicas())
		return nil
	}

	configRef := fmt.Sprintf("%s/%s", db.Namespace, db.Name)
	hash := injectionSpecHash(db)

	tmplAnnotations := wt.podTemplateAnnotations()
	tmplLabels := wt.podTemplateLabels()
	if tmplAnnotations[injectAnnotation] == injectEnabled &&
		tmplAnnotations[configAnnotation] == configRef &&
		tmplAnnotations[injectionSpecHashAnnotation] == hash &&
		tmplLabels[injectAnnotation] == injectEnabled {
		return nil
	}

	return r.patchWorkloadPodTemplate(ctx, wt,
		map[string]string{
			injectAnnotation:            injectEnabled,
			configAnnotation:            configRef,
			injectionSpecHashAnnotation: hash,
		},
		map[string]string{
			injectAnnotation: injectEnabled,
		},
	)
}

// litestreamContainerState inspects pods belonging to the target workload
// and returns the state of the Litestream sidecar container across all
// running pods. It returns (healthy, message) where healthy is true only
// when at least one pod has the sidecar in a Running state and no pods have
// it in a terminal failure state (CrashLoopBackOff / OOMKilled / Error).
func (r *LitestreamReplicaReconciler) litestreamContainerState(ctx context.Context, db *databasev1.LitestreamReplica, wt *workloadTarget) (healthy bool, message string) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(db.Namespace),
		client.MatchingLabels(wt.selectorLabels()),
	); err != nil {
		return false, fmt.Sprintf("failed to list pods: %v", err)
	}

	var (
		runningCount int
		failedPods   []string
		noneFound    = true
	)

	for i := range podList.Items {
		pod := &podList.Items[i]
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name != litestreamSidecar {
				continue
			}
			noneFound = false
			if cs.State.Running != nil {
				runningCount++
			}
			if cs.State.Waiting != nil && isFailureReason(cs.State.Waiting.Reason) {
				failedPods = append(failedPods, fmt.Sprintf("%s (%s)", pod.Name, cs.State.Waiting.Reason))
			}
			if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
				failedPods = append(failedPods, fmt.Sprintf("%s (exit %d)", pod.Name, cs.State.Terminated.ExitCode))
			}
		}
	}

	switch {
	case noneFound:
		return false, "no pods with Litestream sidecar found; waiting for rollout"
	case len(failedPods) > 0:
		return false, fmt.Sprintf("Litestream sidecar unhealthy in pods: %v", failedPods)
	case runningCount > 0:
		return true, fmt.Sprintf("Litestream sidecar running in %d pod(s)", runningCount)
	default:
		return false, "Litestream sidecar not yet running"
	}
}

// isFailureReason returns true for container waiting reasons that indicate a
// non-transient failure (as opposed to normal startup states like
// ContainerCreating or PodInitializing).
func isFailureReason(reason string) bool {
	switch reason {
	case "CrashLoopBackOff", "OOMKilled", "Error", "ImagePullBackOff", "ErrImagePull":
		return true
	}
	return false
}

// updateStatus computes and writes the status subresource for the LitestreamReplica,
// including sidecar injection state and backup health from live pod inspection.
func (r *LitestreamReplicaReconciler) updateStatus(ctx context.Context, db *databasev1.LitestreamReplica) error {
	wt, err := r.getTargetWorkload(ctx, db)

	patch := client.MergeFrom(db.DeepCopy())
	now := metav1.Now()
	db.Status.ObservedGeneration = db.Generation

	if err != nil {
		db.Status.Phase = databasev1.PhaseError
		db.Status.Ready = false
		db.Status.BackupHealthy = false
		notFoundReason := "WorkloadNotFound"
		notFoundMsg := fmt.Sprintf("target workload not found: %v", err)
		setCondition(&db.Status.Conditions, databasev1.ConditionTargetReady,
			metav1.ConditionFalse, notFoundReason, notFoundMsg,
			db.Generation, now)
		setCondition(&db.Status.Conditions, databasev1.ConditionSidecarReady,
			metav1.ConditionFalse, notFoundReason, notFoundMsg,
			db.Generation, now)
		setCondition(&db.Status.Conditions, databasev1.ConditionReplicationHealthy,
			metav1.ConditionFalse, notFoundReason, notFoundMsg,
			db.Generation, now)
		setCondition(&db.Status.Conditions, databasev1.ConditionReady,
			metav1.ConditionFalse, notFoundReason, notFoundMsg,
			db.Generation, now)
		return r.Status().Patch(ctx, db, patch)
	}

	// --- TargetReady condition ---
	setCondition(&db.Status.Conditions, databasev1.ConditionTargetReady,
		metav1.ConditionTrue, "WorkloadFound",
		fmt.Sprintf("target %s %q found", wt.typeName(), wt.name()),
		db.Generation, now)

	// --- Replica count guard: Litestream requires exactly one writer ---
	if wt.desiredReplicas() > 1 {
		db.Status.Phase = databasev1.PhaseError
		db.Status.Ready = false
		setCondition(&db.Status.Conditions, databasev1.ConditionReplicaCountExceeded,
			metav1.ConditionTrue, "TooManyReplicas",
			fmt.Sprintf("target %s %q has %d replicas; Litestream requires exactly 1 writer",
				wt.typeName(), wt.name(), wt.desiredReplicas()),
			db.Generation, now)
		return r.Status().Patch(ctx, db, patch)
	}
	// Clear the condition when replicas are back to 1.
	meta.RemoveStatusCondition(&db.Status.Conditions, databasev1.ConditionReplicaCountExceeded)

	// --- Rollout strategy guard: prevent concurrent writers ---
	if wt.deployment != nil {
		if !isRolloutStrategySafe(wt.deployment) {
			db.Status.Phase = databasev1.PhaseError
			db.Status.Ready = false
			setCondition(&db.Status.Conditions, databasev1.ConditionUnsafeRolloutStrategy,
				metav1.ConditionTrue, "UnsafeRolloutStrategy",
				fmt.Sprintf("target Deployment %q uses RollingUpdate strategy which can temporarily run two pods; "+
					"use Recreate or RollingUpdate with maxSurge=0 for single-writer SQLite safety", wt.name()),
				db.Generation, now)
			r.Recorder.Eventf(db, corev1.EventTypeWarning, "UnsafeRolloutStrategy",
				"target Deployment %q uses unsafe rollout strategy for single-writer SQLite", wt.name())
			return r.Status().Patch(ctx, db, patch)
		}
		meta.RemoveStatusCondition(&db.Status.Conditions, databasev1.ConditionUnsafeRolloutStrategy)
	}

	// --- ReplicationPaused condition ---
	isPaused := db.Annotations[pauseAnnotation] == injectEnabled
	if isPaused {
		setCondition(&db.Status.Conditions, databasev1.ConditionReplicationPaused,
			metav1.ConditionTrue, "PauseAnnotationSet",
			"replication paused via annotation",
			db.Generation, now)
	} else {
		setCondition(&db.Status.Conditions, databasev1.ConditionReplicationPaused,
			metav1.ConditionFalse, "ReplicationActive",
			"replication is active",
			db.Generation, now)
	}

	// --- RecoverySafe condition (from archive-check init container status) ---
	if archiveCheckFailed, msg := r.archiveCheckState(ctx, db, wt); archiveCheckFailed {
		setCondition(&db.Status.Conditions, databasev1.ConditionRecoverySafe,
			metav1.ConditionFalse, "ArchiveCheckFailed", msg,
			db.Generation, now)
		db.Status.Phase = databasev1.PhaseError
		db.Status.Ready = false
		return r.Status().Patch(ctx, db, patch)
	} else {
		setCondition(&db.Status.Conditions, databasev1.ConditionRecoverySafe,
			metav1.ConditionTrue, "ArchiveCheckPassed", msg,
			db.Generation, now)
	}

	// --- SidecarReady condition ---
	annotated := wt.podTemplateAnnotations()[injectAnnotation] == injectEnabled

	// --- ReplicationHealthy condition (pod inspection) ---
	prevHealthy := db.Status.BackupHealthy
	if db.Spec.Backup.Enabled {
		healthy, msg := r.litestreamContainerState(ctx, db, wt)
		db.Status.BackupHealthy = healthy

		condStatus := metav1.ConditionFalse
		sidecarReason := "SidecarUnhealthy"
		replicationReason := "SidecarUnhealthy"
		if healthy {
			condStatus = metav1.ConditionTrue
			sidecarReason = "SidecarRunning"
			replicationReason = "SidecarRunning"
		}
		setCondition(&db.Status.Conditions, databasev1.ConditionSidecarReady,
			condStatus, sidecarReason, msg, db.Generation, now)

		// Refine replication health with actual metrics when configured.
		if healthy && db.Spec.Health.MaxReplicationLag != "" {
			maxLag, parseErr := time.ParseDuration(db.Spec.Health.MaxReplicationLag)
			if parseErr == nil {
				lastSync, scrapeErr := r.scrapeReplicationLag(ctx, db, wt)
				if scrapeErr == nil {
					lag := time.Since(lastSync)
					syncTime := metav1.NewTime(lastSync)
					db.Status.LastSuccessfulReplicationTime = &syncTime
					db.Status.ReplicationLag = lag.Truncate(time.Second).String()

					if lag > maxLag {
						condStatus = metav1.ConditionFalse
						replicationReason = "ReplicationLagExceeded"
						msg = fmt.Sprintf("replication lag %s exceeds threshold %s",
							lag.Truncate(time.Second), maxLag)
						db.Status.BackupHealthy = false
					}
				} else {
					logf.FromContext(ctx).V(1).Info("metrics scrape failed; using container state as health signal",
						"error", scrapeErr)
				}
			}
		}

		setCondition(&db.Status.Conditions, databasev1.ConditionReplicationHealthy,
			condStatus, replicationReason, msg, db.Generation, now)

		if healthy && !prevHealthy {
			r.Recorder.Event(db, corev1.EventTypeNormal, "ReplicationHealthy",
				"Litestream sidecar is running and replicating")
		} else if !healthy && prevHealthy {
			r.Recorder.Event(db, corev1.EventTypeWarning, "ReplicationUnhealthy", msg)
		}

		if healthy && db.Annotations[skipArchiveAnnotation] == injectEnabled {
			if err := r.clearSkipArchiveCheck(ctx, db); err != nil {
				logf.FromContext(ctx).Error(err, "failed to clear skip-archive-check annotation; will retry")
			}
		}
	} else {
		db.Status.BackupHealthy = false
		setCondition(&db.Status.Conditions, databasev1.ConditionSidecarReady,
			metav1.ConditionFalse, "BackupDisabled",
			"backup is not enabled in spec",
			db.Generation, now)
		setCondition(&db.Status.Conditions, databasev1.ConditionReplicationHealthy,
			metav1.ConditionFalse, "BackupDisabled",
			"backup is not enabled in spec",
			db.Generation, now)
	}

	// --- Paused phase (overrides Ready/Pending) ---
	if isPaused {
		db.Status.Phase = databasev1.PhasePaused
		db.Status.Ready = false
		setCondition(&db.Status.Conditions, databasev1.ConditionReady,
			metav1.ConditionFalse, "ReplicationPaused",
			"replication is paused; waiting for pause annotation to be removed",
			db.Generation, now)
		return r.Status().Patch(ctx, db, patch)
	}

	// --- InjectedSpecHash ---
	db.Status.InjectedSpecHash = injectionSpecHash(db)

	// --- Ready condition ---
	if annotated && wt.readyReplicas() > 0 {
		db.Status.Phase = databasev1.PhaseReady
		db.Status.Ready = true
		setCondition(&db.Status.Conditions, databasev1.ConditionReady,
			metav1.ConditionTrue, "WorkloadReady",
			fmt.Sprintf("target %s has ready replicas", wt.typeName()),
			db.Generation, now)
	} else {
		db.Status.Phase = databasev1.PhasePending
		db.Status.Ready = false
		setCondition(&db.Status.Conditions, databasev1.ConditionReady,
			metav1.ConditionFalse, "WorkloadNotReady",
			fmt.Sprintf("waiting for target %s to have ready replicas", wt.typeName()),
			db.Generation, now)
	}

	return r.Status().Patch(ctx, db, patch)
}

// isRolloutStrategySafe returns true if the Deployment's rollout strategy
// cannot produce concurrent writers (Recreate, or RollingUpdate with maxSurge=0).
func isRolloutStrategySafe(deploy *appsv1.Deployment) bool {
	strategy := deploy.Spec.Strategy
	if strategy.Type != "" && strategy.Type != appsv1.RollingUpdateDeploymentStrategyType {
		return true
	}
	if strategy.RollingUpdate == nil || strategy.RollingUpdate.MaxSurge == nil {
		return false
	}
	ms := strategy.RollingUpdate.MaxSurge
	if ms.Type == intstr.Int && ms.IntValue() == 0 {
		return true
	}
	return ms.Type == intstr.String && ms.StrVal == "0"
}

// scrapeReplicationLag finds a running pod with the Litestream sidecar and scrapes
// the /metrics endpoint for litestream_replica_last_sync_seconds. Returns the
// timestamp of the last successful sync. This is best-effort: it requires
// in-cluster connectivity to pod IPs.
func (r *LitestreamReplicaReconciler) scrapeReplicationLag(ctx context.Context, db *databasev1.LitestreamReplica, wt *workloadTarget) (time.Time, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(db.Namespace),
		client.MatchingLabels(wt.selectorLabels()),
	); err != nil {
		return time.Time{}, fmt.Errorf("listing pods: %w", err)
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.PodIP == "" {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name == litestreamSidecar && cs.State.Running != nil {
				return r.fetchLastSyncTime(ctx, pod.Status.PodIP)
			}
		}
	}
	return time.Time{}, fmt.Errorf("no running pod with Litestream sidecar found")
}

// fetchLastSyncTime scrapes the Prometheus metrics endpoint at the given pod IP
// and extracts litestream_replica_last_sync_seconds.
func (r *LitestreamReplicaReconciler) fetchLastSyncTime(ctx context.Context, podIP string) (time.Time, error) {
	url := fmt.Sprintf("http://%s:9090/metrics", podIP)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return time.Time{}, err
	}

	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return time.Time{}, err
	}

	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") || !strings.Contains(line, "litestream_replica_last_sync_seconds") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			ts, parseErr := strconv.ParseFloat(parts[len(parts)-1], 64)
			if parseErr != nil {
				continue
			}
			return time.Unix(int64(ts), int64((ts-float64(int64(ts)))*1e9)), nil
		}
	}
	return time.Time{}, fmt.Errorf("litestream_replica_last_sync_seconds metric not found")
}

// archiveCheckState inspects pods to detect whether the archive-check init container
// has failed. Returns (failed, message). A failure means the DB is missing but S3
// has existing backup data — the pod is blocked until a LitestreamRestore resolves it.
func (r *LitestreamReplicaReconciler) archiveCheckState(ctx context.Context, db *databasev1.LitestreamReplica, wt *workloadTarget) (bool, string) {
	if !db.Spec.Backup.Enabled {
		return false, "backup not enabled"
	}

	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(db.Namespace),
		client.MatchingLabels(wt.selectorLabels()),
	); err != nil {
		return false, fmt.Sprintf("failed to list pods: %v", err)
	}

	const archiveCheckContainer = "litestream-archive-check"
	for i := range podList.Items {
		pod := &podList.Items[i]
		for _, cs := range pod.Status.InitContainerStatuses {
			if cs.Name != archiveCheckContainer {
				continue
			}
			if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
				return true, fmt.Sprintf(
					"archive check failed in pod %s: S3 has existing backup data but local database is missing; create a LitestreamRestore CR to recover",
					pod.Name,
				)
			}
		}
	}
	return false, "archive check passed"
}

// clearSkipArchiveCheck removes the skip-archive-check annotation from the LitestreamReplica.
// It re-fetches the object to avoid version conflicts before patching.
func (r *LitestreamReplicaReconciler) clearSkipArchiveCheck(ctx context.Context, db *databasev1.LitestreamReplica) error {
	latest := &databasev1.LitestreamReplica{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: db.Namespace, Name: db.Name}, latest); err != nil {
		return err
	}
	if latest.Annotations[skipArchiveAnnotation] != injectEnabled {
		return nil // already absent
	}
	patch := client.MergeFrom(latest.DeepCopy())
	delete(latest.Annotations, skipArchiveAnnotation)
	return r.Patch(ctx, latest, patch)
}

// handleReplicaFinalization cleans up operator-owned annotations from the target
// workload when the LitestreamReplica CR is deleted.
func (r *LitestreamReplicaReconciler) handleReplicaFinalization(ctx context.Context, db *databasev1.LitestreamReplica) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(db, replicaFinalizerName) {
		return ctrl.Result{}, nil
	}

	log.Info("Handling finalization of LitestreamReplica")

	wt, err := r.getTargetWorkload(ctx, db)
	if err == nil {
		if removeErr := r.removeWorkloadAnnotations(ctx, wt); removeErr != nil {
			log.Error(removeErr, "Failed to remove annotations from target workload during finalization")
		} else {
			r.Recorder.Event(db, corev1.EventTypeNormal, "ReplicaDeinstrumented",
				fmt.Sprintf("Removed Litestream annotations from %s %s", wt.typeName(), wt.name()))
		}
	}

	controllerutil.RemoveFinalizer(db, replicaFinalizerName)
	if err := r.Update(ctx, db); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// injectionSpecHash returns a deterministic hash of all CR fields that affect
// injected containers. A change in any of these fields triggers a rollout.
func injectionSpecHash(db *databasev1.LitestreamReplica) string {
	h := sha256.New()
	w := func(s string) { _, _ = io.WriteString(h, s) }
	w(fmt.Sprintf("image=%s\n", db.Spec.Image))
	w(fmt.Sprintf("recovery.mode=%s\n", db.Spec.Recovery.Mode))
	w(fmt.Sprintf("databaseName=%s\n", db.Spec.DatabaseName))
	w(fmt.Sprintf("databasePath=%s\n", db.Spec.DatabasePath))
	w(fmt.Sprintf("backup.enabled=%t\n", db.Spec.Backup.Enabled))
	if db.Spec.Backup.Destination.S3 != nil {
		w(fmt.Sprintf("s3.endpoint=%s\n", db.Spec.Backup.Destination.S3.Endpoint))
		w(fmt.Sprintf("s3.bucket=%s\n", db.Spec.Backup.Destination.S3.Bucket))
		w(fmt.Sprintf("s3.path=%s\n", db.Spec.Backup.Destination.S3.Path))
		w(fmt.Sprintf("s3.secretRef=%s\n", db.Spec.Backup.Destination.S3.SecretRef))
	}
	w(fmt.Sprintf("backup.logLevel=%s\n", db.Spec.Backup.LogLevel))
	w(fmt.Sprintf("backup.syncInterval=%s\n", db.Spec.Backup.SyncInterval))
	w(fmt.Sprintf("bootstrap.sql=%s\n", db.Spec.Bootstrap.SQL))
	if db.Spec.RunAsUser != nil {
		w(fmt.Sprintf("runAsUser=%d\n", *db.Spec.RunAsUser))
	}
	if db.Spec.RunAsGroup != nil {
		w(fmt.Sprintf("runAsGroup=%d\n", *db.Spec.RunAsGroup))
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// setCondition is a thin wrapper around meta.SetStatusCondition that fills in
// ObservedGeneration and LastTransitionTime on every call.
func setCondition(conditions *[]metav1.Condition, condType string, status metav1.ConditionStatus, reason, message string, gen int64, now metav1.Time) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: gen,
		LastTransitionTime: now,
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *LitestreamReplicaReconciler) SetupWithManager(mgr ctrl.Manager) error {
	podToLitestreamReplica := func(ctx context.Context, obj client.Object) []ctrl.Request {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return nil
		}
		ref := pod.Annotations[configAnnotation]
		if ref == "" {
			return nil
		}
		ns, name := "", ref
		for i, c := range ref {
			if c == '/' {
				ns, name = ref[:i], ref[i+1:]
				break
			}
		}
		if ns == "" {
			ns = pod.Namespace
		}
		return []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}}
	}

	deploymentToLitestreamReplica := func(ctx context.Context, obj client.Object) []ctrl.Request {
		dep, ok := obj.(*appsv1.Deployment)
		if !ok {
			return nil
		}
		var replicas databasev1.LitestreamReplicaList
		if err := r.List(ctx, &replicas, client.InNamespace(dep.Namespace)); err != nil {
			return nil
		}
		var requests []ctrl.Request
		for _, replica := range replicas.Items {
			if replica.Spec.TargetDeployment == dep.Name {
				requests = append(requests, ctrl.Request{
					NamespacedName: types.NamespacedName{Namespace: replica.Namespace, Name: replica.Name},
				})
			}
		}
		return requests
	}

	statefulSetToLitestreamReplica := func(ctx context.Context, obj client.Object) []ctrl.Request {
		ss, ok := obj.(*appsv1.StatefulSet)
		if !ok {
			return nil
		}
		var replicas databasev1.LitestreamReplicaList
		if err := r.List(ctx, &replicas, client.InNamespace(ss.Namespace)); err != nil {
			return nil
		}
		var requests []ctrl.Request
		for _, replica := range replicas.Items {
			if replica.Spec.TargetStatefulSet == ss.Name {
				requests = append(requests, ctrl.Request{
					NamespacedName: types.NamespacedName{Namespace: replica.Namespace, Name: replica.Name},
				})
			}
		}
		return requests
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&databasev1.LitestreamReplica{}).
		Owns(&corev1.ConfigMap{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(podToLitestreamReplica)).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(deploymentToLitestreamReplica)).
		Watches(&appsv1.StatefulSet{}, handler.EnqueueRequestsFromMapFunc(statefulSetToLitestreamReplica)).
		Named("litestreamreplica").
		Complete(r)
}
