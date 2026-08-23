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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// S3Destination defines an S3-compatible backup destination.
type S3Destination struct {
	// Endpoint is the S3-compatible endpoint URL (e.g. "minio.homelab:9000").
	// Leave empty for AWS S3.
	Endpoint string `json:"endpoint,omitempty"`

	// Bucket is the name of the S3 bucket.
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`

	// Path is the key prefix within the bucket (e.g. "paperless/").
	Path string `json:"path,omitempty"`

	// SecretRef names a Secret in the same namespace containing S3 credentials.
	// The Secret must have keys: ACCESS_KEY_ID, SECRET_ACCESS_KEY.
	// +kubebuilder:validation:Required
	SecretRef string `json:"secretRef"`
}

// BackupDestination selects which backup backend to use.
// Exactly one field must be set.
type BackupDestination struct {
	// S3 configures an S3-compatible backup destination.
	S3 *S3Destination `json:"s3,omitempty"`
}

// RetentionPolicy defines how long Litestream retains backup data.
type RetentionPolicy struct {
	// Duration is how long to retain backup data, expressed as a Go duration
	// string (e.g. "720h" for 30 days, "168h" for 7 days).
	// Litestream 0.5.x uses duration-based retention.
	// +kubebuilder:default="720h"
	Duration string `json:"duration,omitempty"`
}

// BackupSpec defines the Litestream backup configuration.
type BackupSpec struct {
	// Enabled controls whether Litestream replication is active.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled"`

	// Destination specifies where backups are stored.
	Destination BackupDestination `json:"destination,omitempty"`

	// Retention controls how long backup data is kept.
	Retention RetentionPolicy `json:"retention,omitempty"`

	// SyncInterval overrides the Litestream sync-interval config key, controlling
	// how frequently WAL changes are synced to the replica. Expressed as a Go
	// duration string (e.g. "1s", "500ms"). When empty, the Litestream default (1s)
	// is used and the key is omitted from the generated config.
	SyncInterval string `json:"syncInterval,omitempty"`

	// LogLevel sets the Litestream logging verbosity via the LITESTREAM_LOG_LEVEL
	// environment variable. Valid values: "debug", "info", "warn", "error".
	// When empty, Litestream's default ("info") is used.
	// +kubebuilder:validation:Enum=debug;info;warn;error;""
	LogLevel string `json:"logLevel,omitempty"`

	// Resources specifies the compute resource requirements for the Litestream
	// sidecar container. When omitted, a default ephemeral-storage limit is applied
	// to guard against silent disk-fill (upstream issue #1310).
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// RecoveryMode controls how the operator handles pod startup when local
// database state is missing or inconsistent with the remote archive.
// +kubebuilder:validation:Enum=Manual;Automatic
type RecoveryMode string

const (
	// RecoveryModeManual blocks workload startup if local state is
	// inconsistent with the remote archive, requiring explicit LitestreamRestore.
	RecoveryModeManual RecoveryMode = "Manual"

	// RecoveryModeAutomatic uses upstream litestream restore flags
	// (-if-db-not-exists, -if-replica-exists) with integrity checking.
	// Any genuine restore failure blocks pod startup.
	RecoveryModeAutomatic RecoveryMode = "Automatic"
)

// RecoverySpec configures how the operator handles database recovery
// on pod startup when local state is missing or inconsistent.
type RecoverySpec struct {
	// Mode controls the recovery strategy.
	// Manual (default): blocks startup if inconsistent, requiring explicit LitestreamRestore.
	// Automatic: runs litestream restore with upstream idempotent flags and integrity check.
	// +kubebuilder:default=Manual
	Mode RecoveryMode `json:"mode,omitempty"`
}

// BootstrapSpec defines SQL to execute when a database is created for the
// first time. The SQL runs ONLY when no local database file exists AND no
// remote archive is available — i.e., genuinely new databases.
type BootstrapSpec struct {
	// SQL contains one or more SQL statements to execute against the database
	// when it is genuinely new. Use IF NOT EXISTS guards for safety.
	SQL string `json:"sql,omitempty"`

	// Image is the container image used for the bootstrap init container.
	// Must include the sqlite3 CLI.
	// +kubebuilder:default="keinos/sqlite3:latest"
	Image string `json:"image,omitempty"`
}

// HealthSpec configures backup health thresholds.
type HealthSpec struct {
	// MaxReplicationLag is the maximum acceptable time since the last successful
	// replication sync. When exceeded, ReplicationHealthy becomes False.
	// Expressed as a Go duration string (e.g. "5m", "1h").
	// When empty, no lag-based health check is performed and health is
	// inferred from sidecar container state only.
	MaxReplicationLag string `json:"maxReplicationLag,omitempty"`
}

// LitestreamReplicaSpec defines the desired state of LitestreamReplica.
type LitestreamReplicaSpec struct {
	// DatabaseName is the filename of the SQLite database (e.g. "paperless.db").
	// +kubebuilder:validation:Required
	DatabaseName string `json:"databaseName"`

	// DatabasePath is the directory path inside the application container where
	// the database file lives (e.g. "/data"). The Litestream sidecar will be
	// configured to watch DatabasePath/DatabaseName.
	// +kubebuilder:validation:Required
	DatabasePath string `json:"databasePath"`

	// TargetDeployment is the name of the existing Deployment in this namespace
	// that the Litestream sidecar should be injected into.
	// Mutually exclusive with TargetStatefulSet.
	TargetDeployment string `json:"targetDeployment,omitempty"`

	// TargetStatefulSet is the name of an existing StatefulSet in this namespace
	// that the Litestream sidecar should be injected into.
	// Mutually exclusive with TargetDeployment.
	TargetStatefulSet string `json:"targetStatefulSet,omitempty"`

	// Container is the name of the application container that mounts the database
	// volume. When empty, the first container in the pod spec is used.
	// Set this when the pod has multiple containers and the database volume is
	// not mounted in the first one.
	// +optional
	Container string `json:"container,omitempty"`

	// Image overrides the Litestream container image used for the sidecar.
	// +kubebuilder:default="litestream/litestream:0.5.14"
	Image string `json:"image,omitempty"`

	// Backup defines the Litestream replication / backup configuration.
	Backup BackupSpec `json:"backup,omitempty"`

	// Recovery configures startup recovery behavior when the local database
	// is missing or inconsistent with the remote archive.
	Recovery RecoverySpec `json:"recovery,omitempty"`

	// Health configures backup health thresholds and SLOs.
	Health HealthSpec `json:"health,omitempty"`

	// Bootstrap defines SQL to execute only when the database is genuinely new
	// (no local DB file AND no remote archive). Applications should own schema
	// migrations after bootstrap.
	Bootstrap BootstrapSpec `json:"bootstrap,omitempty"`

	// RunAsUser sets the UID for Litestream-managed init containers injected by the
	// webhook (archive-check, auto-restore, bootstrap). When set, restored database files
	// are owned by this UID, allowing non-root application containers to read them.
	// When omitted, the container image's default user (root for litestream) is used.
	// +optional
	RunAsUser *int64 `json:"runAsUser,omitempty"`

	// RunAsGroup sets the GID for Litestream-managed init containers injected by the webhook.
	// When omitted, the container image's default group is used.
	// +optional
	RunAsGroup *int64 `json:"runAsGroup,omitempty"`
}

// Annotation keys placed on a Deployment's pod template by the controller.
// Pods inherit these annotations, signalling the mutating webhook to inject
// the Litestream sidecar.
const (
	// AnnotationInject signals that the Litestream sidecar should be injected.
	AnnotationInject = "litestream.io/inject"

	// AnnotationConfig records the "namespace/name" reference to the LitestreamReplica CR
	// that configures the sidecar for a given pod.
	AnnotationConfig = "litestream.io/config"

	// AnnotationPause, when set to "true" on a LitestreamReplica CR, causes the controller
	// to write an empty dbs list to the Litestream ConfigMap, pausing replication
	// without killing the sidecar process. Used by the restore controller during
	// coordinated restores and available for manual operational use.
	AnnotationPause = "litestream.io/pause"

	// AnnotationSkipArchiveCheck, when set to "true" on a LitestreamReplica CR, disables
	// the archive-check init container injected by the webhook. Use when
	// intentionally starting fresh against an existing S3 backup chain.
	AnnotationSkipArchiveCheck = "litestream.io/skip-archive-check"
)

// Condition type constants.
const (
	// ConditionTargetReady indicates the target workload exists and is valid.
	ConditionTargetReady = "TargetReady"

	// ConditionSidecarReady indicates the Litestream sidecar is injected and running.
	ConditionSidecarReady = "SidecarReady"

	// ConditionReplicationHealthy indicates active, healthy replication.
	ConditionReplicationHealthy = "ReplicationHealthy"

	// ConditionRecoverySafe indicates no archive mismatch detected at startup.
	// True = safe, False = mismatch detected (blocks startup).
	ConditionRecoverySafe = "RecoverySafe"

	// ConditionBootstrapApplied indicates bootstrap SQL has been configured and
	// the init container is ready to apply it when the database is genuinely new.
	ConditionBootstrapApplied = "BootstrapApplied"

	// ConditionReplicationPaused indicates that Litestream replication has been
	// intentionally paused via the AnnotationPause annotation.
	ConditionReplicationPaused = "ReplicationPaused"

	// ConditionReplicaCountExceeded indicates that the target workload has more
	// than one replica. Litestream requires exactly one writer to avoid database
	// corruption from concurrent writes.
	ConditionReplicaCountExceeded = "ReplicaCountExceeded"

	// ConditionUnsafeRolloutStrategy indicates the target Deployment uses a rollout
	// strategy that can temporarily run two pods, risking concurrent SQLite writers.
	ConditionUnsafeRolloutStrategy = "UnsafeRolloutStrategy"

	// ConditionReady is the top-level readiness condition.
	ConditionReady = "Ready"
)

// Phase constants for LitestreamReplicaStatus.Phase.
const (
	PhaseConfiguring = "Configuring"
	PhasePending     = "Pending"
	PhaseReady       = "Ready"
	PhasePaused      = "Paused"
	PhaseError       = "Error"
)

// LitestreamReplicaStatus defines the observed state of LitestreamReplica.
type LitestreamReplicaStatus struct {
	// Phase is the high-level lifecycle state: Configuring, Pending, Ready, Error.
	Phase string `json:"phase,omitempty"`

	// Ready mirrors the Ready condition status for quick kubectl output.
	Ready bool `json:"ready,omitempty"`

	// BackupHealthy indicates the last backup/replication check succeeded.
	BackupHealthy bool `json:"backupHealthy,omitempty"`

	// LastSuccessfulReplicationTime is the timestamp of the most recent successful
	// replication sync, as reported by Litestream metrics.
	LastSuccessfulReplicationTime *metav1.Time `json:"lastSuccessfulReplicationTime,omitempty"`

	// ReplicationLag is the duration since the last successful sync (human-readable).
	ReplicationLag string `json:"replicationLag,omitempty"`

	// InjectedSpecHash is the hash of the injection-relevant spec fields currently
	// applied to the target workload's pod template.
	InjectedSpecHash string `json:"injectedSpecHash,omitempty"`

	// ObservedGeneration is the .metadata.generation this status was computed from.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions holds standard Kubernetes condition entries.
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=".spec.targetDeployment"
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=".spec.databaseName"
// +kubebuilder:printcolumn:name="Backup",type=boolean,JSONPath=".spec.backup.enabled"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=".status.ready"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// LitestreamReplica is the Schema for the litestreamreplicas API.
type LitestreamReplica struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LitestreamReplicaSpec   `json:"spec,omitempty"`
	Status LitestreamReplicaStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LitestreamReplicaList contains a list of LitestreamReplica.
type LitestreamReplicaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LitestreamReplica `json:"items"`
}
