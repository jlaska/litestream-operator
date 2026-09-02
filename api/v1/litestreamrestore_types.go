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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RestoreMode determines how a restore is executed.
// +kubebuilder:validation:Enum=InPlace;ToPVC
type RestoreMode string

const (
	// RestoreModeInPlace derives workload, PVC, and database path from the
	// source LitestreamReplica. Fences the application, restores, resumes.
	RestoreModeInPlace RestoreMode = "InPlace"

	// RestoreModeToPVC restores to a separate PVC without touching the
	// source application workload.
	RestoreModeToPVC RestoreMode = "ToPVC"
)

// RestoreSourceRef identifies the LitestreamReplica that provides the
// backup configuration (S3 destination + credentials).
type RestoreSourceRef struct {
	// Name is the name of the LitestreamReplica CR in the same namespace.
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// RestoreTarget specifies the PVC and path for ToPVC restores.
type RestoreTarget struct {
	// PVC is the name of the PersistentVolumeClaim to restore into.
	// +kubebuilder:validation:Required
	PVC string `json:"pvc"`

	// Path is the full path (including filename) where the restored
	// database file will be written inside the PVC.
	// +kubebuilder:validation:Required
	Path string `json:"path"`
}

// RestorePhase constants for LitestreamRestoreStatus.Phase.
const (
	RestorePhasePending       = "Pending"
	RestorePhaseAcquiringLock = "AcquiringLock" // acquiring restore lock (placeholder for Phase 3)
	RestorePhaseFencing       = "Fencing"       // pausing replication + scaling workload to 0
	RestorePhaseRestoring     = "Restoring"     // restore Job running
	RestorePhaseValidating    = "Validating"    // PRAGMA quick_check integrity check
	RestorePhaseResuming      = "Resuming"      // scaling workload back up
	RestorePhaseCompleted     = "Completed"
	RestorePhaseFailed        = "Failed"
)

// Restore condition type constants.
const (
	// RestoreConditionLocked indicates the restore has acquired its lock.
	RestoreConditionLocked = "Locked"

	// RestoreConditionApplicationFenced indicates the source application is fenced.
	RestoreConditionApplicationFenced = "ApplicationFenced"

	// RestoreConditionRestoreSucceeded indicates the restore Job completed successfully.
	RestoreConditionRestoreSucceeded = "RestoreSucceeded"

	// RestoreConditionApplicationResumed indicates the source application has been resumed.
	RestoreConditionApplicationResumed = "ApplicationResumed"
)

// LitestreamRestoreSpec defines the desired state of a LitestreamRestore operation.
type LitestreamRestoreSpec struct {
	// SourceRef identifies the LitestreamReplica whose backup configuration
	// (S3 destination + credentials) should be used as the restore source.
	// The LitestreamReplica must be in the same namespace.
	// +kubebuilder:validation:Required
	SourceRef RestoreSourceRef `json:"sourceRef"`

	// Mode selects the restore strategy.
	// InPlace: derives target from source LitestreamReplica, fences app, restores, resumes.
	// ToPVC: restores to a separate PVC without touching the source application.
	// +kubebuilder:default=InPlace
	Mode RestoreMode `json:"mode,omitempty"`

	// Target is required when mode is ToPVC. Specifies the PVC and path
	// for the restored database.
	// +optional
	Target *RestoreTarget `json:"target,omitempty"`

	// Timestamp enables point-in-time recovery. When set, Litestream restores
	// the database to the state it was in at this instant.
	// Format: RFC 3339, e.g. "2026-06-17T10:00:00Z".
	// When omitted the most recent snapshot is restored.
	Timestamp string `json:"timestamp,omitempty"`

	// Image overrides the Litestream container image used for the restore Job.
	// Defaults to the image specified in the referenced LitestreamReplica, or
	// litestream/litestream:0.5.14 if neither is set.
	Image string `json:"image,omitempty"`

	// Force causes the restore Job to pass -force to litestream, overwriting an
	// existing database file at TargetPath. By default, litestream refuses to
	// overwrite a non-empty file. Set this to true only when you intentionally
	// want to replace an existing database (e.g. recovering from a diverged DB).
	// +optional
	Force bool `json:"force,omitempty"`

	// RunAsUser sets the UID for both the restore Job and the validation Job pods.
	// +optional
	RunAsUser *int64 `json:"runAsUser,omitempty"`

	// RunAsGroup sets the GID for both the restore Job and the validation Job pods.
	// +optional
	RunAsGroup *int64 `json:"runAsGroup,omitempty"`
}

// LitestreamRestoreStatus defines the observed state of a LitestreamRestore operation.
type LitestreamRestoreStatus struct {
	// Phase is the current lifecycle state.
	Phase string `json:"phase,omitempty"`

	// JobName is the name of the Kubernetes Job created to perform the restore.
	JobName string `json:"jobName,omitempty"`

	// OriginalReplicas records the target Deployment's replica count before
	// scale-down so it can be restored after the restore Job completes.
	OriginalReplicas *int32 `json:"originalReplicas,omitempty"`

	// StartTime is when the restore Job was created.
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the restore Job finished (successfully or not).
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Message contains a human-readable description of the current phase,
	// including any error details on failure.
	Message string `json:"message,omitempty"`

	// ResolvedPVC is the PVC name used for the restore, resolved at reconcile time.
	ResolvedPVC string `json:"resolvedPVC,omitempty"`

	// ResolvedPath is the database path used for the restore, resolved at reconcile time.
	ResolvedPath string `json:"resolvedPath,omitempty"`

	// ResolvedSubPath is the volume mount subPath for the target PVC, if any.
	ResolvedSubPath string `json:"resolvedSubPath,omitempty"`

	// Conditions holds standard Kubernetes condition entries.
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=".spec.sourceRef.name"
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=".spec.mode"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// LitestreamRestore triggers a one-shot restore of a SQLite database from a
// Litestream backup stored in S3-compatible object storage.
type LitestreamRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LitestreamRestoreSpec   `json:"spec,omitempty"`
	Status LitestreamRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LitestreamRestoreList contains a list of LitestreamRestore.
type LitestreamRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LitestreamRestore `json:"items"`
}
