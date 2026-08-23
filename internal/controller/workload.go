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

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
)

// workloadTarget holds the resolved target workload (Deployment or StatefulSet)
// for annotation and status operations. Exactly one of the two fields is non-nil.
type workloadTarget struct {
	deployment  *appsv1.Deployment
	statefulSet *appsv1.StatefulSet
}

// name returns the object name of the underlying workload.
func (wt *workloadTarget) name() string {
	if wt.deployment != nil {
		return wt.deployment.Name
	}
	return wt.statefulSet.Name
}

// typeName returns a human-readable workload kind: "Deployment" or "StatefulSet".
func (wt *workloadTarget) typeName() string {
	if wt.deployment != nil {
		return "Deployment"
	}
	return "StatefulSet"
}

// selectorLabels returns the pod selector labels from the workload spec.
func (wt *workloadTarget) selectorLabels() map[string]string {
	if wt.deployment != nil {
		return wt.deployment.Spec.Selector.MatchLabels
	}
	return wt.statefulSet.Spec.Selector.MatchLabels
}

// podTemplateAnnotations returns the annotations map on the pod template.
// The returned map must not be modified directly; use patchWorkloadPodTemplate instead.
func (wt *workloadTarget) podTemplateAnnotations() map[string]string {
	if wt.deployment != nil {
		return wt.deployment.Spec.Template.Annotations
	}
	return wt.statefulSet.Spec.Template.Annotations
}

// podTemplateLabels returns the labels map on the pod template.
// The returned map must not be modified directly; use patchWorkloadPodTemplate instead.
func (wt *workloadTarget) podTemplateLabels() map[string]string {
	if wt.deployment != nil {
		return wt.deployment.Spec.Template.Labels
	}
	return wt.statefulSet.Spec.Template.Labels
}

// desiredReplicas returns the desired (spec) replica count, defaulting to 1 when
// spec.Replicas is nil (matching the Kubernetes default).
func (wt *workloadTarget) desiredReplicas() int32 {
	if wt.deployment != nil {
		if wt.deployment.Spec.Replicas != nil {
			return *wt.deployment.Spec.Replicas
		}
		return 1
	}
	if wt.statefulSet.Spec.Replicas != nil {
		return *wt.statefulSet.Spec.Replicas
	}
	return 1
}

// runningReplicas returns the current (status) replica count — pods that exist,
// regardless of readiness. Use this to wait for pods to terminate.
func (wt *workloadTarget) runningReplicas() int32 {
	if wt.deployment != nil {
		return wt.deployment.Status.Replicas
	}
	return wt.statefulSet.Status.Replicas
}

// readyReplicas returns the number of pods that are Ready.
func (wt *workloadTarget) readyReplicas() int32 {
	if wt.deployment != nil {
		return wt.deployment.Status.ReadyReplicas
	}
	return wt.statefulSet.Status.ReadyReplicas
}

// getTargetWorkload resolves the LitestreamReplica's target workload (Deployment or StatefulSet).
// Returns an error wrapping a not-found error when the workload does not exist.
func (r *LitestreamReplicaReconciler) getTargetWorkload(ctx context.Context, db *databasev1.LitestreamReplica) (*workloadTarget, error) {
	if db.Spec.TargetStatefulSet != "" {
		ss := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{
			Namespace: db.Namespace,
			Name:      db.Spec.TargetStatefulSet,
		}, ss); err != nil {
			return nil, fmt.Errorf("target StatefulSet %q: %w", db.Spec.TargetStatefulSet, err)
		}
		return &workloadTarget{statefulSet: ss}, nil
	}
	dep := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: db.Namespace,
		Name:      db.Spec.TargetDeployment,
	}, dep); err != nil {
		return nil, fmt.Errorf("target Deployment %q: %w", db.Spec.TargetDeployment, err)
	}
	return &workloadTarget{deployment: dep}, nil
}

// patchWorkloadPodTemplate merges addAnnotations and addLabels into the pod template
// of the target workload using a MergePatch.
func (r *LitestreamReplicaReconciler) patchWorkloadPodTemplate(ctx context.Context, wt *workloadTarget, addAnnotations, addLabels map[string]string) error {
	if wt.deployment != nil {
		dep := wt.deployment
		patch := client.MergeFrom(dep.DeepCopy())
		if dep.Spec.Template.Annotations == nil {
			dep.Spec.Template.Annotations = map[string]string{}
		}
		for k, v := range addAnnotations {
			dep.Spec.Template.Annotations[k] = v
		}
		if dep.Spec.Template.Labels == nil {
			dep.Spec.Template.Labels = map[string]string{}
		}
		for k, v := range addLabels {
			dep.Spec.Template.Labels[k] = v
		}
		return r.Patch(ctx, dep, patch)
	}
	ss := wt.statefulSet
	patch := client.MergeFrom(ss.DeepCopy())
	if ss.Spec.Template.Annotations == nil {
		ss.Spec.Template.Annotations = map[string]string{}
	}
	for k, v := range addAnnotations {
		ss.Spec.Template.Annotations[k] = v
	}
	if ss.Spec.Template.Labels == nil {
		ss.Spec.Template.Labels = map[string]string{}
	}
	for k, v := range addLabels {
		ss.Spec.Template.Labels[k] = v
	}
	return r.Patch(ctx, ss, patch)
}

// getTargetWorkloadForRestore resolves the LitestreamReplica's target workload for use by
// the restore controller. The two reconcilers intentionally keep separate helper
// methods so each can evolve its error-handling independently.
func (r *LitestreamRestoreReconciler) getTargetWorkloadForRestore(ctx context.Context, db *databasev1.LitestreamReplica) (*workloadTarget, error) {
	if db.Spec.TargetStatefulSet != "" {
		ss := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{
			Namespace: db.Namespace,
			Name:      db.Spec.TargetStatefulSet,
		}, ss); err != nil {
			return nil, err
		}
		return &workloadTarget{statefulSet: ss}, nil
	}
	dep := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: db.Namespace,
		Name:      db.Spec.TargetDeployment,
	}, dep); err != nil {
		return nil, err
	}
	return &workloadTarget{deployment: dep}, nil
}

// removeWorkloadAnnotations removes operator-owned injection annotations and labels
// from the target workload's pod template during finalization.
func (r *LitestreamReplicaReconciler) removeWorkloadAnnotations(ctx context.Context, wt *workloadTarget) error {
	if wt.deployment != nil {
		dep := wt.deployment
		patch := client.MergeFrom(dep.DeepCopy())
		delete(dep.Spec.Template.Annotations, injectAnnotation)
		delete(dep.Spec.Template.Annotations, configAnnotation)
		delete(dep.Spec.Template.Annotations, skipArchiveAnnotation)
		delete(dep.Spec.Template.Annotations, injectionSpecHashAnnotation)
		delete(dep.Spec.Template.Labels, injectAnnotation)
		return r.Patch(ctx, dep, patch)
	}
	ss := wt.statefulSet
	patch := client.MergeFrom(ss.DeepCopy())
	delete(ss.Spec.Template.Annotations, injectAnnotation)
	delete(ss.Spec.Template.Annotations, configAnnotation)
	delete(ss.Spec.Template.Annotations, skipArchiveAnnotation)
	delete(ss.Spec.Template.Annotations, injectionSpecHashAnnotation)
	delete(ss.Spec.Template.Labels, injectAnnotation)
	return r.Patch(ctx, ss, patch)
}

// scaleWorkload patches the replica count of the target workload.
func (r *LitestreamRestoreReconciler) scaleWorkload(ctx context.Context, wt *workloadTarget, replicas int32) error {
	if wt.deployment != nil {
		dep := wt.deployment
		if dep.Spec.Replicas != nil && *dep.Spec.Replicas == replicas {
			return nil
		}
		patch := client.MergeFrom(dep.DeepCopy())
		dep.Spec.Replicas = &replicas
		return r.Patch(ctx, dep, patch)
	}
	ss := wt.statefulSet
	if ss.Spec.Replicas != nil && *ss.Spec.Replicas == replicas {
		return nil
	}
	patch := client.MergeFrom(ss.DeepCopy())
	ss.Spec.Replicas = &replicas
	return r.Patch(ctx, ss, patch)
}
