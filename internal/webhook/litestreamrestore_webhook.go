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

package webhook

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
)

// LitestreamRestoreValidator validates LitestreamRestore CRs.
type LitestreamRestoreValidator struct{}

// SetupWithManager registers the validating webhook with the manager.
func (v *LitestreamRestoreValidator) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &databasev1.LitestreamRestore{}).
		WithValidator(v).
		Complete()
}

// +kubebuilder:webhook:path=/validate-litestream-io-v1-litestreamrestore,mutating=false,failurePolicy=fail,sideEffects=None,groups=litestream.io,resources=litestreamrestores,verbs=create;update,versions=v1,name=vlitestreamrestore.kb.io,admissionReviewVersions=v1

// ValidateCreate validates a new LitestreamRestore resource.
func (v *LitestreamRestoreValidator) ValidateCreate(_ context.Context, restore *databasev1.LitestreamRestore) (admission.Warnings, error) {
	return validateLitestreamRestore(restore)
}

// ValidateUpdate validates an update to a LitestreamRestore resource.
func (v *LitestreamRestoreValidator) ValidateUpdate(_ context.Context, _ *databasev1.LitestreamRestore, newRestore *databasev1.LitestreamRestore) (admission.Warnings, error) {
	return validateLitestreamRestore(newRestore)
}

// ValidateDelete is a no-op — deletion is always permitted.
func (v *LitestreamRestoreValidator) ValidateDelete(_ context.Context, _ *databasev1.LitestreamRestore) (admission.Warnings, error) {
	return nil, nil
}

func validateLitestreamRestore(restore *databasev1.LitestreamRestore) (admission.Warnings, error) {
	var errs field.ErrorList
	spec := field.NewPath("spec")

	if restore.Spec.Mode == databasev1.RestoreModeInPlace && restore.Spec.Target != nil {
		errs = append(errs, field.Forbidden(
			spec.Child("target"),
			"target must not be set when mode is InPlace; the operator derives the target from the source LitestreamReplica"))
	}

	if restore.Spec.Mode == databasev1.RestoreModeToPVC && restore.Spec.Target == nil {
		errs = append(errs, field.Required(
			spec.Child("target"),
			"target is required when mode is ToPVC"))
	}

	if restore.Spec.Target != nil && restore.Spec.Target.Path != "" && !strings.HasPrefix(restore.Spec.Target.Path, "/") {
		errs = append(errs, field.Invalid(
			spec.Child("target", "path"),
			restore.Spec.Target.Path,
			"must be an absolute path (start with /)"))
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("%w", errs.ToAggregate())
	}
	return nil, nil
}
