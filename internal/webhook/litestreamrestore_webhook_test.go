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

package webhook_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
	"github.com/jlaska/litestream-operator/internal/webhook"
)

func newValidRestore(mode databasev1.RestoreMode) *databasev1.LitestreamRestore {
	restore := &databasev1.LitestreamRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: databasev1.LitestreamRestoreSpec{
			SourceRef: databasev1.RestoreSourceRef{Name: "my-replica"},
			Mode:      mode,
		},
	}
	if mode == databasev1.RestoreModeToPVC {
		restore.Spec.Target = &databasev1.RestoreTarget{
			PVC:  "my-pvc",
			Path: "/data/app.db",
		}
	}
	return restore
}

var _ = Describe("LitestreamRestoreValidator", func() {
	var validator *webhook.LitestreamRestoreValidator

	BeforeEach(func() {
		validator = &webhook.LitestreamRestoreValidator{}
	})

	ctx := context.Background()

	Describe("ValidateDelete", func() {
		It("always permits deletion", func() {
			restore := newValidRestore(databasev1.RestoreModeInPlace)
			warnings, err := validator.ValidateDelete(ctx, restore)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})
	})

	Describe("ValidateCreate and ValidateUpdate", func() {
		type testCase struct {
			description string
			mutate      func(*databasev1.LitestreamRestore)
			mode        databasev1.RestoreMode
			expectError bool
			errContains string
		}

		cases := []testCase{
			{
				description: "valid: InPlace restore with no target",
				mode:        databasev1.RestoreModeInPlace,
				mutate:      func(_ *databasev1.LitestreamRestore) {},
				expectError: false,
			},
			{
				description: "valid: ToPVC restore with target",
				mode:        databasev1.RestoreModeToPVC,
				mutate:      func(_ *databasev1.LitestreamRestore) {},
				expectError: false,
			},
			{
				description: "invalid: InPlace restore with target set",
				mode:        databasev1.RestoreModeInPlace,
				mutate: func(r *databasev1.LitestreamRestore) {
					r.Spec.Target = &databasev1.RestoreTarget{PVC: "extra-pvc", Path: "/data/db"}
				},
				expectError: true,
				errContains: "target",
			},
			{
				description: "invalid: ToPVC restore without target",
				mode:        databasev1.RestoreModeToPVC,
				mutate: func(r *databasev1.LitestreamRestore) {
					r.Spec.Target = nil
				},
				expectError: true,
				errContains: "target",
			},
			{
				description: "invalid: ToPVC restore with relative path",
				mode:        databasev1.RestoreModeToPVC,
				mutate: func(r *databasev1.LitestreamRestore) {
					r.Spec.Target = &databasev1.RestoreTarget{PVC: "my-pvc", Path: "relative/path.db"}
				},
				expectError: true,
				errContains: "absolute path",
			},
			{
				description: "valid: ToPVC restore with absolute path",
				mode:        databasev1.RestoreModeToPVC,
				mutate: func(r *databasev1.LitestreamRestore) {
					r.Spec.Target = &databasev1.RestoreTarget{PVC: "my-pvc", Path: "/data/restored.db"}
				},
				expectError: false,
			},
		}

		for _, tc := range cases {
			tc := tc
			It(tc.description, func() {
				restore := newValidRestore(tc.mode)
				tc.mutate(restore)

				warnings, err := validator.ValidateCreate(ctx, restore)
				if tc.expectError {
					Expect(err).To(HaveOccurred(), "ValidateCreate should have returned an error")
					Expect(err.Error()).To(ContainSubstring(tc.errContains),
						"error should mention %q", tc.errContains)
				} else {
					Expect(err).NotTo(HaveOccurred(), "ValidateCreate should not have returned an error")
				}
				Expect(warnings).To(BeEmpty())

				oldRestore := newValidRestore(tc.mode)
				_, updateErr := validator.ValidateUpdate(ctx, oldRestore, restore)
				if tc.expectError {
					Expect(updateErr).To(HaveOccurred())
				} else {
					Expect(updateErr).NotTo(HaveOccurred())
				}
			})
		}
	})
})
