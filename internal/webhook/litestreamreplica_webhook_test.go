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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
	"github.com/jlaska/litestream-operator/internal/webhook"
)

// newValidDB returns a minimal valid LitestreamReplica for use in validation tests.
func newValidDB() *databasev1.LitestreamReplica {
	return &databasev1.LitestreamReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "test-db", Namespace: "default"},
		Spec: databasev1.LitestreamReplicaSpec{
			TargetDeployment: "my-app",
			DatabaseName:     "app.db",
			DatabasePath:     "/data",
		},
	}
}

// newValidDBWithStatefulSet returns a minimal valid LitestreamReplica targeting a StatefulSet.
func newValidDBWithStatefulSet() *databasev1.LitestreamReplica {
	return &databasev1.LitestreamReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "test-db-sts", Namespace: "default"},
		Spec: databasev1.LitestreamReplicaSpec{
			TargetStatefulSet: "my-app-sts",
			DatabaseName:      "app.db",
			DatabasePath:      "/data",
		},
	}
}

var _ = Describe("LitestreamReplicaValidator", func() {
	var validator *webhook.LitestreamReplicaValidator

	BeforeEach(func() {
		validator = &webhook.LitestreamReplicaValidator{}
	})

	ctx := context.Background()

	Describe("ValidateDelete", func() {
		It("always permits deletion", func() {
			db := newValidDB()
			warnings, err := validator.ValidateDelete(ctx, db)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})
	})

	Describe("ValidateCreate and ValidateUpdate", func() {
		type testCase struct {
			description    string
			mutate         func(*databasev1.LitestreamReplica)
			expectError    bool
			errContains    string
			expectWarnings bool
		}

		cases := []testCase{
			{
				description: "valid: all required fields set with targetDeployment",
				mutate:      func(_ *databasev1.LitestreamReplica) {},
				expectError: false,
			},
			{
				description: "valid: all required fields set with targetStatefulSet",
				mutate: func(db *databasev1.LitestreamReplica) {
					db.Spec.TargetDeployment = ""
					db.Spec.TargetStatefulSet = "my-statefulset"
				},
				expectError: false,
			},
			{
				description: "invalid: neither targetDeployment nor targetStatefulSet set",
				mutate:      func(db *databasev1.LitestreamReplica) { db.Spec.TargetDeployment = "" },
				expectError: true,
				errContains: "targetDeployment",
			},
			{
				description: "invalid: both targetDeployment and targetStatefulSet set",
				mutate: func(db *databasev1.LitestreamReplica) {
					db.Spec.TargetStatefulSet = "my-statefulset"
				},
				expectError: true,
				errContains: "targetStatefulSet",
			},
			{
				description:    "valid: recovery.mode=Automatic yields a warning but no error",
				mutate:         func(db *databasev1.LitestreamReplica) { db.Spec.Recovery.Mode = databasev1.RecoveryModeAutomatic },
				expectError:    false,
				expectWarnings: true,
			},
			{
				description: "invalid: missing databaseName",
				mutate:      func(db *databasev1.LitestreamReplica) { db.Spec.DatabaseName = "" },
				expectError: true,
				errContains: "databaseName",
			},
			{
				description: "invalid: missing databasePath",
				mutate:      func(db *databasev1.LitestreamReplica) { db.Spec.DatabasePath = "" },
				expectError: true,
				errContains: "databasePath",
			},
			{
				description: "invalid: relative databasePath",
				mutate:      func(db *databasev1.LitestreamReplica) { db.Spec.DatabasePath = "relative/path" },
				expectError: true,
				errContains: "absolute path",
			},
			{
				description: "invalid: backup enabled but S3 not configured",
				mutate: func(db *databasev1.LitestreamReplica) {
					db.Spec.Backup.Enabled = true
					// Destination.S3 is nil by default
				},
				expectError: true,
				errContains: "s3",
			},
			{
				description: "invalid: backup enabled, S3 set, but bucket empty",
				mutate: func(db *databasev1.LitestreamReplica) {
					db.Spec.Backup = databasev1.BackupSpec{
						Enabled: true,
						Destination: databasev1.BackupDestination{
							S3: &databasev1.S3Destination{Bucket: "", SecretRef: "my-secret"},
						},
					}
				},
				expectError: true,
				errContains: "bucket",
			},
			{
				description: "invalid: backup enabled, S3 set, but secretRef empty",
				mutate: func(db *databasev1.LitestreamReplica) {
					db.Spec.Backup = databasev1.BackupSpec{
						Enabled: true,
						Destination: databasev1.BackupDestination{
							S3: &databasev1.S3Destination{Bucket: "my-bucket", SecretRef: ""},
						},
					}
				},
				expectError: true,
				errContains: "secretRef",
			},
			{
				description: "valid: backup enabled with fully-set S3 config",
				mutate: func(db *databasev1.LitestreamReplica) {
					db.Spec.Backup = databasev1.BackupSpec{
						Enabled: true,
						Destination: databasev1.BackupDestination{
							S3: &databasev1.S3Destination{Bucket: "my-bucket", SecretRef: "my-secret"},
						},
					}
				},
				expectError: false,
			},
			{
				description: "valid: backup disabled, S3 not set",
				mutate: func(db *databasev1.LitestreamReplica) {
					db.Spec.Backup = databasev1.BackupSpec{Enabled: false}
				},
				expectError: false,
			},
			{
				description: "invalid: multiple missing fields produces aggregate error",
				mutate: func(db *databasev1.LitestreamReplica) {
					db.Spec.TargetDeployment = ""
					db.Spec.DatabaseName = ""
				},
				expectError: true,
				errContains: "targetDeployment",
			},
		}

		for _, tc := range cases {
			tc := tc // capture loop variable
			It(tc.description, func() {
				db := newValidDB()
				tc.mutate(db)

				// ValidateCreate
				warnings, err := validator.ValidateCreate(ctx, db)
				if tc.expectError {
					Expect(err).To(HaveOccurred(), "ValidateCreate should have returned an error")
					Expect(err.Error()).To(ContainSubstring(tc.errContains),
						"error should mention %q", tc.errContains)
				} else {
					Expect(err).NotTo(HaveOccurred(), "ValidateCreate should not have returned an error")
				}
				if tc.expectWarnings {
					Expect(warnings).NotTo(BeEmpty(), "ValidateCreate should have returned warnings")
				} else {
					Expect(warnings).To(BeEmpty())
				}

				// ValidateUpdate uses the same validation logic — spot-check it produces
				// the same outcome so both wrappers are exercised.
				oldDB := newValidDB() // old state doesn't affect validation
				updateWarnings, updateErr := validator.ValidateUpdate(ctx, oldDB, db)
				if tc.expectError {
					Expect(updateErr).To(HaveOccurred())
				} else {
					Expect(updateErr).NotTo(HaveOccurred())
				}
				if tc.expectWarnings {
					Expect(updateWarnings).NotTo(BeEmpty())
				}
			})
		}
	})

	Describe("replica count validation with live client", func() {
		It("rejects targetDeployment with replicas > 1", func() { //nolint:dupl
			const depName = "too-many-replicas-dep"
			replicas := int32(3)
			dep := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: depName, Namespace: "default"},
				Spec: appsv1.DeploymentSpec{
					Replicas: &replicas,
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": depName},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": depName}},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, dep) }()

			db := newValidDB()
			db.Spec.TargetDeployment = depName
			v := &webhook.LitestreamReplicaValidator{Client: k8sClient}
			_, err := v.ValidateCreate(ctx, db)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("replicas"))
		})

		It("rejects targetStatefulSet with replicas > 1", func() { //nolint:dupl
			const stsName = "too-many-replicas-sts"
			replicas := int32(2)
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: stsName, Namespace: "default"},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &replicas,
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": stsName},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": stsName}},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, sts)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, sts) }()

			db := newValidDBWithStatefulSet()
			db.Spec.TargetStatefulSet = stsName
			v := &webhook.LitestreamReplicaValidator{Client: k8sClient}
			_, err := v.ValidateCreate(ctx, db)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("replicas"))
		})

		It("accepts targetDeployment with replicas == 1 but warns on RollingUpdate strategy", func() {
			const depName = "single-replica-dep"
			replicas := int32(1)
			dep := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: depName, Namespace: "default"},
				Spec: appsv1.DeploymentSpec{
					Replicas: &replicas,
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": depName},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": depName}},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, dep) }()

			db := newValidDB()
			db.Spec.TargetDeployment = depName
			v := &webhook.LitestreamReplicaValidator{Client: k8sClient}
			warnings, err := v.ValidateCreate(ctx, db)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).NotTo(BeEmpty())
			Expect(warnings[0]).To(ContainSubstring("RollingUpdate"))
		})

		It("does not warn on Recreate strategy", func() {
			const depName = "recreate-dep"
			replicas := int32(1)
			dep := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: depName, Namespace: "default"},
				Spec: appsv1.DeploymentSpec{
					Replicas: &replicas,
					Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": depName},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": depName}},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, dep) }()

			db := newValidDB()
			db.Spec.TargetDeployment = depName
			v := &webhook.LitestreamReplicaValidator{Client: k8sClient}
			warnings, err := v.ValidateCreate(ctx, db)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("accepts targetDeployment that does not yet exist (defers check to reconciler)", func() {
			db := newValidDB()
			db.Spec.TargetDeployment = "nonexistent-deployment"
			v := &webhook.LitestreamReplicaValidator{Client: k8sClient}
			_, err := v.ValidateCreate(ctx, db)
			Expect(err).NotTo(HaveOccurred())
		})

		It("accepts targetDeployment with nil spec.Replicas (defaults to 1)", func() { //nolint:dupl
			const depName = "nil-replicas-dep"
			dep := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: depName, Namespace: "default"},
				Spec: appsv1.DeploymentSpec{
					Replicas: nil, // nil → Kubernetes defaults to 1
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": depName},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": depName}},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, dep)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, dep) }()

			db := newValidDB()
			db.Spec.TargetDeployment = depName
			v := &webhook.LitestreamReplicaValidator{Client: k8sClient}
			_, err := v.ValidateCreate(ctx, db)
			Expect(err).NotTo(HaveOccurred())
		})

		It("accepts targetStatefulSet with nil spec.Replicas (defaults to 1)", func() { //nolint:dupl
			const stsName = "nil-replicas-sts"
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: stsName, Namespace: "default"},
				Spec: appsv1.StatefulSetSpec{
					Replicas: nil, // nil → Kubernetes defaults to 1
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": stsName},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": stsName}},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, sts)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, sts) }()

			db := newValidDBWithStatefulSet()
			db.Spec.TargetStatefulSet = stsName
			v := &webhook.LitestreamReplicaValidator{Client: k8sClient}
			_, err := v.ValidateCreate(ctx, db)
			Expect(err).NotTo(HaveOccurred())
		})

		It("accepts targetStatefulSet that does not yet exist (defers check to reconciler)", func() {
			db := newValidDBWithStatefulSet()
			db.Spec.TargetStatefulSet = "nonexistent-sts"
			v := &webhook.LitestreamReplicaValidator{Client: k8sClient}
			_, err := v.ValidateCreate(ctx, db)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
