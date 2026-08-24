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
	"encoding/json"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	jsonpatch "gomodules.xyz/jsonpatch/v2"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
	"github.com/jlaska/litestream-operator/internal/webhook"
)

var _ = Describe("SidecarInjector", func() {
	const (
		namespace             = "default"
		litestreamReplicaName = "test-db"
		deploymentName        = "test-app"
		databaseName          = "myapp.db"
		databasePath          = "/data"
		volumeName            = "app-data"
		appContainerName      = "app"        // goconst
		appContainerImage     = "busybox"    // goconst
		litestreamName        = "litestream" // goconst
		injectTrue            = "true"       // goconst
	)

	ctx := context.Background()

	// Helper: build a LitestreamReplica CR.
	newLitestreamReplica := func(backupEnabled bool) *databasev1.LitestreamReplica {
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{
				Name:      litestreamReplicaName,
				Namespace: namespace,
			},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     databaseName,
				DatabasePath:     databasePath,
				TargetDeployment: deploymentName,
			},
		}
		if backupEnabled {
			db.Spec.Backup = databasev1.BackupSpec{
				Enabled: true,
				Destination: databasev1.BackupDestination{
					S3: &databasev1.S3Destination{
						Bucket:    "my-bucket",
						SecretRef: "s3-creds",
					},
				},
			}
		}
		return db
	}

	// Helper: build a pod with the inject annotation and a volume at databasePath.
	newAnnotatedPod := func(configRef string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: namespace,
				Annotations: map[string]string{
					databasev1.AnnotationInject: injectTrue,
					databasev1.AnnotationConfig: configRef,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  appContainerName,
						Image: appContainerImage,
						VolumeMounts: []corev1.VolumeMount{
							{Name: volumeName, MountPath: databasePath},
						},
					},
				},
				Volumes: []corev1.Volume{
					{Name: volumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				},
			},
		}
	}

	// Helper: build an admission.Request for a pod.
	makeRequest := func(pod *corev1.Pod) admission.Request {
		raw, err := json.Marshal(pod)
		Expect(err).NotTo(HaveOccurred())
		return admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				UID:       types.UID("test-uid"),
				Namespace: namespace,
				Operation: admissionv1.Create,
				Object:    runtime.RawExtension{Raw: raw},
			},
		}
	}

	// Helper: build an injector backed by the envtest client.
	newInjector := func() *webhook.SidecarInjector {
		return &webhook.SidecarInjector{
			Client:  k8sClient,
			Decoder: admission.NewDecoder(k8sClient.Scheme()),
		}
	}

	BeforeEach(func() {
		db := newLitestreamReplica(false)
		Expect(k8sClient.Create(ctx, db)).To(Succeed())
	})

	AfterEach(func() {
		db := &databasev1.LitestreamReplica{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: litestreamReplicaName, Namespace: namespace}, db)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Delete(ctx, db)).To(Succeed())
	})

	It("allows pods without the injection annotation", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "plain-pod", Namespace: namespace},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: appContainerImage}},
			},
		}
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())
		Expect(resp.Patches).To(BeEmpty())
	})

	It("allows and no-ops when inject annotation is set but config annotation is absent", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "no-config-pod", Namespace: namespace,
				Annotations: map[string]string{
					databasev1.AnnotationInject: injectTrue,
					// No AnnotationConfig — resolveLitestreamReplica returns nil.
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: appContainerImage}},
			},
		}
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())
		Expect(resp.Patches).To(BeEmpty())
	})

	It("injects the Litestream sidecar into annotated pods", func() {
		pod := newAnnotatedPod(namespace + "/" + litestreamReplicaName)
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		// Apply the JSON patch and inspect the resulting pod.
		patched := applyPatches(pod, resp.Patches)
		containerNames := make([]string, len(patched.Spec.Containers))
		for i, c := range patched.Spec.Containers {
			containerNames[i] = c.Name
		}
		Expect(containerNames).To(ContainElement(litestreamName))

		// The sidecar must mount the same data volume.
		var sidecar corev1.Container
		for _, c := range patched.Spec.Containers {
			if c.Name == litestreamName {
				sidecar = c
				break
			}
		}
		mountPaths := make([]string, len(sidecar.VolumeMounts))
		for i, vm := range sidecar.VolumeMounts {
			mountPaths[i] = vm.MountPath
		}
		Expect(mountPaths).To(ContainElement(databasePath))
		Expect(mountPaths).To(ContainElement("/etc/litestream"))
	})

	It("is idempotent — does not inject twice", func() {
		pod := newAnnotatedPod(namespace + "/" + litestreamReplicaName)
		injector := newInjector()

		first := injector.Handle(ctx, makeRequest(pod))
		Expect(first.Allowed).To(BeTrue())

		// Simulate the pod already having the sidecar injected.
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
			Name: litestreamName, Image: "litestream/litestream:0.5.14",
		})
		second := injector.Handle(ctx, makeRequest(pod))
		Expect(second.Allowed).To(BeTrue())
		Expect(second.Patches).To(BeEmpty())
	})

	It("injects S3 credential env vars when backup is enabled", func() {
		// Replace the CR with one that has backup enabled.
		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: litestreamReplicaName, Namespace: namespace}, db)).To(Succeed())
		Expect(k8sClient.Delete(ctx, db)).To(Succeed())

		Expect(k8sClient.Create(ctx, newLitestreamReplica(true))).To(Succeed())

		pod := newAnnotatedPod(namespace + "/" + litestreamReplicaName)
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyPatches(pod, resp.Patches)
		var sidecar corev1.Container
		for _, c := range patched.Spec.Containers {
			if c.Name == litestreamName {
				sidecar = c
				break
			}
		}
		envNames := make([]string, len(sidecar.Env))
		for i, e := range sidecar.Env {
			envNames[i] = e.Name
		}
		Expect(envNames).To(ContainElements("LITESTREAM_ACCESS_KEY_ID", "LITESTREAM_SECRET_ACCESS_KEY"))
	})

	It("injects metrics port 9090 on the sidecar container", func() {
		pod := newAnnotatedPod(namespace + "/" + litestreamReplicaName)
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyPatches(pod, resp.Patches)
		var sidecar corev1.Container
		for _, c := range patched.Spec.Containers {
			if c.Name == litestreamName {
				sidecar = c
				break
			}
		}
		Expect(sidecar.Ports).To(HaveLen(1))
		Expect(sidecar.Ports[0].Name).To(Equal("metrics"))
		Expect(sidecar.Ports[0].ContainerPort).To(BeNumerically("==", 9090))
	})

	It("adds Prometheus scrape annotations to the pod", func() {
		pod := newAnnotatedPod(namespace + "/" + litestreamReplicaName)
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyPatches(pod, resp.Patches)
		Expect(patched.Annotations).To(HaveKeyWithValue("prometheus.io/scrape", "true"))
		Expect(patched.Annotations).To(HaveKeyWithValue("prometheus.io/port", "9090"))
		Expect(patched.Annotations).To(HaveKeyWithValue("prometheus.io/path", "/metrics"))
	})

	It("preserves existing Prometheus annotations instead of overwriting them", func() {
		pod := newAnnotatedPod(namespace + "/" + litestreamReplicaName)
		pod.Annotations["prometheus.io/port"] = "8080"
		pod.Annotations["prometheus.io/path"] = "/app-metrics"

		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(pod, resp.Patches)
		Expect(patched.Annotations).To(HaveKeyWithValue("prometheus.io/scrape", "true"))
		Expect(patched.Annotations).To(HaveKeyWithValue("prometheus.io/port", "8080"))
		Expect(patched.Annotations).To(HaveKeyWithValue("prometheus.io/path", "/app-metrics"))
	})

	It("injects default ephemeral-storage limit on sidecar when no resources specified", func() {
		pod := newAnnotatedPod(namespace + "/" + litestreamReplicaName)
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyPatches(pod, resp.Patches)
		var sidecar corev1.Container
		for _, c := range patched.Spec.Containers {
			if c.Name == litestreamName {
				sidecar = c
				break
			}
		}
		Expect(sidecar.Resources.Limits).To(HaveKey(corev1.ResourceEphemeralStorage))
	})

	It("injects LITESTREAM_LOG_LEVEL env var when logLevel is set", func() {
		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: litestreamReplicaName, Namespace: namespace}, db)).To(Succeed())
		Expect(k8sClient.Delete(ctx, db)).To(Succeed())

		dbWithLogLevel := newLitestreamReplica(false)
		dbWithLogLevel.Spec.Backup.LogLevel = "debug"
		Expect(k8sClient.Create(ctx, dbWithLogLevel)).To(Succeed())

		pod := newAnnotatedPod(namespace + "/" + litestreamReplicaName)
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyPatches(pod, resp.Patches)
		var sidecar corev1.Container
		for _, c := range patched.Spec.Containers {
			if c.Name == litestreamName {
				sidecar = c
				break
			}
		}
		var found bool
		for _, e := range sidecar.Env {
			if e.Name == "LITESTREAM_LOG_LEVEL" && e.Value == "debug" {
				found = true
				break
			}
		}
		Expect(found).To(BeTrue(), "expected LITESTREAM_LOG_LEVEL=debug env var")
	})

	It("uses custom resource requirements when spec.backup.resources is set", func() {
		db := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: litestreamReplicaName, Namespace: namespace}, db)).To(Succeed())
		Expect(k8sClient.Delete(ctx, db)).To(Succeed())

		dbWithResources := newLitestreamReplica(false)
		cpuLimit := "50m"
		dbWithResources.Spec.Backup.Resources = &corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse(cpuLimit),
			},
		}
		Expect(k8sClient.Create(ctx, dbWithResources)).To(Succeed())

		pod := newAnnotatedPod(namespace + "/" + litestreamReplicaName)
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyPatches(pod, resp.Patches)
		var sidecar corev1.Container
		for _, c := range patched.Spec.Containers {
			if c.Name == litestreamName {
				sidecar = c
				break
			}
		}
		// Should use the custom CPU limit, not the default ephemeral-storage limit.
		Expect(sidecar.Resources.Limits).To(HaveKey(corev1.ResourceCPU))
		Expect(sidecar.Resources.Limits).NotTo(HaveKey(corev1.ResourceEphemeralStorage))
	})
})

var _ = Describe("SidecarInjector init container", func() {
	const (
		namespace             = "default"
		litestreamReplicaName = "init-test-db"
		deployName            = "init-test-app"
		databaseName          = "app.db"
		databasePath          = "/data"
		volumeName            = "app-data"
		initSQL               = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);"
		injectTrue            = "true"    // goconst: mirrors constant in first Describe
		appContainerName      = "app"     // goconst: mirrors constant in first Describe
		appContainerImage     = "busybox" // goconst: mirrors constant in first Describe
	)

	ctx := context.Background()

	newInjector := func() *webhook.SidecarInjector {
		return &webhook.SidecarInjector{
			Client:  k8sClient,
			Decoder: admission.NewDecoder(k8sClient.Scheme()),
		}
	}

	newPod := func(annotations map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod", Namespace: namespace,
				Annotations: annotations,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "app", Image: appContainerImage,
					VolumeMounts: []corev1.VolumeMount{{Name: volumeName, MountPath: databasePath}},
				}},
				Volumes: []corev1.Volume{{
					Name:         volumeName,
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}},
			},
		}
	}

	makePodRequest := func(pod *corev1.Pod) admission.Request {
		raw, err := json.Marshal(pod)
		Expect(err).NotTo(HaveOccurred())
		return admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				UID: "test-uid", Namespace: namespace,
				Operation: admissionv1.Create,
				Object:    runtime.RawExtension{Raw: raw},
			},
		}
	}

	BeforeEach(func() {
		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: litestreamReplicaName, Namespace: namespace}, db); err != nil {
			db = &databasev1.LitestreamReplica{
				ObjectMeta: metav1.ObjectMeta{Name: litestreamReplicaName, Namespace: namespace},
				Spec: databasev1.LitestreamReplicaSpec{
					DatabaseName:     databaseName,
					DatabasePath:     databasePath,
					TargetDeployment: deployName,
					Bootstrap:        databasev1.BootstrapSpec{SQL: initSQL},
				},
			}
			Expect(k8sClient.Create(ctx, db)).To(Succeed())
		}
	})

	AfterEach(func() {
		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: litestreamReplicaName, Namespace: namespace}, db); err == nil {
			Expect(k8sClient.Delete(ctx, db)).To(Succeed())
		}
	})

	It("injects a bootstrap init container when Bootstrap.SQL is set", func() {
		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + litestreamReplicaName,
		}
		pod := newPod(annotations)
		resp := newInjector().Handle(ctx, makePodRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyInitPatches(pod, resp.Patches)
		initNames := make([]string, len(patched.Spec.InitContainers))
		for i, c := range patched.Spec.InitContainers {
			initNames[i] = c.Name
		}
		Expect(initNames).To(ContainElement("db-bootstrap"))
	})

	It("bootstrap container script references the correct database path", func() {
		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + litestreamReplicaName,
		}
		pod := newPod(annotations)
		resp := newInjector().Handle(ctx, makePodRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyInitPatches(pod, resp.Patches)
		var initContainer corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "db-bootstrap" {
				initContainer = c
				break
			}
		}
		Expect(initContainer.Command).To(ContainElement(ContainSubstring(databasePath + "/" + databaseName)))
	})

	It("does not inject a bootstrap container when Bootstrap.SQL is empty", func() {
		// Create a second DB with no bootstrap SQL.
		noInitDB := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: "no-init-db", Namespace: namespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     databaseName,
				DatabasePath:     databasePath,
				TargetDeployment: deployName,
			},
		}
		Expect(k8sClient.Create(ctx, noInitDB)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, noInitDB) }()

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/no-init-db",
		}
		pod := newPod(annotations)
		resp := newInjector().Handle(ctx, makePodRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyInitPatches(pod, resp.Patches)
		Expect(patched.Spec.InitContainers).To(BeEmpty())
	})

	It("uses custom Bootstrap.Image when spec.bootstrap.image is set", func() {
		const customBootstrapImage = "my-org/sqlite3-custom:v1.2"

		// Replace the default DB with one that has a custom bootstrap image.
		existing := &databasev1.LitestreamReplica{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: litestreamReplicaName, Namespace: namespace}, existing)).To(Succeed())
		Expect(k8sClient.Delete(ctx, existing)).To(Succeed())

		customDB := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: litestreamReplicaName, Namespace: namespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     databaseName,
				DatabasePath:     databasePath,
				TargetDeployment: deployName,
				Bootstrap:        databasev1.BootstrapSpec{SQL: initSQL, Image: customBootstrapImage},
			},
		}
		Expect(k8sClient.Create(ctx, customDB)).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + litestreamReplicaName,
		}
		pod := newPod(annotations)
		resp := newInjector().Handle(ctx, makePodRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyInitPatches(pod, resp.Patches)
		var initContainer corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "db-bootstrap" {
				initContainer = c
				break
			}
		}
		Expect(initContainer.Image).To(Equal(customBootstrapImage))
	})
})

var _ = Describe("SidecarInjector archive check", func() {
	const (
		namespace         = "default"
		acDBName          = "archive-check-db"
		acDeployName      = "archive-check-app"
		acDatabaseName    = "app.db"
		acDatabasePath    = "/data"
		acVolumeName      = "app-data"
		acSecretRef       = "s3-creds"
		injectTrue        = "true"    // goconst
		appContainerName  = "app"     // goconst
		appContainerImage = "busybox" // goconst
	)

	ctx := context.Background()

	newInjector := func() *webhook.SidecarInjector {
		return &webhook.SidecarInjector{
			Client:  k8sClient,
			Decoder: admission.NewDecoder(k8sClient.Scheme()),
		}
	}

	newPod := func(annotations map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod", Namespace: namespace,
				Annotations: annotations,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: appContainerName, Image: appContainerImage,
					VolumeMounts: []corev1.VolumeMount{{Name: acVolumeName, MountPath: acDatabasePath}},
				}},
				Volumes: []corev1.Volume{{
					Name:         acVolumeName,
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}},
			},
		}
	}

	makeRequest := func(pod *corev1.Pod) admission.Request {
		raw, err := json.Marshal(pod)
		Expect(err).NotTo(HaveOccurred())
		return admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				UID: "test-uid", Namespace: namespace,
				Operation: admissionv1.Create,
				Object:    runtime.RawExtension{Raw: raw},
			},
		}
	}

	newBackupDB := func(annotations map[string]string) *databasev1.LitestreamReplica {
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: acDBName, Namespace: namespace, Annotations: annotations},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     acDatabaseName,
				DatabasePath:     acDatabasePath,
				TargetDeployment: acDeployName,
				Backup: databasev1.BackupSpec{
					Enabled: true,
					Destination: databasev1.BackupDestination{
						S3: &databasev1.S3Destination{Bucket: "test-bucket", SecretRef: acSecretRef},
					},
				},
			},
		}
		return db
	}

	AfterEach(func() {
		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: acDBName, Namespace: namespace}, db); err == nil {
			_ = k8sClient.Delete(ctx, db)
		}
		restoreList := &databasev1.LitestreamRestoreList{}
		if err := k8sClient.List(ctx, restoreList); err == nil {
			for i := range restoreList.Items {
				_ = k8sClient.Delete(ctx, &restoreList.Items[i])
			}
		}
	})

	It("injects archive-check init container when backup is enabled", func() {
		Expect(k8sClient.Create(ctx, newBackupDB(nil))).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		initNames := make([]string, len(patched.Spec.InitContainers))
		for i, c := range patched.Spec.InitContainers {
			initNames[i] = c.Name
		}
		Expect(initNames).To(ContainElement("litestream-archive-check"))
	})

	It("archive-check init container is first (before db-bootstrap)", func() {
		db := newBackupDB(nil)
		db.Spec.Bootstrap.SQL = "CREATE TABLE t (id INTEGER PRIMARY KEY);"
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		Expect(patched.Spec.InitContainers).To(HaveLen(2))
		Expect(patched.Spec.InitContainers[0].Name).To(Equal("litestream-archive-check"))
		Expect(patched.Spec.InitContainers[1].Name).To(Equal("db-bootstrap"))
	})

	It("archive-check init container has correct volume mounts", func() {
		Expect(k8sClient.Create(ctx, newBackupDB(nil))).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var archiveCheck corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-archive-check" {
				archiveCheck = c
				break
			}
		}

		mountPaths := make([]string, len(archiveCheck.VolumeMounts))
		for i, vm := range archiveCheck.VolumeMounts {
			mountPaths[i] = vm.MountPath
		}
		Expect(mountPaths).To(ContainElement(acDatabasePath))
		Expect(mountPaths).To(ContainElement("/etc/litestream"))
	})

	It("archive-check init container has S3 credential env vars", func() {
		Expect(k8sClient.Create(ctx, newBackupDB(nil))).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var archiveCheck corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-archive-check" {
				archiveCheck = c
				break
			}
		}

		envNames := make([]string, len(archiveCheck.Env))
		for i, e := range archiveCheck.Env {
			envNames[i] = e.Name
		}
		Expect(envNames).To(ContainElements("LITESTREAM_ACCESS_KEY_ID", "LITESTREAM_SECRET_ACCESS_KEY"))

		for _, e := range archiveCheck.Env {
			if e.Name == "LITESTREAM_ACCESS_KEY_ID" {
				Expect(e.ValueFrom.SecretKeyRef.Name).To(Equal(acSecretRef))
			}
		}
	})

	It("archive-check script references the correct database path", func() {
		Expect(k8sClient.Create(ctx, newBackupDB(nil))).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var archiveCheck corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-archive-check" {
				archiveCheck = c
				break
			}
		}
		script := strings.Join(archiveCheck.Command, " ")
		Expect(script).To(ContainSubstring(acDatabasePath + "/" + acDatabaseName))
		// -if-replica-exists makes litestream exit 0 for "no backups found" while still
		// exiting 1 for real errors (broken chain, credentials, network, corruption).
		Expect(script).To(ContainSubstring("-if-replica-exists"),
			"archive-check must use -if-replica-exists to distinguish empty bucket from errors")
		// Probe file existence distinguishes "restored" (file present) from "no data" (absent).
		Expect(script).To(ContainSubstring(`-f "${PROBE}"`),
			"archive-check must check probe file existence after a successful restore")
		// Litestream errors must not be suppressed — surfacing them is essential for
		// diagnosing why archive-check blocks startup.
		Expect(script).NotTo(ContainSubstring("2>/dev/null"),
			"archive-check must not suppress litestream stderr")
		Expect(script).To(ContainSubstring("2>&1"),
			"archive-check must capture litestream output for logging")
	})

	It("archive-check script blocks startup on non-zero litestream exit", func() {
		Expect(k8sClient.Create(ctx, newBackupDB(nil))).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var archiveCheck corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-archive-check" {
				archiveCheck = c
				break
			}
		}
		script := strings.Join(archiveCheck.Command, " ")
		// Non-zero exit from litestream restore must block startup and surface the error output.
		Expect(script).To(ContainSubstring("RESTORE_EXIT} -ne 0"),
			"archive-check must exit 1 on any non-zero litestream restore exit")
		Expect(script).To(ContainSubstring("${RESTORE_OUTPUT}"),
			"archive-check must surface the litestream error output on failure")
	})

	It("does not inject archive-check when backup is disabled", func() {
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: acDBName, Namespace: namespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     acDatabaseName,
				DatabasePath:     acDatabasePath,
				TargetDeployment: acDeployName,
				Backup:           databasev1.BackupSpec{Enabled: false},
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		for _, c := range patched.Spec.InitContainers {
			Expect(c.Name).NotTo(Equal("litestream-archive-check"))
		}
	})

	It("does not inject archive-check when skip-archive-check annotation is set on LitestreamReplica", func() {
		annotations := map[string]string{
			databasev1.AnnotationSkipArchiveCheck: "true",
		}
		Expect(k8sClient.Create(ctx, newBackupDB(annotations))).To(Succeed())

		podAnnotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(podAnnotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(podAnnotations), resp.Patches)
		for _, c := range patched.Spec.InitContainers {
			Expect(c.Name).NotTo(Equal("litestream-archive-check"))
		}
	})

	It("skips archive-check via hasActiveRestore when a completed restore exists", func() {
		db := newBackupDB(nil)
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		restore := &databasev1.LitestreamRestore{
			ObjectMeta: metav1.ObjectMeta{Name: "ac-restore", Namespace: namespace},
			Spec: databasev1.LitestreamRestoreSpec{
				SourceRef: databasev1.RestoreSourceRef{Name: acDBName},
			},
		}
		Expect(k8sClient.Create(ctx, restore)).To(Succeed())
		restore.Status.Phase = databasev1.RestorePhaseCompleted
		Expect(k8sClient.Status().Update(ctx, restore)).To(Succeed())

		podAnnotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(podAnnotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(podAnnotations), resp.Patches)
		for _, c := range patched.Spec.InitContainers {
			Expect(c.Name).NotTo(Equal("litestream-archive-check"))
		}
	})

	It("skips archive-check via hasActiveRestore when a resuming restore exists", func() {
		db := newBackupDB(nil)
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		restore := &databasev1.LitestreamRestore{
			ObjectMeta: metav1.ObjectMeta{Name: "ac-restore-resuming", Namespace: namespace},
			Spec: databasev1.LitestreamRestoreSpec{
				SourceRef: databasev1.RestoreSourceRef{Name: acDBName},
			},
		}
		Expect(k8sClient.Create(ctx, restore)).To(Succeed())
		restore.Status.Phase = databasev1.RestorePhaseResuming
		Expect(k8sClient.Status().Update(ctx, restore)).To(Succeed())

		podAnnotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(podAnnotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(podAnnotations), resp.Patches)
		for _, c := range patched.Spec.InitContainers {
			Expect(c.Name).NotTo(Equal("litestream-archive-check"))
		}
	})

	It("archive-check init container uses the same image as the Litestream sidecar", func() {
		const customImage = "litestream/litestream:custom-tag"
		db := newBackupDB(nil)
		db.Spec.Image = customImage
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var archiveCheck corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-archive-check" {
				archiveCheck = c
				break
			}
		}
		var sidecar corev1.Container
		for _, c := range patched.Spec.Containers {
			if c.Name == "litestream" {
				sidecar = c
				break
			}
		}
		Expect(archiveCheck.Image).To(Equal(sidecar.Image))
		Expect(archiveCheck.Image).To(Equal(customImage))
	})

	It("does not inject any startup init container when backup enabled, recovery.mode=Manual, skip-archive-check=true", func() {
		annotations := map[string]string{
			databasev1.AnnotationSkipArchiveCheck: "true",
		}
		db := newBackupDB(annotations)
		db.Spec.Recovery.Mode = databasev1.RecoveryModeManual
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		podAnnotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(podAnnotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(podAnnotations), resp.Patches)
		for _, c := range patched.Spec.InitContainers {
			Expect(c.Name).NotTo(Equal("litestream-archive-check"))
			Expect(c.Name).NotTo(Equal("litestream-restore"))
		}
	})

	It("archive-check script exits 0 only when both DB file and state dir exist", func() {
		Expect(k8sClient.Create(ctx, newBackupDB(nil))).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var archiveCheck corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-archive-check" {
				archiveCheck = c
				break
			}
		}
		script := strings.Join(archiveCheck.Command, " ")
		// The script must check for the state directory alongside the DB file.
		// Both must exist for an early exit — a DB without the state dir falls through
		// to the S3 probe to catch fresh/recreated databases (issue #109).
		Expect(script).To(ContainSubstring(`-d "${STATE_DIR}"`),
			"archive-check must test for state directory existence")
		Expect(script).To(ContainSubstring("STATE_DIR="),
			"archive-check must define STATE_DIR variable")
		// Early exit only when both DB and state dir exist.
		Expect(script).To(ContainSubstring("skipping check"),
			"archive-check must exit 0 when both DB and state dir are present")
	})

	It("archive-check script uses the correct state directory path convention", func() {
		Expect(k8sClient.Create(ctx, newBackupDB(nil))).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var archiveCheck corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-archive-check" {
				archiveCheck = c
				break
			}
		}
		script := strings.Join(archiveCheck.Command, " ")
		// Litestream state dir is .<dbfilename>-litestream (MetaDirSuffix = "-litestream").
		// Documented at https://litestream.io/tips/#deleting-sqlite-databases
		expectedStateDir := acDatabasePath + "/." + acDatabaseName + "-litestream"
		Expect(script).To(ContainSubstring(expectedStateDir),
			"archive-check state directory must use .<dbname>-litestream naming convention")
	})

	It("archive-check script probes S3 when DB exists but state dir is absent", func() {
		Expect(k8sClient.Create(ctx, newBackupDB(nil))).To(Succeed())

		annotations := map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + acDBName,
		}
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var archiveCheck corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-archive-check" {
				archiveCheck = c
				break
			}
		}
		script := strings.Join(archiveCheck.Command, " ")
		// When DB exists but state dir is absent, the script must NOT exit 0.
		// It must continue to the S3 probe (litestream restore).
		// Verify this by checking: (a) state dir test is present, (b) S3 probe follows.
		Expect(script).To(ContainSubstring(`-d "${STATE_DIR}"`),
			"archive-check must test for state directory")
		Expect(script).To(ContainSubstring("probing S3"),
			"archive-check must continue to S3 probe when state dir is absent")
		Expect(script).To(ContainSubstring("litestream restore"),
			"archive-check must invoke litestream restore as the S3 probe")
	})
})

var _ = Describe("SidecarInjector auto-restore", func() {
	const (
		namespace         = "default"
		arDBName          = "auto-restore-db"
		arDeployName      = "auto-restore-app"
		arDatabaseName    = "app.db"
		arDatabasePath    = "/data"
		arVolumeName      = "ar-data"
		injectTrue        = "true"    // goconst
		appContainerImage = "busybox" // goconst
	)

	ctx := context.Background()

	newInjector := func() *webhook.SidecarInjector {
		return &webhook.SidecarInjector{
			Client:  k8sClient,
			Decoder: admission.NewDecoder(k8sClient.Scheme()),
		}
	}

	newPod := func(annotations map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "ar-pod", Namespace: namespace, Annotations: annotations},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "app", Image: appContainerImage,
					VolumeMounts: []corev1.VolumeMount{{Name: arVolumeName, MountPath: arDatabasePath}},
				}},
				Volumes: []corev1.Volume{{
					Name:         arVolumeName,
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}},
			},
		}
	}

	makeRequest := func(pod *corev1.Pod) admission.Request {
		raw, err := json.Marshal(pod)
		Expect(err).NotTo(HaveOccurred())
		return admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				UID: "ar-uid", Namespace: namespace,
				Operation: admissionv1.Create,
				Object:    runtime.RawExtension{Raw: raw},
			},
		}
	}

	newAutoRestoreDB := func() *databasev1.LitestreamReplica {
		return &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: arDBName, Namespace: namespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     arDatabaseName,
				DatabasePath:     arDatabasePath,
				TargetDeployment: arDeployName,
				Recovery:         databasev1.RecoverySpec{Mode: databasev1.RecoveryModeAutomatic},
				Backup: databasev1.BackupSpec{
					Enabled: true,
					Destination: databasev1.BackupDestination{
						S3: &databasev1.S3Destination{
							Bucket:    "ar-bucket",
							SecretRef: "ar-s3-creds",
						},
					},
				},
			},
		}
	}

	BeforeEach(func() {
		Expect(k8sClient.Create(ctx, newAutoRestoreDB())).To(Succeed())
	})

	AfterEach(func() {
		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: arDBName, Namespace: namespace}, db); err == nil {
			_ = k8sClient.Delete(ctx, db)
		}
	})

	annotations := func() map[string]string {
		return map[string]string{
			databasev1.AnnotationInject: injectTrue,
			databasev1.AnnotationConfig: namespace + "/" + arDBName,
		}
	}

	It("injects auto-restore init container (not archive-check) when recovery.mode=Automatic", func() {
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations())))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations()), resp.Patches)
		initNames := make([]string, len(patched.Spec.InitContainers))
		for i, c := range patched.Spec.InitContainers {
			initNames[i] = c.Name
		}
		Expect(initNames).To(ContainElement("litestream-restore"))
		Expect(initNames).NotTo(ContainElement("litestream-archive-check"))
	})

	It("auto-restore init container is the first init container", func() {
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations())))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations()), resp.Patches)
		Expect(patched.Spec.InitContainers).NotTo(BeEmpty())
		Expect(patched.Spec.InitContainers[0].Name).To(Equal("litestream-restore"))
	})

	It("auto-restore init container script references -if-db-not-exists and -if-replica-exists flags", func() {
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations())))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations()), resp.Patches)
		var restore corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-restore" {
				restore = c
				break
			}
		}
		Expect(restore.Command).To(ContainElement(ContainSubstring("-if-db-not-exists")))
		Expect(restore.Command).To(ContainElement(ContainSubstring("-if-replica-exists")))
	})

	It("auto-restore init container script uses native -integrity-check and fails closed", func() {
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations())))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations()), resp.Patches)
		var restore corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-restore" {
				restore = c
				break
			}
		}
		script := strings.Join(restore.Command, " ")
		Expect(script).To(ContainSubstring("-integrity-check quick"))
		Expect(script).NotTo(ContainSubstring("sqlite3"))
		Expect(script).NotTo(ContainSubstring("PRAGMA"))
		Expect(script).To(ContainSubstring("exit 1"))
		Expect(script).NotTo(ContainSubstring("starting fresh"))
	})

	It("auto-restore init container has S3 credential env vars", func() {
		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations())))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations()), resp.Patches)
		var restore corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-restore" {
				restore = c
				break
			}
		}
		envNames := make([]string, len(restore.Env))
		for i, e := range restore.Env {
			envNames[i] = e.Name
		}
		Expect(envNames).To(ContainElements("LITESTREAM_ACCESS_KEY_ID", "LITESTREAM_SECRET_ACCESS_KEY"))
	})
})

var _ = Describe("SidecarInjector error paths", func() {
	const (
		namespace         = "default"
		errDBName         = "err-test-db"
		errDeployName     = "err-test-app"
		errDatabasePath   = "/data"
		errVolumeName     = "err-data"
		appContainerImage = "busybox" // goconst
	)

	ctx := context.Background()

	newInjector := func() *webhook.SidecarInjector {
		return &webhook.SidecarInjector{
			Client:  k8sClient,
			Decoder: admission.NewDecoder(k8sClient.Scheme()),
		}
	}

	makeRequest := func(pod *corev1.Pod) admission.Request {
		raw, err := json.Marshal(pod)
		Expect(err).NotTo(HaveOccurred())
		return admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				UID: "test-uid", Namespace: namespace,
				Operation: admissionv1.Create,
				Object:    runtime.RawExtension{Raw: raw},
			},
		}
	}

	BeforeEach(func() {
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: errDBName, Namespace: namespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     "app.db",
				DatabasePath:     errDatabasePath,
				TargetDeployment: errDeployName,
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())
	})

	AfterEach(func() {
		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: errDBName, Namespace: namespace}, db); err == nil {
			_ = k8sClient.Delete(ctx, db)
		}
	})

	It("returns an error when the config annotation is malformed (no '/' separator)", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "err-pod", Namespace: namespace,
				Annotations: map[string]string{
					databasev1.AnnotationInject: "true",
					databasev1.AnnotationConfig: "bad-ref-no-slash", // malformed
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "app", Image: appContainerImage,
					VolumeMounts: []corev1.VolumeMount{{Name: errVolumeName, MountPath: errDatabasePath}},
				}},
				Volumes: []corev1.Volume{{
					Name:         errVolumeName,
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}},
			},
		}
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeFalse())
		Expect(resp.Result.Code).To(BeNumerically("==", 500))
	})

	It("returns an error when the referenced LitestreamReplica does not exist", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "err-pod-missing-db", Namespace: namespace,
				Annotations: map[string]string{
					databasev1.AnnotationInject: "true",
					databasev1.AnnotationConfig: namespace + "/nonexistent-db",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "app", Image: appContainerImage,
					VolumeMounts: []corev1.VolumeMount{{Name: errVolumeName, MountPath: errDatabasePath}},
				}},
				Volumes: []corev1.Volume{{
					Name:         errVolumeName,
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}},
			},
		}
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeFalse())
		Expect(resp.Result.Code).To(BeNumerically("==", 500))
	})

	It("returns an error when no volume mount covers the database path", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "err-pod-no-vol", Namespace: namespace,
				Annotations: map[string]string{
					databasev1.AnnotationInject: "true",
					databasev1.AnnotationConfig: namespace + "/" + errDBName,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "app",
					Image: appContainerImage,
					// Mount at /other — does NOT cover errDatabasePath (/data).
					VolumeMounts: []corev1.VolumeMount{{Name: errVolumeName, MountPath: "/other"}},
				}},
				Volumes: []corev1.Volume{{
					Name:         errVolumeName,
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}},
			},
		}
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeFalse())
		Expect(resp.Result.Code).To(BeNumerically("==", 500))
	})

	It("returns an error when the first container has no volume mounts", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "err-pod-no-mounts", Namespace: namespace,
				Annotations: map[string]string{
					databasev1.AnnotationInject: "true",
					databasev1.AnnotationConfig: namespace + "/" + errDBName,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "app",
					Image: appContainerImage,
					// No VolumeMounts at all.
				}},
			},
		}
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeFalse())
		Expect(resp.Result.Code).To(BeNumerically("==", 500))
	})

	It("returns an error when the pod has no containers at all", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "err-pod-no-containers", Namespace: namespace,
				Annotations: map[string]string{
					databasev1.AnnotationInject: "true",
					databasev1.AnnotationConfig: namespace + "/" + errDBName,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{}, // zero containers
			},
		}
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeFalse())
		Expect(resp.Result.Code).To(BeNumerically("==", 500))
	})
})

var _ = Describe("SidecarInjector container selection and subPath", func() {
	const (
		namespace         = "default"
		csDBName          = "cs-test-db"
		csDeployName      = "cs-test-app"
		csDatabaseName    = "app.db"
		csDatabasePath    = "/data"
		csVolumeName      = "cs-data"
		appContainerImage = "busybox" // goconst
		injectTrue        = "true"    // goconst
	)

	ctx := context.Background()

	newInjector := func() *webhook.SidecarInjector {
		return &webhook.SidecarInjector{
			Client:  k8sClient,
			Decoder: admission.NewDecoder(k8sClient.Scheme()),
		}
	}

	makeRequest := func(pod *corev1.Pod) admission.Request {
		raw, err := json.Marshal(pod)
		Expect(err).NotTo(HaveOccurred())
		return admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				UID: "test-uid", Namespace: namespace,
				Operation: admissionv1.Create,
				Object:    runtime.RawExtension{Raw: raw},
			},
		}
	}

	AfterEach(func() {
		db := &databasev1.LitestreamReplica{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: csDBName, Namespace: namespace}, db); err == nil {
			_ = k8sClient.Delete(ctx, db)
		}
	})

	It("selects the second container when spec.container is set", func() {
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: csDBName, Namespace: namespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     csDatabaseName,
				DatabasePath:     csDatabasePath,
				TargetDeployment: csDeployName,
				Container:        "second",
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "cs-pod", Namespace: namespace,
				Annotations: map[string]string{
					databasev1.AnnotationInject: injectTrue,
					databasev1.AnnotationConfig: namespace + "/" + csDBName,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "first", Image: appContainerImage,
						VolumeMounts: []corev1.VolumeMount{{Name: "other-vol", MountPath: "/other"}},
					},
					{
						Name: "second", Image: appContainerImage,
						VolumeMounts: []corev1.VolumeMount{{Name: csVolumeName, MountPath: csDatabasePath}},
					},
				},
				Volumes: []corev1.Volume{
					{Name: "other-vol", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					{Name: csVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				},
			},
		}
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(pod, resp.Patches)
		var sidecar corev1.Container
		for _, c := range patched.Spec.Containers {
			if c.Name == "litestream" {
				sidecar = c
				break
			}
		}
		Expect(sidecar.Name).To(Equal("litestream"))
		Expect(sidecar.VolumeMounts[0].Name).To(Equal(csVolumeName))
	})

	It("preserves subPath from the application volume mount", func() {
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: csDBName, Namespace: namespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     csDatabaseName,
				DatabasePath:     csDatabasePath,
				TargetDeployment: csDeployName,
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "subpath-pod", Namespace: namespace,
				Annotations: map[string]string{
					databasev1.AnnotationInject: injectTrue,
					databasev1.AnnotationConfig: namespace + "/" + csDBName,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "app", Image: appContainerImage,
					VolumeMounts: []corev1.VolumeMount{{
						Name: csVolumeName, MountPath: csDatabasePath, SubPath: "db-subdir",
					}},
				}},
				Volumes: []corev1.Volume{{
					Name:         csVolumeName,
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}},
			},
		}
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(pod, resp.Patches)
		var sidecar corev1.Container
		for _, c := range patched.Spec.Containers {
			if c.Name == "litestream" {
				sidecar = c
				break
			}
		}
		Expect(sidecar.VolumeMounts[0].SubPath).To(Equal("db-subdir"))
	})

	It("returns an error when spec.container references a nonexistent container", func() {
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: csDBName, Namespace: namespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     csDatabaseName,
				DatabasePath:     csDatabasePath,
				TargetDeployment: csDeployName,
				Container:        "nonexistent",
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "bad-container-pod", Namespace: namespace,
				Annotations: map[string]string{
					databasev1.AnnotationInject: injectTrue,
					databasev1.AnnotationConfig: namespace + "/" + csDBName,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "app", Image: appContainerImage,
					VolumeMounts: []corev1.VolumeMount{{Name: csVolumeName, MountPath: csDatabasePath}},
				}},
				Volumes: []corev1.Volume{{
					Name:         csVolumeName,
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}},
			},
		}
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeFalse())
		Expect(resp.Result.Code).To(BeNumerically("==", 500))
	})

	It("selects the best (longest prefix) volume mount", func() {
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: csDBName, Namespace: namespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     csDatabaseName,
				DatabasePath:     "/data/nested",
				TargetDeployment: csDeployName,
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "nested-pod", Namespace: namespace,
				Annotations: map[string]string{
					databasev1.AnnotationInject: injectTrue,
					databasev1.AnnotationConfig: namespace + "/" + csDBName,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "app", Image: appContainerImage,
					VolumeMounts: []corev1.VolumeMount{
						{Name: "root-vol", MountPath: "/data"},
						{Name: "nested-vol", MountPath: "/data/nested"},
					},
				}},
				Volumes: []corev1.Volume{
					{Name: "root-vol", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					{Name: "nested-vol", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				},
			},
		}
		resp := newInjector().Handle(ctx, makeRequest(pod))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(pod, resp.Patches)
		var sidecar corev1.Container
		for _, c := range patched.Spec.Containers {
			if c.Name == "litestream" {
				sidecar = c
				break
			}
		}
		Expect(sidecar.VolumeMounts[0].Name).To(Equal("nested-vol"))
	})
})

var _ = Describe("SidecarInjector SecurityContext from runAsUser/runAsGroup", func() {
	const (
		namespace         = "default"
		scDBName          = "sc-test-db"
		scDeployName      = "sc-test-app"
		scDatabaseName    = "app.db"
		scDatabasePath    = "/data"
		scVolumeName      = "sc-data"
		scSecretRef       = "s3-creds"
		injectTrue        = "true"    // goconst
		appContainerImage = "busybox" // goconst
	)

	ctx := context.Background()

	newInjector := func() *webhook.SidecarInjector {
		return &webhook.SidecarInjector{
			Client:  k8sClient,
			Decoder: admission.NewDecoder(k8sClient.Scheme()),
		}
	}

	newPod := func(annotations map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "sc-pod", Namespace: namespace, Annotations: annotations},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "app", Image: appContainerImage,
					VolumeMounts: []corev1.VolumeMount{{Name: scVolumeName, MountPath: scDatabasePath}},
				}},
				Volumes: []corev1.Volume{{
					Name:         scVolumeName,
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}},
			},
		}
	}

	makeRequest := func(pod *corev1.Pod) admission.Request {
		raw, err := json.Marshal(pod)
		Expect(err).NotTo(HaveOccurred())
		return admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
				Object:    runtime.RawExtension{Raw: raw},
			},
		}
	}

	// newBackupDB creates a backup-enabled LitestreamReplica with optional RunAsUser/RunAsGroup.
	newBackupDB := func(uid, gid *int64) *databasev1.LitestreamReplica {
		db := &databasev1.LitestreamReplica{
			ObjectMeta: metav1.ObjectMeta{Name: scDBName, Namespace: namespace},
			Spec: databasev1.LitestreamReplicaSpec{
				DatabaseName:     scDatabaseName,
				DatabasePath:     scDatabasePath,
				TargetDeployment: scDeployName,
				RunAsUser:        uid,
				RunAsGroup:       gid,
				Backup: databasev1.BackupSpec{
					Enabled: true,
					Destination: databasev1.BackupDestination{
						S3: &databasev1.S3Destination{
							Bucket:    "bucket",
							SecretRef: scSecretRef,
						},
					},
				},
			},
		}
		return db
	}

	AfterEach(func() {
		db := &databasev1.LitestreamReplica{}
		_ = k8sClient.Get(ctx, types.NamespacedName{Name: scDBName, Namespace: namespace}, db)
		_ = k8sClient.Delete(ctx, db)
	})

	annotations := map[string]string{
		databasev1.AnnotationInject: injectTrue,
		databasev1.AnnotationConfig: namespace + "/" + scDBName,
	}

	It("archive-check init container has no SecurityContext when RunAsUser/RunAsGroup omitted", func() {
		Expect(k8sClient.Create(ctx, newBackupDB(nil, nil))).To(Succeed())

		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var archiveCheck corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-archive-check" {
				archiveCheck = c
				break
			}
		}
		Expect(archiveCheck.SecurityContext).To(BeNil())
	})

	It("archive-check init container gets SecurityContext when RunAsUser set", func() {
		uid, gid := int64(1000), int64(2000)
		Expect(k8sClient.Create(ctx, newBackupDB(&uid, &gid))).To(Succeed())

		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var archiveCheck corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "litestream-archive-check" {
				archiveCheck = c
				break
			}
		}
		Expect(archiveCheck.SecurityContext).NotTo(BeNil())
		Expect(*archiveCheck.SecurityContext.RunAsUser).To(Equal(int64(1000)))
		Expect(*archiveCheck.SecurityContext.RunAsGroup).To(Equal(int64(2000)))
	})

	It("db-bootstrap init container gets SecurityContext when RunAsUser set", func() {
		uid := int64(1000)
		db := newBackupDB(&uid, nil)
		db.Spec.Bootstrap.SQL = "CREATE TABLE t (id INTEGER PRIMARY KEY);"
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var dbInit corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "db-bootstrap" {
				dbInit = c
				break
			}
		}
		Expect(dbInit.SecurityContext).NotTo(BeNil())
		Expect(*dbInit.SecurityContext.RunAsUser).To(Equal(int64(1000)))
		Expect(dbInit.SecurityContext.RunAsGroup).To(BeNil())
	})

	It("db-bootstrap init container has no SecurityContext when RunAsUser/RunAsGroup omitted", func() {
		db := newBackupDB(nil, nil)
		db.Spec.Bootstrap.SQL = "CREATE TABLE t (id INTEGER PRIMARY KEY);"
		Expect(k8sClient.Create(ctx, db)).To(Succeed())

		resp := newInjector().Handle(ctx, makeRequest(newPod(annotations)))
		Expect(resp.Allowed).To(BeTrue())

		patched := applyAllPatches(newPod(annotations), resp.Patches)
		var dbInit corev1.Container
		for _, c := range patched.Spec.InitContainers {
			if c.Name == "db-bootstrap" {
				dbInit = c
				break
			}
		}
		Expect(dbInit.SecurityContext).To(BeNil())
	})
})

// applyInitPatches reconstructs the mutated pod from JSON-patch add operations
// produced by admission.PatchResponseFromRaw. The path for appended array
// elements uses a numeric index (e.g. /spec/containers/1), so we match on
// the path prefix rather than a literal "/-" suffix.
// applyPatches / applyInitPatches reconstruct the mutated pod from JSON-patch
// "add" operations produced by admission.PatchResponseFromRaw.
func applyPatches(pod *corev1.Pod, patches []jsonpatch.JsonPatchOperation) *corev1.Pod {
	return applyAllPatches(pod, patches)
}

func applyInitPatches(pod *corev1.Pod, patches []jsonpatch.JsonPatchOperation) *corev1.Pod {
	return applyAllPatches(pod, patches)
}

func applyAllPatches(pod *corev1.Pod, patches []jsonpatch.JsonPatchOperation) *corev1.Pod {
	if len(patches) == 0 {
		return pod
	}
	out := pod.DeepCopy()
	for _, op := range patches {
		if op.Operation != "add" && op.Operation != "replace" {
			continue
		}
		raw, err := json.Marshal(op.Value)
		if err != nil {
			continue
		}
		switch {
		case strings.HasPrefix(op.Path, "/spec/containers/") || op.Path == "/spec/containers/-":
			var c corev1.Container
			if err := json.Unmarshal(raw, &c); err == nil && c.Name != "" {
				out.Spec.Containers = append(out.Spec.Containers, c)
			}
		case op.Path == "/spec/initContainers":
			// Whole array added (field was null before injection).
			var cs []corev1.Container
			if err := json.Unmarshal(raw, &cs); err == nil {
				out.Spec.InitContainers = append(out.Spec.InitContainers, cs...)
			}
		case strings.HasPrefix(op.Path, "/spec/initContainers/") || op.Path == "/spec/initContainers/-":
			var c corev1.Container
			if err := json.Unmarshal(raw, &c); err == nil && c.Name != "" {
				out.Spec.InitContainers = append(out.Spec.InitContainers, c)
			}
		case strings.HasPrefix(op.Path, "/spec/volumes/") || op.Path == "/spec/volumes/-":
			var v corev1.Volume
			if err := json.Unmarshal(raw, &v); err == nil && v.Name != "" {
				out.Spec.Volumes = append(out.Spec.Volumes, v)
			}
		case op.Path == "/metadata/annotations":
			var annotations map[string]string
			if err := json.Unmarshal(raw, &annotations); err == nil {
				if out.Annotations == nil {
					out.Annotations = make(map[string]string)
				}
				for k, v := range annotations {
					out.Annotations[k] = v
				}
			}
		case strings.HasPrefix(op.Path, "/metadata/annotations/"):
			var val string
			if err := json.Unmarshal(raw, &val); err == nil {
				if out.Annotations == nil {
					out.Annotations = make(map[string]string)
				}
				// Path ends with the annotation key (URL-encoded '~1' for '/').
				key := strings.TrimPrefix(op.Path, "/metadata/annotations/")
				key = strings.ReplaceAll(key, "~1", "/")
				out.Annotations[key] = val
			}
		}
	}
	return out
}
