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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
)

// litestreamContainerName is the name given to the injected sidecar container.
const litestreamContainerName = "litestream"

// litestreamConfigVolume is the name of the volume that mounts litestream.yml.
const litestreamConfigVolume = "litestream-config"

// litestreamConfigMount is the path where the Litestream config is mounted.
const litestreamConfigMount = "/etc/litestream"

// litestreamDefaultImage is the default Litestream container image.
const litestreamDefaultImage = databasev1.LitestreamDefaultImage

// injectTrue is the value used for the injection annotation.
const injectTrue = "true"

// dbBootstrapContainerName is the name given to the injected bootstrap init container.
const dbBootstrapContainerName = "db-bootstrap"

// dbBootstrapSQLVolume is the name of the volume that mounts bootstrap.sql.
const dbBootstrapSQLVolume = "db-bootstrap-sql"

// SidecarInjector is a mutating admission webhook that injects a Litestream
// replication sidecar into pods belonging to annotated Deployments.
//
// It is registered as a raw admission.Handler (not a typed CRD webhook) because
// it operates on core/v1 Pod resources.
type SidecarInjector struct {
	// Client is an uncached API reader (mgr.GetAPIReader()) so the webhook always
	// fetches a fresh LitestreamReplica, bypassing the informer cache. This
	// prevents a race where skip-archive-check is set by the restore controller
	// but the webhook reads a stale cached version and injects the archive-check
	// init container anyway.
	Client  client.Reader
	Decoder admission.Decoder
}

// Handle processes an admission request for a Pod and injects the Litestream
// sidecar when the pod carries the injection annotation.
func (s *SidecarInjector) Handle(ctx context.Context, req admission.Request) admission.Response {
	pod := &corev1.Pod{}
	if err := s.Decoder.DecodeRaw(req.Object, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decoding pod: %w", err))
	}

	// Only act on pods that carry the injection annotation.
	if pod.Annotations[databasev1.AnnotationInject] != injectTrue {
		return admission.Allowed("no injection annotation")
	}

	// Skip if already injected (idempotency guard).
	if s.alreadyInjected(pod) {
		return admission.Allowed("sidecar already present")
	}

	// Resolve the LitestreamReplica CR from the config annotation.
	db, err := s.resolveLitestreamReplica(ctx, pod, req.Namespace)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if db == nil {
		return admission.Allowed("no LitestreamReplica config reference found")
	}

	// Inject the sidecar and return the patch.
	if err := s.inject(pod, db); err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("injecting sidecar: %w", err))
	}

	marshalled, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("marshalling patched pod: %w", err))
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshalled)
}

// alreadyInjected returns true if the Litestream container is already present,
// preventing duplicate injection on pod updates or retries.
func (s *SidecarInjector) alreadyInjected(pod *corev1.Pod) bool {
	for _, c := range pod.Spec.Containers {
		if c.Name == litestreamContainerName {
			return true
		}
	}
	return false
}

// resolveLitestreamReplica looks up the LitestreamReplica CR referenced by the config annotation.
// The annotation value is "namespace/name". Returns nil (no error) when the
// annotation is absent.
func (s *SidecarInjector) resolveLitestreamReplica(ctx context.Context, pod *corev1.Pod, podNamespace string) (*databasev1.LitestreamReplica, error) {
	ref := pod.Annotations[databasev1.AnnotationConfig]
	if ref == "" {
		return nil, nil
	}

	ns, name, found := strings.Cut(ref, "/")
	if !found {
		return nil, fmt.Errorf("malformed %s annotation %q: expected namespace/name", databasev1.AnnotationConfig, ref)
	}
	if ns == "" {
		ns = podNamespace
	}

	db := &databasev1.LitestreamReplica{}
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, db); err != nil {
		return nil, fmt.Errorf("getting LitestreamReplica %s/%s: %w", ns, name, err)
	}
	return db, nil
}

// defaultEphemeralStorageLimit is applied to the Litestream sidecar when no
// explicit resource limits are set. Litestream's LTX staging can silently fill
// disk with no error (upstream #1310); this limit surfaces the failure visibly.
const defaultEphemeralStorageLimit = "1Gi"

// inject mutates the pod spec in-place to add the Litestream sidecar.
func (s *SidecarInjector) inject(pod *corev1.Pod, db *databasev1.LitestreamReplica) error {
	// Resolve the volume mount covering the database path, using explicit
	// container selection when spec.container is set.
	mount, err := s.findVolumeForPath(pod, db.Spec.DatabasePath, db.Spec.Container)
	if err != nil {
		return err
	}

	image := db.Spec.Image
	if image == "" {
		image = litestreamDefaultImage
	}

	sidecarDataMount := corev1.VolumeMount{
		Name:      mount.volumeName,
		MountPath: mount.mountPath,
	}
	if mount.subPath != "" {
		sidecarDataMount.SubPath = mount.subPath
	}

	sidecar := corev1.Container{
		Name:  litestreamContainerName,
		Image: image,
		Args:  []string{"replicate", "-config", "/etc/litestream/litestream.yml"},
		Ports: []corev1.ContainerPort{
			{Name: "metrics", ContainerPort: 9090, Protocol: corev1.ProtocolTCP},
		},
		VolumeMounts: []corev1.VolumeMount{
			sidecarDataMount,
			{
				Name:      litestreamConfigVolume,
				MountPath: litestreamConfigMount,
				ReadOnly:  true,
			},
		},
		Resources: litestreamResources(db),
	}

	// Inject S3 credentials and optional log level from the referenced Secret.
	if db.Spec.Backup.Enabled && db.Spec.Backup.Destination.S3 != nil {
		sidecar.Env = s3CredsEnvVars(db.Spec.Backup.Destination.S3.SecretRef)
	}
	if db.Spec.Backup.LogLevel != "" {
		sidecar.Env = append(sidecar.Env, corev1.EnvVar{
			Name:  "LITESTREAM_LOG_LEVEL",
			Value: db.Spec.Backup.LogLevel,
		})
	}

	pod.Spec.Containers = append(pod.Spec.Containers, sidecar)

	// Add the ConfigMap volume for litestream.yml (only once).
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: litestreamConfigVolume,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: db.Name + "-litestream",
				},
			},
		},
	})

	// Add Prometheus scrape annotations to the pod so standard service monitors
	// can discover the sidecar's /metrics endpoint. Preserve existing values so
	// application-level metrics annotations are not silently overwritten.
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	if pod.Annotations["prometheus.io/scrape"] == "" {
		pod.Annotations["prometheus.io/scrape"] = "true"
	}
	if pod.Annotations["prometheus.io/port"] == "" {
		pod.Annotations["prometheus.io/port"] = "9090"
	}
	if pod.Annotations["prometheus.io/path"] == "" {
		pod.Annotations["prometheus.io/path"] = "/metrics"
	}

	// Inject the startup init container:
	//   recovery.mode=Automatic → upstream-style restore with mandatory integrity gate
	//   recovery.mode=Manual    → archive-check that blocks if S3 has data but DB missing
	if db.Spec.Backup.Enabled && db.Spec.Recovery.Mode == databasev1.RecoveryModeAutomatic {
		s.injectAutoRestoreContainer(pod, db, mount)
	}

	// Inject the bootstrap SQL init container when Bootstrap.SQL is configured.
	if db.Spec.Bootstrap.SQL != "" {
		s.injectBootstrapContainer(pod, db, mount)
	}

	return nil
}

// litestreamResources returns the resource requirements for the Litestream sidecar.
// When the user has not specified resources, a default ephemeral-storage limit is
// applied to surface the silent disk-fill failure mode (upstream #1310).
func litestreamResources(db *databasev1.LitestreamReplica) corev1.ResourceRequirements {
	if db.Spec.Backup.Resources != nil {
		return *db.Spec.Backup.Resources
	}
	return corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceEphemeralStorage: resource.MustParse(defaultEphemeralStorageLimit),
		},
	}
}

// autoRestoreContainerName is the name of the auto-restore init container.
const autoRestoreContainerName = "litestream-restore"

// buildLitestreamInitContainer builds the shared container structure for both
// the archive-check and auto-restore init containers. Both containers use the
// same image, env vars, and volume mounts; only the name and script differ.
func buildLitestreamInitContainer(name, script, image string, mount resolvedMount, envVars []corev1.EnvVar, runAsUser, runAsGroup *int64) corev1.Container {
	dataMount := corev1.VolumeMount{
		Name:      mount.volumeName,
		MountPath: mount.mountPath,
	}
	if mount.subPath != "" {
		dataMount.SubPath = mount.subPath
	}
	c := corev1.Container{
		Name:    name,
		Image:   image,
		Command: []string{"sh", "-c", script},
		Env:     envVars,
		VolumeMounts: []corev1.VolumeMount{
			dataMount,
			{Name: litestreamConfigVolume, MountPath: litestreamConfigMount, ReadOnly: true},
		},
	}
	if runAsUser != nil || runAsGroup != nil {
		c.SecurityContext = &corev1.SecurityContext{
			RunAsUser:  runAsUser,
			RunAsGroup: runAsGroup,
		}
	}
	return c
}

// injectAutoRestoreContainer adds an init container that uses native Litestream
// restore with built-in integrity checking:
//
//	litestream restore -if-db-not-exists -if-replica-exists -integrity-check quick
//
// Any genuine restore failure (bad credentials, network, corruption) exits non-zero
// and blocks pod startup — the operator never silently starts fresh.
func (s *SidecarInjector) injectAutoRestoreContainer(pod *corev1.Pod, db *databasev1.LitestreamReplica, mount resolvedMount) {
	image := db.Spec.Image
	if image == "" {
		image = litestreamDefaultImage
	}

	dbFullPath := db.Spec.DatabasePath + "/" + db.Spec.DatabaseName

	script := fmt.Sprintf(`
DB_PATH="%s"
if [ -f "${DB_PATH}" ]; then
  echo "litestream-restore: database exists, skipping restore"
  exit 0
fi
echo "litestream-restore: database missing, attempting restore from backup..."
litestream restore \
  -if-db-not-exists \
  -if-replica-exists \
  -integrity-check quick \
  -config /etc/litestream/litestream.yml \
  "${DB_PATH}"
RESTORE_EXIT=$?
if [ $RESTORE_EXIT -ne 0 ]; then
  echo "litestream-restore: FAILED — litestream restore exited with code ${RESTORE_EXIT}"
  echo "litestream-restore: the application will NOT start against unverified data."
  echo "Options:"
  echo "  1. Check S3 credentials and connectivity."
  echo "  2. Use a LitestreamRestore CR with a different -timestamp."
  echo "  3. Set annotation litestream.io/skip-archive-check=true to start fresh."
  exit 1
fi
echo "litestream-restore: restore and integrity check passed"
exit 0
`, dbFullPath)

	envVars := []corev1.EnvVar{}
	if db.Spec.Backup.Destination.S3 != nil {
		envVars = s3CredsEnvVars(db.Spec.Backup.Destination.S3.SecretRef)
	}

	c := buildLitestreamInitContainer(autoRestoreContainerName, script, image, mount, envVars, db.Spec.RunAsUser, db.Spec.RunAsGroup)
	pod.Spec.InitContainers = append([]corev1.Container{c}, pod.Spec.InitContainers...)
}

// s3CredsEnvVars builds S3 credential env vars from a Secret reference.
func s3CredsEnvVars(secretRef string) []corev1.EnvVar {
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

// injectBootstrapContainer adds an init container that applies bootstrap SQL
// only when the database is genuinely new (no DB file exists after the
// archive-check/auto-restore init container has run).
func (s *SidecarInjector) injectBootstrapContainer(pod *corev1.Pod, db *databasev1.LitestreamReplica, mount resolvedMount) {
	bootstrapImage := db.Spec.Bootstrap.Image
	if bootstrapImage == "" {
		bootstrapImage = "keinos/sqlite3:latest"
	}

	dbFullPath := db.Spec.DatabasePath + "/" + db.Spec.DatabaseName

	script := fmt.Sprintf(`
DB_PATH="%s"
if [ -f "${DB_PATH}" ]; then
  echo "db-bootstrap: database already exists, skipping bootstrap"
  exit 0
fi
echo "db-bootstrap: database is genuinely new, applying bootstrap SQL"
sqlite3 "${DB_PATH}" < /bootstrap/bootstrap.sql
chmod 666 "${DB_PATH}"
echo "db-bootstrap: bootstrap SQL applied"
`, dbFullPath)

	dataMount := corev1.VolumeMount{
		Name:      mount.volumeName,
		MountPath: mount.mountPath,
	}
	if mount.subPath != "" {
		dataMount.SubPath = mount.subPath
	}
	bootstrapContainer := corev1.Container{
		Name:    dbBootstrapContainerName,
		Image:   bootstrapImage,
		Command: []string{"sh", "-c", script},
		VolumeMounts: []corev1.VolumeMount{
			dataMount,
			{
				Name:      dbBootstrapSQLVolume,
				MountPath: "/bootstrap",
				ReadOnly:  true,
			},
		},
	}
	if db.Spec.RunAsUser != nil || db.Spec.RunAsGroup != nil {
		bootstrapContainer.SecurityContext = &corev1.SecurityContext{
			RunAsUser:  db.Spec.RunAsUser,
			RunAsGroup: db.Spec.RunAsGroup,
		}
	}

	pod.Spec.InitContainers = append(pod.Spec.InitContainers, bootstrapContainer)

	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: dbBootstrapSQLVolume,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: db.Name + "-bootstrap-sql",
				},
			},
		},
	})
}

// resolvedMount holds the resolved volume mount information for the database path.
type resolvedMount struct {
	volumeName string
	mountPath  string
	subPath    string
}

// findVolumeForPath resolves the volume mount covering the database path.
// When containerName is set, it searches that specific container; otherwise
// it uses the first container. Returns the best (longest-prefix) match.
func (s *SidecarInjector) findVolumeForPath(pod *corev1.Pod, dbPath, containerName string) (resolvedMount, error) {
	if len(pod.Spec.Containers) == 0 {
		return resolvedMount{}, fmt.Errorf("pod has no containers")
	}

	var targetContainer *corev1.Container
	if containerName != "" {
		for i := range pod.Spec.Containers {
			if pod.Spec.Containers[i].Name == containerName {
				targetContainer = &pod.Spec.Containers[i]
				break
			}
		}
		if targetContainer == nil {
			return resolvedMount{}, fmt.Errorf("container %q not found in pod spec", containerName)
		}
	} else {
		targetContainer = &pod.Spec.Containers[0]
	}

	var bestMatch *corev1.VolumeMount
	var bestLen int
	for i := range targetContainer.VolumeMounts {
		vm := &targetContainer.VolumeMounts[i]
		if vm.MountPath == dbPath || strings.HasPrefix(dbPath, vm.MountPath+"/") {
			if len(vm.MountPath) > bestLen {
				bestMatch = vm
				bestLen = len(vm.MountPath)
			}
		}
	}
	if bestMatch == nil {
		return resolvedMount{}, fmt.Errorf(
			"no volume mount in container %q covers database path %q; "+
				"ensure the application mounts a volume at %q",
			targetContainer.Name, dbPath, dbPath,
		)
	}

	return resolvedMount{
		volumeName: bestMatch.Name,
		mountPath:  bestMatch.MountPath,
		subPath:    bestMatch.SubPath,
	}, nil
}
