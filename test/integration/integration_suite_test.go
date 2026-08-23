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

package integration

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jlaska/litestream-operator/test/utils"
)

const (
	operatorNamespace = "litestream-operator-system"
	testNamespace     = "litestream-integration"
	defaultBucket     = "litestream-backups"
)

var (
	useExternalS3 bool
	s3AccessKey   string
	s3SecretKey   string
	s3Bucket      string
	s3Endpoint    string // what Litestream pods use (in-cluster DNS or external hostname)
	mcEndpoint    string // scheme-qualified URL for configuring the mc alias
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting litestream-operator integration test suite\n")
	RunSpecs(t, "Integration Suite")
}

var _ = BeforeSuite(func() {
	SetDefaultEventuallyTimeout(5 * time.Minute)
	SetDefaultEventuallyPollingInterval(5 * time.Second)

	By("detecting S3 backend configuration")
	if ep := os.Getenv("S3_ENDPOINT"); ep != "" {
		useExternalS3 = true
		s3AccessKey = os.Getenv("S3_ACCESS_KEY")
		s3SecretKey = os.Getenv("S3_SECRET_KEY")
		Expect(s3AccessKey).NotTo(BeEmpty(), "S3_ACCESS_KEY required when S3_ENDPOINT is set")
		Expect(s3SecretKey).NotTo(BeEmpty(), "S3_SECRET_KEY required when S3_ENDPOINT is set")
		s3Bucket = os.Getenv("S3_BUCKET")
		if s3Bucket == "" {
			s3Bucket = defaultBucket
		}

		// Auto-detect scheme for mc alias URL.
		if strings.HasPrefix(ep, "https://") || strings.HasPrefix(ep, "http://") {
			mcEndpoint = ep
			// Litestream s3Endpoint: the controller handles scheme detection,
			// so pass through as-is (with scheme).
			s3Endpoint = ep
		} else {
			mcEndpoint = "http://" + ep
			s3Endpoint = ep
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "Using external S3 backend: %s (bucket: %s)\n", s3Endpoint, s3Bucket)
	} else {
		s3AccessKey = "minioadmin"
		s3SecretKey = "minioadmin"
		s3Bucket = defaultBucket
		s3Endpoint = "minio." + testNamespace + ".svc.cluster.local:9000"
		mcEndpoint = "http://minio:9000"
		_, _ = fmt.Fprintf(GinkgoWriter, "Using in-cluster MinIO (bucket: %s)\n", s3Bucket)
	}

	By("creating test namespace")
	runIgnoreError("kubectl", "create", "namespace", testNamespace)

	if !useExternalS3 {
		By("deploying MinIO in test namespace")
		applyLiteral(minioManifest())

		By("waiting for MinIO pod to be Running")
		kubectl("wait", "-n", testNamespace, "deployment/minio",
			"--for=condition=Available", "--timeout=3m")

		By("creating MinIO bucket via mc Job")
		applyLiteral(createBucketJobManifest())
		kubectl("wait", "-n", testNamespace, "job/minio-create-bucket",
			"--for=condition=Complete", "--timeout=2m")
	}

	By("creating S3 credentials Secret")
	runIgnoreError("kubectl", "delete", "secret", "minio-creds", "-n", testNamespace, "--ignore-not-found")
	kubectl("create", "secret", "generic", "minio-creds",
		"-n", testNamespace,
		"--from-literal=ACCESS_KEY_ID="+s3AccessKey,
		"--from-literal=SECRET_ACCESS_KEY="+s3SecretKey,
	)

	By("starting persistent mc client pod")
	applyLiteral(mcClientPodManifest())
	kubectl("wait", "-n", testNamespace, "pod/mc-client",
		"--for=condition=Ready", "--timeout=2m")
	kubectl("exec", "-n", testNamespace, "mc-client", "--",
		"/bin/sh", "-c",
		fmt.Sprintf("mc alias set local %s %s %s > /dev/null 2>&1", mcEndpoint, s3AccessKey, s3SecretKey),
	)
})

var _ = AfterSuite(func() {
	if os.Getenv("INTEGRATION_KEEP_NAMESPACE") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter, "INTEGRATION_KEEP_NAMESPACE=true — skipping namespace cleanup\n")
		return
	}
	By("removing test namespace")
	runIgnoreError("kubectl", "delete", "namespace", testNamespace,
		"--ignore-not-found", "--timeout=3m")
})

// ── helpers ────────────────────────────────────────────────────────────────

// kubectl runs a kubectl command and fails the test immediately on error.
// Do NOT call this inside Eventually — use kubectlQ instead.
func kubectl(args ...string) string {
	out, err := kubectlQ(args...)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "kubectl %v failed:\n%s", args, out)
	return out
}

// kubectlQ runs kubectl and returns (output, error) without failing the test.
// Use inside Eventually so errors cause a retry rather than aborting the spec.
func kubectlQ(args ...string) (string, error) {
	cmd := exec.Command("kubectl", args...)
	return utils.Run(cmd)
}

// runIgnoreError runs a command and swallows any error (for idempotent ops).
func runIgnoreError(name string, args ...string) {
	cmd := exec.Command(name, args...)
	_, _ = utils.Run(cmd)
}

// applyLiteral writes a YAML string to a temp file and applies it.
// Fails the test immediately if kubectl exits non-zero.
func applyLiteral(yaml string) {
	f, err := os.CreateTemp("", "litestream-integration-*.yaml")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer func() { _ = os.Remove(f.Name()) }()
	_, err = f.WriteString(yaml)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	_ = f.Close()
	kubectl("apply", "-f", f.Name())
}

// applyLiteralQ writes a YAML string to a temp file and applies it, returning
// (combined output, error) without failing the test. Use when the apply is
// expected to fail (e.g. webhook rejection tests).
func applyLiteralQ(yaml string) (string, error) {
	f, err := os.CreateTemp("", "litestream-integration-*.yaml")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err = f.WriteString(yaml); err != nil {
		return "", err
	}
	_ = f.Close()
	return kubectlQ("apply", "-f", f.Name())
}

// ── static manifests ───────────────────────────────────────────────────────

func minioManifest() string {
	return fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: minio-data
  namespace: %s
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 2Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
        - name: minio
          image: quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z
          args: ["server", "/data", "--console-address", ":9001"]
          env:
            - name: MINIO_ROOT_USER
              value: "%s"
            - name: MINIO_ROOT_PASSWORD
              value: "%s"
          ports:
            - containerPort: 9000
            - containerPort: 9001
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: minio-data
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: %s
spec:
  selector:
    app: minio
  ports:
    - name: api
      port: 9000
      targetPort: 9000
`, testNamespace, testNamespace, s3AccessKey, s3SecretKey, testNamespace)
}

func mcClientPodManifest() string {
	return fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: mc-client
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: mc
      image: quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z
      command: ["sleep", "infinity"]
`, testNamespace)
}

func createBucketJobManifest() string {
	return fmt.Sprintf(`
apiVersion: batch/v1
kind: Job
metadata:
  name: minio-create-bucket
  namespace: %s
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: mc
          image: quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z
          command:
            - /bin/sh
            - -c
            - |
              mc alias set local http://minio:9000 %s %s
              mc mb --ignore-existing local/%s
              echo "Bucket %s ready"
`, testNamespace, s3AccessKey, s3SecretKey, s3Bucket, s3Bucket)
}
