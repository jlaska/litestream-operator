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

	s3Bucket = "litestream-backups"
	// s3Endpoint is the in-cluster address Litestream uses.
	s3Endpoint = "garage." + testNamespace + ".svc.cluster.local:3900"
)

// Set during BeforeSuite by garage key create.
var (
	s3AccessKey string
	s3SecretKey string
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting litestream-operator integration test suite\n")
	RunSpecs(t, "Integration Suite")
}

var _ = BeforeSuite(func() {
	SetDefaultEventuallyTimeout(5 * time.Minute)
	SetDefaultEventuallyPollingInterval(5 * time.Second)

	By("creating test namespace")
	// --dry-run + apply pattern is idempotent.
	kubectl("create", "namespace", testNamespace, "--dry-run=client", "-o", "yaml")
	runIgnoreError("kubectl", "create", "namespace", testNamespace)

	By("deploying Garage in test namespace")
	applyLiteral(garageManifest())

	By("waiting for Garage pod to be Running")
	kubectl("wait", "-n", testNamespace, "deployment/garage",
		"--for=condition=Available", "--timeout=3m")

	garagePod := strings.TrimSpace(kubectl("get", "pods", "-n", testNamespace,
		"-l", "app=garage", "-o", "jsonpath={.items[0].metadata.name}"))

	By("configuring Garage layout")
	// garage node id returns a multi-line message; the full node ID
	// (64 hex chars) appears after "node connect " in the output.
	nodeIDOut := kubectl("exec", "-n", testNamespace, garagePod, "--",
		"/garage", "node", "id")
	var nodeID string
	for _, line := range strings.Split(nodeIDOut, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "garage") && strings.Contains(line, "node connect ") {
			parts := strings.Fields(line)
			nodeID = parts[len(parts)-1]
			break
		}
	}
	Expect(nodeID).NotTo(BeEmpty(), "failed to parse node ID from:\n%s", nodeIDOut)
	// Use just the short ID (first 16 hex chars) for layout assignment.
	if idx := strings.Index(nodeID, "@"); idx > 0 {
		nodeID = nodeID[:idx]
	}
	kubectl("exec", "-n", testNamespace, garagePod, "--",
		"/garage", "layout", "assign", "-z", "dc1", "-c", "1G", nodeID)
	kubectl("exec", "-n", testNamespace, garagePod, "--",
		"/garage", "layout", "apply", "--version", "1")

	By("creating S3 bucket")
	kubectl("exec", "-n", testNamespace, garagePod, "--",
		"/garage", "bucket", "create", s3Bucket)

	By("creating API key")
	keyOutput := kubectl("exec", "-n", testNamespace, garagePod, "--",
		"/garage", "key", "create", "testkey")
	s3AccessKey, s3SecretKey = parseGarageKeyOutput(keyOutput)
	GinkgoWriter.Printf("Garage key created: access=%s\n", s3AccessKey)

	By("granting bucket permissions")
	kubectl("exec", "-n", testNamespace, garagePod, "--",
		"/garage", "bucket", "allow", "--read", "--write", "--owner",
		s3Bucket, "--key", "testkey")

	By("creating S3 credentials Secret")
	runIgnoreError("kubectl", "create", "secret", "generic", "s3-creds",
		"-n", testNamespace,
		"--from-literal=ACCESS_KEY_ID="+s3AccessKey,
		"--from-literal=SECRET_ACCESS_KEY="+s3SecretKey,
	)

	By("starting persistent S3 client pod")
	applyLiteral(s3ClientPodManifest())
	kubectl("wait", "-n", testNamespace, "pod/s3-client",
		"--for=condition=Ready", "--timeout=2m")
	// Pre-configure the mc alias so s3List() calls can skip it.
	kubectl("exec", "-n", testNamespace, "s3-client", "--",
		"/bin/sh", "-c",
		fmt.Sprintf("mc alias set local http://garage:3900 %s %s > /dev/null 2>&1", s3AccessKey, s3SecretKey),
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

// parseGarageKeyOutput extracts the access key ID and secret key from
// the output of garage key create.
func parseGarageKeyOutput(output string) (accessKey, secretKey string) {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Key ID:") {
			accessKey = strings.TrimSpace(strings.TrimPrefix(line, "Key ID:"))
		} else if strings.HasPrefix(line, "Secret key:") {
			secretKey = strings.TrimSpace(strings.TrimPrefix(line, "Secret key:"))
		}
	}
	ExpectWithOffset(1, accessKey).NotTo(BeEmpty(),
		"failed to parse access key from garage output:\n%s", output)
	ExpectWithOffset(1, secretKey).NotTo(BeEmpty(),
		"failed to parse secret key from garage output:\n%s", output)
	return
}

// ── static manifests ───────────────────────────────────────────────────────

func garageManifest() string {
	return fmt.Sprintf(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: garage-config
  namespace: %[1]s
data:
  garage.toml: |
    metadata_dir = "/var/lib/garage/meta"
    data_dir = "/var/lib/garage/data"
    db_engine = "sqlite"
    replication_factor = 1

    rpc_bind_addr = "[::]:3901"
    rpc_public_addr = "127.0.0.1:3901"
    rpc_secret = "1799bae21c46ed46e43d76dbdab14277c1edd3eaed403e47a8b4e3016b4de382"

    [s3_api]
    s3_region = "us-east-1"
    api_bind_addr = "[::]:3900"

    [admin]
    api_bind_addr = "[::]:3903"
    admin_token = "integration-test-token"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: garage-data
  namespace: %[1]s
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 2Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: garage
  namespace: %[1]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: garage
  template:
    metadata:
      labels:
        app: garage
    spec:
      containers:
        - name: garage
          image: dxflrs/garage:v1.1.0
          ports:
            - containerPort: 3900
            - containerPort: 3901
            - containerPort: 3903
          volumeMounts:
            - name: data
              mountPath: /var/lib/garage
            - name: config
              mountPath: /etc/garage.toml
              subPath: garage.toml
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: garage-data
        - name: config
          configMap:
            name: garage-config
---
apiVersion: v1
kind: Service
metadata:
  name: garage
  namespace: %[1]s
spec:
  selector:
    app: garage
  ports:
    - name: s3
      port: 3900
      targetPort: 3900
    - name: rpc
      port: 3901
      targetPort: 3901
    - name: admin
      port: 3903
      targetPort: 3903
`, testNamespace)
}

func s3ClientPodManifest() string {
	return fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: s3-client
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: mc
      image: quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z
      command: ["sleep", "infinity"]
`, testNamespace)
}
