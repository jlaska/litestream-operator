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
	s3Region      string // S3 signing region (e.g. "garage" for Garage)
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

		if strings.HasPrefix(ep, "https://") || strings.HasPrefix(ep, "http://") {
			mcEndpoint = ep
			s3Endpoint = ep
		} else {
			mcEndpoint = "http://" + ep
			s3Endpoint = ep
		}
		s3Region = os.Getenv("S3_REGION")
		_, _ = fmt.Fprintf(GinkgoWriter, "Using external S3 backend: %s (bucket: %s)\n", s3Endpoint, s3Bucket)
	} else {
		s3Bucket = defaultBucket
		s3Endpoint = "garage." + testNamespace + ".svc.cluster.local:3900"
		s3Region = "garage"
		mcEndpoint = "http://garage:3900"
		_, _ = fmt.Fprintf(GinkgoWriter, "Using in-cluster Garage (bucket: %s)\n", s3Bucket)
	}

	By("creating test namespace")
	runIgnoreError("kubectl", "create", "namespace", testNamespace)

	if !useExternalS3 {
		By("deploying Garage in test namespace")
		applyLiteral(garageManifest())

		By("waiting for Garage deployment to be Available")
		kubectl("wait", "-n", testNamespace, "deployment/garage",
			"--for=condition=Available", "--timeout=3m")

		garagePod := strings.TrimSpace(kubectl("get", "pods", "-n", testNamespace,
			"-l", "app=garage", "-o", "jsonpath={.items[0].metadata.name}"))
		kubectl("wait", "-n", testNamespace, "pod/"+garagePod,
			"--for=condition=Ready", "--timeout=2m")

		By("configuring Garage layout")
		nodeIDOut := kubectl("exec", "-n", testNamespace, garagePod, "--",
			"/garage", "node", "id")
		var nodeID string
		for _, line := range strings.Split(nodeIDOut, "\n") {
			line = strings.TrimSpace(line)
			if idx := strings.Index(line, "@"); idx > 0 {
				parts := strings.Fields(line[:idx])
				nodeID = parts[len(parts)-1]
				break
			}
		}
		Expect(nodeID).NotTo(BeEmpty(), "failed to parse node ID from:\n%s", nodeIDOut)
		kubectl("exec", "-n", testNamespace, garagePod, "--",
			"/garage", "layout", "assign", "-z", "dc1", "-c", "1G", nodeID)
		kubectl("exec", "-n", testNamespace, garagePod, "--",
			"/garage", "layout", "apply", "--version", "1")

		By("creating S3 bucket in Garage")
		kubectl("exec", "-n", testNamespace, garagePod, "--",
			"/garage", "bucket", "create", s3Bucket)

		By("creating API key in Garage")
		keyOutput := kubectl("exec", "-n", testNamespace, garagePod, "--",
			"/garage", "key", "create", "testkey")
		s3AccessKey, s3SecretKey = parseGarageKeyOutput(keyOutput)
		GinkgoWriter.Printf("Garage key created: access=%s\n", s3AccessKey)

		By("granting bucket permissions to API key")
		kubectl("exec", "-n", testNamespace, garagePod, "--",
			"/garage", "bucket", "allow", "--read", "--write", "--owner",
			s3Bucket, "--key", "testkey")
	}

	By("creating S3 credentials Secret")
	runIgnoreError("kubectl", "delete", "secret", "s3-creds", "-n", testNamespace, "--ignore-not-found")
	kubectl("create", "secret", "generic", "s3-creds",
		"-n", testNamespace,
		"--from-literal=ACCESS_KEY_ID="+s3AccessKey,
		"--from-literal=SECRET_ACCESS_KEY="+s3SecretKey,
	)

	By("starting persistent S3 client pod")
	applyLiteral(s3ClientPodManifest())
	kubectl("wait", "-n", testNamespace, "pod/s3-client",
		"--for=condition=Ready", "--timeout=2m")
	kubectl("exec", "-n", testNamespace, "s3-client", "--",
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

// parseGarageKeyOutput extracts the access key ID and secret key from
// `garage key create` output.
func parseGarageKeyOutput(output string) (accessKey, secretKey string) {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Key ID:") {
			accessKey = strings.TrimSpace(strings.TrimPrefix(line, "Key ID:"))
		}
		if strings.HasPrefix(line, "Secret key:") {
			secretKey = strings.TrimSpace(strings.TrimPrefix(line, "Secret key:"))
		}
	}
	Expect(accessKey).NotTo(BeEmpty(), "failed to parse Key ID from garage key create output:\n%s", output)
	Expect(secretKey).NotTo(BeEmpty(), "failed to parse Secret key from garage key create output:\n%s", output)
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
    db_engine = "lmdb"
    replication_factor = 1
    rpc_bind_addr = "0.0.0.0:3901"

    [s3_api]
    s3_region = "garage"
    api_bind_addr = "0.0.0.0:3900"
    root_domain = ".s3.garage.localhost"

    [s3_web]
    bind_addr = "0.0.0.0:3902"
    root_domain = ".web.garage.localhost"

    [admin]
    api_bind_addr = "0.0.0.0:3903"
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
          env:
            - name: GARAGE_RPC_SECRET
              value: "%[2]s"
          ports:
            - containerPort: 3900
              name: s3
            - containerPort: 3901
              name: rpc
            - containerPort: 3903
              name: admin
          readinessProbe:
            tcpSocket:
              port: 3900
            initialDelaySeconds: 2
            periodSeconds: 3
          volumeMounts:
            - name: config
              mountPath: /etc/garage.toml
              subPath: garage.toml
            - name: data
              mountPath: /var/lib/garage
      volumes:
        - name: config
          configMap:
            name: garage-config
        - name: data
          emptyDir: {}
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
`, testNamespace, strings.Repeat("ab", 32))
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
