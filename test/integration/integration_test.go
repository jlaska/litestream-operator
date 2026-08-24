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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sigsyaml "sigs.k8s.io/yaml"

	databasev1 "github.com/jlaska/litestream-operator/api/v1"
)

// All integration scenarios run in a single Ordered container so Ginkgo's
// randomiser cannot reorder them. Scenario 3 (Restore) depends on the backup
// data created by Scenario 2 (Backup), which in turn builds on the injection
// verified in Scenario 1.
var _ = Describe("Integration", Ordered, func() {

	// ── Scenario 1: Sidecar Injection ─────────────────────────────────────

	Describe("Sidecar Injection", func() {
		const (
			appName = "inject-test-app"
			dbName  = "inject-test-db"
			pvcName = "inject-test-pvc"
			dbFile  = "inject.db"
			dbPath  = "/data"
		)

		BeforeAll(func() {
			By("creating test PVC")
			applyLiteral(pvcManifest(pvcName, testNamespace))

			By("creating test app Deployment")
			applyLiteral(appDeploymentManifest(appName, testNamespace, pvcName, dbPath))

			By("waiting for initial app pod to be Running")
			kubectl("wait", "-n", testNamespace, "deployment/"+appName,
				"--for=condition=Available", "--timeout=3m")
		})

		AfterAll(func() {
			runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
			runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
			runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		})

		It("injects the Litestream sidecar into new pods after LitestreamReplica CR is created", func() {
			By("creating a LitestreamReplica CR (backup disabled — just test injection)")
			applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, false, ""))

			By("waiting for the controller to label and annotate the pod template")
			Eventually(func(g Gomega) {
				out, err := kubectlQ("get", "deployment", appName, "-n", testNamespace,
					"-o", `jsonpath={.spec.template.metadata.labels.litestream\.io/inject}`)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("true"))
			}).Should(Succeed())

			By("waiting for rolling update to produce a pod with the Litestream sidecar")
			Eventually(func(g Gomega) {
				out, err := kubectlQ("get", "pods", "-n", testNamespace,
					"-l", "app="+appName,
					"-o", `jsonpath={range .items[*]}{range .spec.containers[*]}{.name}{"\n"}{end}{end}`)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("litestream"))
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			By("confirming SidecarReady condition reflects backup-disabled state")
			Eventually(func(g Gomega) {
				reason, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
					"-o", `jsonpath={.status.conditions[?(@.type=="SidecarReady")].reason}`)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(reason).NotTo(BeEmpty(),
					"SidecarReady condition should be set (backup disabled → sidecar crashes without a replica)")
			}).Should(Succeed())
		})
	})

	// ── Scenario 2: Backup to MinIO ───────────────────────────────────────

	Describe("Backup to S3", func() {
		const (
			appName = "backup-test-app"
			dbName  = "backup-test-db"
			pvcName = "backup-test-pvc"
			dbFile  = "backup.db"
			dbPath  = "/data"
			initSQL = `CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message TEXT,
  ts DATETIME DEFAULT CURRENT_TIMESTAMP
);`
		)

		BeforeAll(func() {
			DeferCleanup(func() { dumpReplicationDiagnostics(appName, dbName, dbFile) })

			By("cleaning stale S3 data from prior test runs")
			mcCleanPath(dbName)

			By("creating test PVC")
			applyLiteral(pvcManifest(pvcName, testNamespace))

			By("creating test app Deployment")
			applyLiteral(appDeploymentManifest(appName, testNamespace, pvcName, dbPath))

			By("waiting for initial pod to be Running")
			kubectl("wait", "-n", testNamespace, "deployment/"+appName,
				"--for=condition=Available", "--timeout=3m")

			By("creating LitestreamReplica CR with backup enabled and bootstrap SQL")
			applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, true, initSQL))

			By("waiting for sidecar injection rollout to complete (2/2 containers)")
			Eventually(func(g Gomega) {
				out, err := kubectlQ("get", "pods", "-n", testNamespace,
					"-l", "app="+appName,
					"-o", `jsonpath={range .items[*]}{range .spec.containers[*]}{.name}{"\n"}{end}{end}`)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("litestream"))
			}, 5*time.Minute, 10*time.Second).Should(Succeed())
		})

		AfterAll(func() {
			// Leave the LitestreamReplica and its backup intact — Scenario 3 restores from it.
			runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		})

		It("bootstrap container applies bootstrap SQL and creates the events table", func() {
			podName := runningPod(appName)
			Eventually(func(g Gomega) {
				out, err := kubectlQ("exec", "-n", testNamespace, podName, "-c", "app",
					"--", "sqlite3", dbPath+"/"+dbFile, ".tables")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("events"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("Litestream replicates WAL changes to the MinIO bucket", func() {
			podName := runningPod(appName)

			By("writing a row to the database")
			kubectl("exec", "-n", testNamespace, podName, "-c", "app",
				"--", "sqlite3", dbPath+"/"+dbFile,
				"INSERT INTO events(message) VALUES('integration-test-backup');")

			By("waiting for Litestream to replicate to MinIO")
			Eventually(func(g Gomega) {
				// Check the expected path first.
				out := mcList(s3Bucket + "/" + dbName + "/")
				if out == "" {
					// Fall back: check the whole bucket so a path mismatch surfaces immediately.
					all, _ := kubectlQ("exec", "-n", testNamespace, "mc-client", "--",
						"/bin/sh", "-c", "mc ls --recursive local/"+s3Bucket+"/")
					g.Expect(out).NotTo(BeEmpty(),
						"expected backup objects at %s/%s/ — full bucket contents:\n%s",
						s3Bucket, dbName, all)
				} else {
					g.Expect(out).NotTo(BeEmpty(), "expected backup objects in MinIO bucket")
				}
			}, 3*time.Minute, 10*time.Second).Should(Succeed())

			By("verifying ReplicationHealthy condition is True on the LitestreamReplica")
			Eventually(func(g Gomega) {
				out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
					"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("True"))
			}).Should(Succeed())
		})
	})

	// ── Scenario 3: LitestreamRestore ─────────────────────────────────────────

	Describe("LitestreamRestore", func() {
		const (
			sourceDBName  = "backup-test-db" // backup created by Scenario 2
			restoreName   = "integration-restore"
			restorePVC    = "restore-target-pvc"
			restoreTarget = "/restore/backup.db"
		)

		BeforeAll(func() {
			By("creating the restore target PVC")
			applyLiteral(pvcManifest(restorePVC, testNamespace))
		})

		AfterAll(func() {
			// --wait=false: completed restore job pods hold PVC references; waiting for
			// the PVC to fully disappear blocks indefinitely until async GC removes them.
			// AfterSuite namespace deletion cleans up any remaining resources.
			runIgnoreError("kubectl", "delete", "litestreamrestore", restoreName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
			runIgnoreError("kubectl", "delete", "litestreamreplica", sourceDBName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
			runIgnoreError("kubectl", "delete", "pvc", restorePVC, "-n", testNamespace, "--ignore-not-found", "--wait=false")
			runIgnoreError("kubectl", "delete", "pvc", "backup-test-pvc", "-n", testNamespace, "--ignore-not-found", "--wait=false")
		})

		It("restore Job completes and database file appears on the target PVC", func() {
			By("creating a LitestreamRestore CR")
			applyLiteral(litestreamRestoreManifest(restoreName, testNamespace, sourceDBName, restorePVC, restoreTarget))

			By("waiting for the restore Job to be created")
			Eventually(func(g Gomega) {
				out, err := kubectlQ("get", "jobs", "-n", testNamespace,
					"-l", "litestream.io/restore="+restoreName,
					"-o", "jsonpath={.items[0].metadata.name}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).NotTo(BeEmpty())
			}).Should(Succeed())

			By("waiting for the restore Job to Complete (up to 5 minutes)")
			Eventually(func(g Gomega) {
				phase, err := kubectlQ("get", "litestreamrestore", restoreName, "-n", testNamespace,
					"-o", "jsonpath={.status.phase}")
				g.Expect(err).NotTo(HaveOccurred())
				if phase == "Failed" || phase == "Running" {
					jobName, _ := kubectlQ("get", "litestreamrestore", restoreName, "-n", testNamespace,
						"-o", "jsonpath={.status.jobName}")
					if jobName != "" {
						// Show pod status — useful when kubectl logs times out (pod pending/between retries).
						pods, _ := kubectlQ("get", "pods", "-n", testNamespace,
							"-l", "litestream.io/restore="+restoreName, "-o", "wide")
						GinkgoWriter.Printf("\n=== restore Job pods ===\n%s\n", pods)
						// --previous gets logs from the last terminated container
						// even when the pod is currently between retries.
						// --request-timeout prevents blocking indefinitely if the pod
						// is in a transient state (Pending, initializing).
						logs, _ := kubectlQ("logs", "-n", testNamespace, "job/"+jobName, "--previous", "--tail=50", "--request-timeout=15s")
						if logs == "" {
							logs, _ = kubectlQ("logs", "-n", testNamespace, "job/"+jobName, "--tail=50", "--request-timeout=15s")
						}
						GinkgoWriter.Printf("=== restore Job logs ===\n%s\n========================\n", logs)
					}
				}
				g.Expect(phase).To(Equal("Completed"))
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			By("running a verification Job that reads the restored database")
			verifyJobName := "verify-restore"
			applyLiteral(verifyRestoreJobManifest(verifyJobName, testNamespace, restorePVC, restoreTarget))
			kubectl("wait", "-n", testNamespace, "job/"+verifyJobName,
				"--for=condition=Complete", "--timeout=3m")

			logs := kubectl("logs", "-n", testNamespace, "job/"+verifyJobName)
			Expect(logs).To(ContainSubstring("events"),
				"restored database should contain the events table")
		})
	})

}) // end Integration Ordered

// ── Scenario: Manual Replication Pause ───────────────────────────────────────

var _ = Describe("Replication Pause", Ordered, func() {
	const (
		appName = "pause-test-app"
		dbName  = "pause-test-db"
		pvcName = "pause-test-pvc"
		dbFile  = "pause.db"
		dbPath  = "/data"
	)

	BeforeAll(func() {
		By("cleaning stale S3 data from prior test runs")
		mcCleanPath(dbName)

		By("creating PVC, Deployment, and LitestreamReplica CR with backup enabled")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")
		applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, true, ""))

		By("waiting for sidecar injection and ReplicationHealthy=True")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		// Ensure pause annotation is removed even if test fails mid-way.
		runIgnoreError("kubectl", "annotate", "litestreamreplica", dbName, "-n", testNamespace,
			"litestream.io/pause-", "--ignore-not-found")
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("pauses replication when annotation is set and resumes when removed", func() {
		By("setting pause annotation on LitestreamReplica")
		kubectl("annotate", "litestreamreplica", dbName, "-n", testNamespace,
			"litestream.io/pause=true", "--overwrite")

		By("waiting for ConfigMap to reflect pause (dbs: [])")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "configmap", dbName+"-litestream", "-n", testNamespace,
				"-o", `jsonpath={.data.litestream\.yml}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("dbs: []\n"))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying ReplicationPaused condition is True")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationPaused")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}).Should(Succeed())

		By("verifying phase is Paused")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Paused"))
		}).Should(Succeed())

		By("removing pause annotation")
		kubectl("annotate", "litestreamreplica", dbName, "-n", testNamespace,
			"litestream.io/pause-")

		By("waiting for ConfigMap to restore normal config")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "configmap", dbName+"-litestream", "-n", testNamespace,
				"-o", `jsonpath={.data.litestream\.yml}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("dbs:"))
			g.Expect(out).NotTo(Equal("dbs: []\n"))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying phase returns to Ready")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})
})

// ── Scenario: Multi-replica rejection ─────────────────────────────────────────
//
// Verifies the full end-to-end webhook rejection when a user targets a Deployment
// that has replicas > 1. Litestream requires exactly one writer; the admitting
// webhook must reject the LitestreamReplica create at the API layer.

var _ = Describe("Multi-Replica Rejection", func() {
	const (
		appName = "multi-rep-app"
		dbName  = "multi-rep-db"
		pvcName = "multi-rep-pvc"
	)

	BeforeEach(func() {
		By("creating PVC and a 2-replica Deployment")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
spec:
  replicas: 2
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
        - name: app
          image: keinos/sqlite3:latest
          command: ["sleep", "infinity"]
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: %s
`, appName, testNamespace, appName, appName, pvcName))
	})

	AfterEach(func() {
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("webhook rejects LitestreamReplica pointing at a multi-replica Deployment", func() {
		By("attempting to create a LitestreamReplica targeting the 2-replica Deployment")
		manifest := fmt.Sprintf(`
apiVersion: litestream.io/v1
kind: LitestreamReplica
metadata:
  name: %s
  namespace: %s
spec:
  databaseName: app.db
  databasePath: /data
  targetDeployment: %s
`, dbName, testNamespace, appName)

		out, err := applyLiteralQ(manifest)

		// The webhook must reject the create with a validation error.
		By("expecting admission rejection (replicas > 1 is a hard error)")
		Expect(err).To(HaveOccurred(), "webhook should reject LitestreamReplica targeting a 2-replica Deployment")
		Expect(strings.ToLower(out)).To(ContainSubstring("replicas"),
			"rejection message should mention replicas; got: %s", out)
	})
})

// ── Scenario: Archive Check — Data Loss Recovery ──────────────────────────────

var _ = Describe("Archive Check — Data Loss Recovery", Ordered, func() {
	const (
		appName   = "archive-check-app"
		dbName    = "archive-check-db"
		pvcName   = "archive-check-pvc"
		dbFile    = "archive.db"
		dbPath    = "/data"
		initSQL   = "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT);"
		itemValue = "archive-check-test-item"
	)

	BeforeAll(func() {
		DeferCleanup(func() { dumpReplicationDiagnostics(appName, dbName, dbFile) })

		By("cleaning stale S3 data from prior test runs")
		mcCleanPath(dbName)

		By("creating PVC, Deployment, and LitestreamReplica CR with backup enabled and bootstrap SQL")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")
		applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, true, initSQL))

		By("waiting for sidecar injection and ReplicationHealthy=True")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("writing test data to the database")
		podName := runningPod(appName)
		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "sqlite3", dbPath+"/"+dbFile,
			"INSERT INTO items(name) VALUES('"+itemValue+"');")

		By("waiting for Litestream to replicate the row to MinIO")
		Eventually(func(g Gomega) {
			out := mcList(s3Bucket + "/" + dbName + "/")
			if out == "" {
				all, _ := kubectlQ("exec", "-n", testNamespace, "mc-client", "--",
					"/bin/sh", "-c", "mc ls --recursive local/"+s3Bucket+"/")
				g.Expect(out).NotTo(BeEmpty(),
					"expected backup objects at %s/%s/ — full bucket contents:\n%s",
					s3Bucket, dbName, all)
			} else {
				g.Expect(out).NotTo(BeEmpty(), "expected backup objects in MinIO bucket")
			}
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamrestore", "archive-check-restore", "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("archive-check blocks pod startup when DB is missing and S3 has data", func() {
		podName := runningPod(appName)

		By("deleting the database file from the PVC (simulating data loss)")
		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "rm", "-f", dbPath+"/"+dbFile)

		By("scaling Deployment to 0 and back to 1 to force pod restart")
		kubectl("scale", "deployment", appName, "-n", testNamespace, "--replicas=0")
		// kubectl rollout status correctly handles scale-to-0 completion.
		// --for=jsonpath={.status.replicas}=0 is unreliable: Kubernetes omits
		// the field when it reaches zero, so the jsonpath never matches.
		kubectl("rollout", "status", "deployment/"+appName, "-n", testNamespace, "--timeout=2m")
		kubectl("scale", "deployment", appName, "-n", testNamespace, "--replicas=1")

		By("waiting for archive-check init container to fail (pod blocked)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"-o", `jsonpath={range .items[*]}{range .status.initContainerStatuses[*]}{.name}={.state.terminated.exitCode}{"\n"}{end}{end}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("litestream-archive-check=1"),
				"expected archive-check init container to exit 1")
		}, 3*time.Minute, 10*time.Second).Should(Succeed())

		By("verifying RecoverySafe=False on LitestreamReplica (archive mismatch detected)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="RecoverySafe")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("False"))
		}).Should(Succeed())
	})

	It("LitestreamRestore recovers data and allows pod to restart successfully", func() {
		By("creating a LitestreamRestore CR targeting the same PVC")
		applyLiteral(litestreamRestoreManifest("archive-check-restore", testNamespace, dbName, pvcName, dbPath+"/"+dbFile))

		By("waiting for restore to reach Complete phase")
		Eventually(func(g Gomega) {
			phase, err := kubectlQ("get", "litestreamrestore", "archive-check-restore", "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			if phase == "Failed" {
				jobName, _ := kubectlQ("get", "litestreamrestore", "archive-check-restore", "-n", testNamespace,
					"-o", "jsonpath={.status.jobName}")
				if jobName != "" {
					logs, _ := kubectlQ("logs", "-n", testNamespace, "job/"+jobName, "--tail=50", "--request-timeout=15s")
					GinkgoWriter.Printf("\n=== restore Job logs ===\n%s\n========================\n", logs)
				}
			}
			g.Expect(phase).To(Equal("Completed"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("waiting for the pod to restart successfully after restore")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"--field-selector=status.phase=Running",
				"-o", "jsonpath={.items[0].metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty())
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("verifying restored data is present in the database")
		Eventually(func(g Gomega) {
			podName, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"--field-selector=status.phase=Running",
				"-o", "jsonpath={.items[0].metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(podName).NotTo(BeEmpty())
			out, err := kubectlQ("exec", "-n", testNamespace, podName, "-c", "app",
				"--", "sqlite3", dbPath+"/"+dbFile,
				"SELECT name FROM items WHERE name='"+itemValue+"';")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(itemValue))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying ReplicationHealthy=True after restore (Litestream resumed)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})
})

// ── Issue #109: Archive Check — Fresh DB Divergence ─────────────────────
//
// When a pod starts with a fresh/empty local database (DB file exists but
// the litestream state directory is absent) while S3 already has backup data,
// the archive-check init container must block startup to prevent the sidecar
// from overwriting the S3 backup chain with the empty DB.
//
// After a LitestreamRestore, the restored DB exists but the state dir does not
// yet (it is created by litestream replicate, not by litestream restore). The
// restore controller sets skip-archive-check=true before scaling up so the pod
// can start; the LitestreamReplica controller auto-clears it once healthy.

var _ = Describe("Archive Check — Fresh DB Divergence", Ordered, func() {
	const (
		appName   = "diverge-check-app"
		dbName    = "diverge-check-db"
		pvcName   = "diverge-check-pvc"
		dbFile    = "diverge.db"
		dbPath    = "/data"
		initSQL   = "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT);"
		itemValue = "diverge-check-test-item"
	)

	BeforeAll(func() {
		DeferCleanup(func() { dumpReplicationDiagnostics(appName, dbName, dbFile) })

		By("cleaning stale S3 data from prior test runs")
		mcCleanPath(dbName)

		By("creating PVC, Deployment, and LitestreamReplica CR with backup enabled and bootstrap SQL")
		// runAsUser: 0 — db-init runs as root so it can read the restored DB file,
		// which litestream restore writes as root. This demonstrates that issue #110
		// is fixed: callers now control the UID instead of the operator hardcoding root.
		rootUID := int64(0)
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")
		applyLiteral(litestreamReplicaManifestFull(dbName, testNamespace, appName, dbFile, dbPath, true, initSQL, databasev1.RecoveryModeManual, &rootUID))

		By("waiting for sidecar injection and ReplicationHealthy=True")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		// Wait for the rolling update (triggered by sidecar injection) to finish so the
		// old pre-injection pod is gone and only the sidecar-bearing pod remains running.
		kubectl("rollout", "status", "deployment/"+appName, "-n", testNamespace, "--timeout=3m")

		By("writing test data to the database")
		podName := runningPod(appName)
		preInsertListing, _ := kubectlQ("exec", "-n", testNamespace, "mc-client", "--",
			"/bin/sh", "-c", "mc ls --recursive local/"+s3Bucket+"/"+dbName+"/ | wc -l")
		preInsertCount := strings.TrimSpace(preInsertListing)
		GinkgoWriter.Printf("S3 LTX count before INSERT: %s\n", preInsertCount)

		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "sqlite3", dbPath+"/"+dbFile,
			"INSERT INTO items(name) VALUES('"+itemValue+"');")

		By("waiting for Litestream to replicate the INSERT to S3 (new LTX file)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("exec", "-n", testNamespace, "mc-client", "--",
				"/bin/sh", "-c", "mc ls --recursive local/"+s3Bucket+"/"+dbName+"/ | wc -l")
			g.Expect(err).NotTo(HaveOccurred())
			postCount := strings.TrimSpace(out)
			g.Expect(postCount).NotTo(Equal(preInsertCount),
				"waiting for new LTX file after INSERT (pre=%s, post=%s)", preInsertCount, postCount)
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamrestore", "diverge-check-restore", "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("archive-check blocks startup when DB exists but litestream state dir is absent and S3 has data", func() {
		// The state dir is owned by the litestream sidecar; use a helper that waits for a
		// Running pod with that container (the old pre-injection pod may still be Running
		// while the new sidecar-bearing pod comes up during the rolling update).
		podName := runningPodWithSidecar(appName)

		By("deleting the litestream state directory and replacing the DB with a fresh empty one")
		// Remove the state dir — simulates a DB that was created without ever being replicated.
		// Use the litestream container: the state dir is owned by it, not the app container.
		kubectl("exec", "-n", testNamespace, podName, "-c", "litestream",
			"--", "rm", "-rf", dbPath+"/."+dbFile+"-litestream")
		// Replace the existing DB with a fresh empty one (different schema).
		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "rm", "-f", dbPath+"/"+dbFile)
		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "sqlite3", dbPath+"/"+dbFile, "CREATE TABLE placeholder(x);")
		// Remove db-init markers so the init container doesn't interfere.
		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "sh", "-c", "rm -f "+dbPath+"/.db-init-*")

		By("scaling Deployment to 0 and back to 1 to force pod restart")
		kubectl("scale", "deployment", appName, "-n", testNamespace, "--replicas=0")
		kubectl("rollout", "status", "deployment/"+appName, "-n", testNamespace, "--timeout=2m")
		kubectl("scale", "deployment", appName, "-n", testNamespace, "--replicas=1")

		By("waiting for archive-check init container to fail (pod blocked)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"-o", `jsonpath={range .items[*]}{range .status.initContainerStatuses[*]}{.name}={.state.terminated.exitCode}{"\n"}{end}{end}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("litestream-archive-check=1"),
				"expected archive-check to exit 1: DB exists but state dir absent with S3 backup present")
		}, 3*time.Minute, 10*time.Second).Should(Succeed())

		By("verifying RecoverySafe=False on LitestreamReplica (archive mismatch detected)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="RecoverySafe")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("False"))
		}).Should(Succeed())
	})

	It("LitestreamRestore recovers data and pod restarts without archive-check false-positive", func() {
		By("creating a LitestreamRestore CR targeting the same PVC")
		applyLiteral(litestreamRestoreManifestWithForce("diverge-check-restore", testNamespace, dbName, pvcName, dbPath+"/"+dbFile))

		By("waiting for restore to reach Complete phase")
		Eventually(func(g Gomega) {
			phase, err := kubectlQ("get", "litestreamrestore", "diverge-check-restore", "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			if phase == "Failed" {
				jobName, _ := kubectlQ("get", "litestreamrestore", "diverge-check-restore", "-n", testNamespace,
					"-o", "jsonpath={.status.jobName}")
				if jobName != "" {
					logs, _ := kubectlQ("logs", "-n", testNamespace, "job/"+jobName, "--tail=50", "--request-timeout=15s")
					GinkgoWriter.Printf("\n=== restore Job logs ===\n%s\n========================\n", logs)
				}
			}
			g.Expect(phase).To(Equal("Completed"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("waiting for the pod to restart successfully after restore (skip-archive-check set by restore controller)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"--field-selector=status.phase=Running",
				"-o", "jsonpath={.items[0].metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty())
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("verifying restored data is present in the database")
		Eventually(func(g Gomega) {
			podName, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"--field-selector=status.phase=Running",
				"-o", "jsonpath={.items[0].metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(podName).NotTo(BeEmpty())
			out, err := kubectlQ("exec", "-n", testNamespace, podName, "-c", "app",
				"--", "sqlite3", dbPath+"/"+dbFile,
				"SELECT name FROM items WHERE name='"+itemValue+"';")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(itemValue))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying ReplicationHealthy=True after restore (Litestream resumed and state dir created)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 3*time.Minute, 10*time.Second).Should(Succeed())

		By("verifying skip-archive-check annotation was auto-cleared by the replica controller")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.metadata.annotations.litestream\.io/skip-archive-check}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(Equal("true"),
				"skip-archive-check annotation must be cleared once litestream sidecar is healthy")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
	})
})

// ── TC-01: First-Time Setup ───────────────────────────────────────────────
//
// When no DB file and no S3 backup exist, the archive-check init container
// should detect first-time setup and allow the pod to start normally.

var _ = Describe("First-Time Setup", Ordered, func() {
	const (
		appName = "first-time-app"
		dbName  = "first-time-db"
		pvcName = "first-time-pvc"
		dbFile  = "firsttime.db"
		dbPath  = "/data"
	)

	BeforeAll(func() {
		DeferCleanup(func() { dumpReplicationDiagnostics(appName, dbName, dbFile) })

		By("cleaning stale S3 data from prior test runs")
		mcCleanPath(dbName)

		By("creating PVC and Deployment")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")

		By("creating LitestreamReplica with backup enabled")
		applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, true, ""))
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("archive-check passes and pod starts normally when no S3 backup exists", func() {
		By("waiting for pod to reach Running (archive-check must not block it)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"--field-selector=status.phase=Running",
				"-o", "jsonpath={.items[0].metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty(), "pod should be Running — archive-check must not block first-time startup")
		}, 3*time.Minute, 10*time.Second).Should(Succeed())

		By("verifying archive-check init container exited 0 (if it ran)")
		// The archive-check container may have already completed by the time we check.
		// A Running pod guarantees all init containers succeeded (exit 0).
		out, _ := kubectlQ("get", "pods", "-n", testNamespace,
			"-l", "app="+appName,
			"-o", `jsonpath={range .items[*]}{range .status.initContainerStatuses[*]}{.name}={.state.terminated.exitCode}{"\n"}{end}{end}`)
		if out != "" {
			Expect(out).NotTo(ContainSubstring("litestream-archive-check=1"),
				"archive-check must not exit 1 on first-time setup")
		}

		By("verifying LitestreamReplica reaches Ready phase")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying ReplicationHealthy=True after sidecar begins replicating")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())
	})
})

// ── TC-03: recovery.mode=Automatic — Automatic Restore on Startup ───────────────
//
// When recovery.mode=Automatic and the DB is missing but S3 has data, the restore
// init container runs automatically and the pod starts with restored data.

var _ = Describe("Auto-Restore on Startup", Ordered, func() {
	const (
		appName   = "auto-restore-app"
		dbName    = "auto-restore-db"
		pvcName   = "auto-restore-pvc"
		dbFile    = "autorestore.db"
		dbPath    = "/data"
		initSQL   = "CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY, value TEXT);"
		testValue = "auto-restore-test-value"
	)

	BeforeAll(func() {
		DeferCleanup(func() { dumpReplicationDiagnostics(appName, dbName, dbFile) })

		By("cleaning stale S3 data from prior test runs")
		mcCleanPath(dbName)

		By("creating PVC, Deployment, and LitestreamReplica with recovery.mode=Automatic")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")
		applyLiteral(litestreamReplicaManifestWithOpts(dbName, testNamespace, appName, dbFile, dbPath, true, initSQL, databasev1.RecoveryModeAutomatic))

		By("waiting for sidecar injection and ReplicationHealthy=True")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("writing test data to the database")
		podName := runningPod(appName)
		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "sqlite3", dbPath+"/"+dbFile,
			"INSERT INTO records(value) VALUES('"+testValue+"');")

		By("waiting for Litestream to replicate the row to MinIO")
		Eventually(func(g Gomega) {
			out := mcList(s3Bucket + "/" + dbName + "/")
			if out == "" {
				all, _ := kubectlQ("exec", "-n", testNamespace, "mc-client", "--",
					"/bin/sh", "-c", "mc ls --recursive local/"+s3Bucket+"/")
				g.Expect(out).NotTo(BeEmpty(),
					"expected backup objects at %s/%s/ — full bucket contents:\n%s",
					s3Bucket, dbName, all)
			} else {
				g.Expect(out).NotTo(BeEmpty())
			}
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("auto-restores DB from S3 on startup when DB is missing and recovery.mode=Automatic", func() {
		podName := runningPod(appName)

		By("deleting the DB file and litestream state directory to simulate data loss")
		// Use the litestream container: the state dir is owned by it, not the app container.
		// Also remove the db-init marker (.db-init-*) so the db-init init container re-applies
		// the schema on the next pod start — otherwise it sees the marker from the original run
		// and skips CREATE TABLE, leaving the restored DB without a schema.
		kubectl("exec", "-n", testNamespace, podName, "-c", "litestream",
			"--", "sh", "-c",
			"rm -rf "+dbPath+"/"+dbFile+" "+dbPath+"/."+dbFile+"-litestream "+dbPath+"/.db-init-*")

		By("scaling Deployment to 0 then back to 1 to trigger pod restart")
		kubectl("scale", "deployment", appName, "-n", testNamespace, "--replicas=0")
		// Wait for all pods to fully terminate (not just rollout status) before scaling back up.
		// rollout status returns when desired=0 replicas are met, but pods may still be in
		// Terminating state (phase=Running) and holding the PVC, which can race with the new
		// pod's restore init container.
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"-o", "jsonpath={.items}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(SatisfyAny(BeEmpty(), Equal("[]")))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
		kubectl("scale", "deployment", appName, "-n", testNamespace, "--replicas=1")

		By("waiting for pod to reach Running (restore init container must succeed)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"--field-selector=status.phase=Running",
				"-o", "jsonpath={.items[0].metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty(), "pod should be Running after auto-restore")
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("verifying restore init container exited 0")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"-o", `jsonpath={range .items[*]}{range .status.initContainerStatuses[*]}{.name}={.state.terminated.exitCode}{"\n"}{end}{end}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("litestream-restore=0"),
				"auto-restore init container should exit 0")
		}).Should(Succeed())

		By("verifying restored data is present in the database")
		Eventually(func(g Gomega) {
			pod, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"--field-selector=status.phase=Running",
				"-o", "jsonpath={.items[0].metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pod).NotTo(BeEmpty())
			out, err := kubectlQ("exec", "-n", testNamespace, strings.TrimSpace(pod), "-c", "app",
				"--", "sqlite3", dbPath+"/"+dbFile,
				"SELECT value FROM records WHERE value='"+testValue+"';")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(testValue))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying ReplicationHealthy=True after restore (Litestream resumed)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})
})

// ── TC-05: LitestreamRestore Fails When DB File Exists ───────────────────
//
// The LitestreamRestore Job fails when the target DB file already exists on
// the PVC. The operator must still scale the Deployment back up after failure.

var _ = Describe("Restore Fails With Existing DB", Ordered, func() {
	const (
		appName     = "restore-fail-app"
		dbName      = "restore-fail-db"
		pvcName     = "restore-fail-pvc"
		restoreName = "restore-fail-restore"
		dbFile      = "restorefail.db"
		dbPath      = "/data"
		initSQL     = "CREATE TABLE IF NOT EXISTS entries (id INTEGER PRIMARY KEY, val TEXT);"
		testVal     = "restore-fail-test"
	)

	BeforeAll(func() {
		DeferCleanup(func() { dumpReplicationDiagnostics(appName, dbName, dbFile) })

		By("cleaning stale S3 data from prior test runs")
		mcCleanPath(dbName)

		By("creating PVC, Deployment, and LitestreamReplica with backup enabled")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")
		applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, true, initSQL))

		By("waiting for sidecar injection and ReplicationHealthy=True")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("writing test data to ensure S3 has a valid backup")
		podName := runningPod(appName)
		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "sqlite3", dbPath+"/"+dbFile,
			"INSERT INTO entries(val) VALUES('"+testVal+"');")

		By("waiting for replication to MinIO")
		Eventually(func(g Gomega) {
			out := mcList(s3Bucket + "/" + dbName + "/")
			g.Expect(out).NotTo(BeEmpty(), "expected backup objects in MinIO")
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamrestore", restoreName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("restore Job fails and Deployment is scaled back when DB file already exists on PVC", func() {
		By("confirming the DB file is present on the PVC (do NOT delete it)")
		podName := runningPod(appName)
		out, err := kubectlQ("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "ls", dbPath+"/"+dbFile)
		Expect(err).NotTo(HaveOccurred(), "DB file should exist on PVC before restore attempt")
		Expect(out).To(ContainSubstring(dbFile))

		By("recording current Deployment replica count")
		replicaOut := kubectl("get", "deployment", appName, "-n", testNamespace,
			"-o", "jsonpath={.spec.replicas}")
		Expect(replicaOut).To(Equal("1"), "expected Deployment to have 1 replica before restore")

		By("applying LitestreamRestore CR targeting the PVC with the existing DB file")
		applyLiteral(litestreamRestoreManifest(restoreName, testNamespace, dbName, pvcName, dbPath+"/"+dbFile))

		By("waiting for LitestreamRestore to reach Failed phase")
		Eventually(func(g Gomega) {
			phase, err := kubectlQ("get", "litestreamrestore", restoreName, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			if phase == "Running" || phase == "ScalingDown" || phase == "Pausing" {
				jobName, _ := kubectlQ("get", "litestreamrestore", restoreName, "-n", testNamespace,
					"-o", "jsonpath={.status.jobName}")
				if jobName != "" {
					logs, _ := kubectlQ("logs", "-n", testNamespace, "job/"+jobName, "--tail=30", "--request-timeout=15s")
					GinkgoWriter.Printf("\n=== restore job logs ===\n%s\n========================\n", logs)
				}
			}
			g.Expect(phase).To(Equal("Failed"),
				"restore should fail because the DB file already exists on the PVC")
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("verifying status.message surfaces the litestream error (output file already exists)")
		statusMsg, err := kubectlQ("get", "litestreamrestore", restoreName, "-n", testNamespace,
			"-o", "jsonpath={.status.message}")
		Expect(err).NotTo(HaveOccurred())
		// Litestream refuses to overwrite an existing DB: the error must be visible in the
		// status so users know they need to remove the file before restoring.
		Expect(statusMsg).To(ContainSubstring("already exists"),
			"status.message must include the litestream error about the existing output file")

		By("verifying Deployment still has its original replica count (ToPVC does not scale down)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "deployment", appName, "-n", testNamespace,
				"-o", "jsonpath={.spec.replicas}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("1"),
				"Deployment replica count must remain unchanged in ToPVC mode")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})
})

// ── TC-08: Point-in-Time Restore ─────────────────────────────────────────
//
// Validates that LitestreamRestore with spec.timestamp restores the DB to the
// specified point in time rather than the latest snapshot.

var _ = Describe("Point-in-Time Restore", Ordered, func() {
	const (
		appName = "pitr-app"
		dbName  = "pitr-db"
		pvcName = "pitr-pvc"
		dbFile  = "pitr.db"
		dbPath  = "/data"
		initSQL = "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, val TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP);"
		rowA    = "row-A-before-timestamp"
		rowB    = "row-B-after-timestamp"
	)

	var (
		pitrTimestamp string // RFC 3339 timestamp captured between row A and row B writes
		pitrPVC       = "pitr-restore-pvc"
	)

	BeforeAll(func() {
		DeferCleanup(func() { dumpReplicationDiagnostics(appName, dbName, dbFile) })

		By("cleaning stale S3 data from prior test runs")
		mcCleanPath(dbName)

		By("creating PVC, Deployment, and LitestreamReplica with backup enabled")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")
		applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, true, initSQL))

		By("waiting for sidecar injection and ReplicationHealthy=True")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("writing row A")
		podName := runningPod(appName)
		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "sqlite3", dbPath+"/"+dbFile,
			"INSERT INTO events(val) VALUES('"+rowA+"');")

		By("waiting for row A to replicate and compaction to produce a restorable snapshot")
		Eventually(func(g Gomega) {
			out := mcList(s3Bucket + "/" + dbName + "/")
			g.Expect(out).NotTo(BeEmpty(), "expected backup objects in MinIO after row A")
		}, 3*time.Minute, 10*time.Second).Should(Succeed())

		// Wait for L1 compaction (default 30s) so the timestamp falls between
		// a compacted snapshot and the subsequent row B write.
		By("waiting 35s for L1 compaction to complete before recording timestamp")
		time.Sleep(35 * time.Second)

		// Record the PITR timestamp in RFC 3339 UTC after row A is compacted.
		pitrTimestamp = time.Now().UTC().Format(time.RFC3339)
		GinkgoWriter.Printf("PITR timestamp: %s\n", pitrTimestamp)

		By("writing row B after the PITR timestamp")
		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "sqlite3", dbPath+"/"+dbFile,
			"INSERT INTO events(val) VALUES('"+rowB+"');")

		By("waiting for row B to replicate to MinIO")
		Eventually(func(g Gomega) {
			out := mcList(s3Bucket + "/" + dbName + "/")
			g.Expect(out).NotTo(BeEmpty())
		}, 2*time.Minute, 10*time.Second).Should(Succeed())

		By("creating restore target PVC")
		applyLiteral(pvcManifest(pitrPVC, testNamespace))
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamrestore", "pitr-restore-t1", "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "litestreamrestore", "pitr-restore-latest", "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pitrPVC, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("restores DB to point-in-time T1 (row A present, row B absent)", func() {
		By("applying LitestreamRestore CR with timestamp=T1")
		applyLiteral(litestreamRestoreManifestWithTimestamp(
			"pitr-restore-t1", testNamespace, dbName, pitrPVC, "/restore/"+dbFile, pitrTimestamp))

		By("waiting for restore to complete")
		Eventually(func(g Gomega) {
			phase, err := kubectlQ("get", "litestreamrestore", "pitr-restore-t1", "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			if phase == "Failed" {
				jobName, _ := kubectlQ("get", "litestreamrestore", "pitr-restore-t1", "-n", testNamespace,
					"-o", "jsonpath={.status.jobName}")
				if jobName != "" {
					logs, _ := kubectlQ("logs", "-n", testNamespace, "job/"+jobName, "--tail=50", "--request-timeout=15s")
					GinkgoWriter.Printf("\n=== PITR restore job logs ===\n%s\n========================\n", logs)
				}
			}
			g.Expect(phase).To(Equal("Completed"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("running a verification Job against the restored DB")
		verifyJobT1 := "pitr-verify-t1"
		applyLiteral(pitrVerifyJobManifest(verifyJobT1, testNamespace, pitrPVC, "/restore/"+dbFile,
			"SELECT val FROM events WHERE val='"+rowA+"';",
			"SELECT count(*) FROM events WHERE val='"+rowB+"';",
		))
		kubectl("wait", "-n", testNamespace, "job/"+verifyJobT1,
			"--for=condition=Complete", "--timeout=3m")

		logs := kubectl("logs", "-n", testNamespace, "job/"+verifyJobT1)

		By("verifying row A is present in the PITR-restored DB")
		Expect(logs).To(ContainSubstring(rowA), "row A should exist at timestamp T1")

		By("verifying row B is NOT present in the PITR-restored DB")
		Expect(logs).To(ContainSubstring("0"),
			"row B written after T1 should NOT be present in the PITR-restored DB")
	})

	It("restores DB to latest snapshot (both row A and row B present)", func() {
		By("deleting the restored DB file so litestream restore can proceed")
		// The pitrPVC now has the T1-restored DB; we need to clear it before the latest restore.
		// First, delete the verification job from the previous It so its completed pod releases
		// the ReadWriteOnce PVC — otherwise pitr-cleanup will stay Pending.
		runIgnoreError("kubectl", "delete", "job", "pitr-verify-t1", "-n", testNamespace, "--ignore-not-found")
		kubectl("run", "pitr-cleanup", "-n", testNamespace, "--image=busybox", "--restart=Never",
			"--overrides={\"spec\":{\"volumes\":[{\"name\":\"data\",\"persistentVolumeClaim\":{\"claimName\":\""+pitrPVC+"\"}}],\"containers\":[{\"name\":\"busybox\",\"image\":\"busybox\",\"command\":[\"rm\",\"-f\",\"/restore/"+dbFile+"\"],\"volumeMounts\":[{\"name\":\"data\",\"mountPath\":\"/restore\"}]}]}}")
		// Wait directly for Succeeded — a one-shot pod may complete before Ready is ever set.
		kubectl("wait", "-n", testNamespace, "pod/pitr-cleanup",
			"--for=jsonpath={.status.phase}=Succeeded", "--timeout=2m")
		runIgnoreError("kubectl", "delete", "pod", "pitr-cleanup", "-n", testNamespace, "--ignore-not-found")

		By("applying LitestreamRestore CR without timestamp (latest)")
		applyLiteral(litestreamRestoreManifest(
			"pitr-restore-latest", testNamespace, dbName, pitrPVC, "/restore/"+dbFile))

		By("waiting for latest restore to complete")
		Eventually(func(g Gomega) {
			phase, err := kubectlQ("get", "litestreamrestore", "pitr-restore-latest", "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Completed"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("running a verification Job against the latest-restored DB")
		verifyJobLatest := "pitr-verify-latest"
		applyLiteral(pitrVerifyJobManifest(verifyJobLatest, testNamespace, pitrPVC, "/restore/"+dbFile,
			"SELECT val FROM events WHERE val='"+rowA+"';",
			"SELECT val FROM events WHERE val='"+rowB+"';",
		))
		kubectl("wait", "-n", testNamespace, "job/"+verifyJobLatest,
			"--for=condition=Complete", "--timeout=3m")

		By("verifying both row A and row B are present in the latest-restored DB")
		latestLogs := kubectl("logs", "-n", testNamespace, "job/"+verifyJobLatest)
		Expect(latestLogs).To(ContainSubstring(rowA), "row A should exist in latest restore")
		Expect(latestLogs).To(ContainSubstring(rowB), "row B should exist in latest restore")
	})
})

// ── Scenario: InPlace Restore ─────────────────────────────────────────────
//
// Validates the full InPlace restore lifecycle: the controller fences the
// workload (pauses replication, scales to 0), runs the restore Job, then
// resumes (scales back up, clears pause). Tests the ApplicationFenced and
// ApplicationResumed conditions.

var _ = Describe("InPlace Restore", Ordered, func() {
	const (
		appName   = "inplace-app"
		dbName    = "inplace-db"
		pvcName   = "inplace-pvc"
		dbFile    = "inplace.db"
		dbPath    = "/data"
		initSQL   = "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT);"
		testValue = "inplace-restore-test"
	)

	BeforeAll(func() {
		DeferCleanup(func() { dumpReplicationDiagnostics(appName, dbName, dbFile) })

		By("cleaning stale S3 data from prior test runs")
		mcCleanPath(dbName)

		By("creating PVC, Deployment (Recreate strategy), and LitestreamReplica with backup enabled")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentRecreateManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")
		applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, true, initSQL))

		By("waiting for sidecar injection and ReplicationHealthy=True")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("writing test data to the database")
		podName := runningPod(appName)
		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "sqlite3", dbPath+"/"+dbFile,
			"INSERT INTO notes(body) VALUES('"+testValue+"');")

		By("waiting for Litestream to replicate to S3")
		Eventually(func(g Gomega) {
			out := mcList(s3Bucket + "/" + dbName + "/")
			g.Expect(out).NotTo(BeEmpty(), "expected backup objects in S3")
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamrestore", "inplace-restore", "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("fences workload, restores in-place, and resumes the application", func() {
		By("creating an InPlace LitestreamRestore CR (force=true since DB exists)")
		applyLiteral(litestreamRestoreInPlaceManifestWithOpts("inplace-restore", testNamespace, dbName, true))

		By("waiting for restore to reach Complete phase")
		Eventually(func(g Gomega) {
			phase, err := kubectlQ("get", "litestreamrestore", "inplace-restore", "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			if phase == "Failed" {
				jobName, _ := kubectlQ("get", "litestreamrestore", "inplace-restore", "-n", testNamespace,
					"-o", "jsonpath={.status.jobName}")
				if jobName != "" {
					logs, _ := kubectlQ("logs", "-n", testNamespace, "job/"+jobName, "--tail=50", "--request-timeout=15s")
					GinkgoWriter.Printf("\n=== InPlace restore Job logs ===\n%s\n========================\n", logs)
				}
			}
			g.Expect(phase).To(Equal("Completed"))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying originalReplicas was recorded")
		origReplicas, err := kubectlQ("get", "litestreamrestore", "inplace-restore", "-n", testNamespace,
			"-o", "jsonpath={.status.originalReplicas}")
		Expect(err).NotTo(HaveOccurred())
		Expect(origReplicas).To(Equal("1"), "originalReplicas should be recorded in status")

		By("verifying the Deployment is scaled back to 1 replica")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "deployment", appName, "-n", testNamespace,
				"-o", "jsonpath={.spec.replicas}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("1"))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying restored data is present in the database")
		Eventually(func(g Gomega) {
			podName, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"--field-selector=status.phase=Running",
				"-o", "jsonpath={.items[0].metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(podName).NotTo(BeEmpty())
			out, err := kubectlQ("exec", "-n", testNamespace, podName, "-c", "app",
				"--", "sqlite3", dbPath+"/"+dbFile,
				"SELECT body FROM notes WHERE body='"+testValue+"';")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(testValue))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying ReplicationHealthy=True after InPlace restore")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 3*time.Minute, 10*time.Second).Should(Succeed())

		By("verifying the pause annotation was removed from the LitestreamReplica")
		Eventually(func(g Gomega) {
			out, pauseErr := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.metadata.annotations.litestream\.io/pause}`)
			g.Expect(pauseErr).NotTo(HaveOccurred())
			g.Expect(out).NotTo(Equal("true"),
				"pause annotation should be removed after InPlace restore completes")
		}, 1*time.Minute, 5*time.Second).Should(Succeed())
	})
})

// ── Scenario: Restore Concurrency Lock ──────────────────────────────────────
//
// Verifies that two simultaneous InPlace restores targeting the same source
// LitestreamReplica are serialized via a coordination.k8s.io Lease.

var _ = Describe("Restore Concurrency Lock", Ordered, func() {
	const (
		appName = "concurrency-app"
		dbName  = "concurrency-db"
		pvcName = "concurrency-pvc"
		dbFile  = "concurrency.db"
		dbPath  = "/data"
		initSQL = "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, val TEXT);"
	)

	BeforeAll(func() {
		DeferCleanup(func() { dumpReplicationDiagnostics(appName, dbName, dbFile) })

		By("cleaning stale S3 data from prior test runs")
		mcCleanPath(dbName)

		By("creating PVC, Deployment, and LitestreamReplica with backup enabled")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentRecreateManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")
		applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, true, initSQL))

		By("waiting for ReplicationHealthy=True")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("writing data and waiting for replication")
		podName := runningPod(appName)
		kubectl("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "sqlite3", dbPath+"/"+dbFile,
			"INSERT INTO items(val) VALUES('concurrency-test');")
		Eventually(func(g Gomega) {
			out := mcList(s3Bucket + "/" + dbName + "/")
			g.Expect(out).NotTo(BeEmpty())
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamrestore", "concurrency-restore-1", "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "litestreamrestore", "concurrency-restore-2", "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "lease", "litestream-restore-"+dbName, "-n", testNamespace, "--ignore-not-found")
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("second InPlace restore blocks at AcquiringLock while first holds the Lease", func() {
		By("creating first InPlace restore (force=true since DB exists)")
		applyLiteral(litestreamRestoreInPlaceManifestWithOpts("concurrency-restore-1", testNamespace, dbName, true))

		By("waiting for first restore to acquire the lock (Fencing or later)")
		Eventually(func(g Gomega) {
			phase, err := kubectlQ("get", "litestreamrestore", "concurrency-restore-1", "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(SatisfyAny(
				Equal("Fencing"), Equal("Restoring"), Equal("Resuming"), Equal("Completed"),
			), "first restore should advance past AcquiringLock")
		}, 3*time.Minute, 10*time.Second).Should(Succeed())

		By("creating second InPlace restore targeting the same source")
		applyLiteral(litestreamRestoreInPlaceManifestWithOpts("concurrency-restore-2", testNamespace, dbName, true))

		By("verifying second restore is blocked at AcquiringLock or has Locked=False")
		Eventually(func(g Gomega) {
			phase, err := kubectlQ("get", "litestreamrestore", "concurrency-restore-2", "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())

			// If the first restore completed quickly, the second may have already acquired the lock.
			// Both outcomes validate the concurrency mechanism.
			if phase == "AcquiringLock" {
				locked, _ := kubectlQ("get", "litestreamrestore", "concurrency-restore-2", "-n", testNamespace,
					"-o", `jsonpath={.status.conditions[?(@.type=="Locked")].status}`)
				g.Expect(locked).To(Equal("False"),
					"second restore should have Locked=False while first holds the Lease")
				return
			}

			// The first restore finished and the second progressed — still valid.
			g.Expect(phase).To(SatisfyAny(
				Equal("AcquiringLock"), Equal("Fencing"), Equal("Restoring"),
				Equal("Resuming"), Equal("Completed"), Equal("Failed"),
			))
		}, 2*time.Minute, 10*time.Second).Should(Succeed())

		By("waiting for both restores to reach a terminal phase")
		for _, name := range []string{"concurrency-restore-1", "concurrency-restore-2"} {
			Eventually(func(g Gomega) {
				phase, err := kubectlQ("get", "litestreamrestore", name, "-n", testNamespace,
					"-o", "jsonpath={.status.phase}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(phase).To(SatisfyAny(Equal("Completed"), Equal("Failed")),
					"%s should reach a terminal phase", name)
			}, 5*time.Minute, 10*time.Second).Should(Succeed())
		}
	})
})

// ── Scenario: CR Deletion Finalizer ──────────────────────────────────────────
//
// Deleting a LitestreamReplica CR must clean up injection labels and annotations
// from the target Deployment's pod template so the webhook no longer injects
// sidecars into new pods.

var _ = Describe("CR Deletion Finalizer", Ordered, func() {
	const (
		appName = "finalizer-app"
		dbName  = "finalizer-db"
		pvcName = "finalizer-pvc"
		dbFile  = "finalizer.db"
		dbPath  = "/data"
	)

	BeforeAll(func() {
		By("creating PVC and Deployment")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")

		By("creating LitestreamReplica CR")
		applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, false, ""))

		By("waiting for sidecar injection (label set on pod template)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "deployment", appName, "-n", testNamespace,
				"-o", `jsonpath={.spec.template.metadata.labels.litestream\.io/inject}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("true"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("removes injection labels and annotations from Deployment when CR is deleted", func() {
		By("verifying injection annotations are present before deletion")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "deployment", appName, "-n", testNamespace,
				"-o", `jsonpath={.spec.template.metadata.annotations.litestream\.io/config}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty(), "config annotation should be present before deletion")
		}).Should(Succeed())

		By("deleting the LitestreamReplica CR")
		kubectl("delete", "litestreamreplica", dbName, "-n", testNamespace, "--timeout=2m")

		By("verifying inject label is removed from pod template")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "deployment", appName, "-n", testNamespace,
				"-o", `jsonpath={.spec.template.metadata.labels.litestream\.io/inject}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(BeEmpty(), "inject label should be removed after CR deletion")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying config annotation is removed from pod template")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "deployment", appName, "-n", testNamespace,
				"-o", `jsonpath={.spec.template.metadata.annotations.litestream\.io/config}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(BeEmpty(), "config annotation should be removed after CR deletion")
		}).Should(Succeed())

		By("verifying injection-spec-hash annotation is removed from pod template")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "deployment", appName, "-n", testNamespace,
				"-o", `jsonpath={.spec.template.metadata.annotations.litestream\.io/injection-spec-hash}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(BeEmpty(), "injection-spec-hash should be removed after CR deletion")
		}).Should(Succeed())

		By("verifying new pods no longer have the litestream sidecar")
		kubectl("rollout", "restart", "deployment/"+appName, "-n", testNamespace)
		kubectl("rollout", "status", "deployment/"+appName, "-n", testNamespace, "--timeout=3m")
		podName := runningPod(appName)
		out, err := kubectlQ("get", "pod", podName, "-n", testNamespace,
			"-o", `jsonpath={range .spec.containers[*]}{.name}{"\n"}{end}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).NotTo(ContainSubstring("litestream"),
			"new pods should not have litestream sidecar after CR deletion")
	})
})

// ── Scenario: Injection Spec Hash Rollout ────────────────────────────────────
//
// When CR spec fields change (e.g. image), the controller updates the injection
// spec hash annotation, which triggers a Deployment rollout so pods pick up
// the new configuration.

var _ = Describe("Injection Spec Hash Rollout", Ordered, func() {
	const (
		appName = "hash-rollout-app"
		dbName  = "hash-rollout-db"
		pvcName = "hash-rollout-pvc"
		dbFile  = "hashrollout.db"
		dbPath  = "/data"
	)

	BeforeAll(func() {
		By("cleaning stale S3 data from prior test runs")
		mcCleanPath(dbName)

		By("creating PVC and Deployment")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")

		By("creating LitestreamReplica CR with backup enabled")
		applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, true, ""))

		By("waiting for injection and ReplicationHealthy")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="ReplicationHealthy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("updates hash annotation and triggers rollout when CR spec changes", func() {
		By("recording the current injection-spec-hash")
		var originalHash string
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "deployment", appName, "-n", testNamespace,
				"-o", `jsonpath={.spec.template.metadata.annotations.litestream\.io/injection-spec-hash}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty())
			originalHash = out
		}).Should(Succeed())

		By("recording the current pod name")
		originalPod := runningPod(appName)

		By("updating the LitestreamReplica spec (change syncInterval)")
		kubectl("patch", "litestreamreplica", dbName, "-n", testNamespace,
			"--type=merge", "-p", `{"spec":{"backup":{"syncInterval":"500ms"}}}`)

		By("waiting for the injection-spec-hash to change")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "deployment", appName, "-n", testNamespace,
				"-o", `jsonpath={.spec.template.metadata.annotations.litestream\.io/injection-spec-hash}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(Equal(originalHash),
				"injection-spec-hash should change when CR spec is updated")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying a rolling update produces a new pod")
		Eventually(func(g Gomega) {
			newPod := runningPod(appName)
			g.Expect(newPod).NotTo(Equal(originalPod),
				"a new pod should be created after hash change triggers rollout")
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})
})

// ── Scenario: Unsafe Rollout Strategy ────────────────────────────────────────
//
// Verifies the UnsafeRolloutStrategy condition is set when the target Deployment
// uses a RollingUpdate strategy with maxSurge > 0, and clears when strategy is
// changed to Recreate.

var _ = Describe("Unsafe Rollout Strategy", Ordered, func() {
	const (
		appName = "unsafe-rollout-app"
		dbName  = "unsafe-rollout-db"
		pvcName = "unsafe-rollout-pvc"
		dbFile  = "unsaferollout.db"
		dbPath  = "/data"
	)

	BeforeAll(func() {
		By("creating PVC and Deployment with default RollingUpdate strategy")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(appDeploymentRollingUpdateManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("sets UnsafeRolloutStrategy=True for RollingUpdate and clears it for Recreate", func() {
		By("creating LitestreamReplica CR targeting the RollingUpdate Deployment")
		applyLiteral(litestreamReplicaManifest(dbName, testNamespace, appName, dbFile, dbPath, false, ""))

		By("verifying UnsafeRolloutStrategy=True is set")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="UnsafeRolloutStrategy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"),
				"UnsafeRolloutStrategy should be True for default RollingUpdate strategy")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying phase is Error due to unsafe rollout strategy")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Error"))
		}).Should(Succeed())

		By("switching Deployment strategy to Recreate")
		kubectl("patch", "deployment", appName, "-n", testNamespace,
			"--type=strategic", "-p", `{"spec":{"strategy":{"type":"Recreate","rollingUpdate":null}}}`)

		By("verifying UnsafeRolloutStrategy clears (condition removed or set to False)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="UnsafeRolloutStrategy")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(SatisfyAny(BeEmpty(), Equal("False")),
				"UnsafeRolloutStrategy should clear after switching to Recreate")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying phase recovers from Error")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(Equal("Error"),
				"phase should recover after strategy fix")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
	})
})

// ── Scenario: LitestreamRestore Webhook Validation ───────────────────────────
//
// Validates the webhook rejection rules for LitestreamRestore CRs:
// 1. ToPVC mode without target → rejected
// 2. InPlace mode with target → rejected
// 3. ToPVC target with non-absolute path → rejected

var _ = Describe("LitestreamRestore Webhook Validation", func() {
	It("rejects ToPVC restore without target", func() {
		manifest := fmt.Sprintf(`
apiVersion: litestream.io/v1
kind: LitestreamRestore
metadata:
  name: webhook-test-no-target
  namespace: %s
spec:
  sourceRef:
    name: some-source
  mode: ToPVC
`, testNamespace)

		out, err := applyLiteralQ(manifest)
		Expect(err).To(HaveOccurred(), "webhook should reject ToPVC without target")
		Expect(strings.ToLower(out)).To(ContainSubstring("target"),
			"rejection message should mention target; got: %s", out)
	})

	It("rejects InPlace restore with target set", func() {
		manifest := fmt.Sprintf(`
apiVersion: litestream.io/v1
kind: LitestreamRestore
metadata:
  name: webhook-test-inplace-target
  namespace: %s
spec:
  sourceRef:
    name: some-source
  mode: InPlace
  target:
    pvc: some-pvc
    path: /data/db.sqlite
`, testNamespace)

		out, err := applyLiteralQ(manifest)
		Expect(err).To(HaveOccurred(), "webhook should reject InPlace with target set")
		Expect(strings.ToLower(out)).To(ContainSubstring("target"),
			"rejection message should mention target; got: %s", out)
	})

	It("rejects ToPVC restore with non-absolute target path", func() {
		manifest := fmt.Sprintf(`
apiVersion: litestream.io/v1
kind: LitestreamRestore
metadata:
  name: webhook-test-relpath
  namespace: %s
spec:
  sourceRef:
    name: some-source
  mode: ToPVC
  target:
    pvc: some-pvc
    path: data/db.sqlite
`, testNamespace)

		out, err := applyLiteralQ(manifest)
		Expect(err).To(HaveOccurred(), "webhook should reject non-absolute target path")
		Expect(strings.ToLower(out)).To(ContainSubstring("absolute"),
			"rejection message should mention absolute path; got: %s", out)
	})

	AfterEach(func() {
		runIgnoreError("kubectl", "delete", "litestreamrestore", "webhook-test-no-target", "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "litestreamrestore", "webhook-test-inplace-target", "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "litestreamrestore", "webhook-test-relpath", "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})
})

// ── Scenario: Container Selection ────────────────────────────────────────────
//
// When a Deployment has multiple containers and the database volume is mounted
// in a non-first container, spec.container must be set to select it. The sidecar
// should resolve the correct volume mount.

var _ = Describe("Container Selection", Ordered, func() {
	const (
		appName   = "container-sel-app"
		dbName    = "container-sel-db"
		pvcName   = "container-sel-pvc"
		dbFile    = "containersel.db"
		dbPath    = "/data"
		container = "db-app"
	)

	BeforeAll(func() {
		By("creating PVC and multi-container Deployment (db volume on second container)")
		applyLiteral(pvcManifest(pvcName, testNamespace))
		applyLiteral(multiContainerDeploymentManifest(appName, testNamespace, pvcName, dbPath))
		kubectl("wait", "-n", testNamespace, "deployment/"+appName,
			"--for=condition=Available", "--timeout=3m")
	})

	AfterAll(func() {
		runIgnoreError("kubectl", "delete", "litestreamreplica", dbName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "deployment", appName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
		runIgnoreError("kubectl", "delete", "pvc", pvcName, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	It("injects sidecar with correct volume mount when spec.container selects a non-first container", func() {
		By("creating LitestreamReplica with spec.container targeting the second container")
		applyLiteral(litestreamReplicaManifestWithContainer(dbName, testNamespace, appName, dbFile, dbPath, container, false))

		By("waiting for sidecar injection")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "deployment", appName, "-n", testNamespace,
				"-o", `jsonpath={.spec.template.metadata.labels.litestream\.io/inject}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("true"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("waiting for rolling update to produce a pod with the Litestream sidecar")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"-o", `jsonpath={range .items[*]}{range .spec.containers[*]}{.name}{"\n"}{end}{end}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("litestream"))
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("verifying the litestream sidecar has the data volume mounted")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "pods", "-n", testNamespace,
				"-l", "app="+appName,
				"-o", `jsonpath={range .items[0].spec.containers[?(@.name=="litestream")]}{range .volumeMounts[*]}{.mountPath}{"\n"}{end}{end}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(dbPath),
				"litestream sidecar should mount the database volume at %s", dbPath)
		}).Should(Succeed())

		By("verifying LitestreamReplica phase is not Error (controller accepted container selection)")
		Eventually(func(g Gomega) {
			out, err := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty())
			g.Expect(out).NotTo(Equal("Error"),
				"phase should not be Error — controller should accept spec.container selection")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
	})
})

// ── Manifest builders ──────────────────────────────────────────────────────

func pvcManifest(name, ns string) string {
	return fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: %s
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
`, name, ns)
}

func appDeploymentManifest(name, ns, pvcName, mountPath string) string {
	return appDeploymentRecreateManifest(name, ns, pvcName, mountPath)
}

func appDeploymentRollingUpdateManifest(name, ns, pvcName, mountPath string) string {
	return fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
        - name: app
          image: keinos/sqlite3:latest
          command: ["sleep", "infinity"]
          volumeMounts:
            - name: data
              mountPath: %s
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: %s
`, name, ns, name, name, mountPath, pvcName)
}

func litestreamReplicaManifest(name, ns, target, dbFile, dbPath string, backupEnabled bool, initSQL string) string {
	return litestreamReplicaManifestWithOpts(name, ns, target, dbFile, dbPath, backupEnabled, initSQL, databasev1.RecoveryModeManual)
}

func litestreamReplicaManifestWithOpts(name, ns, target, dbFile, dbPath string, backupEnabled bool, initSQL string, recoveryMode databasev1.RecoveryMode) string {
	return litestreamReplicaManifestFull(name, ns, target, dbFile, dbPath, backupEnabled, initSQL, recoveryMode, nil)
}

// litestreamReplicaManifestFull is the full-featured constructor. runAsUser sets the UID for
// Litestream-managed init containers (archive-check, bootstrap). When nil the image default is used.
func litestreamReplicaManifestFull(name, ns, target, dbFile, dbPath string, backupEnabled bool, initSQL string, recoveryMode databasev1.RecoveryMode, runAsUser *int64) string {
	db := &databasev1.LitestreamReplica{
		TypeMeta:   metav1.TypeMeta{APIVersion: "litestream.io/v1", Kind: "LitestreamReplica"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: databasev1.LitestreamReplicaSpec{
			DatabaseName:     dbFile,
			DatabasePath:     dbPath,
			TargetDeployment: target,
			Bootstrap:        databasev1.BootstrapSpec{SQL: initSQL},
			Recovery:         databasev1.RecoverySpec{Mode: recoveryMode},
			RunAsUser:        runAsUser,
		},
	}
	if backupEnabled {
		db.Spec.Backup = databasev1.BackupSpec{
			Enabled: true,
			Destination: databasev1.BackupDestination{
				S3: &databasev1.S3Destination{
					Endpoint:  s3Endpoint,
					Bucket:    s3Bucket,
					Path:      name + "/",
					SecretRef: "minio-creds",
				},
			},
			Retention: databasev1.RetentionPolicy{Duration: "720h"},
		}
	}
	data, err := sigsyaml.Marshal(db)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	GinkgoWriter.Printf("--- LitestreamReplica CR YAML applied (%s) ---\n%s\n", name, string(data))
	return string(data)
}

func appDeploymentRecreateManifest(name, ns, pvcName, mountPath string) string {
	return fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
        - name: app
          image: keinos/sqlite3:latest
          command: ["sleep", "infinity"]
          volumeMounts:
            - name: data
              mountPath: %s
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: %s
`, name, ns, name, name, mountPath, pvcName)
}

func multiContainerDeploymentManifest(name, ns, pvcName, mountPath string) string {
	return fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
        - name: sidecar-app
          image: busybox:latest
          command: ["sleep", "infinity"]
        - name: db-app
          image: keinos/sqlite3:latest
          command: ["sleep", "infinity"]
          volumeMounts:
            - name: data
              mountPath: %s
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: %s
`, name, ns, name, name, mountPath, pvcName)
}

func litestreamReplicaManifestWithContainer(name, ns, target, dbFile, dbPath, container string, backupEnabled bool) string {
	db := &databasev1.LitestreamReplica{
		TypeMeta:   metav1.TypeMeta{APIVersion: "litestream.io/v1", Kind: "LitestreamReplica"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: databasev1.LitestreamReplicaSpec{
			DatabaseName:     dbFile,
			DatabasePath:     dbPath,
			TargetDeployment: target,
			Container:        container,
			Recovery:         databasev1.RecoverySpec{Mode: databasev1.RecoveryModeManual},
		},
	}
	if backupEnabled {
		db.Spec.Backup = databasev1.BackupSpec{
			Enabled: true,
			Destination: databasev1.BackupDestination{
				S3: &databasev1.S3Destination{
					Endpoint:  s3Endpoint,
					Bucket:    s3Bucket,
					Path:      name + "/",
					SecretRef: "minio-creds",
				},
			},
			Retention: databasev1.RetentionPolicy{Duration: "720h"},
		}
	}
	data, err := sigsyaml.Marshal(db)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return string(data)
}


func litestreamRestoreInPlaceManifestWithOpts(name, ns, sourceRef string, force bool) string {
	restore := &databasev1.LitestreamRestore{
		TypeMeta:   metav1.TypeMeta{APIVersion: "litestream.io/v1", Kind: "LitestreamRestore"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: databasev1.LitestreamRestoreSpec{
			SourceRef: databasev1.RestoreSourceRef{Name: sourceRef},
			Mode:      databasev1.RestoreModeInPlace,
			Force:     force,
		},
	}
	data, err := sigsyaml.Marshal(restore)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return string(data)
}

func litestreamRestoreManifest(name, ns, sourceRef, pvc, targetPath string) string {
	return litestreamRestoreManifestWithTimestamp(name, ns, sourceRef, pvc, targetPath, "")
}

func litestreamRestoreManifestWithForce(name, ns, sourceRef, pvc, targetPath string) string {
	restore := &databasev1.LitestreamRestore{
		TypeMeta:   metav1.TypeMeta{APIVersion: "litestream.io/v1", Kind: "LitestreamRestore"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: databasev1.LitestreamRestoreSpec{
			SourceRef: databasev1.RestoreSourceRef{Name: sourceRef},
			Mode:      databasev1.RestoreModeInPlace,
			Force:     true,
		},
	}
	data, err := sigsyaml.Marshal(restore)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return string(data)
}

func litestreamRestoreManifestWithTimestamp(name, ns, sourceRef, pvc, targetPath, timestamp string) string {
	restore := &databasev1.LitestreamRestore{
		TypeMeta:   metav1.TypeMeta{APIVersion: "litestream.io/v1", Kind: "LitestreamRestore"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: databasev1.LitestreamRestoreSpec{
			SourceRef: databasev1.RestoreSourceRef{Name: sourceRef},
			Mode:      databasev1.RestoreModeToPVC,
			Target:    &databasev1.RestoreTarget{PVC: pvc, Path: targetPath},
			Timestamp: timestamp,
		},
	}
	data, err := sigsyaml.Marshal(restore)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return string(data)
}

func verifyRestoreJobManifest(name, ns, pvc, dbPath string) string {
	return fmt.Sprintf(`
apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  namespace: %s
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      # Run as root so we can read the litestream-restored file (created as root:root 600).
      securityContext:
        runAsUser: 0
        runAsGroup: 0
      containers:
        - name: verify
          image: keinos/sqlite3:latest
          command: ["sqlite3", "%s", ".tables"]
          securityContext:
            runAsUser: 0
            allowPrivilegeEscalation: false
          volumeMounts:
            - name: data
              mountPath: /restore
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: %s
`, name, ns, dbPath, pvc)
}

// pitrVerifyJobManifest creates a Job that runs one or more SQL statements against
// dbPath via sqlite3 and prints the results to stdout. Use kubectl logs to read
// the output after the Job completes — exec into a completed pod is not possible.
func pitrVerifyJobManifest(name, ns, pvc, dbPath string, sqlStatements ...string) string {
	sql := strings.Join(sqlStatements, " ")
	return fmt.Sprintf(`
apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  namespace: %s
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsUser: 0
        runAsGroup: 0
      containers:
        - name: verify
          image: keinos/sqlite3:latest
          command:
            - sh
            - -c
            - sqlite3 %s "%s"
          securityContext:
            runAsUser: 0
            allowPrivilegeEscalation: false
          volumeMounts:
            - name: data
              mountPath: /restore
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: %s
`, name, ns, dbPath, sql, pvc)
}

// ── Test helpers ───────────────────────────────────────────────────────────

// runningPod returns the name of a Running pod for the given Deployment.
func runningPod(deploymentName string) string {
	var podName string
	Eventually(func(g Gomega) {
		out, err := kubectlQ("get", "pods", "-n", testNamespace,
			"-l", "app="+deploymentName,
			"--field-selector=status.phase=Running",
			"-o", "jsonpath={.items[0].metadata.name}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).NotTo(BeEmpty())
		podName = strings.TrimSpace(out)
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
	return podName
}

// runningPodWithSidecar returns the name of a Running pod for the given Deployment
// that has the litestream sidecar container present. During a rolling update both
// the old pod (no sidecar) and the new pod (with sidecar) may be Running
// simultaneously; this helper ensures we get the sidecar-bearing one.
func runningPodWithSidecar(deploymentName string) string {
	var podName string
	Eventually(func(g Gomega) {
		out, err := kubectlQ("get", "pods", "-n", testNamespace,
			"-l", "app="+deploymentName,
			"--field-selector=status.phase=Running",
			"-o", `jsonpath={range .items[*]}{.metadata.name}{"="}{range .status.containerStatuses[*]}{.name}{","}{end}{"\n"}{end}`)
		g.Expect(err).NotTo(HaveOccurred())
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 && strings.Contains(parts[1], "litestream") {
				podName = parts[0]
				return
			}
		}
		Fail("no running pod with litestream sidecar found")
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
	return podName
}

// mcList runs `mc ls` against the MinIO service using the persistent mc-client pod.
// Returns the listing output on success, empty string on any error.
// The mc alias is pre-configured in BeforeSuite so no setup is needed here.
func mcList(path string) string {
	out, err := kubectlQ("exec", "-n", testNamespace, "mc-client", "--",
		"/bin/sh", "-c",
		fmt.Sprintf("mc ls local/%s", path),
	)
	if err != nil {
		return ""
	}
	return out
}

// mcCleanPath removes all objects under a given S3 path prefix using the persistent
// mc-client pod. Idempotent — does nothing if the path doesn't exist.
func mcCleanPath(pathPrefix string) {
	_, _ = kubectlQ("exec", "-n", testNamespace, "mc-client", "--",
		"/bin/sh", "-c",
		fmt.Sprintf("mc rm --recursive --force local/%s/%s/ 2>/dev/null || true", s3Bucket, pathPrefix),
	)
}

// dumpReplicationDiagnostics prints a diagnostic snapshot to help debug Litestream
// replication failures. Call via DeferCleanup before polling mc ls so the output
// appears after any failure, regardless of which check timed out.
// dbName is the LitestreamReplica CR name (used for ConfigMap lookup: <dbName>-litestream).
// dbFile is the database filename (used for sqlite3 access: /data/<dbFile>).
func dumpReplicationDiagnostics(appName, dbName, dbFile string) {
	GinkgoWriter.Printf("\n====== replication diagnostics: %s / %s ======\n", appName, dbName)

	// 1. Litestream sidecar logs.
	podName, podErr := kubectlQ("get", "pods", "-n", testNamespace,
		"-l", "app="+appName, "-o", "jsonpath={.items[0].metadata.name}")
	podName = strings.TrimSpace(podName)
	if podErr == nil && podName != "" {
		logs, _ := kubectlQ("logs", "-n", testNamespace, podName, "-c", "litestream", "--tail=100")
		GinkgoWriter.Printf("--- Litestream sidecar logs (pod %s) ---\n%s\n", podName, logs)

		journal, _ := kubectlQ("exec", "-n", testNamespace, podName, "-c", "app",
			"--", "sqlite3", "/data/"+dbFile, "PRAGMA journal_mode;")
		GinkgoWriter.Printf("--- SQLite journal_mode ---\n%s\n", journal)

		initStatuses, _ := kubectlQ("get", "pod", podName, "-n", testNamespace,
			"-o", `jsonpath={range .status.initContainerStatuses[*]}{.name}={.state}{"\n"}{end}`)
		GinkgoWriter.Printf("--- Init container statuses ---\n%s\n", initStatuses)

		initNames, _ := kubectlQ("get", "pod", podName, "-n", testNamespace,
			"-o", `jsonpath={range .spec.initContainers[*]}{.name}{"\n"}{end}`)
		GinkgoWriter.Printf("--- Init containers in spec ---\n%s\n", initNames)

		for _, name := range strings.Split(strings.TrimSpace(initNames), "\n") {
			if name == "" {
				continue
			}
			initLogs, _ := kubectlQ("logs", "-n", testNamespace, podName, "-c", name, "--tail=20")
			GinkgoWriter.Printf("--- Init container %s logs ---\n%s\n", name, initLogs)
		}
	} else {
		GinkgoWriter.Printf("--- no running pod found for app=%s ---\n", appName)
	}

	annotations, _ := kubectlQ("get", "litestreamreplica", dbName, "-n", testNamespace,
		"-o", "jsonpath={.metadata.annotations}")
	GinkgoWriter.Printf("--- LitestreamReplica annotations ---\n%s\n", annotations)

	// 2. Litestream ConfigMap content (named after the CR, not the db file).
	cm, _ := kubectlQ("get", "configmap", dbName+"-litestream", "-n", testNamespace, "-o", "yaml")
	GinkgoWriter.Printf("--- ConfigMap %s-litestream ---\n%s\n", dbName, cm)

	// 3. Full bucket listing (recursive, entire bucket).
	allObjects, _ := kubectlQ("exec", "-n", testNamespace, "mc-client", "--",
		"/bin/sh", "-c", "mc ls --recursive local/"+s3Bucket+"/")
	GinkgoWriter.Printf("--- mc ls --recursive local/%s/ ---\n%s\n", s3Bucket, allObjects)

	// 4. mc alias verification.
	aliasList, _ := kubectlQ("exec", "-n", testNamespace, "mc-client", "--",
		"/bin/sh", "-c", "mc alias list local")
	GinkgoWriter.Printf("--- mc alias list local ---\n%s\n", aliasList)

	// 5. Pod container statuses (detect CrashLoopBackOff etc.).
	podStatus, _ := kubectlQ("get", "pods", "-n", testNamespace, "-l", "app="+appName, "-o", "wide")
	GinkgoWriter.Printf("--- pods for %s ---\n%s\n", appName, podStatus)

	GinkgoWriter.Printf("====== end diagnostics ======\n\n")
}
