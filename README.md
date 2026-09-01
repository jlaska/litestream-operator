# litestream-operator

[![Tests](https://github.com/jlaska/litestream-operator/actions/workflows/test.yml/badge.svg)](https://github.com/jlaska/litestream-operator/actions/workflows/test.yml)
[![Lint](https://raw.githubusercontent.com/jlaska/litestream-operator/badges/.badges/main/lint.svg)](https://github.com/jlaska/litestream-operator/actions/workflows/test.yml)
[![Coverage](https://raw.githubusercontent.com/jlaska/litestream-operator/badges/.badges/main/coverage.svg)](https://github.com/jlaska/litestream-operator/actions/workflows/test.yml)
[![CI/CD Pipeline](https://github.com/jlaska/litestream-operator/actions/workflows/ci.yaml/badge.svg)](https://github.com/jlaska/litestream-operator/actions/workflows/ci.yaml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Container Image](https://img.shields.io/badge/container-ghcr.io-blue)](https://github.com/jlaska/litestream-operator/pkgs/container/litestream-operator)

> **Continuous S3 backup for SQLite databases running in Kubernetes** — no application changes required.

litestream-operator injects a [Litestream](https://litestream.io) sidecar into your existing application pods,
streaming WAL changes to any S3-compatible object store (Garage, AWS S3, Backblaze B2, ...) in real time.
Declare a `LitestreamReplica` resource, point it at your app's Deployment,
and get point-in-time-recoverable database backups without touching your application code.

---

## Table of Contents

- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [CRD reference](#crd-reference)
  - [LitestreamReplica](#litestreamreplica)
  - [LitestreamRestore](#litestreamrestore)
- [Recovery modes](#recovery-modes)
- [Production checklist](#production-checklist)
- [Usage examples](#usage-examples)
- [Annotations](#annotations)
- [Kubernetes events](#kubernetes-events)
- [Prometheus metrics](#prometheus-metrics)
- [Troubleshooting](#troubleshooting)
- [Helm chart values](#helm-chart-values)
- [Development](#development)
- [License](#license)

---

## Quick start

### 1. Install the operator

```bash
helm install litestream-operator oci://ghcr.io/jlaska/charts/litestream-operator \
  --namespace litestream-operator-system \
  --create-namespace
```

> **Prerequisites**: Kubernetes >= 1.28, Helm 3, [cert-manager](https://cert-manager.io/) installed in the cluster.
>
> To skip cert-manager (bring your own webhook TLS secret):
>
> ```bash
> helm install litestream-operator oci://ghcr.io/jlaska/charts/litestream-operator \
>   --namespace litestream-operator-system \
>   --create-namespace \
>   --set certManager.enabled=false \
>   --set certManager.secretName=my-tls-secret
> ```

### 2. Create an S3 credentials Secret

```bash
kubectl create secret generic s3-credential \
  --from-literal=ACCESS_KEY_ID=<your-access-key> \
  --from-literal=SECRET_ACCESS_KEY=<your-secret-key> \
  --namespace example
```

> **Note**: The Secret must be in the same namespace as the `LitestreamReplica` resource (and the target workload).

### 3. Declare a LitestreamReplica resource

```yaml
apiVersion: litestream.io/v1
kind: LitestreamReplica
metadata:
  name: my-app-db
  namespace: example
spec:
  targetDeployment: my-app
  databasePath: /data
  databaseName: app.db

  # Recovery mode: automatic (default) uses Litestream's native restore flags
  # with integrity checking; manual requires explicit LitestreamRestore CR.
  recovery:
    mode: automatic

  backup:
    enabled: true
    destination:
      s3:
        endpoint: s3.example.com:9000    # omit for AWS S3
        bucket: litestream-backups
        path: my-app/
        secretRef: s3-credential
    retention:
      duration: "720h"              # 30 days

  # Backup health SLO — mark ReplicationHealthy=False if no sync within 5m.
  health:
    maxReplicationLag: 5m
```

Apply it:

```bash
kubectl apply -f litestreamreplica.yaml
```

The operator annotates `my-app`, which triggers a rolling update. New pods get the Litestream sidecar injected automatically — no Deployment changes required.

### 4. Verify

```bash
# Check injection and backup health
kubectl get litestreamreplica my-app-db -n example
# NAME        TARGET   DATABASE   BACKUP  PHASE  READY  AGE
# my-app-db   my-app   app.db     true    Ready  true   3d

kubectl describe litestreamreplica my-app-db -n example
# Conditions:
#   TargetReady          True   DeploymentFound
#   SidecarReady         True   SidecarRunning
#   ReplicationHealthy   True   ReplicationWithinThreshold
#   Ready                True   AllConditionsMet
```

---

## How it works

litestream-operator is to SQLite what [CloudNativePG](https://cloudnative-pg.io) is to PostgreSQL —
a Kubernetes-native orchestration layer that handles backup, lifecycle, and observability at the database layer.
Litestream does for SQLite what Barman Cloud does for PostgreSQL.

```text
┌─────────────────────────────────────┐
│  Application Pod (after rollout)    │
│                                     │
│  ┌─────────────┐ ┌───────────────┐  │
│  │   app       │ │  litestream   │  │
│  │  container  │ │   sidecar     │  │
│  │             │ │               │  │
│  │  reads/     │ │  streams WAL  │──┼──► S3
│  │  writes     │ │  changes      │  │
│  │  /data/     │ │  continuously │  │
│  │  app.db     │ │               │  │
│  └──────┬──────┘ └───────────────┘  │
│         │ shared volume             │
│  ┌──────▼──────┐                    │
│  │  PVC        │                    │
│  └─────────────┘                    │
└─────────────────────────────────────┘
```

**Injection flow:**

1. You create a `LitestreamReplica` CR pointing at an existing Deployment
2. The controller annotates the Deployment's pod template (`litestream.io/inject: "true"`)
3. The annotation triggers a rolling update — new pods inherit the label
4. The mutating webhook intercepts pod creation and injects the Litestream sidecar, plus init containers for recovery and bootstrap
5. Litestream streams WAL changes to S3 continuously; the operator monitors replication health via Litestream metrics

---

## CRD reference

### LitestreamReplica

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.targetDeployment` | string | * | Name of the Deployment to inject into (mutually exclusive with `targetStatefulSet`) |
| `spec.targetStatefulSet` | string | * | Name of the StatefulSet to inject into (mutually exclusive with `targetDeployment`) |
| `spec.databasePath` | string | yes | Directory path inside the app container (e.g. `/data`) |
| `spec.databaseName` | string | yes | Filename of the SQLite database (e.g. `app.db`) |
| `spec.container` | string | | Application container name. Defaults to the first container. Set when the database volume is mounted in a non-first container |
| `spec.image` | string | | Litestream image override (default: `litestream/litestream:0.5.14`) |
| `spec.recovery.mode` | `manual` \| `automatic` | | Recovery strategy on pod startup (default: `automatic`) |
| `spec.backup.autoRecover` | bool | | Enable upstream auto-recover for LTX state corruption (default: `false`) |
| `spec.backup.enabled` | bool | | Enable Litestream replication (default: `false`) |
| `spec.backup.destination.s3.endpoint` | string | | S3-compatible endpoint URL; omit for AWS S3 |
| `spec.backup.destination.s3.bucket` | string | when enabled | S3 bucket name |
| `spec.backup.destination.s3.path` | string | | Key prefix within the bucket |
| `spec.backup.destination.s3.secretRef` | string | when enabled | Secret containing `ACCESS_KEY_ID` and `SECRET_ACCESS_KEY` |
| `spec.backup.retention.duration` | string | | Backup retention as a Go duration string (default: `"720h"`) |
| `spec.backup.syncInterval` | string | | Litestream sync interval override (e.g. `"1s"`, `"500ms"`) |
| `spec.backup.logLevel` | string | | Litestream log level: `debug`, `info`, `warn`, `error` |
| `spec.backup.resources` | ResourceRequirements | | Compute resources for the Litestream sidecar container |
| `spec.health.maxReplicationLag` | string | | Maximum acceptable replication lag (e.g. `"5m"`). Sets `ReplicationHealthy` condition |
| `spec.bootstrap.sql` | string | | SQL executed only when the database is genuinely new (no local DB and no remote archive) |
| `spec.bootstrap.image` | string | | Image for bootstrap init container (default: `keinos/sqlite3:latest`) |
| `spec.runAsUser` | int64 | | UID for Litestream init containers |
| `spec.runAsGroup` | int64 | | GID for Litestream init containers |

**Status conditions:**

| Condition | Meaning |
|---|---|
| `TargetReady` | Target workload exists and is valid |
| `SidecarReady` | Litestream sidecar is injected and running |
| `ReplicationHealthy` | Replication lag is within `maxReplicationLag` threshold |
| `BootstrapApplied` | Bootstrap SQL configured and init container ready |
| `ReplicationPaused` | Replication intentionally paused (e.g. during restore) |
| `ReplicaCountExceeded` | Workload has more than one replica (unsafe for SQLite) |
| `UnsafeRolloutStrategy` | Deployment rollout strategy can create concurrent writers |
| `Ready` | Top-level readiness (all safety conditions met) |

**Status fields:**

| Field | Description |
|---|---|
| `status.phase` | Lifecycle state: `Configuring`, `Pending`, `Ready`, `Paused`, `Error` |
| `status.ready` | Quick readiness flag for `kubectl get` |
| `status.backupHealthy` | Last replication health check result |
| `status.lastSuccessfulReplicationTime` | Timestamp of most recent successful sync |
| `status.replicationLag` | Duration since last successful sync (human-readable) |
| `status.injectedSpecHash` | Hash of injection-relevant spec fields on the target workload |
| `status.observedGeneration` | `.metadata.generation` this status was computed from |

```bash
kubectl get litestreamreplica -A
# NAMESPACE    NAME          TARGET         DATABASE   BACKUP  PHASE  READY  AGE
# example    my-app-db  my-app  app.db     true    Ready  true   3d
```

### LitestreamRestore

Trigger a restore from any `LitestreamReplica` backup. Two modes:

**InPlace** (default) — fences the application, restores in place, resumes:

```yaml
apiVersion: litestream.io/v1
kind: LitestreamRestore
metadata:
  name: my-app-restore
  namespace: example
spec:
  sourceRef:
    name: my-app-db
  mode: InPlace
  timestamp: "2026-06-17T10:00:00Z"  # optional: point-in-time recovery
```

**ToPVC** — restores to a separate PVC without touching the source application (for recovery testing, forensic inspection, migration, or cloning):

```yaml
apiVersion: litestream.io/v1
kind: LitestreamRestore
metadata:
  name: my-app-clone
  namespace: example
spec:
  sourceRef:
    name: my-app-db
  mode: ToPVC
  target:
    pvc: my-app-restore
    path: /data/app.db
```

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.sourceRef.name` | string | yes | Name of the LitestreamReplica whose backup to restore from |
| `spec.mode` | `InPlace` \| `ToPVC` | | Restore strategy (default: `InPlace`) |
| `spec.target.pvc` | string | ToPVC | PVC to write the restored database into |
| `spec.target.path` | string | ToPVC | Full path including filename for the restored database |
| `spec.timestamp` | string | | RFC 3339 timestamp for point-in-time recovery |
| `spec.image` | string | | Litestream image override for the restore Job |
| `spec.force` | bool | | Pass `-force` to litestream, overwriting existing database file |
| `spec.runAsUser` | int64 | | UID for the restore Job pod |
| `spec.runAsGroup` | int64 | | GID for the restore Job pod |

**Restore phases:** `Pending` -> `AcquiringLock` -> `Fencing` -> `Restoring` -> `Validating` -> `Resuming` -> `Completed` (or `Failed`)

**Restore conditions:**

| Condition | Meaning |
|---|---|
| `Locked` | Restore has acquired its concurrency lock (one active InPlace restore per source) |
| `ApplicationFenced` | Source application scaled to zero and replication paused |
| `RestoreSucceeded` | Restore Job completed successfully |
| `ApplicationResumed` | Source application scaled back up |

Monitor progress:

```bash
kubectl get litestreamrestore my-app-restore -n example
# NAME                SOURCE         MODE      PHASE      AGE
# my-app-restore   my-app-db   InPlace   Completed  2m
```

---

## Recovery modes

### automatic (default)

Uses upstream Litestream's native restore with idempotent flags (`-if-db-not-exists`, `-if-replica-exists`)
and integrity checking (`-integrity-check quick`). This matches the [upstream recommended deployment pattern](https://litestream.io/reference/restore/#idempotent-deployment-script).

Any genuine restore failure blocks pod startup — the operator never converts a restore error into a fresh database.

```yaml
spec:
  recovery:
    mode: automatic
```

**Startup behavior:**

| Local DB | Remote Archive | Result |
|---|---|---|
| Exists | Any | Skip restore (normal restart) |
| Missing | Exists | **Restore from S3** with integrity check |
| Missing | Missing | Allow startup (genuinely new database) |

**Recovery procedure**: Scale to 0, delete the DB file, scale to 1. The init container restores from S3 automatically.

### manual

Does not inject a restore init container. Recovery requires creating an explicit `LitestreamRestore` CR with `mode: InPlace`.

```yaml
spec:
  recovery:
    mode: manual
```

### Auto-recover for LTX corruption

Enable upstream Litestream's [automatic recovery](https://litestream.io/docs/troubleshooting/#option-2-automatic-recovery) to handle LTX state corruption without manual intervention:

```yaml
spec:
  backup:
    enabled: true
    autoRecover: true
```

When Litestream encounters persistent LTX errors, it automatically resets local tracking state and creates a fresh snapshot. Recommended for unattended deployments.

---

## Production checklist

- [ ] **Single replica**: Set `replicas: 1` on the target Deployment/StatefulSet
- [ ] **Safe rollout strategy**: Use `Recreate` or `RollingUpdate` with `maxSurge: 0` to prevent concurrent SQLite writers
- [ ] **ReadWriteOncePod PVC**: Use `ReadWriteOncePod` access mode where your CSI driver supports it (stronger than `ReadWriteOnce`)
- [ ] **Backup health monitoring**: Set `spec.health.maxReplicationLag` and alert on `ReplicationHealthy=False`
- [ ] **Tested restore**: Regularly test restores with `mode: ToPVC` to verify backup integrity without downtime
- [ ] **S3 durability**: Use a durable object store with versioning enabled
- [ ] **Resource requests**: Set `spec.backup.resources` on the Litestream sidecar
- [ ] **PodDisruptionBudget**: Consider a PDB for the target workload

---

## Usage examples

### Bootstrap SQL for new databases

Use `spec.bootstrap.sql` to seed the database schema on genuinely new databases. Unlike `initSQL` (removed), this runs only when no local database file exists AND no remote archive is available:

```yaml
spec:
  bootstrap:
    sql: |
      CREATE TABLE IF NOT EXISTS users (
        id    INTEGER PRIMARY KEY AUTOINCREMENT,
        name  TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE
      );
      CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
```

### Multi-container Deployments

When the database volume is mounted in a non-first container, use `spec.container`:

```yaml
spec:
  targetDeployment: my-app
  container: database-writer    # select the container with the DB volume
  databasePath: /data
  databaseName: app.db
```

### AWS S3 (no custom endpoint)

Omit `endpoint` to use standard AWS S3:

```yaml
spec:
  backup:
    enabled: true
    destination:
      s3:
        bucket: my-litestream-backups
        path: production/my-app/
        secretRef: aws-creds
```

### Disable backup (injection only)

Deploy Litestream without enabling backup — useful for testing injection:

```yaml
spec:
  targetDeployment: my-app
  databasePath: /data
  databaseName: app.db
  backup:
    enabled: false
```

---

## Annotations

The operator uses these annotations on target workloads:

| Annotation | Description |
|---|---|
| `litestream.io/inject` | Signals the mutating webhook to inject the Litestream sidecar |
| `litestream.io/config` | References the LitestreamReplica CR (`namespace/name`) that configures injection |
| `litestream.io/injection-spec-hash` | Deterministic hash of injection-relevant config; changes trigger rollouts |
| `litestream.io/pause` | When `"true"` on a CR, pauses replication without killing the sidecar |

---

## Kubernetes events

The operator emits events for operationally important transitions:

| Event | Type | Description |
|---|---|---|
| `ReplicaCountExceeded` | Warning | Target workload has more than one replica |
| `UnsafeRolloutStrategy` | Warning | Deployment uses RollingUpdate with maxSurge > 0 |
| `ReplicationHealthy` | Normal | Replication health check passed |
| `ReplicationUnhealthy` | Warning | Replication lag exceeded threshold |
| `ReplicaDeinstrumented` | Normal | Injection annotations removed on CR deletion |
| `RestoreStarted` | Normal | Restore fencing or Job creation started |
| `PausingReplication` | Normal | Replication paused before restore |
| `ApplicationFenced` | Normal | Workload scaled to zero for restore |
| `RestoreComplete` | Normal | Restore Job finished successfully |
| `ApplicationResumed` | Normal | Workload scaled back up after restore |
| `RestoreFailed` | Warning | Restore Job or operation failed |
| `DeletedMidRestore` | Warning | Restore CR deleted while in-progress |

---

## Prometheus metrics

The injected Litestream sidecar exposes metrics on port 9090. The webhook automatically sets Prometheus discovery annotations on the pod:

```yaml
prometheus.io/scrape: "true"
prometheus.io/port: "9090"
prometheus.io/path: "/metrics"
```

Existing Prometheus annotations on the pod are preserved (not overwritten).

**Recommended alerts:**

- Replication lag exceeds threshold (`ReplicationHealthy=False`)
- No successful sync within threshold
- Archive unreachable
- `LitestreamReplica` not Ready
- Restore failed / application fenced

---

## Troubleshooting

### UnsafeRolloutStrategy condition

**Symptom**: `UnsafeRolloutStrategy=True`, `Ready=False`.

**Cause**: The target Deployment uses `RollingUpdate` with `maxSurge > 0`, which can temporarily run two pods and corrupt the SQLite database.

**Fix**: Change the rollout strategy:

```yaml
strategy:
  type: Recreate
```

or:

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 0
    maxUnavailable: 1
```

### ReplicationHealthy=False

**Symptom**: Backup appears unhealthy despite Litestream sidecar running.

**Cause**: Replication lag exceeds `spec.health.maxReplicationLag` threshold, or S3 credentials are invalid, or the object store is unreachable.

**Fix**: Check Litestream sidecar logs, verify S3 credentials, and confirm network connectivity to the object store.

### Restore stuck in Fencing phase

**Symptom**: `LitestreamRestore` phase is `Fencing` and not progressing.

**Cause**: The restore controller couldn't scale the workload to zero or pause replication.

**Fix**: Check operator logs. If the workload has a PDB preventing scale-down, temporarily relax it.

### Restore failure leaves application fenced

**By design**: If a restore fails after fencing the application, the workload remains at `replicas=0`. This prevents starting against unverified data.

**Fix**: Investigate the restore Job logs, fix the issue, and create a new `LitestreamRestore`. Or manually scale the workload back up if you've verified the database state.

### CR deletion leaves stale annotations

**Symptom**: After deleting a `LitestreamReplica`, the Deployment still has injection annotations.

**Cause**: The CR's finalizer should have cleaned these up. If the operator was down during deletion, annotations may remain.

**Fix**: Manually remove `litestream.io/inject` and `litestream.io/config` annotations from the Deployment's pod template.

---

## Helm chart values

```bash
helm show values oci://ghcr.io/jlaska/charts/litestream-operator
```

Key values:

| Value | Default | Description |
|---|---|---|
| `image.repository` | `ghcr.io/jlaska/litestream-operator` | Operator image |
| `image.tag` | chart `appVersion` | Image tag |
| `replicaCount` | `1` | Operator replicas |
| `webhook.enabled` | `true` | Enable mutating/validating webhooks |
| `webhook.failurePolicy` | `Fail` | Webhook failure policy |
| `certManager.enabled` | `true` | Use cert-manager for webhook TLS |
| `certManager.secretName` | `litestream-operator-webhook-cert` | TLS secret name |
| `litestream.defaultImage` | `litestream/litestream:0.5.14` | Default sidecar image |

---

## Development

```bash
# Clone and build
git clone https://github.com/jlaska/litestream-operator
cd litestream-operator
make build

# Run unit tests
make test

# Run full integration tests (creates a Kind cluster)
make kind-test-integration

# Build and push container image
make docker-build docker-push

# Install CRDs and deploy operator locally (requires KUBECONFIG)
helm install litestream-operator charts/litestream-operator \
  --namespace litestream-operator-system \
  --create-namespace \
  --set image.pullPolicy=Never
```

See [docs/BUILD.md](./docs/BUILD.md) for full development instructions.

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](./LICENSE) for details.
