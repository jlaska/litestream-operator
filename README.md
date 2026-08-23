# litestream-operator

[![Tests](https://github.com/jlaska/litestream-operator/actions/workflows/test.yml/badge.svg)](https://github.com/jlaska/litestream-operator/actions/workflows/test.yml)
[![Lint](https://raw.githubusercontent.com/jlaska/litestream-operator/badges/.badges/main/lint.svg)](https://github.com/jlaska/litestream-operator/actions/workflows/test.yml)
[![Coverage](https://raw.githubusercontent.com/jlaska/litestream-operator/badges/.badges/main/coverage.svg)](https://github.com/jlaska/litestream-operator/actions/workflows/test.yml)
[![CI/CD Pipeline](https://github.com/jlaska/litestream-operator/actions/workflows/ci.yaml/badge.svg)](https://github.com/jlaska/litestream-operator/actions/workflows/ci.yaml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Container Image](https://img.shields.io/badge/container-ghcr.io-blue)](https://github.com/jlaska/litestream-operator/pkgs/container/litestream-operator)

> **Continuous S3 backup for SQLite databases running in Kubernetes** — no application changes required.

litestream-operator injects a [Litestream](https://litestream.io) sidecar into your existing application pods, streaming WAL changes to any S3-compatible object store (MinIO, AWS S3, Backblaze B2, …) in real time. Declare a `LitestreamReplica` resource, point it at your app's Deployment, and get point-in-time-recoverable database backups without touching your application code.

---

## Quick start

### 1. Install the operator

```bash
helm install litestream-operator oci://ghcr.io/jlaska/charts/litestream-operator \
  --version 0.4.1 \
  --namespace litestream-operator-system \
  --create-namespace
```

> **Prerequisites**: Kubernetes ≥ 1.28, Helm 3, [cert-manager](https://cert-manager.io/) installed in the cluster.
>
> To skip cert-manager (bring your own webhook TLS secret):
> ```bash
> helm install litestream-operator oci://ghcr.io/jlaska/charts/litestream-operator \
>   --version 0.4.1 \
>   --namespace litestream-operator-system \
>   --create-namespace \
>   --set certManager.enabled=false \
>   --set certManager.secretName=my-tls-secret
> ```

### 2. Create an S3 credentials Secret

```bash
kubectl create secret generic minio-creds \
  --from-literal=ACCESS_KEY_ID=<your-access-key> \
  --from-literal=SECRET_ACCESS_KEY=<your-secret-key> \
  --namespace my-app
```

### 3. Declare a LitestreamReplica resource

```yaml
apiVersion: litestream.io/v1
kind: LitestreamReplica
metadata:
  name: paperless-db
  namespace: paperless
spec:
  # The existing Deployment that owns the database file.
  targetDeployment: paperless-webserver

  # Where the database file lives inside the app container.
  databasePath: /usr/src/paperless/data
  databaseName: paperless.db

  # Backup configuration — streams WAL changes continuously to S3.
  backup:
    enabled: true
    destination:
      s3:
        endpoint: minio.homelab:9000     # omit for AWS S3
        bucket: litestream-backups
        path: paperless/
        secretRef: minio-creds
    retention:
      duration: "720h"              # 30 days (Litestream 0.5.x duration-based retention)
```

Apply it:

```bash
kubectl apply -f paperless-db.yaml
```

The operator annotates `paperless-webserver`, which triggers a rolling update. New pods get the Litestream sidecar injected automatically — no Deployment changes required.

### 4. Verify

```bash
# Check injection and backup health
kubectl get litestreamreplica paperless-db -n paperless

# NAME           TARGET                DATABASE       BACKUP  PHASE  READY
# paperless-db   paperless-webserver   paperless.db   true    Ready  true

kubectl describe litestreamreplica paperless-db -n paperless
# Conditions:
#   SidecarInjected  True   Annotated
#   BackupHealthy    True   SidecarRunning
#   Ready            True   DeploymentReady
```

---

## How it works

litestream-operator is to SQLite what [CloudNativePG](https://cloudnative-pg.io) is to PostgreSQL — a Kubernetes-native orchestration layer that handles backup, lifecycle, and observability at the database layer. Litestream does for SQLite what Barman Cloud does for PostgreSQL.

```
┌─────────────────────────────────────┐
│  Application Pod (after rollout)    │
│                                     │
│  ┌─────────────┐ ┌───────────────┐  │
│  │   app       │ │  litestream   │  │
│  │  container  │ │   sidecar     │  │
│  │             │ │               │  │
│  │  reads/     │ │  streams WAL  │──┼──► S3 / MinIO
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
4. The mutating webhook intercepts pod creation and injects the Litestream sidecar
5. Litestream streams WAL changes to S3 continuously; the operator monitors sidecar health

---

## CRD reference

### LitestreamReplica

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.targetDeployment` | string | ✓ | Name of the existing Deployment to inject into |
| `spec.databasePath` | string | ✓ | Directory path inside the app container (e.g. `/data`) |
| `spec.databaseName` | string | ✓ | Filename of the SQLite database (e.g. `app.db`) |
| `spec.image` | string | | Litestream image override (default: `litestream/litestream:0.5.14`) |
| `spec.backup.enabled` | bool | | Enable Litestream replication (default: `false`) |
| `spec.backup.destination.s3.endpoint` | string | | S3-compatible endpoint URL (e.g. `minio.homelab:9000`); omit for AWS S3 |
| `spec.backup.destination.s3.bucket` | string | ✓ (when enabled) | S3 bucket name |
| `spec.backup.destination.s3.path` | string | | Key prefix within the bucket |
| `spec.backup.destination.s3.secretRef` | string | ✓ (when enabled) | Secret containing `ACCESS_KEY_ID` and `SECRET_ACCESS_KEY` |
| `spec.backup.retention.duration` | string | | How long to retain backups as a duration string (default: `"720h"`) |
| `spec.initSQL` | string | | SQL statements applied once on first use (idempotent via content hash) |
| `spec.initImage` | string | | Init container image for applying `initSQL` (default: `keinos/sqlite3:latest`) |

**Status conditions:**

| Condition | Meaning |
|---|---|
| `SidecarInjected` | Litestream sidecar annotation applied to target Deployment |
| `BackupHealthy` | Litestream sidecar is running in ≥1 pod |
| `InitSQLApplied` | `initSQL` ConfigMap is ready (SQL will be applied on next pod start) |
| `Ready` | Target Deployment has ready replicas |

```bash
kubectl get litestreamreplica -A
# NAMESPACE    NAME          TARGET         DATABASE   BACKUP  PHASE  READY  AGE
# paperless    paperless-db  paperless-web  app.db     true    Ready  true   3d
```

### LitestreamRestore

Trigger a point-in-time restore from any `LitestreamReplica` backup:

```yaml
apiVersion: litestream.io/v1
kind: LitestreamRestore
metadata:
  name: paperless-restore
  namespace: paperless
spec:
  sourceRef: paperless-db         # which LitestreamReplica's backup to restore from
  targetPVC: paperless-restore    # PVC to write the restored database into
  targetPath: /data/paperless.db  # full path including filename
  timestamp: "2026-06-17T10:00:00Z"  # optional: point-in-time recovery
```

The operator creates a Kubernetes Job that runs `litestream restore`. Monitor progress:

```bash
kubectl get litestreamrestore paperless-restore -n paperless
# NAME                SOURCE         TARGETPVC           PHASE     AGE
# paperless-restore   paperless-db   paperless-restore   Complete  2m
```

---

## Usage examples

### Idempotent schema initialization

Use `initSQL` to seed the database schema on first use. The operator tracks a SHA-256 hash of the content — the SQL is applied once and re-applied when the content changes (useful for additive schema migrations):

```yaml
spec:
  initSQL: |
    CREATE TABLE IF NOT EXISTS users (
      id    INTEGER PRIMARY KEY AUTOINCREMENT,
      name  TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE
    );
    CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
```

### Disable backup (injection only)

Deploy Litestream without enabling backup — useful for testing injection, or when backup is handled externally:

```yaml
spec:
  targetDeployment: my-app
  databasePath: /data
  databaseName: app.db
  backup:
    enabled: false
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

### Override the Litestream image

```yaml
spec:
  image: litestream/litestream:0.5.14
```

---

## Helm chart values

```bash
# List all available values
helm show values oci://ghcr.io/jlaska/charts/litestream-operator --version 0.2.0
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
