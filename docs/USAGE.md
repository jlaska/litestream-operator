# Litestream Operator Usage Guide

This guide covers day-to-day use of the Litestream Operator for SQLite backup and recovery in Kubernetes.

## Table of Contents

- [Recovery Modes](#recovery-modes)
- [Disaster Recovery](#disaster-recovery)
- [Point-in-Time Restore](#point-in-time-restore)
- [Restore Without Downtime](#restore-without-downtime)
- [Bootstrap SQL](#bootstrap-sql)
- [Multi-Container Deployments](#multi-container-deployments)
- [Operational Annotations](#operational-annotations)
- [Troubleshooting](#troubleshooting)

## Recovery Modes

The operator supports two recovery modes that control pod startup behavior when the local database is missing or inconsistent with the remote S3 archive.

### Manual (default)

Manual mode blocks startup and requires an explicit `LitestreamRestore` to recover. This is the safest option for production.

```yaml
spec:
  recovery:
    mode: Manual
```

**Startup behavior:**

| Local DB | Remote Archive | Result |
|---|---|---|
| Exists, trusted | Any | Allow startup |
| Missing | Missing | Allow startup (genuinely new database) |
| Missing | Exists | **Block startup** — archive available, explicit restore required |
| Exists, untracked | Exists | **Block startup** — possible recreated-database-without-restore |

When startup is blocked, the `RecoverySafe` condition becomes `False` with a message explaining why. Create a `LitestreamRestore` with `mode: InPlace` to recover.

### Automatic

Automatic mode uses upstream Litestream's native restore flags:

```bash
litestream restore \
  -if-db-not-exists \
  -if-replica-exists \
  -integrity-check quick \
  -config /etc/litestream/litestream.yml \
  /path/database.sqlite
```

```yaml
spec:
  recovery:
    mode: Automatic
```

Any genuine restore failure fails the init container and **prevents application startup**. The operator never converts a restore error into a fresh database.

## Disaster Recovery

### Scenario: PVC lost, remote archive intact

**With Automatic recovery mode:**

The pod's init container automatically restores from the S3 archive on next startup. No manual intervention required — the database is restored, integrity-checked, and the application starts.

**With Manual recovery mode (default):**

1. The pod starts but blocks — `RecoverySafe=False` because no local DB exists but the archive is available.

2. Create an InPlace restore:

   ```yaml
   apiVersion: litestream.io/v1
   kind: LitestreamRestore
   metadata:
     name: recover-db
     namespace: my-app
   spec:
     sourceRef:
       name: my-app-db
     mode: InPlace
   ```

3. The restore controller:
   - Acquires a concurrency lock (one active InPlace restore per source)
   - Fences the application (scales workload to zero, pauses replication)
   - Runs the restore Job
   - Validates integrity
   - Resumes the application

4. Monitor progress:

   ```bash
   kubectl get litestreamrestore recover-db -n my-app -w
   ```

### Restore failure safety

If a restore fails after fencing the application, the workload **remains at replicas=0**. This is intentional — starting against unverified data risks corruption.

The `RestoreFailed` event and restore phase will indicate what went wrong. After investigating:

- Fix the issue and create a new `LitestreamRestore`
- Or manually verify the database and scale the workload back up

## Point-in-Time Restore

Restore the database to its state at a specific point in time using the `timestamp` field:

```yaml
apiVersion: litestream.io/v1
kind: LitestreamRestore
metadata:
  name: pitr-restore
  namespace: my-app
spec:
  sourceRef:
    name: my-app-db
  mode: InPlace
  timestamp: "2026-06-17T10:00:00Z"
```

The timestamp must be in RFC 3339 format. Litestream replays WAL entries up to this point, giving you the exact database state at that instant.

When omitted, the most recent snapshot is restored.

## Restore Without Downtime

Use `mode: ToPVC` to restore into a separate PVC without touching the source application. The source workload continues running normally throughout the restore.

```yaml
apiVersion: litestream.io/v1
kind: LitestreamRestore
metadata:
  name: paperless-clone
  namespace: paperless
spec:
  sourceRef:
    name: paperless-db
  mode: ToPVC
  target:
    pvc: paperless-restore-pvc
    path: /data/paperless.db
  timestamp: "2026-06-17T10:00:00Z"  # optional
```

Use cases:

- **Recovery testing**: Regularly verify that backups can be restored
- **Forensic inspection**: Examine database state at a point in time
- **Migration**: Clone a database for migration to another system
- **Cloning**: Create a copy for development or testing

The target PVC must already exist. The operator does not create PVCs.

## Bootstrap SQL

Bootstrap SQL seeds the database schema when a genuinely new database is created — no local database file AND no remote archive.

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

Bootstrap SQL **does not run** when:

- A local database already exists
- A database was restored from an archive

This prevents accidental re-initialization after disaster recovery. Applications should own schema migrations after bootstrap.

## Multi-Container Deployments

When the database volume is mounted in a non-first container, specify which container to use:

```yaml
spec:
  targetDeployment: my-app
  container: database-writer
  databasePath: /var/lib/app/data
  databaseName: app.db
```

The operator resolves the volume mount from the specified container, including `subPath` configurations. The Litestream sidecar and init containers use the same volume mount semantics.

## Operational Annotations

### Pause replication

Set `litestream.io/pause: "true"` on the LitestreamReplica CR to pause replication without killing the sidecar. The operator writes an empty database list to the Litestream ConfigMap.

```bash
kubectl annotate litestreamreplica my-app-db litestream.io/pause=true -n my-app
```

Remove to resume:

```bash
kubectl annotate litestreamreplica my-app-db litestream.io/pause- -n my-app
```

### Skip archive check

The archive-check init container is **automatically skipped** after an InPlace
`LitestreamRestore` completes — the restore controller sets the annotation and
the webhook also checks for a recently completed restore as a fallback. No
manual intervention is needed for the normal restore flow.

For manual bypass (e.g., intentionally starting fresh against an existing S3
backup chain), set the annotation on the LitestreamReplica CR:

```bash
kubectl annotate litestreamreplica my-app-db litestream.io/skip-archive-check=true -n my-app
```

The annotation is automatically cleared once the Litestream sidecar is healthy.

## Troubleshooting

### Pod stuck in Init (Manual recovery mode)

**Symptom**: Pod is stuck with init containers not completing.

**Diagnosis**:

```bash
kubectl describe litestreamreplica my-app-db -n my-app
# Look for RecoverySafe=False condition
```

**Cause**: Local database is missing but a remote archive exists. Manual mode requires explicit recovery.

**Fix**: Create a `LitestreamRestore` with `mode: InPlace`.

### UnsafeRolloutStrategy condition

**Symptom**: `Ready=False`, `UnsafeRolloutStrategy=True`.

**Cause**: The target Deployment uses `RollingUpdate` with `maxSurge > 0`. During a rollout, two pods could run simultaneously, creating concurrent SQLite writers and risking database corruption.

**Fix**:

```yaml
# Option 1: Recreate strategy
strategy:
  type: Recreate

# Option 2: RollingUpdate with maxSurge=0
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 0
    maxUnavailable: 1
```

### ReplicaCountExceeded condition

**Symptom**: `ReplicaCountExceeded=True`.

**Cause**: The target workload has `replicas > 1`. SQLite requires a single writer.

**Fix**: Set `replicas: 1` on the target Deployment/StatefulSet.

### ReplicationHealthy=False

**Symptom**: `ReplicationHealthy=False` despite Litestream sidecar running.

**Diagnosis**:

```bash
# Check Litestream sidecar logs
kubectl logs <pod-name> -c litestream -n my-app

# Check replication status
kubectl get litestreamreplica my-app-db -n my-app -o jsonpath='{.status.replicationLag}'
```

**Common causes**:

- S3 credentials expired or invalid
- Object store unreachable (network issue)
- Replication lag exceeds `spec.health.maxReplicationLag`

### Restore stuck in AcquiringLock

**Cause**: Another InPlace restore is active for the same source. Only one InPlace restore can run per LitestreamReplica at a time.

**Diagnosis**:

```bash
kubectl get litestreamrestore -n my-app
# Look for another restore in Fencing/Restoring/Resuming phase
```

**Fix**: Wait for the active restore to complete, or delete it if stuck.

### Bad S3 credentials

**Symptom**: Litestream sidecar logs show authentication errors. `ReplicationHealthy=False`.

**Fix**: Update the Secret referenced by `spec.backup.destination.s3.secretRef`:

```bash
kubectl create secret generic minio-creds \
  --from-literal=ACCESS_KEY_ID=<new-key> \
  --from-literal=SECRET_ACCESS_KEY=<new-secret> \
  --namespace my-app \
  --dry-run=client -o yaml | kubectl apply -f -
```

Then trigger a pod restart to pick up the new credentials.

### PVC/mount mismatch

**Symptom**: Litestream sidecar can't find the database file.

**Cause**: `spec.databasePath` doesn't match the actual mount path in the application container, or `subPath` is used but not accounted for.

**Diagnosis**:

```bash
# Check what's actually mounted
kubectl exec <pod-name> -c <app-container> -n my-app -- ls -la /data/

# Check the Litestream config
kubectl get configmap <cr-name>-litestream -n my-app -o yaml
```

**Fix**: Ensure `spec.databasePath` matches the directory where the database file lives inside the application container.
If the container uses `subPath`, the operator resolves it automatically — just specify the container-visible path.
