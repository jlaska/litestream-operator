# Litestream Operator Installation

## Prerequisites

- Kubernetes >= 1.28
- Helm 3
- [cert-manager](https://cert-manager.io/) installed in the cluster (or bring your own webhook TLS secret)

## Install with Helm

```bash
helm install litestream-operator oci://ghcr.io/jlaska/charts/litestream-operator \
  --version 0.4.1 \
  --namespace litestream-operator-system \
  --create-namespace
```

### Without cert-manager

If you manage your own TLS certificates for webhooks:

```bash
helm install litestream-operator oci://ghcr.io/jlaska/charts/litestream-operator \
  --version 0.4.1 \
  --namespace litestream-operator-system \
  --create-namespace \
  --set certManager.enabled=false \
  --set certManager.secretName=my-tls-secret
```

The TLS secret must contain `tls.crt` and `tls.key` entries signed by a CA that the API server trusts for webhook traffic.

## Verify the installation

```bash
# Check the operator is running
kubectl get pods -n litestream-operator-system
# NAME                                                    READY   STATUS    RESTARTS   AGE
# litestream-operator-controller-manager-xxxxx-xxxxx      1/1     Running   0          30s

# Check CRDs are installed
kubectl get crd | grep litestream
# litestreamreplicas.litestream.io    2026-01-01T00:00:00Z
# litestreamrestores.litestream.io    2026-01-01T00:00:00Z

# Check webhooks are registered
kubectl get validatingwebhookconfigurations | grep litestream
kubectl get mutatingwebhookconfigurations | grep litestream
```

## Helm chart values

```bash
helm show values oci://ghcr.io/jlaska/charts/litestream-operator --version 0.4.1
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
| `litestream.defaultImage` | `litestream/litestream:0.5.14` | Default Litestream sidecar image |

## Upgrade

```bash
helm upgrade litestream-operator oci://ghcr.io/jlaska/charts/litestream-operator \
  --version <new-version> \
  --namespace litestream-operator-system
```

CRD updates are included in the chart. After upgrading, verify existing `LitestreamReplica` resources reconcile correctly:

```bash
kubectl get litestreamreplica -A
```

## Uninstall

```bash
helm uninstall litestream-operator --namespace litestream-operator-system
```

This removes the operator but leaves CRDs and existing `LitestreamReplica`/`LitestreamRestore` resources in place. To remove CRDs (this deletes all custom resources):

```bash
kubectl delete crd litestreamreplicas.litestream.io litestreamrestores.litestream.io
```

## Development deployment

For local development against a Kind or minikube cluster:

```bash
# Build the operator image
make docker-build

# Load into Kind
kind load docker-image ghcr.io/jlaska/litestream-operator:latest

# Install with Helm using local image
helm install litestream-operator charts/litestream-operator \
  --namespace litestream-operator-system \
  --create-namespace \
  --set image.pullPolicy=Never
```

See [BUILD.md](./BUILD.md) for full development instructions.
