# Build and Development Guide

## Prerequisites

- Go 1.22+
- Docker (or Podman)
- [Kind](https://kind.sigs.k8s.io/) (for integration testing)
- Helm 3
- kubectl

## Local Development

### Build and test

```bash
# Build the operator binary
make build

# Run unit tests (uses envtest — no cluster required)
make test

# Lint
make lint

# Generate CRDs and manifests
make generate && make manifests

# Format
make fmt && make vet
```

### Run against a local cluster

```bash
# Build container image
make docker-build

# Load into Kind and deploy
kind create cluster --name litestream-dev
kind load docker-image ghcr.io/jlaska/litestream-operator:v0.4.2 --name litestream-dev
helm install litestream-operator charts/litestream-operator \
  --namespace litestream-operator-system \
  --create-namespace \
  --set image.pullPolicy=Never
```

### Integration tests

Integration tests use Kind with an in-cluster MinIO (or external S3) backend:

```bash
# Full integration test suite (creates a Kind cluster, deploys operator, runs tests)
make kind-test-integration

# Rebuild and redeploy into existing cluster, then run tests
make test-integration-redeploy
```

## Container Image

The container image is published to `ghcr.io/jlaska/litestream-operator`.

```bash
# Build
make docker-build

# Push (requires authentication to ghcr.io)
make docker-push

# Build with specific version
make docker-build IMG=ghcr.io/jlaska/litestream-operator:v0.5.0
```

## Version Management

Versions are managed in `Makefile.versions`:

```bash
# Check current version
make version

# Bump versions
make version-bump-patch  # v0.4.2 → v0.4.3
make version-bump-minor  # v0.4.2 → v0.5.0
make version-bump-major  # v0.4.2 → v1.0.0
```

## CI/CD Pipeline

GitHub Actions runs on:
- **Push to main**: Builds and runs tests
- **Pull requests**: Runs tests and linting
- **Push tags (v*)**: Builds, pushes container image, creates GitHub release

### GitHub Secrets required

| Secret | Description |
|---|---|
| `GITHUB_TOKEN` | Automatically provided; used for GHCR push and release creation |

### Pipeline stages

1. **Lint**: golangci-lint
2. **Test**: Unit tests with envtest
3. **Build**: Container image build and push to GHCR
4. **Release** (tags only): GitHub release with Helm chart

## Creating a Release

```bash
# Bump version
make version-bump-minor
git add Makefile.versions
git commit -m "bump: version to v0.5.0"

# Create and push tag
git tag v0.5.0
git push origin v0.5.0
```

This triggers the CI pipeline to build the image, push to GHCR, and create a GitHub release.

## Project Structure

```
├── api/v1/                    # CRD type definitions
├── charts/litestream-operator/ # Helm chart
├── cmd/main.go                # Operator entrypoint
├── config/                    # Kustomize bases (CRDs, RBAC, webhooks)
├── internal/
│   ├── controller/            # Reconcilers for LitestreamReplica and LitestreamRestore
│   └── webhook/               # Mutating (sidecar injection) and validating webhooks
├── test/integration/          # Integration tests (Kind + S3)
├── docs/                      # Documentation
├── Makefile                   # Build targets
└── Makefile.versions          # Version and registry configuration
```
