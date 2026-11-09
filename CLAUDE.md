# Claude Code Instructions

Kubernetes operator in Go (Kubebuilder) that injects Litestream sidecars and manages their CRDs, plus a Helm
chart under `charts/`. See [README.md](README.md) for usage and `Makefile` for build, test and lint targets.

## Safety

- **This repository is public.** Every push publishes. Don't commit credentials, internal hostnames or anything
  copied from a private homelab.
- `main` is protected by a ruleset: work on a branch and open a PR.
- CI is GitHub Actions; the image is published to `ghcr.io/jlaska/litestream-operator`. Don't push tags or images
  by hand.
- `config/rbac/` defines the operator's RBAC. Treat changes there as security-relevant and call them out.
- Integration tests use the `litestream-operator-system` and `litestream-integration` namespaces. Prefer a local
  `kind` cluster; `kubectl apply|delete` and `helm install|upgrade` against any other cluster need confirmation.
