# Include version management
include Makefile.versions

# Legacy support - if IMG not set by versions file, use default
IMG ?= controller:latest

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: help

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Setup

.PHONY: setup
setup: ## Install pre-commit hooks and development dependencies.
	@command -v pre-commit >/dev/null 2>&1 || { echo "Error: pre-commit not found. Install with: brew install pre-commit (or pip install pre-commit)"; exit 1; }
	pre-commit install
	pre-commit install --hook-type commit-msg
	@echo ""
	@echo "✓ Pre-commit hooks installed. Run 'pre-commit run --all-files' to check the entire repo."

.PHONY: pre-commit
pre-commit: ## Run all pre-commit hooks against all files.
	pre-commit run --all-files

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases
	cp config/crd/bases/*.yaml charts/litestream-operator/crds/

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate setup-envtest ## Run tests. (go test runs go vet internally; use 'make fmt vet' to check style.)
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
	  go test ./internal/... -coverprofile cover.out

##@ Integration Tests
# Usage:
#   make kind-test-integration            — full CI cycle (create, test, destroy)
#   make test-integration-setup           — create cluster + install prereqs
#   make test-integration                 — run tests (setup already done)
#   make test-integration-teardown        — destroy cluster
#
# Variables:
#   INTEGRATION_KIND_CLUSTER   cluster name (default: litestream-operator-integration)
#   INTEGRATION_KUBECONFIG     kubeconfig written by setup (default: /tmp/litestream-integration-kubeconfig)
#   INTEGRATION_SKIP_CLUSTER_CREATE=true  skip 'kind create' (use existing cluster)
#   INTEGRATION_KEEP_NAMESPACE=true       leave test namespace after run (for debugging)
#
# External S3 backend (optional — skips in-cluster Garage when set):
#   S3_ENDPOINT    S3-compatible endpoint (e.g. https://garage.internal.keener.us)
#   S3_ACCESS_KEY  access key ID
#   S3_SECRET_KEY  secret access key
#   S3_BUCKET      bucket name (default: litestream-backups)
#
# Podman support:
#   When CONTAINER_TOOL=podman (or podman is the only available runtime),
#   the setup target sets KIND_EXPERIMENTAL_PROVIDER=podman automatically.

INTEGRATION_KIND_CLUSTER     ?= litestream-operator-integration
INTEGRATION_KUBECONFIG       ?= /tmp/litestream-integration-kubeconfig
INTEGRATION_SKIP_CLUSTER_CREATE ?= false
CERT_MANAGER_VERSION         ?= v1.16.3
OPERATOR_NAMESPACE           ?= litestream-operator-system

# Detect whether to use the Podman provider for Kind.
# Covers: explicit CONTAINER_TOOL=podman, and the common macOS case where the
# `docker` binary is Podman in Docker-compat mode (identified by "podman"
# appearing in `docker info` output).
# grep -c returns the number of matching lines (0 when absent).
_PODMAN_DETECTED := $(shell $(CONTAINER_TOOL) info 2>/dev/null | grep -ci podman 2>/dev/null || echo 0)
_KIND_PROVIDER_ENV =
ifeq ($(CONTAINER_TOOL),podman)
_KIND_PROVIDER_ENV = KIND_EXPERIMENTAL_PROVIDER=podman
else ifneq ($(_PODMAN_DETECTED),0)
_KIND_PROVIDER_ENV = KIND_EXPERIMENTAL_PROVIDER=podman
endif

.PHONY: test-integration-setup
test-integration-setup: docker-build ## Create Kind cluster (with Podman support) and install prerequisites
	@command -v $(KIND) >/dev/null 2>&1 || { echo "Error: kind not found"; exit 1; }
	@command -v helm   >/dev/null 2>&1 || { echo "Error: helm not found"; exit 1; }

	@if [ "$(INTEGRATION_SKIP_CLUSTER_CREATE)" = "true" ]; then \
		echo "==> INTEGRATION_SKIP_CLUSTER_CREATE=true — skipping cluster creation"; \
	else \
		echo "==> Creating Kind cluster $(INTEGRATION_KIND_CLUSTER) (provider: $(_KIND_PROVIDER_ENV:-docker))"; \
		$(_KIND_PROVIDER_ENV) $(KIND) create cluster \
			--name $(INTEGRATION_KIND_CLUSTER) --wait 120s \
			|| echo "(cluster may already exist — continuing)"; \
	fi

	@echo "==> Exporting kubeconfig → $(INTEGRATION_KUBECONFIG)"
	$(_KIND_PROVIDER_ENV) $(KIND) export kubeconfig \
		--name $(INTEGRATION_KIND_CLUSTER) \
		--kubeconfig $(INTEGRATION_KUBECONFIG)

	@echo "==> Loading operator image into Kind"
	$(_KIND_PROVIDER_ENV) $(KIND) load docker-image $(IMG) \
		--name $(INTEGRATION_KIND_CLUSTER)

	@echo "==> Pre-loading test images into Kind (avoids slow/failed pulls during tests)"
	@for img in \
	  litestream/litestream:0.5.14 \
	  keinos/sqlite3:latest \
	  quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z; do \
	    echo "    pulling $$img"; \
	    $(CONTAINER_TOOL) pull $$img; \
	    $(_KIND_PROVIDER_ENV) $(KIND) load docker-image $$img --name $(INTEGRATION_KIND_CLUSTER); \
	done
	@if [ -z "$(S3_ENDPOINT)" ]; then \
	    echo "    pulling dxflrs/garage:v1.1.0"; \
	    $(CONTAINER_TOOL) pull dxflrs/garage:v1.1.0; \
	    $(_KIND_PROVIDER_ENV) $(KIND) load docker-image dxflrs/garage:v1.1.0 --name $(INTEGRATION_KIND_CLUSTER); \
	else \
	    echo "    (skipping Garage image — using external S3)"; \
	fi

	@echo "==> Installing cert-manager $(CERT_MANAGER_VERSION)"
	KUBECONFIG=$(INTEGRATION_KUBECONFIG) kubectl apply -f \
		https://github.com/cert-manager/cert-manager/releases/download/$(CERT_MANAGER_VERSION)/cert-manager.yaml
	KUBECONFIG=$(INTEGRATION_KUBECONFIG) kubectl -n cert-manager wait \
		--for=condition=Available deployment/cert-manager-webhook --timeout=5m

	@echo "==> Deploying litestream-operator via Helm (pullPolicy=Never — uses loaded image)"
	KUBECONFIG=$(INTEGRATION_KUBECONFIG) helm upgrade --install litestream-operator charts/litestream-operator \
		--namespace $(OPERATOR_NAMESPACE) --create-namespace \
		--set image.repository=$(REGISTRY)/$(ORG)/$(IMAGE_NAME) \
		--set image.tag=$(VERSION) \
		--set image.pullPolicy=Never \
		--wait --timeout 5m

	@echo ""
	@echo "✓ Setup complete. Run 'make test-integration' to execute tests."

.PHONY: test-integration
test-integration: ## Run integration tests (requires setup to be done first)
	KUBECONFIG=$(INTEGRATION_KUBECONFIG) \
	S3_ENDPOINT=$(S3_ENDPOINT) \
	S3_ACCESS_KEY=$(S3_ACCESS_KEY) \
	S3_SECRET_KEY=$(S3_SECRET_KEY) \
	S3_BUCKET=$(S3_BUCKET) \
		go test ./test/integration/... -v -timeout 20m

.PHONY: test-integration-redeploy
test-integration-redeploy: docker-build ## Rebuild image, reload into existing cluster, restart operator, then run tests.
	@echo "==> Loading updated image into Kind cluster $(INTEGRATION_KIND_CLUSTER)"
	$(_KIND_PROVIDER_ENV) $(KIND) load docker-image $(IMG) \
		--name $(INTEGRATION_KIND_CLUSTER)
	@echo "==> Pre-loading test images into Kind (avoids slow pulls and backoff failures)"
	@for img in \
	  litestream/litestream:0.5.14 \
	  keinos/sqlite3:latest \
	  quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z; do \
	    echo "    $$img"; \
	    $(CONTAINER_TOOL) pull $$img 2>/dev/null || true; \
	    $(_KIND_PROVIDER_ENV) $(KIND) load docker-image $$img --name $(INTEGRATION_KIND_CLUSTER); \
	done
	@if [ -z "$(S3_ENDPOINT)" ]; then \
	    echo "    dxflrs/garage:v1.1.0"; \
	    $(CONTAINER_TOOL) pull dxflrs/garage:v1.1.0 2>/dev/null || true; \
	    $(_KIND_PROVIDER_ENV) $(KIND) load docker-image dxflrs/garage:v1.1.0 --name $(INTEGRATION_KIND_CLUSTER); \
	fi
	@echo "==> Restarting operator"
	KUBECONFIG=$(INTEGRATION_KUBECONFIG) kubectl rollout restart \
		deployment/litestream-operator -n $(OPERATOR_NAMESPACE)
	KUBECONFIG=$(INTEGRATION_KUBECONFIG) kubectl rollout status \
		deployment/litestream-operator -n $(OPERATOR_NAMESPACE) --timeout=2m
	$(MAKE) test-integration

.PHONY: test-integration-teardown
test-integration-teardown: ## Destroy the integration test Kind cluster
	$(_KIND_PROVIDER_ENV) $(KIND) delete cluster \
		--name $(INTEGRATION_KIND_CLUSTER) || true
	@rm -f $(INTEGRATION_KUBECONFIG)

.PHONY: kind-test-integration
kind-test-integration: ## Full integration test cycle: setup → test → teardown (CI-ready)
	$(MAKE) test-integration-setup
	$(MAKE) test-integration \
		|| ($(MAKE) test-integration-teardown; exit 1)
	$(MAKE) test-integration-teardown

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify

##@ Build

# Source files that trigger a manager rebuild. Make compares bin/manager's
# mtime against these; if nothing changed, 'make build' is a no-op.
MANAGER_SOURCES := $(wildcard cmd/*.go) \
                   $(wildcard api/v1/*.go) \
                   $(wildcard internal/controller/*.go) \
                   $(wildcard internal/webhook/*.go)

.PHONY: build
build: bin/manager ## Build manager binary (idempotent — skips rebuild when sources are unchanged).

bin/manager: go.mod go.sum $(MANAGER_SOURCES) | $(LOCALBIN)
	go build -o bin/manager ./cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

.PHONY: clean
clean: ## Remove build artifacts: manager binary, coverage report, and release packages.
	rm -f bin/manager cover.out
	rm -rf release/ dist/

.PHONY: clean-tools
clean-tools: ## Remove downloaded tool binaries (controller-gen, kustomize, setup-envtest, golangci-lint).
	rm -rf bin/

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name litestream-operator-builder
	$(CONTAINER_TOOL) buildx use litestream-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm litestream-operator-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default > dist/install.yaml

##@ CI/CD

.PHONY: ci-build
ci-build: manifests generate fmt vet test docker-build ## Full CI build pipeline

.PHONY: ci-deploy-manifests
ci-deploy-manifests: manifests ## Generate release artifacts (Helm chart replaces kustomize deploy/)
	mkdir -p release
	cp deploy/samples/sample-litestreamreplica.yaml release/samples.yaml

.PHONY: helm-lint
helm-lint: ## Lint the Helm chart
	helm lint charts/litestream-operator

.PHONY: helm-package
helm-package: ## Package the Helm chart into release/
	mkdir -p release
	helm package charts/litestream-operator \
		--version $(VERSION:v%=%) \
		--app-version $(VERSION:v%=%) \
		--destination release/

.PHONY: helm-push
helm-push: ## Push the Helm chart OCI artifact to the registry (requires helm registry login)
	helm push release/litestream-operator-$(VERSION:v%=%).tgz oci://$(REGISTRY)/$(ORG)/charts
	@echo "Chart available at: oci://$(REGISTRY)/$(ORG)/charts/litestream-operator"

.PHONY: helm-template
helm-template: ## Render the Helm chart with default values (dry-run)
	helm template litestream-operator charts/litestream-operator \
		--namespace $(OPERATOR_NAMESPACE) \
		--set image.tag=$(VERSION)

.PHONY: release-prepare
release-prepare: version ci-deploy-manifests helm-package ## Prepare files for release
	@echo "Prepared release $(VERSION) with image $(IMG)"
	@echo "Release files:"
	@ls -la release/

# CHART_VER strips the leading 'v' for Chart.yaml (semver doesn't include it).
CHART_VER := $(VERSION:v%=%)

.PHONY: chart-version-sync
chart-version-sync: ## Sync charts/litestream-operator/Chart.yaml version and appVersion from Makefile.versions
	@echo "Syncing Chart.yaml to version $(CHART_VER)"
	@sed -i.bak \
		-e 's/^version:.*/version: $(CHART_VER)/' \
		-e 's/^appVersion:.*/appVersion: "$(CHART_VER)"/' \
		charts/litestream-operator/Chart.yaml
	@rm -f charts/litestream-operator/Chart.yaml.bak

.PHONY: tag
tag: chart-version-sync ## Commit version bump + Chart.yaml, create and push git tag (run after version-bump-*)
	@echo "Tagging release $(VERSION)"
	git add Makefile.versions charts/litestream-operator/Chart.yaml
	git commit -m "chore: release $(VERSION)"
	git tag $(VERSION) -m "Release $(VERSION)"
	git push origin HEAD
	git push origin $(VERSION)
	@echo ""
	@echo "✓ Tag $(VERSION) pushed — CI will build and publish the release"

.PHONY: docker-build-multiarch
docker-build-multiarch: ## Build multi-architecture Docker image
	$(CONTAINER_TOOL) buildx build --platform=linux/amd64,linux/arm64 --push -t ${IMG} .

.PHONY: update-deploy-image
update-deploy-image: ## Update image tag in Helm chart values
	sed -i.bak 's|^  tag:.*|  tag: "$(VERSION)"|' charts/litestream-operator/values.yaml && rm -f charts/litestream-operator/values.yaml.bak

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default | $(KUBECTL) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.6.0
CONTROLLER_TOOLS_VERSION ?= v0.18.0
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell go list -m -f "{{ .Version }}" sigs.k8s.io/controller-runtime | awk -F'[v.]' '{printf "release-%d.%d", $$2, $$3}')
#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
GOLANGCI_LINT_VERSION ?= v2.12.2

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef
