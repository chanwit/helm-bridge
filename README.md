# Helm Bridge

A ConfigHub bridge for managing Helm releases on Kubernetes clusters.

## Architecture

The HelmBridge uses **composition** to delegate operations to the SDK's `KubernetesBridgeWorker`:

```go
type HelmBridge struct {
    *impl.KubernetesBridgeWorker  // Embedded for delegation
    name string
}
```

**Delegation Strategy:**
- ✅ **Info()** - Overridden to discover Kubernetes contexts as targets
- ✅ **Apply/Refresh/Import/Destroy/Finalize** - Delegated to embedded `KubernetesBridgeWorker`

The embedded worker handles all the heavy lifting, including:
- Intelligent applier selection (HelmApplier vs CLIUtilsSSA)
- Retry logic with exponential backoff
- Async operations (Apply → WatchForApply pattern)
- Inventory management
- LiveState updates

## Overview

Operations supported through delegation:
- **Apply**: Install or upgrade Helm releases
- **Refresh**: Check current state and detect drift
- **Import**: Discover existing Helm releases
- **Destroy**: Uninstall Helm releases
- **Finalize**: Cleanup operations

## Configuration

Set the following environment variables:

```bash
export CONFIGHUB_WORKER_ID=your-worker-id
export CONFIGHUB_WORKER_SECRET=your-worker-secret
export CONFIGHUB_URL=https://hub.confighub.com  # Optional: defaults to https://hub.confighub.com
export CONFIGHUB_IN_CLUSTER_TARGET_NAME=my-cluster  # Optional: when running in-cluster
```

## GitHub Actions

The project includes automated CI/CD workflows:

### CI Workflow (`.github/workflows/ci.yml`)
Runs on every push to `main` and on pull requests:
- ✅ Linting with `go vet`
- ✅ Tests with race detector
- ✅ Build static binary
- ✅ Build multi-arch Docker image (test only, no push)
- ✅ Code coverage reporting

### Release Workflow (`.github/workflows/release.yml`)
Triggered when pushing version tags (e.g., `v1.0.0`):
- ✅ Builds multi-arch Docker images (linux/amd64, linux/arm64)
- ✅ Pushes to GitHub Container Registry
- ✅ Generates artifact attestation
- ✅ Creates semantic version tags:
  - `v1.2.3` (exact version)
  - `v1.2` (minor version)
  - `v1` (major version)
  - Git SHA tag

**To create a release:**
```bash
git tag v1.0.0
git push origin v1.0.0
```

## Building

### Build static binary (Linux/amd64)
```bash
make build
```

### Build for local platform
```bash
make build-local
```

### Build Docker image (single platform)
```bash
make docker
```

### Build multi-arch Docker image (amd64, arm64)
```bash
# Setup buildx (one-time)
make docker-buildx-setup

# Build multi-arch image
make docker-multiarch

# Build and push multi-arch image
make docker-multiarch-push
```

### All available targets
```bash
make help
```

## Running

### Run locally
```bash
./helm-bridge
```

### Run with Docker
```bash
docker run -v ~/.kube:/home/nonroot/.kube:ro \
  -e CONFIGHUB_WORKER_ID=your-worker-id \
  -e CONFIGHUB_WORKER_SECRET=your-worker-secret \
  ghcr.io/chanwit/helm-bridge:latest
```

## Target Discovery

The bridge automatically discovers available Kubernetes contexts from your kubeconfig and exposes them as targets. Each target is created with:

- **ToolchainType**: `KubernetesYAML`
- **ProviderType**: `Kubernetes`
- **Parameters**:
  - `KubeContext`: The Kubernetes context name from kubeconfig
  - `WaitTimeout`: Default `10m0s` - timeout for waiting on Helm operations

When running inside a Kubernetes cluster, the bridge uses in-cluster configuration and creates a single target.

## Target Parameters

Each target accepts the following parameters:

```json
{
  "KubeContext": "my-context",
  "WaitTimeout": "10m0s"
}
```

- **KubeContext**: The Kubernetes context to use (from kubeconfig)
- **WaitTimeout**: Duration string for operation timeout (e.g., "5m0s", "10h5m")

## Applier Selection

The bridge intelligently selects the appropriate applier based on the unit configuration:

### HelmApplier (for Helm charts)
Used when a unit has **all** required Helm labels:
- `HelmRelease` - Release name
- `HelmChart` - Chart name
- `HelmChartVersion` - Chart version
- `HelmChartAPIVersion` - Chart API version

**Features:**
- Native Helm install/upgrade/uninstall mechanics
- Helm release tracking and history
- Hook execution (pre/post install, upgrade, delete)
- Garbage collection on upgrade
- Wait for resource readiness

### CLIUtilsSSA (for regular Kubernetes manifests)
Used when:
- Unit is **not** a Helm chart (missing Helm labels)
- Unit is a Helm chart but **only contains CRDs** (exception case)

**Features:**
- Server-Side Apply (SSA) for declarative resource management
- Inventory tracking
- Drift detection

## Status

The bridge is fully implemented with:
- [x] Kubernetes context discovery from kubeconfig
- [x] In-cluster configuration support
- [x] Target parameters (KubeContext, WaitTimeout)
- [x] Dynamic applier selection (HelmApplier vs CLIUtilsSSA)
- [x] Helm install/upgrade operations with hooks
- [x] Helm status checking and drift detection
- [x] Helm release import
- [x] Helm uninstall operations with namespace cleanup
- [x] Proper error handling and logging
- [x] **Clean architecture via delegation** (44 lines in helm_bridge.go)

## Project Structure

```
helm-bridge/
├── main.go                     # Entry point (43 lines)
├── helm_bridge.go              # Bridge implementation with delegation (44 lines)
├── helm_applier.go             # HelmApplier implementation (1,419 lines)
├── Dockerfile                  # Multi-arch container build
├── Makefile                    # Build automation
├── .github/workflows/
│   ├── ci.yml                  # CI workflow (tests, builds)
│   └── release.yml             # Release workflow (tag-triggered)
└── .deps/sdk/                  # SDK reference (read-only)
```

**Key Benefits:**
- ✅ Minimal code duplication (reuses 1,100+ lines from KubernetesBridgeWorker)
- ✅ Automatic updates when SDK improves
- ✅ All SDK features available (retry logic, inventory, async ops)
- ✅ Only customizes what's different (target discovery)

## Docker Image

**Features:**
- ✅ **Static binary** - No CGO dependencies
- ✅ **Multi-arch support** - linux/amd64, linux/arm64
- ✅ **Minimal base** - Distroless (static-debian12:nonroot)
- ✅ **Security** - Runs as non-root user
- ✅ **Small size** - ~80MB compressed

**Binary Size:**
- With debug info: 106M
- Stripped (`-ldflags="-w -s"`): 76M

## License

MIT
