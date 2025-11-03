# Helm Bridge

A ConfigHub bridge worker that intelligently manages both Helm charts and standard Kubernetes manifests.

## Features

- Automatic applier selection: HelmApplier for Helm charts, CLIUtilsSSA for standard manifests
- Full Helm support: install/upgrade/uninstall with hooks and garbage collection
- Multi-architecture: linux/amd64 and linux/arm64
- Static binary with minimal container footprint

## Building

### Build from Source

```bash
# Build static binary for Linux/amd64
make build

# Build for local platform
make build-local

# View all targets
make help
```

### Build Docker Image

```bash
# Single platform
make docker

# Multi-arch (amd64, arm64) - requires one-time setup
make docker-buildx-setup
make docker-multiarch

# Build and push to registry
make docker-multiarch-push
```

## Installation

### Install to Kubernetes

Use the ConfigHub CLI to install the worker:

```bash
cub worker install helm-bridge \
  --image ghcr.io/chanwit/helm-bridge:latest \
  -e CONFIGHUB_IN_CLUSTER_TARGET_NAME=my-cluster
```

This will deploy the worker to your Kubernetes cluster and automatically configure credentials.
The worker will automatically create a Target named `my-cluster` for units to set.

### Configuration

The worker requires the following environment variables:

```bash
CONFIGHUB_WORKER_ID               # Worker ID (automatically set by cub worker install)
CONFIGHUB_WORKER_SECRET           # Worker secret (automatically set by cub worker install)
CONFIGHUB_URL                     # Optional: defaults to https://hub.confighub.com
CONFIGHUB_IN_CLUSTER_TARGET_NAME  # Optional: target name to represent the cluster
```

## Development Testing

### Test with Kind Cluster

Complete workflow to test the Helm Bridge locally with Docker and KinD:

```bash
# 1. Create a Kind cluster
kind create cluster

# 2. Login to ConfigHub
cub auth login

# 3. Create a worker
cub worker create my-worker

# 4. Build and load the Docker image into Kind
make docker
kind load docker-image ghcr.io/chanwit/helm-bridge

# 5. Install the worker to Kubernetes
cub worker install my-worker \
  --image=ghcr.io/chanwit/helm-bridge \
  --image-pull-policy=IfNotPresent \
  -e CONFIGHUB_URL=http://172.17.0.1:9090 \
  -e CONFIGHUB_IN_CLUSTER_TARGET_NAME=dev-cluster \
  --export --include-secret | kubectl apply -f -

# 6. Install a Helm chart using ConfigHub
cub helm install \
  --namespace=prometheus \
  --use-placeholder=false \
  prometheus \
  prometheus-community/kube-prometheus-stack

# 7. Set the target for the unit
cub unit set-target prometheus dev-cluster

# 8. Apply the unit
cub unit apply prometheus
```

### Verify Helm Release

```bash
# Check Helm release
helm list -n prometheus

# Check resources
kubectl get all -n prometheus
```

## License

MIT

