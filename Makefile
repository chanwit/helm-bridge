# Makefile for helm-bridge

# Variables
BINARY_NAME=helm-bridge
DOCKER_IMAGE=ghcr.io/chanwit/helm-bridge
VERSION?=latest
GO=go
DOCKER=docker

# Multi-arch platforms
PLATFORMS?=linux/amd64,linux/arm64

# Build flags
LDFLAGS=-ldflags="-w -s"
BUILD_FLAGS=CGO_ENABLED=0 GOOS=linux GOARCH=amd64

.PHONY: help
help: ## Display this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

.PHONY: build
build: ## Build static binary
	@echo "Building $(BINARY_NAME)..."
	$(BUILD_FLAGS) $(GO) build $(LDFLAGS) -o $(BINARY_NAME) .
	@echo "Build complete: $(BINARY_NAME)"
	@ls -lh $(BINARY_NAME)

.PHONY: build-local
build-local: ## Build binary for local OS/ARCH
	@echo "Building $(BINARY_NAME) for local platform..."
	CGO_ENABLED=0 $(GO) build $(LDFLAGS) -o $(BINARY_NAME) .
	@echo "Build complete: $(BINARY_NAME)"

.PHONY: docker
docker: ## Build Docker image (single platform)
	@echo "Building Docker image: $(DOCKER_IMAGE):$(VERSION)..."
	$(DOCKER) build -t $(DOCKER_IMAGE):$(VERSION) .
	@echo "Docker image built: $(DOCKER_IMAGE):$(VERSION)"

.PHONY: docker-buildx-setup
docker-buildx-setup: ## Setup docker buildx for multi-arch builds
	@echo "Setting up docker buildx..."
	@$(DOCKER) buildx create --name helm-bridge-builder --use 2>/dev/null || true
	@$(DOCKER) buildx inspect --bootstrap
	@echo "Buildx setup complete"

.PHONY: docker-multiarch
docker-multiarch: ## Build multi-arch Docker image (amd64, arm64)
	@echo "Building multi-arch Docker image: $(DOCKER_IMAGE):$(VERSION)..."
	@echo "Platforms: $(PLATFORMS)"
	$(DOCKER) buildx build \
		--platform $(PLATFORMS) \
		-t $(DOCKER_IMAGE):$(VERSION) \
		--load \
		.
	@echo "Multi-arch Docker image built: $(DOCKER_IMAGE):$(VERSION)"

.PHONY: docker-multiarch-push
docker-multiarch-push: ## Build and push multi-arch Docker image
	@echo "Building and pushing multi-arch Docker image: $(DOCKER_IMAGE):$(VERSION)..."
	@echo "Platforms: $(PLATFORMS)"
	$(DOCKER) buildx build \
		--platform $(PLATFORMS) \
		-t $(DOCKER_IMAGE):$(VERSION) \
		--push \
		.
	@echo "Multi-arch Docker image pushed: $(DOCKER_IMAGE):$(VERSION)"

.PHONY: docker-push
docker-push: ## Push Docker image to registry
	@echo "Pushing Docker image: $(DOCKER_IMAGE):$(VERSION)..."
	$(DOCKER) push $(DOCKER_IMAGE):$(VERSION)

.PHONY: clean
clean: ## Clean build artifacts
	@echo "Cleaning..."
	rm -f $(BINARY_NAME)
	@echo "Clean complete"

.PHONY: test
test: ## Run tests
	@echo "Running tests..."
	$(GO) test -v ./...

.PHONY: mod-tidy
mod-tidy: ## Tidy go modules
	@echo "Tidying go modules..."
	$(GO) mod tidy

.PHONY: mod-download
mod-download: ## Download go modules
	@echo "Downloading go modules..."
	$(GO) mod download

.PHONY: all
all: clean build docker ## Clean, build binary and Docker image

.DEFAULT_GOAL := help
