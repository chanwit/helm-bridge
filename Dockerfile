# Multi-stage build for minimal helm-bridge container
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build static binary for target platform
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="-w -s" \
    -o helm-bridge \
    .

# Final stage - minimal runtime image
FROM gcr.io/distroless/static-debian12:nonroot

# Copy binary from builder
COPY --from=builder /build/helm-bridge /usr/local/bin/helm-bridge

# Use non-root user
USER nonroot:nonroot

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/helm-bridge"]
