.PHONY: build run clean test docker-build docker-run

BINARY_NAME=walg-mysql-exporter
DOCKER_IMAGE=walg-mysql-exporter
VERSION?=latest

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	go build -o $(BINARY_NAME) -ldflags="-s -w" .
	@echo "Build complete: $(BINARY_NAME)"

# Build for Linux (useful for Docker)
build-linux:
	@echo "Building $(BINARY_NAME) for Linux..."
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -ldflags="-w -s" -o $(BINARY_NAME)-linux .
	@echo "Build complete: $(BINARY_NAME)-linux"

# Run locally
run:
	@echo "Running $(BINARY_NAME)..."
	go run . --scrape.interval=10s

# Run with config file
run-with-config:
	@echo "Running $(BINARY_NAME) with config..."
	go run . --scrape.interval=10s --walg.config-path=./config.json

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f $(BINARY_NAME) $(BINARY_NAME)-linux
	go clean
	@echo "Clean complete"

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Lint code
lint:
	@echo "Linting code..."
	golangci-lint run

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE):$(VERSION) .
	@echo "Docker image built: $(DOCKER_IMAGE):$(VERSION)"

# Run in Docker
docker-run:
	@echo "Running in Docker..."
	docker run -p 9352:9352 \
		-e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
		-e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
		-e WALG_S3_PREFIX=${WALG_S3_PREFIX} \
		-e WALG_MYSQL_DATASOURCE_NAME=${WALG_MYSQL_DATASOURCE_NAME} \
		$(DOCKER_IMAGE):$(VERSION)

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod download
	go mod tidy

# Show help
help:
	@echo "Available targets:"
	@echo "  build              - Build the binary"
	@echo "  build-linux        - Build for Linux (for Docker)"
	@echo "  run                - Run locally"
	@echo "  run-with-config    - Run with config file"
	@echo "  clean              - Clean build artifacts"
	@echo "  test               - Run tests"
	@echo "  fmt                - Format code"
	@echo "  lint               - Lint code"
	@echo "  docker-build       - Build Docker image"
	@echo "  docker-run         - Run in Docker"
	@echo "  deps               - Install dependencies"
	@echo "  help               - Show this help"
