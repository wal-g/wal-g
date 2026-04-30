# MariaDB + WAL-G Build & Test

.PHONY: all build test clean

# Variables
IMAGE_NAME := mariadb-walg
IMAGE_TAG := latest
WALG_BINARY := ../../main/mysql/wal-g-linux

all: build test

# Build wal-g for Linux
build-walg:
	@echo "Building wal-g for Linux..."
	cd ../.. && $(MAKE) linux-build

# Copy wal-g binary
copy-walg: build-walg
	@echo "Copying wal-g binary..."
	cp $(WALG_BINARY) ./wal-g
	chmod +x ./wal-g

# Build Docker image
build: copy-walg
	@echo "Building Docker image..."
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

# Run E2E tests
test:
	@echo "Running E2E tests..."
	bash test-e2e.sh

# Clean up
clean:
	@echo "Cleaning up..."
	docker-compose -f docker-compose.test.yml down -v || true
	rm -f ./wal-g
	docker rmi $(IMAGE_NAME):$(IMAGE_TAG) || true

# Quick test (assumes image is built)
quick-test:
	bash test-e2e.sh

# Build and test
all-test: build test

# Show logs
logs-source:
	docker logs pitr-source -f

logs-restore:
	docker logs pitr-restore -f
