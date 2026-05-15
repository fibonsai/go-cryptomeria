# Variables
BINARY_NAME=go-cryptomeria
GO_FILES=$(shell find . -name "*.go" -not -path "./vendor/*")
VERSION?=1.0.0
BUILD_DIR=bin
# Check for docker-compose vs podman-compose
DOCKER_COMPOSE := $(shell command -v docker-compose 2> /dev/null || command -v podman-compose 2> /dev/null)

# Setup the environment
.PHONY: all build clean test coverage lint run deps docker-build services-up services-down

all: clean lint test build

## Build:
build:
	@echo "Building binary..."
	go build -ldflags "-X main.version=$(VERSION)" -o $(BUILD_DIR)/$(BINARY_NAME)

clean:
	@echo "Cleaning up..."
	@go clean

## Quality Control:
lint:
	@echo "Running golangci-lint..."
	@golangci-lint run ./...

test:
	@echo "Running tests..."
	@go test -v -race ./...

coverage:
	@echo "Checking test coverage..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out

## Dependencies:
deps:
	@echo "Syncing dependencies..."
	@go mod tidy
	@go mod download

## Execution:
run: services-up build
	@./$(BUILD_DIR)/$(BINARY_NAME)

## Docker:
docker-build:
	@echo "Building Docker image..."
	docker build -t $(BINARY_NAME):$(VERSION) .

## Cross-compilation (Example for Linux/AMD64):
build-linux:
	@echo "Building for Linux..."
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/$(BINARY_NAME)-linux ./cmd/main.go

services-up:
	@if [ -z "$(DOCKER_COMPOSE)" ]; then \
		echo "Error: Neither docker-compose nor podman-compose found in PATH"; \
		exit 1; \
	fi
	@echo "Starting services using $(DOCKER_COMPOSE)..."
	$(DOCKER_COMPOSE) up -d --force-recreate

services-down:
	@if [ -z "$(DOCKER_COMPOSE)" ]; then \
		echo "Error: Neither docker-compose nor podman-compose found in PATH"; \
		exit 1; \
	fi
	@echo "Stopping services..."
	$(DOCKER_COMPOSE) down