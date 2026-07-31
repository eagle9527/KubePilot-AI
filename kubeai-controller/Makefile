# KubePilot AI Controller Makefile

# Variables
VERSION ?= v0.1.0
IMAGE_NAME ?= kubepilot-ai/kubeai-controller
IMG ?= $(IMAGE_NAME):$(VERSION)

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Kubernetes parameters
KUBECTL=kubectl
NAMESPACE=kubeai-system

# Build variables
BUILD_DIR=bin
MAIN_PACKAGE=./cmd/main.go
BINARY_NAME=kubeai-controller

.PHONY: all build clean test deps fmt vet lint docker-build docker-push deploy undeploy help

# Default target
all: clean deps fmt vet test build

## help: Show this help message
help:
	@echo "KubePilot AI Controller Makefile"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## build: Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) -v $(MAIN_PACKAGE)
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@$(GOCLEAN)
	@echo "Clean complete"

## test: Run tests
test:
	@echo "Running tests..."
	@$(GOTEST) -v ./...

## deps: Download dependencies
deps:
	@echo "Downloading dependencies..."
	@$(GOMOD) download
	@$(GOMOD) tidy
	@echo "Dependencies ready"

## fmt: Format Go code
fmt:
	@echo "Formatting code..."
	@gofmt -s -w .
	@echo "Format complete"

## vet: Run go vet
vet:
	@echo "Running go vet..."
	@go vet ./...

## lint: Run linter
lint:
	@echo "Running linter..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed, skipping lint checks"; \
	fi

## docker-build: Build Docker image
docker-build:
	@echo "Building Docker image $(IMG)..."
	@docker build -t $(IMG) -f Dockerfile .
	@echo "Docker image built: $(IMG)"

## docker-push: Push Docker image
docker-push:
	@echo "Pushing Docker image $(IMG)..."
	@docker push $(IMG)
	@echo "Docker image pushed: $(IMG)"

## kind-load: Load image into kind cluster
kind-load:
	@echo "Loading image into kind cluster..."
	@kind load docker-image $(IMG)
	@echo "Image loaded into kind cluster"

## deploy: Deploy to Kubernetes cluster
deploy:
	@echo "Deploying to Kubernetes cluster..."
	@echo "Creating namespace..."
	@$(KUBECTL) apply -f deploy/namespace.yaml
	@echo "Applying CRD..."
	@$(KUBECTL) apply -f deploy/crd.yaml
	@echo "Creating secrets..."
	@echo "WARNING: Please update deploy/secret.yaml with your actual API keys before deploying"
	@$(KUBECTL) apply -f deploy/secret.yaml || true
	@echo "Applying RBAC..."
	@$(KUBECTL) apply -f deploy/rbac.yaml
	@echo "Deploying controller..."
	@$(KUBECTL) apply -f deploy/deployment.yaml
	@echo "Deployment complete!"
	@echo ""
	@echo "Check pod status:"
	@echo "  kubectl get pods -n $(NAMESPACE)"

## undeploy: Remove from Kubernetes cluster
undeploy:
	@echo "Removing from Kubernetes cluster..."
	@$(KUBECTL) delete -f deploy/deployment.yaml --ignore-not-found=true
	@$(KUBECTL) delete -f deploy/rbac.yaml --ignore-not-found=true
	@$(KUBECTL) delete -f deploy/secret.yaml --ignore-not-found=true
	@$(KUBECTL) delete -f deploy/crd.yaml --ignore-not-found=true
	@$(KUBECTL) delete -f deploy/namespace.yaml --ignore-not-found=true
	@echo "Undeployment complete"

## logs: View controller logs
logs:
	@echo "Fetching controller logs..."
	@$(KUBECTL) logs -n $(NAMESPACE) -l app.kubernetes.io/name=kubepilot-ai-controller --tail=100 -f

## status: Check deployment status
status:
	@echo "Checking deployment status..."
	@echo ""
	@echo "Namespace:"
	@$(KUBECTL) get namespace $(NAMESPACE)
	@echo ""
	@echo "Pods:"
	@$(KUBECTL) get pods -n $(NAMESPACE)
	@echo ""
	@echo "Deployments:"
	@$(KUBECTL) get deployments -n $(NAMESPACE)
	@echo ""
	@echo "AIIncidents:"
	@$(KUBECTL) get aiincident -A 2>/dev/null || echo "No AIIncidents found"

## test-example: Create example AIIncident
test-example:
	@echo "Creating example AIIncident..."
	@$(KUBECTL) apply -f deploy/example-aiincident.yaml
	@echo "Example AIIncident created"
	@echo ""
	@echo "Check status:"
	@echo "  kubectl get aiincident -n default"
	@echo "  kubectl describe aiincident example-pod-crash -n default"

# CI/CD targets
## ci: Run all CI checks
ci: deps fmt vet lint test build

## release: Build and push release image
release: ci docker-build docker-push
	@echo "Release $(VERSION) complete"
