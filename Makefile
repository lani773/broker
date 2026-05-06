BINARY     := luma-broker
IMAGE      := ghcr.io/your-org/luma-broker
VERSION    := $(shell git describe --tags --always 2>/dev/null || echo dev)
BUILD_FLAGS := -trimpath -ldflags "-w -s -X main.version=$(VERSION)"
GOARCH     ?= amd64
GOOS       ?= linux

.PHONY: all build run test bench docker docker-push up down logs clean vet tidy k8s-deploy

## ── Build ────────────────────────────────────────────────────────────────────

all: vet build

build:
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build $(BUILD_FLAGS) -o bin/$(BINARY) ./cmd/broker
	@echo "Built bin/$(BINARY) ($(VERSION))"

build-race:
	go build -race $(BUILD_FLAGS) -o bin/$(BINARY)-race ./cmd/broker

run: build
	./bin/$(BINARY)

## ── Quality ──────────────────────────────────────────────────────────────────

vet:
	go vet ./...

tidy:
	go mod tidy

lint:
	golangci-lint run ./...

## ── Tests ────────────────────────────────────────────────────────────────────

test: up-infra
	@sleep 3
	cd clients/go && go run client.go -test all

bench: up-infra
	@sleep 3
	cd clients/go && go run client.go -bench -clients 500 -msgs 1000

unit:
	go test -race -cover ./...

## ── Docker ───────────────────────────────────────────────────────────────────

docker:
	docker build \
		--build-arg VERSION=$(VERSION) \
		-t $(IMAGE):$(VERSION) \
		-t $(IMAGE):latest \
		.

docker-push:
	docker push $(IMAGE):$(VERSION)
	docker push $(IMAGE):latest

## ── Docker Compose ───────────────────────────────────────────────────────────

up:
	docker compose up -d
	@echo "Broker: mqtt://localhost:1883"
	@echo "API:    http://localhost:8080"
	@echo "Grafana: http://localhost:3001 (admin/admin)"

up-infra:
	docker compose up -d postgres redis

down:
	docker compose down

logs:
	docker compose logs -f broker

logs-all:
	docker compose logs -f

ps:
	docker compose ps

## ── Kubernetes ───────────────────────────────────────────────────────────────

k8s-deploy:
	kubectl apply -f k8s/deployment.yaml

k8s-delete:
	kubectl delete -f k8s/deployment.yaml

k8s-status:
	kubectl get pods,svc,hpa,pdb -n luma

k8s-logs:
	kubectl logs -n luma -l app=luma-broker -f --tail=100

## ── Dev Helpers ──────────────────────────────────────────────────────────────

certs:
	@mkdir -p certs
	openssl req -x509 -newkey rsa:4096 -keyout certs/server.key \
		-out certs/server.crt -days 365 -nodes \
		-subj "/CN=luma-broker"
	@echo "Self-signed cert created in certs/"

clean:
	rm -rf bin/

help:
	@grep -E '^[a-zA-Z_-]+:' Makefile | awk -F: '{print "  make " $$1}'
