GO      ?= go
# compose reads .env from deploy/ by default, so pass the root .env if it exists.
COMPOSE ?= docker compose -f deploy/compose.yaml $(if $(wildcard .env),--env-file .env)

export GOTOOLCHAIN := go1.27.4

.PHONY: build test test-integration lint vuln up down logs

build:
	CGO_ENABLED=0 $(GO) build -o bin/ ./cmd/...

test:
	$(GO) test -race ./...

# Needs Docker: starts real Kafka and Redis with Testcontainers. -p 1 runs
# the packages one after another, because each one starts its own containers.
INTEGRATION_PKGS := ./internal/bus/... ./internal/normalize/... ./internal/conv/...

test-integration:
	$(GO) test -race -count=1 -p 1 -tags integration $(INTEGRATION_PKGS)

lint:
	golangci-lint run ./...

vuln:
	govulncheck ./...

# Starts Kafka, Redis, ingest, normalize and conv. Add --profile record to
# also start the recorder.
up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100
