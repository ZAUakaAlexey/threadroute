GO      ?= go
# compose reads .env from deploy/ by default, so pass the root .env if it exists.
COMPOSE ?= docker compose -f deploy/compose.yaml $(if $(wildcard .env),--env-file .env)

export GOTOOLCHAIN := go1.27.4

.PHONY: build test test-integration lint vuln up down logs

build:
	CGO_ENABLED=0 $(GO) build -o bin/ ./cmd/...

test:
	$(GO) test -race ./...

# Needs Docker: starts a real Kafka broker with Testcontainers.
test-integration:
	$(GO) test -race -count=1 -tags integration ./internal/bus/...

lint:
	golangci-lint run ./...

vuln:
	govulncheck ./...

# Starts Kafka and ingest. Add --profile record to also start the recorder.
up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100
