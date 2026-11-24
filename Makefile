GO      ?= go
# compose reads .env from deploy/ by default, so pass the root .env if it exists.
COMPOSE ?= docker compose -f deploy/compose.yaml $(if $(wildcard .env),--env-file .env)

export GOTOOLCHAIN := go1.27.4

.PHONY: build test test-integration lint vuln generate up down logs

build:
	CGO_ENABLED=0 $(GO) build -o bin/ ./cmd/...

test:
	$(GO) test -race ./...

# Needs Docker: starts real Kafka, Redis and PostgreSQL with Testcontainers. -p 1 runs
# the packages one after another, because each one starts its own containers.
INTEGRATION_PKGS := ./internal/bus/... ./internal/normalize/... ./internal/conv/... ./internal/pg/... ./internal/router/... ./internal/sim/...

test-integration:
	$(GO) test -race -count=1 -p 1 -tags integration $(INTEGRATION_PKGS)

lint:
	golangci-lint run ./...

vuln:
	govulncheck ./...

# sqlc is pinned here and run with `go run pkg@version`, so its many
# dependencies stay out of go.mod. The version is written into every
# generated file, so a different version shows up as a diff. sqlc parses
# SQL with cgo, so CGO_ENABLED=1 is set even where the default is 0.
SQLC_VERSION := v1.31.1

generate:
	cd internal/pg && CGO_ENABLED=1 $(GO) run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION) generate

# Starts Kafka, Redis, PostgreSQL, ingest, normalize, conv, router and sim.
# Add --profile record to also start the recorder.
up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100
