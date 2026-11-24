//go:build integration

// Package pgtest starts a real PostgreSQL for integration tests through
// Testcontainers. It is built only with the integration build tag.
//
// Most other integration helpers of this project (startKafka, startRedis)
// are copied into each package that needs them, because they are short.
// The router service and the simulator share internal/bus/kafkatest. This
// one is a package, because it must stay in step with the
// migrations in internal/pg, and the router and later services all need a
// migrated database.
//
// Each call starts its own container on a random host port, so tests never
// collide with a database on the host.
package pgtest

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ZAUakaAlexey/threadroute/internal/pg"
)

// Image is the official PostgreSQL image, with the exact minor version.
const Image = "postgres:19.0-alpine"

// Synthetic credentials of the test database.
const (
	database = "threadroute"
	user     = "threadroute"
	password = "threadroute-test"
)

// Start starts an empty PostgreSQL and returns its DSN. The container is
// removed when the test ends.
func Start(t testing.TB) string {
	t.Helper()
	ctx := t.Context()
	ctr, err := postgres.Run(ctx, Image,
		postgres.WithDatabase(database),
		postgres.WithUsername(user),
		postgres.WithPassword(password),
		// The image starts PostgreSQL twice (once to run the init scripts),
		// so the wait checks for the second "ready" log line and the port.
		postgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start %s: %v", Image, err)
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	return dsn
}

// Open opens a pool to dsn with pg.Open. The pool is closed when the test
// ends.
func Open(t testing.TB, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pg.Open(t.Context(), pg.Config{DSN: dsn, MaxConns: 8, ConnectWait: 30 * time.Second}, nil)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// MigratedPool starts PostgreSQL, applies every migration with pg.Migrate
// and returns a pool. This is the usual start of an integration test that
// needs the database.
func MigratedPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool := Open(t, Start(t))
	if err := pg.Migrate(t.Context(), pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}
