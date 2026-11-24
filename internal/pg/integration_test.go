//go:build integration

package pg_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/ZAUakaAlexey/threadroute/internal/pg"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgtest"
)

func TestIntegrationPostgres(t *testing.T) {
	pool := pgtest.Open(t, pgtest.Start(t))

	t.Run("migrations on an empty database", func(t *testing.T) {
		if err := pg.Migrate(t.Context(), pool, nil); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
		want := schemaVersion(t, pool)
		if want < 1 {
			t.Fatalf("schema version = %d after migrating", want)
		}

		// A second run finds nothing to do and changes nothing.
		before := tableList(t, pool)
		if err := pg.Migrate(t.Context(), pool, nil); err != nil {
			t.Fatalf("second Migrate() error = %v", err)
		}
		if got := schemaVersion(t, pool); got != want {
			t.Errorf("schema version = %d after the second run, want %d", got, want)
		}
		if after := tableList(t, pool); !slices.Equal(before, after) {
			t.Errorf("tables changed:\nbefore %v\n after %v", before, after)
		}
		for _, table := range []string{"operators", "operator_languages"} {
			if !slices.Contains(before, table) {
				t.Errorf("table %s is missing, tables are %v", table, before)
			}
		}
	})

	t.Run("operator queries", func(t *testing.T) {
		testOperatorQueries(t, pool)
	})
}

func testOperatorQueries(t *testing.T, pool *pgxpool.Pool) {
	ctx := t.Context()
	q := pgdb.New(pool)
	const id = "sim-op-0001"

	if _, err := q.GetOperator(ctx, id); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("GetOperator() of an unknown operator: error = %v, want pgx.ErrNoRows", err)
	}

	upserts := []struct {
		name  string
		arg   pgdb.UpsertOperatorParams
		want  pgdb.UpsertOperatorRow
		langs []string
	}{
		{
			name:  "create",
			arg:   pgdb.UpsertOperatorParams{ID: id, Capacity: 4, Languages: []string{"und", "en"}},
			want:  pgdb.UpsertOperatorRow{Created: true, LanguagesAdded: 2},
			langs: []string{"en", "und"},
		},
		{
			name:  "same again",
			arg:   pgdb.UpsertOperatorParams{ID: id, Capacity: 4, Languages: []string{"en", "und"}},
			want:  pgdb.UpsertOperatorRow{},
			langs: []string{"en", "und"},
		},
		{
			name:  "change capacity and languages",
			arg:   pgdb.UpsertOperatorParams{ID: id, Capacity: 6, Languages: []string{"ja", "und"}},
			want:  pgdb.UpsertOperatorRow{CapacityChanged: true, LanguagesRemoved: 1, LanguagesAdded: 1},
			langs: []string{"ja", "und"},
		},
		{
			name:  "change priority",
			arg:   pgdb.UpsertOperatorParams{ID: id, Capacity: 6, Priority: 1, Languages: []string{"ja", "und"}},
			want:  pgdb.UpsertOperatorRow{PriorityChanged: true},
			langs: []string{"ja", "und"},
		},
		{
			name:  "priority back to 0",
			arg:   pgdb.UpsertOperatorParams{ID: id, Capacity: 6, Languages: []string{"ja", "und"}},
			want:  pgdb.UpsertOperatorRow{PriorityChanged: true},
			langs: []string{"ja", "und"},
		},
	}
	for _, u := range upserts {
		got, err := q.UpsertOperator(ctx, u.arg)
		if err != nil {
			t.Fatalf("%s: UpsertOperator() error = %v", u.name, err)
		}
		if got != u.want {
			t.Errorf("%s: UpsertOperator() = %+v, want %+v", u.name, got, u.want)
		}
		op, err := q.GetOperator(ctx, id)
		if err != nil {
			t.Fatalf("%s: GetOperator() error = %v", u.name, err)
		}
		if op.Capacity != u.arg.Capacity || op.Priority != u.arg.Priority || !slices.Equal(op.Languages, u.langs) {
			t.Errorf("%s: operator = %+v, want capacity %d priority %d languages %v", u.name, op, u.arg.Capacity, u.arg.Priority, u.langs)
		}
		if op.Presence != "offline" || op.DedicatedCompanyID != nil {
			t.Errorf("%s: presence %q, dedicated company %v", u.name, op.Presence, op.DedicatedCompanyID)
		}
	}

	presence := []struct {
		id       string
		presence string
		want     int64
	}{
		{id: id, presence: "online", want: 1},
		{id: id, presence: "online", want: 0},
		{id: id, presence: "offline", want: 1},
		{id: "sim-op-unknown", presence: "online", want: 0},
	}
	for _, p := range presence {
		n, err := q.SetOperatorPresence(ctx, pgdb.SetOperatorPresenceParams{ID: p.id, Presence: p.presence})
		if err != nil {
			t.Fatalf("SetOperatorPresence(%s, %s) error = %v", p.id, p.presence, err)
		}
		if n != p.want {
			t.Errorf("SetOperatorPresence(%s, %s) = %d rows, want %d", p.id, p.presence, n, p.want)
		}
	}
	op, err := q.GetOperator(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if op.Presence != "offline" || op.PresenceChangedAt.Before(op.CreatedAt) {
		t.Errorf("operator after presence changes = %+v", op)
	}

	// The checks in the schema reject values the API also rejects.
	bad := []pgdb.UpsertOperatorParams{
		{ID: "op/1", Capacity: 4, Languages: []string{"en"}},
		{ID: "sim-op-0002", Capacity: 0, Languages: []string{"en"}},
		{ID: "sim-op-0003", Capacity: 4, Languages: []string{"EN"}},
		{ID: "sim-op-0004", Capacity: 4, Priority: 10, Languages: []string{"en"}},
	}
	for _, arg := range bad {
		if _, err := q.UpsertOperator(ctx, arg); err == nil {
			t.Errorf("UpsertOperator(%+v) error = nil, want a check violation", arg)
		}
	}
	if _, err := q.SetOperatorPresence(ctx, pgdb.SetOperatorPresenceParams{ID: id, Presence: "away"}); err == nil {
		t.Error("SetOperatorPresence(away) error = nil, want a check violation")
	}
}

func schemaVersion(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var v int64
	if err := pool.QueryRow(t.Context(), "SELECT max(version_id) FROM goose_db_version").Scan(&v); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	return v
}

// tableList returns the tables and columns of the public schema, so a
// second migration run that changed anything shows up.
func tableList(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(t.Context(),
		`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
		 UNION ALL
		 SELECT table_name || '.' || column_name || ':' || data_type FROM information_schema.columns WHERE table_schema = 'public'
		 ORDER BY 1`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	return names
}

// TestIntegrationMigratedPool checks the helper that later packages use.
func TestIntegrationMigratedPool(t *testing.T) {
	pool := pgtest.MigratedPool(t)
	if v := schemaVersion(t, pool); v < 1 {
		t.Errorf("schema version = %d", v)
	}
	if _, err := pgdb.New(pool).GetOperator(t.Context(), "sim-op-0001"); !errors.Is(err, pgx.ErrNoRows) {
		t.Errorf("GetOperator() error = %v, want pgx.ErrNoRows", err)
	}
}

// TestIntegrationOperatorEventsMigration checks migration 00003 on a
// database that already holds routing data: a resolved conversation gets
// the offer ID of its last accepted offer, the outbox takes the operator
// family, and existing operators start without operator events.
func TestIntegrationOperatorEventsMigration(t *testing.T) {
	pool := pgtest.Open(t, pgtest.Start(t))
	ctx := t.Context()
	files, err := pg.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = db.Close() })
	provider, err := goose.NewProvider(goose.DialectPostgres, db, files, goose.WithDisableGlobalRegistry(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 2); err != nil {
		t.Fatalf("migrate to 2: %v", err)
	}
	// Synthetic data as the router wrote it before stage 4: one
	// conversation with two accepted offers (the second one resolved), and
	// one that never had an operator.
	for _, q := range []string{
		`INSERT INTO operators (id, capacity) VALUES ('sim-op-0001', 4), ('sim-op-0002', 4)`,
		`INSERT INTO conversations (company_id, customer_id, conv_session, conv_seq, lang, state, queued_at,
		     last_operator_id, updated_at)
		 VALUES ('test-co-0001', 'test-cu-000001', 1, 5, 'en', 'resolved', now(), 'sim-op-0002', now()),
		        ('test-co-0001', 'test-cu-000002', 1, 1, 'en', 'waiting', now(), NULL, now())`,
		`INSERT INTO offers (company_id, customer_id, session, operator_id, state, capacity, queued_at, offered_at,
		     accept_by, accepted_at)
		 VALUES ('test-co-0001', 'test-cu-000001', 1, 'sim-op-0001', 'assignment_ended', 4, now(), now(), now(), now()),
		        ('test-co-0001', 'test-cu-000001', 1, 'sim-op-0002', 'refused', 4, now(), now(), now(), NULL),
		        ('test-co-0001', 'test-cu-000001', 1, 'sim-op-0002', 'resolved', 4, now(), now(), now(), now())`,
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if err := pg.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	var last *string
	err = pool.QueryRow(ctx, `SELECT last_offer_id FROM conversations WHERE customer_id = 'test-cu-000001'`).Scan(&last)
	if err != nil || last == nil || *last != "of-3" {
		t.Errorf("last offer of the resolved conversation = %v, %v; want of-3", last, err)
	}
	err = pool.QueryRow(ctx, `SELECT last_offer_id FROM conversations WHERE customer_id = 'test-cu-000002'`).Scan(&last)
	if err != nil || last != nil {
		t.Errorf("last offer of a conversation without operator = %v, %v; want NULL", last, err)
	}
	var seq int64
	var at *time.Time
	err = pool.QueryRow(ctx, `SELECT op_seq, op_event_at FROM operators WHERE id = 'sim-op-0001'`).Scan(&seq, &at)
	if err != nil || seq != 0 || at != nil {
		t.Errorf("existing operator op_seq %d, op_event_at %v, %v", seq, at, err)
	}

	insert := func(family string) error {
		_, err := pool.Exec(ctx, `INSERT INTO outbox (family, record_key, seq, event_type, payload, created_at)
			VALUES ($1, 'sim-op-0001', 1, 'operator_updated', '{}', now())`, family)
		return err
	}
	if err := insert("operator"); err != nil {
		t.Errorf("outbox row of the operator family: %v", err)
	}
	if err := insert("other"); err == nil {
		t.Error("outbox row of an unknown family: no error, want a check violation")
	}
	if _, err := pool.Exec(ctx, `UPDATE operators SET op_seq = -1 WHERE id = 'sim-op-0001'`); err == nil {
		t.Error("negative op_seq: no error, want a check violation")
	}
}

// TestIntegrationOperatorPriorityMigration checks migration 00004 up and
// down on a database that already holds an operator: the operator gets
// priority 0, the range is checked, and the down step removes the column
// and keeps the operator.
func TestIntegrationOperatorPriorityMigration(t *testing.T) {
	pool := pgtest.Open(t, pgtest.Start(t))
	ctx := t.Context()
	files, err := pg.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = db.Close() })
	provider, err := goose.NewProvider(goose.DialectPostgres, db, files, goose.WithDisableGlobalRegistry(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 3); err != nil {
		t.Fatalf("migrate to 3: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO operators (id, capacity) VALUES ('sim-op-0001', 4)`); err != nil {
		t.Fatal(err)
	}
	hasColumn := func() bool {
		t.Helper()
		var n int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns
			WHERE table_name = 'operators' AND column_name = 'priority'`).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n == 1
	}

	if _, err := provider.UpTo(ctx, 4); err != nil {
		t.Fatalf("migrate to 4: %v", err)
	}
	var p int
	if err := pool.QueryRow(ctx, `SELECT priority FROM operators WHERE id = 'sim-op-0001'`).Scan(&p); err != nil || p != 0 {
		t.Fatalf("priority of an existing operator = %d, %v; want 0", p, err)
	}
	for _, v := range []int{0, 9} {
		if _, err := pool.Exec(ctx, `UPDATE operators SET priority = $1 WHERE id = 'sim-op-0001'`, v); err != nil {
			t.Errorf("priority %d: %v", v, err)
		}
	}
	for _, v := range []int{-1, 10} {
		if _, err := pool.Exec(ctx, `UPDATE operators SET priority = $1 WHERE id = 'sim-op-0001'`, v); err == nil {
			t.Errorf("priority %d: no error, want a check violation", v)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE operators SET priority = NULL WHERE id = 'sim-op-0001'`); err == nil {
		t.Error("priority NULL: no error, want a not-null violation")
	}

	if _, err := provider.DownTo(ctx, 3); err != nil {
		t.Fatalf("down to 3: %v", err)
	}
	if hasColumn() {
		t.Error("the priority column is still there after the down step")
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM operators`).Scan(&n); err != nil || n != 1 {
		t.Errorf("operators after the down step = %d, %v; want 1", n, err)
	}
	if err := pg.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("Migrate() after the down step: %v", err)
	}
	if !hasColumn() {
		t.Error("the priority column is missing after migrating up again")
	}
}
