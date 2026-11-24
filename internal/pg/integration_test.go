//go:build integration

package pg_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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
		if op.Capacity != u.arg.Capacity || !slices.Equal(op.Languages, u.langs) {
			t.Errorf("%s: operator = %+v, want capacity %d languages %v", u.name, op, u.arg.Capacity, u.langs)
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
