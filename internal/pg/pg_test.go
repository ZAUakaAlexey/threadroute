package pg

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		errs []string
	}{
		{name: "default", cfg: DefaultConfig()},
		{name: "key value DSN", cfg: Config{DSN: "host=db user=app dbname=app", MaxConns: 1, ConnectWait: time.Second}},
		{name: "zero", cfg: Config{}, errs: []string{"dsn is not set", "max conns", "connect wait"}},
		{name: "bad DSN", cfg: Config{DSN: "postgres://u:p@host:notaport/db", MaxConns: 1, ConnectWait: time.Second},
			errs: []string{"dsn cannot be parsed"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if len(tt.errs) == 0 {
				if err != nil {
					t.Errorf("Validate() error = %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() error = nil, want %v", tt.errs)
			}
			for _, w := range tt.errs {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}
		})
	}
}

// TestOpenHidesPassword checks that the error for a DSN that cannot be
// parsed does not contain the password.
func TestOpenHidesPassword(t *testing.T) {
	const secret = "synthetic-secret-value"
	cfg := Config{DSN: "postgres://app:" + secret + "@db:notaport/app", MaxConns: 1, ConnectWait: time.Second}
	_, err := Open(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("Open() error = nil")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error %q contains the password", err)
	}
}

// TestOpenRefusedHidesPassword checks that the error for a database that
// does not answer does not contain the password. Port 1 on the loopback
// address refuses connections, so the test runs offline.
func TestOpenRefusedHidesPassword(t *testing.T) {
	const secret = "synthetic-secret-value"
	cfg := Config{DSN: "postgres://app:" + secret + "@127.0.0.1:1/app?sslmode=disable", MaxConns: 1, ConnectWait: 700 * time.Millisecond}
	_, err := Open(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("Open() error = nil")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error %q contains the password", err)
	}
	if !strings.Contains(err.Error(), "no answer within") {
		t.Errorf("error %q is not a wait error", err)
	}
}

func TestPoolConfig(t *testing.T) {
	cfg := DefaultConfig()
	pc, err := cfg.poolConfig()
	if err != nil {
		t.Fatal(err)
	}
	if pc.MaxConns != DefaultMaxConns || pc.MinConns != 0 || pc.ConnConfig.ConnectTimeout != connectTimeout {
		t.Errorf("pool config = max %d min %d timeout %s", pc.MaxConns, pc.MinConns, pc.ConnConfig.ConnectTimeout)
	}
	if got := pc.ConnConfig.RuntimeParams["application_name"]; got != applicationName {
		t.Errorf("application_name = %q", got)
	}

	cfg.DSN += "&application_name=other"
	pc, err = cfg.poolConfig()
	if err != nil {
		t.Fatal(err)
	}
	if got := pc.ConnConfig.RuntimeParams["application_name"]; got != "other" {
		t.Errorf("application_name = %q, want the DSN value", got)
	}
}

type fakePinger struct {
	fails int
	calls int
}

func (f *fakePinger) Ping(ctx context.Context) error {
	f.calls++
	if f.calls <= f.fails {
		return errors.New("connection refused")
	}
	return nil
}

func TestWaitForPing(t *testing.T) {
	t.Run("answers after retries", func(t *testing.T) {
		p := &fakePinger{fails: 2}
		if err := waitForPing(context.Background(), p, 10*time.Second, slog.New(slog.DiscardHandler)); err != nil {
			t.Fatalf("waitForPing() error = %v", err)
		}
		if p.calls != 3 {
			t.Errorf("calls = %d, want 3", p.calls)
		}
	})
	t.Run("gives up after the wait", func(t *testing.T) {
		p := &fakePinger{fails: 1 << 30}
		start := time.Now()
		err := waitForPing(context.Background(), p, 50*time.Millisecond, slog.New(slog.DiscardHandler))
		if err == nil || !strings.Contains(err.Error(), "connection refused") {
			t.Fatalf("waitForPing() error = %v", err)
		}
		if d := time.Since(start); d > 5*time.Second {
			t.Errorf("waitForPing took %s", d)
		}
	})
}

func TestMigrationsEmbedded(t *testing.T) {
	files, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 || names[0] != "00001_operators.sql" {
		t.Errorf("migrations = %v", names)
	}
	for _, n := range names {
		b, err := fs.ReadFile(files, n)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), "-- +goose Up") || !strings.Contains(string(b), "-- +goose Down") {
			t.Errorf("%s has no goose Up and Down sections", n)
		}
	}
}
