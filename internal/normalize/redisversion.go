package normalize

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// MinRedisVersion is the oldest Redis that Deduper works with. SET with NX
// and GET together was added in Redis 7.0.
const MinRedisVersion = "7.0.0"

// RedisDoer runs a raw Redis command. *redis.Client has this method.
type RedisDoer interface {
	Do(ctx context.Context, args ...any) *redis.Cmd
}

// CheckRedisVersion asks the server for its version once and fails when it
// is older than MinRedisVersion, so the problem shows at start and not at
// the first batch. It sends HELLO 3, the protocol go-redis uses by default.
// A server without HELLO is older than Redis 6 and fails too.
func CheckRedisVersion(ctx context.Context, rdb RedisDoer) error {
	reply, err := rdb.Do(ctx, "HELLO", "3").Result()
	if err != nil {
		return fmt.Errorf("check redis version: %w", err)
	}
	version, ok := helloVersion(reply)
	if !ok {
		return errors.New("check redis version: the HELLO reply has no version")
	}
	if err := requireVersion(version, MinRedisVersion); err != nil {
		return fmt.Errorf("check redis version: %w", err)
	}
	return nil
}

// helloVersion returns the "version" field of a HELLO reply, which is a map
// with RESP3 and a flat list of keys and values with RESP2.
func helloVersion(reply any) (string, bool) {
	switch r := reply.(type) {
	case map[any]any:
		v, ok := r["version"].(string)
		return v, ok
	case map[string]any:
		v, ok := r["version"].(string)
		return v, ok
	case []any:
		for i := 0; i+1 < len(r); i += 2 {
			if k, _ := r[i].(string); k == "version" {
				v, ok := r[i+1].(string)
				return v, ok
			}
		}
	}
	return "", false
}

// requireVersion fails when version is older than minimum. Both have the
// form major.minor.patch. Missing parts count as 0.
func requireVersion(version, minimum string) error {
	got, err := parseVersion(version)
	if err != nil {
		return fmt.Errorf("server version %q: %w", version, err)
	}
	want, err := parseVersion(minimum)
	if err != nil {
		return fmt.Errorf("minimum version %q: %w", minimum, err)
	}
	for i := range got {
		if got[i] != want[i] {
			if got[i] < want[i] {
				return fmt.Errorf("redis %s is too old, normalize needs %s or newer (SET with NX and GET)", version, minimum)
			}
			return nil
		}
	}
	return nil
}

func parseVersion(v string) ([3]int, error) {
	var out [3]int
	parts := strings.Split(strings.TrimSpace(v), ".")
	if len(parts) == 0 || len(parts) > 3 || parts[0] == "" {
		return out, errors.New("not a version of the form major.minor.patch")
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return out, errors.New("not a version of the form major.minor.patch")
		}
		out[i] = n
	}
	return out, nil
}
