package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

// Identity is who the caller of a connection is, as the gateway
// established it (decision 0012). Everything a client may see or do is
// decided from it, never from client input.
type Identity struct {
	// Level is LevelFull only after the site API named a role from the
	// configured list.
	Level gwapi.Level
	// UserID is the site user ID, or 0 for an anonymous caller (no token,
	// or a token the site did not confirm).
	UserID int64
}

// Anonymous is the identity of a caller without a confirmed token.
var Anonymous = Identity{Level: gwapi.LevelDemo}

// CheckOutcome is the result of one token check, the label of the site API
// metric.
type CheckOutcome string

// Check outcomes. Every outcome but CheckFull gives the demo level.
const (
	// CheckNoToken: the hello had no token.
	CheckNoToken CheckOutcome = "no_token"
	// CheckDisabled: no site API URL is configured.
	CheckDisabled CheckOutcome = "disabled"
	// CheckFull: the role is in the list.
	CheckFull CheckOutcome = "full"
	// CheckSignedIn: a valid user whose role is not in the list.
	CheckSignedIn CheckOutcome = "signed_in"
	// CheckCached: the answer came from the cache.
	CheckCached CheckOutcome = "cached"
	// CheckRejected: the site answered 401 or 403.
	CheckRejected CheckOutcome = "rejected"
	// CheckUnavailable: no answer in time, a network error or a 5xx.
	CheckUnavailable CheckOutcome = "unavailable"
	// CheckMalformed: any other answer.
	CheckMalformed CheckOutcome = "malformed"
)

// TokenChecker checks a site token.
type TokenChecker interface {
	Check(ctx context.Context, token string) (Identity, CheckOutcome)
}

// maxSiteAnswer bounds the answer of the site API that is read.
const maxSiteAnswer = 64 << 10

// cacheMax bounds the cache of the site checker.
const cacheMax = 10_000

// SiteChecker asks the site API who the caller is (decision 0012, point
// 1B): one GET to a configured URL with the token as a bearer token. The
// answer is JSON with at least {"user":{"id":<number>},"role":{"name":
// "<string>"}}; nothing else of it is read. A successful answer is cached
// for a short time under the SHA-256 of the token, never the token. The
// token is never logged or stored.
type SiteChecker struct {
	url     string
	roles   []string
	hc      *http.Client
	timeout time.Duration
	ttl     time.Duration
	now     func() time.Time

	mu    sync.Mutex
	cache map[[32]byte]cacheEntry
}

type cacheEntry struct {
	id    Identity
	until time.Time
}

// NewSiteChecker returns a checker. An empty url gives a checker that
// confirms nobody.
func NewSiteChecker(url string, roles []string, timeout, ttl time.Duration, hc *http.Client, now func() time.Time) *SiteChecker {
	if hc == nil {
		hc = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	if now == nil {
		now = time.Now
	}
	// A redirect could carry the token to another host.
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &SiteChecker{
		url: url, roles: slices.Clone(roles), hc: hc, timeout: timeout, ttl: ttl, now: now,
		cache: map[[32]byte]cacheEntry{},
	}
}

// siteAnswer is the part of the answer that the gateway reads.
type siteAnswer struct {
	User *struct {
		ID *json.Number `json:"id"`
	} `json:"user"`
	Role *struct {
		Name *string `json:"name"`
	} `json:"role"`
}

// Check implements TokenChecker.
func (s *SiteChecker) Check(ctx context.Context, token string) (Identity, CheckOutcome) {
	if token == "" {
		return Anonymous, CheckNoToken
	}
	if s.url == "" {
		return Anonymous, CheckDisabled
	}
	h := sha256.Sum256([]byte(token))
	now := s.now()
	s.mu.Lock()
	if e, ok := s.cache[h]; ok && now.Before(e.until) {
		s.mu.Unlock()
		return e.id, CheckCached
	}
	s.mu.Unlock()

	id, outcome := s.ask(ctx, token)
	if outcome == CheckFull || outcome == CheckSignedIn {
		s.mu.Lock()
		if len(s.cache) >= cacheMax {
			for k, e := range s.cache {
				if !now.Before(e.until) {
					delete(s.cache, k)
				}
			}
			if len(s.cache) >= cacheMax {
				s.cache = map[[32]byte]cacheEntry{}
			}
		}
		s.cache[h] = cacheEntry{id: id, until: now.Add(s.ttl)}
		s.mu.Unlock()
	}
	return id, outcome
}

func (s *SiteChecker) ask(ctx context.Context, token string) (Identity, CheckOutcome) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return Anonymous, CheckMalformed
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := s.hc.Do(req)
	if err != nil {
		return Anonymous, CheckUnavailable
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSiteAnswer+1))
	switch {
	case err != nil:
		return Anonymous, CheckUnavailable
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return Anonymous, CheckRejected
	case resp.StatusCode >= 500:
		return Anonymous, CheckUnavailable
	case resp.StatusCode != http.StatusOK || len(body) > maxSiteAnswer:
		return Anonymous, CheckMalformed
	}
	id, err := parseSiteAnswer(body, s.roles)
	if err != nil {
		return Anonymous, CheckMalformed
	}
	if id.Level == gwapi.LevelFull {
		return id, CheckFull
	}
	return id, CheckSignedIn
}

// parseSiteAnswer reads the user ID and the role name. The error never
// holds a value of the answer.
func parseSiteAnswer(body []byte, roles []string) (Identity, error) {
	var a siteAnswer
	if err := json.Unmarshal(body, &a); err != nil {
		return Identity{}, errors.New("site answer is not the expected JSON")
	}
	if a.User == nil || a.User.ID == nil || a.Role == nil || a.Role.Name == nil {
		return Identity{}, errors.New("site answer has no user.id or role.name")
	}
	uid, err := a.User.ID.Int64()
	if err != nil || uid <= 0 {
		return Identity{}, fmt.Errorf("site answer: user.id is not a positive whole number")
	}
	id := Identity{Level: gwapi.LevelDemo, UserID: uid}
	if *a.Role.Name != "" && slices.Contains(roles, *a.Role.Name) {
		id.Level = gwapi.LevelFull
	}
	return id, nil
}
