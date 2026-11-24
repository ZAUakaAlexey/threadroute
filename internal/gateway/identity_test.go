package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

// testCookie is a made-up name of the sign-in cookie of the site.
const testCookie = "site_session_example"

// recordingAuth is fakeAuth that records the tokens it was asked about.
type recordingAuth struct {
	mu     sync.Mutex
	tokens []string
}

func (a *recordingAuth) Check(ctx context.Context, token string) (Identity, CheckOutcome) {
	a.mu.Lock()
	a.tokens = append(a.tokens, token)
	a.mu.Unlock()
	return fakeAuth{}.Check(ctx, token)
}

func (a *recordingAuth) asked() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.tokens)
}

func TestIdentifyOrder(t *testing.T) {
	long := strings.Repeat("a", gwapi.MaxTokenBytes+1)
	tests := []struct {
		name          string
		local         string
		cookie, token string
		want          Identity
		outcome       CheckOutcome
		source        string
		asked         []string
	}{
		{name: "cookie wins over the token", cookie: settingsToken, token: fullToken,
			want: Identity{Level: gwapi.LevelDemo, UserID: 9, MayChangeSettings: true}, outcome: CheckSignedIn,
			source: sourceCookie, asked: []string{settingsToken}},
		{name: "a rejected cookie does not fall back to the token", cookie: rejectedToken, token: fullToken,
			want: Anonymous, outcome: CheckRejected, source: sourceCookie, asked: []string{rejectedToken}},
		{name: "a cookie over the token limit is not sent", cookie: long, token: fullToken,
			want: Anonymous, outcome: CheckMalformed, source: sourceCookie},
		{name: "token without a cookie", token: fullToken,
			want: Identity{Level: gwapi.LevelFull, UserID: 7}, outcome: CheckFull, source: sourceToken, asked: []string{fullToken}},
		{name: "nothing", want: Anonymous, outcome: CheckNoToken, source: sourceNone},
		{name: "local role wins over everything", local: "guest", cookie: settingsToken, token: fullToken,
			want: Identity{Level: gwapi.LevelFull, Local: true}, outcome: CheckLocal, source: sourceLocal},
		{name: "local role without a list", local: "viewer",
			want: Identity{Level: gwapi.LevelDemo, Local: true}, outcome: CheckLocal, source: sourceLocal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			auth := &recordingAuth{}
			e := newEnvAuth(t, auth, func(c *Config) {
				c.FullRoles = []string{"guest"}
				c.SettingsRoles = []string{"admin"}
				if tt.local != "" {
					c.LocalRole = tt.local
					c.AllowedOrigins = []string{"http://127.0.0.1:5173"}
				}
			})
			id, outcome, source := e.g.identify(t.Context(), tt.cookie, tt.token)
			if id != tt.want || outcome != tt.outcome || source != tt.source {
				t.Errorf("identify = %+v %s %s, want %+v %s %s", id, outcome, source, tt.want, tt.outcome, tt.source)
			}
			if got := auth.asked(); !slices.Equal(got, tt.asked) {
				t.Errorf("asked the site about %d tokens, want %d", len(got), len(tt.asked))
			}
		})
	}
}

// siteAPI is a fake site API that counts its calls: the token "cookie-value"
// is user 21 with the role "guest", "hello-value" is user 22 with the role
// "reviewer".
type siteAPI struct {
	srv   *httptest.Server
	mu    sync.Mutex
	calls int
}

func newSiteAPI(t *testing.T) *siteAPI {
	t.Helper()
	s := &siteAPI{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.calls++
		s.mu.Unlock()
		switch r.Header.Get("Authorization") {
		case "Bearer cookie-value":
			_, _ = w.Write([]byte(`{"user":{"id":21},"role":{"name":"guest"}}`))
		case "Bearer hello-value":
			_, _ = w.Write([]byte(`{"user":{"id":22},"role":{"name":"reviewer"}}`))
		default:
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *siteAPI) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// dialWith opens a WebSocket with headers and sends hello. It returns the
// welcome, or the HTTP status of a refused upgrade.
func dialWith(t *testing.T, url string, h http.Header, hello gwapi.Hello) (gwapi.Welcome, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: h})
	if err != nil {
		if resp == nil {
			t.Fatalf("dial: %v", err)
		}
		return gwapi.Welcome{}, resp.StatusCode
	}
	defer c.CloseNow()
	c.SetReadLimit(1 << 20)
	hello.Version = gwapi.ProtocolVersion
	b, _ := gwapi.EncodeClient(hello)
	if err := c.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
	_, b, err = c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var w gwapi.Welcome
	if frameType(t, b) != gwapi.TypeWelcome {
		t.Fatalf("frame %s", b)
	}
	if err := json.Unmarshal(b, &w); err != nil {
		t.Fatal(err)
	}
	if _, err := gwapi.DecodeServer(b, audOf(w)); err != nil {
		t.Fatalf("welcome %s: %v", b, err)
	}
	return w, http.StatusSwitchingProtocols
}

// cookieEnv is a gateway behind a real HTTP server with the real site
// checker and a fake router with stand settings, so the Welcome shows the
// right of the connection.
func cookieEnv(t *testing.T, mutate func(c *Config)) (string, *siteAPI) {
	t.Helper()
	site := newSiteAPI(t)
	roles := Roles{Full: []string{"reviewer"}, Settings: []string{"guest"}}
	auth := NewSiteChecker(site.srv.URL, roles, time.Second, time.Minute, nil, nil)
	e := newEnvAuth(t, auth, func(c *Config) {
		c.SiteAPIURL, c.FullRoles, c.SettingsRoles = site.srv.URL, roles.Full, roles.Settings
		c.SiteCookie = testCookie
		c.AllowedOrigins = []string{"localhost:*"}
		if mutate != nil {
			mutate(c)
		}
	})
	attachRouter(t, e, routerStand, testStand())
	srv := httptest.NewServer(e.g.Handler(e.ctx, e.g.NewPublicSnapshots()))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + gwapi.WebSocketPath, site
}

func TestSiteCookie(t *testing.T) {
	url, site := cookieEnv(t, nil)
	sup := gwapi.Hello{View: gwapi.ViewSupervisor}
	header := func(origin, cookie string) http.Header {
		h := http.Header{"Origin": {origin}}
		if cookie != "" {
			h.Set("Cookie", testCookie+"="+cookie+"; other_example=x")
		}
		return h
	}

	// A foreign origin is refused before the cookie is used.
	if _, status := dialWith(t, url, header("https://elsewhere.invalid", "cookie-value"), sup); status != http.StatusForbidden {
		t.Fatalf("foreign origin: status %d", status)
	}
	if n := site.count(); n != 0 {
		t.Fatalf("the site was asked %d times for a refused origin", n)
	}

	// The cookie gives the identity, and wins over the token.
	withToken := sup
	withToken.Token = "hello-value"
	w, status := dialWith(t, url, header("http://localhost:5173", "cookie-value"), withToken)
	if status != http.StatusSwitchingProtocols || w.Level != gwapi.LevelDemo || w.Settings == nil || !w.Settings.MayChange {
		t.Fatalf("with the cookie: %d %+v", status, w)
	}
	// The token alone still works.
	w, _ = dialWith(t, url, header("http://localhost:5173", ""), withToken)
	if w.Level != gwapi.LevelFull || w.Settings.MayChange {
		t.Fatalf("with the token: %+v", w)
	}
	// An unknown cookie gives the demo level, also with a valid token.
	w, _ = dialWith(t, url, header("http://localhost:5173", "unknown-value"), withToken)
	if w.Level != gwapi.LevelDemo || w.Settings.MayChange {
		t.Fatalf("with an unknown cookie: %+v", w)
	}
	if n := site.count(); n != 3 {
		t.Fatalf("site calls %d, want 3", n)
	}

	// Without the setting the cookie is not read.
	url, _ = cookieEnv(t, func(c *Config) { c.SiteCookie = "" })
	w, _ = dialWith(t, url, header("http://localhost:5173", "cookie-value"), withToken)
	if w.Level != gwapi.LevelFull || w.Settings.MayChange {
		t.Fatalf("cookie off: %+v", w)
	}
}

func TestLocalRole(t *testing.T) {
	url, site := cookieEnv(t, func(c *Config) {
		c.LocalRole = "guest"
		c.AllowedOrigins = []string{"http://127.0.0.1:5173", "http://[::1]:5173"}
	})
	hello := gwapi.Hello{View: gwapi.ViewSupervisor, Token: "hello-value"}
	for _, origin := range []string{"http://127.0.0.1:5173", "http://[::1]:5173"} {
		h := http.Header{"Origin": {origin}, "Cookie": {testCookie + "=cookie-value"}}
		w, status := dialWith(t, url, h, hello)
		if status != http.StatusSwitchingProtocols || w.Level != gwapi.LevelDemo || !w.Settings.MayChange {
			t.Fatalf("%s: %d %+v", origin, status, w)
		}
	}
	for _, h := range []http.Header{
		{},
		{"Origin": {"https://site.invalid"}},
		{"Origin": {"http://localhost:5173"}},
		{"Origin": {"http://127.0.0.1:5174"}},
	} {
		if _, status := dialWith(t, url, h, hello); status != http.StatusForbidden {
			t.Errorf("origin %q: status %d", h.Get("Origin"), status)
		}
	}
	if n := site.count(); n != 0 {
		t.Fatalf("the site was asked %d times", n)
	}
}

func TestLocalRoleNeedsLoopbackOrigins(t *testing.T) {
	good := []string{
		"http://127.0.0.1:5173", "https://127.0.0.1", "http://localhost:9472", "http://127.10.20.30:80",
		"http://[::1]:5173", "https://[::1]",
	}
	for _, o := range good {
		if err := checkLoopbackOrigin(o); err != nil {
			t.Errorf("%q: %v", o, err)
		}
	}
	bad := []string{
		"", "localhost:*", "127.0.0.1:*", "http://localhost:*", "http://127.0.0.*:80", "http://127.0.0.1:517?",
		"http://[::1]:*", "http://*", "127.0.0.1:5173", "ws://127.0.0.1:5173", "http://127.0.0.1:5173/",
		"http://127.0.0.1:5173/x", "http://user@127.0.0.1:5173", "http://127.0.0.1:5173?x", "http://127.0.0.1:5173#x",
		"http://127.0.0.1:0", "http://127.0.0.1:65536", "http://127.0.0.1:", "http://127.0.0.1:080",
		"http://LOCALHOST:5173", "http://localhost.:5173", "http://localhost.site.invalid", "http://127.0.0.1.site.invalid",
		"http://128.0.0.1", "http://0x7f.0.0.1", "http://127.1", "http://2130706433", "http://[::ffff:127.0.0.1]:1",
		"http://[0:0:0:0:0:0:0:1]:1", "http://[::1%25lo0]:1", "http://::1", "http://[localhost]", "https://site.invalid",
	}
	for _, o := range bad {
		if checkLoopbackOrigin(o) == nil {
			t.Errorf("%q passes", o)
		}
	}

	lookup := func(env map[string]string) LookupFunc {
		return func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	}
	for _, origins := range []string{"", "localhost:*,127.0.0.1:*", "http://127.0.0.1:5173,https://site.invalid", "http://127.0.0.*:5173"} {
		env := map[string]string{EnvLocalRole: "guest"}
		if origins != "" {
			env[EnvAllowedOrigins] = origins
		}
		_, err := Load(lookup(env))
		if err == nil || !strings.Contains(err.Error(), "local role") {
			t.Errorf("origins %q: %v", origins, err)
		}
	}
	cfg, err := Load(lookup(map[string]string{
		EnvLocalRole: "guest", EnvAllowedOrigins: "http://127.0.0.1:5173, http://[::1]:5173",
		EnvSettingsRoles: "guest, admin", EnvSiteCookie: testCookie,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LocalRole != "guest" || !slices.Equal(cfg.SettingsRoles, []string{"guest", "admin"}) || cfg.SiteCookie != testCookie {
		t.Fatalf("config %+v", cfg)
	}
	if _, err := Load(lookup(map[string]string{EnvSiteCookie: "bad name"})); err == nil || !strings.Contains(err.Error(), EnvSiteCookie) {
		t.Errorf("bad cookie name: %v", err)
	}
	d, err := Load(lookup(nil))
	if err != nil || d.LocalRole != "" || d.SiteCookie != "" || d.SettingsRoles != nil {
		t.Fatalf("defaults %+v %v", d, err)
	}
}
