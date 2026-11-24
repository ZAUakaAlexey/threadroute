package gateway

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

// wsConn is a Conn on coder/websocket.
type wsConn struct{ c *websocket.Conn }

// Read implements Conn. It reads at most one byte more than the limit, so
// a larger message is closed with 1009 and the reason "message_too_big",
// not with the reason text of the library.
func (w wsConn) Read(ctx context.Context) ([]byte, error) {
	typ, r, err := w.c.Reader(ctx)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(r, gwapi.MaxClientMessageBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > gwapi.MaxClientMessageBytes {
		return nil, ErrTooBig
	}
	if typ != websocket.MessageText {
		// A binary frame is not valid JSON text for the protocol.
		return []byte{}, nil
	}
	return b, nil
}

// Write implements Conn.
func (w wsConn) Write(ctx context.Context, b []byte) error {
	return w.c.Write(ctx, websocket.MessageText, b)
}

// Ping implements Conn.
func (w wsConn) Ping(ctx context.Context) error { return w.c.Ping(ctx) }

// Close implements Conn.
func (w wsConn) Close(code gwapi.CloseCode) error {
	return w.c.Close(websocket.StatusCode(code), code.Name())
}

// Handler returns the HTTP handler of the WebSocket and the public
// snapshot. ctx ends every connection.
func (g *Gateway) Handler(ctx context.Context, pub *PublicSnapshots) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+g.cfg.PathPrefix+gwapi.WebSocketPath, func(w http.ResponseWriter, r *http.Request) {
		g.serveWS(ctx, w, r)
	})
	mux.Handle("GET "+g.cfg.PathPrefix+gwapi.PublicSnapshotPath, pub)
	return mux
}

func (g *Gateway) serveWS(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	if ctx.Err() != nil {
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	addr := clientAddr(r, g.cfg.TrustProxy)
	// The local role goes to every connection, so it needs more than the
	// origin check of the library, which lets in a request without an
	// Origin and one whose Origin host is the Host of the request. Only a
	// page of a loopback origin gets it (decision 0014, point L1).
	if g.cfg.LocalRole != "" && checkLoopbackOrigin(r.Header.Get("Origin")) != nil {
		g.logger.Debug("websocket upgrade refused: the local role needs a loopback origin")
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	// The HTTP server deadlines stay on the connection after the upgrade;
	// a WebSocket lives longer than any of them.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})
	// Accept checks the origin (decision 0011) before anything else reads
	// the request; a refused origin never gets to the cookie below.
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: g.originPatterns})
	if err != nil {
		// Accept has written the HTTP error. The error can name the
		// origin, which is not personal data.
		g.logger.Debug("websocket upgrade refused", "error", err)
		return
	}
	// The library limit is above the protocol limit; wsConn.Read checks
	// the protocol limit itself.
	c.SetReadLimit(gwapi.MaxClientMessageBytes + 1)
	g.conns.Add(1)
	defer g.conns.Done()
	g.serve(ctx, wsConn{c: c}, addr, g.siteCookie(r))
}

// siteCookie returns the value of the sign-in cookie of the site in the
// opening request (decision 0014, point 7A), or "" when the cookie is off,
// the local role is set, or the request has no such cookie. The value is a
// token: it is never logged.
func (g *Gateway) siteCookie(r *http.Request) string {
	if g.cfg.SiteCookie == "" || g.cfg.LocalRole != "" {
		return ""
	}
	ck, err := r.Cookie(g.cfg.SiteCookie)
	if err != nil {
		return ""
	}
	return ck.Value
}

// originPatterns returns the allowed origins as patterns of
// coder/websocket. With the local role every allowed origin is an exact
// origin (checkLoopbackOrigin), so its brackets are escaped: in
// "http://[::1]:5173" they are an IPv6 address, not a character class.
func originPatterns(cfg Config) []string {
	if cfg.LocalRole == "" {
		return cfg.AllowedOrigins
	}
	esc := strings.NewReplacer("[", `\[`, "]", `\]`)
	out := make([]string, len(cfg.AllowedOrigins))
	for i, p := range cfg.AllowedOrigins {
		out[i] = esc.Replace(p)
	}
	return out
}

// clientAddr returns the client address for the limits: the right-most
// X-Forwarded-For entry (the one the trusted web server added) when
// trustProxy is set, else the remote address. The port is dropped.
func clientAddr(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Values("X-Forwarded-For"); len(xff) > 0 {
			last := xff[len(xff)-1]
			if i := strings.LastIndexByte(last, ','); i >= 0 {
				last = last[i+1:]
			}
			if last = strings.TrimSpace(last); last != "" {
				return last
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// PublicSnapshots builds the public snapshot once a second (decision
// 0013, point 1B) and serves the last one.
type PublicSnapshots struct {
	g    *Gateway
	last atomic.Pointer[[]byte]
}

// NewPublicSnapshots returns the server of the public snapshot.
func (g *Gateway) NewPublicSnapshots() *PublicSnapshots { return &PublicSnapshots{g: g} }

// Build builds the snapshot at now.
func (p *PublicSnapshots) Build(now time.Time) ([]byte, error) {
	g := p.g
	speed := g.timing().SpeedFactor
	snap := gwapi.PublicSnapshot{
		Version: gwapi.ProtocolVersion, GeneratedAtUS: now.UnixMicro(), Stale: g.Status().Stale,
		SpeedFactor: speed, Queues: []gwapi.PublicQueue{},
	}
	g.view.Read(func(rd Reader) {
		snap.Totals = rd.Totals(now)
		for _, l := range rd.QueueLangs() {
			q, ok := rd.Queue(l)
			if !ok {
				continue
			}
			pq := gwapi.PublicQueue{Lang: q.ID, Waiting: q.Waiting, Offered: q.Offered, MissedAssignment: q.MissedAssignment}
			if q.OldestQueuedAtUS > 0 {
				pq.OldestWaitUS = max(int64(float64(now.UnixMicro()-q.OldestQueuedAtUS)*speed), 0)
			}
			snap.Queues = append(snap.Queues, pq)
		}
	})
	b, err := gwapi.EncodePublic(snap)
	if err != nil {
		return nil, err
	}
	p.last.Store(&b)
	return b, nil
}

// Run rebuilds the snapshot every gwapi.PublicSnapshotEvery until ctx
// ends.
func (p *PublicSnapshots) Run(ctx context.Context) {
	t := time.NewTicker(gwapi.PublicSnapshotEvery)
	defer t.Stop()
	for {
		if _, err := p.Build(p.g.clock.Now()); err != nil {
			p.g.logger.Error("public snapshot failed validation", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ServeHTTP serves the last snapshot, with cache headers for one second.
func (p *PublicSnapshots) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	b := p.last.Load()
	if b == nil {
		built, err := p.Build(p.g.clock.Now())
		if err != nil {
			http.Error(w, "snapshot not ready", http.StatusServiceUnavailable)
			return
		}
		b = &built
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=1")
	_, _ = w.Write(*b)
}
