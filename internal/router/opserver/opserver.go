// Package opserver serves the operator API of decision 0010 over HTTP: the
// paths, bodies and error codes of internal/opapi, on the Go 1.22
// ServeMux. The handlers only decode, validate and encode; every action is
// a call to the router core through the Operators interface, and the core
// checks it against the current state.
//
// The API has no authentication in stage 3 (decision 0010), so the router
// binds it to localhost by default.
package opserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
)

// Operators is what the handlers need from the router core. Each action
// returns an opapi result, or an *router.ActionError with the code to
// answer, or another error, which is a database error.
// *router.Service has it.
type Operators interface {
	PutOperator(ctx context.Context, id string, req opapi.OperatorRequest) (opapi.OperatorResponse, error)
	Online(ctx context.Context, id string) (opapi.Result, error)
	Offline(ctx context.Context, id string) (opapi.Result, error)
	Accept(ctx context.Context, operatorID, offerID string) (opapi.Result, error)
	Refuse(ctx context.Context, operatorID, offerID string) (opapi.Result, error)
	Reply(ctx context.Context, operatorID, offerID, replyID string) (opapi.Result, error)
	Resolve(ctx context.Context, operatorID, offerID string) (opapi.Result, error)
	Settings() opapi.Settings
}

// DefaultTimeout bounds the work of one request.
const DefaultTimeout = 10 * time.Second

// Options are the optional dependencies of the handler.
type Options struct {
	// Logger may be nil.
	Logger *slog.Logger
	// Registerer gets the request metrics. It may be nil.
	Registerer prometheus.Registerer
	// Timeout bounds the work of one request. DefaultTimeout when 0.
	Timeout time.Duration
}

// handler serves the API.
type handler struct {
	ops     Operators
	logger  *slog.Logger
	timeout time.Duration
	m       *metrics
}

// NewHandler returns the handler of every route in opapi.Patterns. Other
// paths get the ServeMux answers (404, or 405 for a wrong method).
func NewHandler(ops Operators, o Options) (http.Handler, error) {
	if ops == nil {
		return nil, errors.New("opserver: operators is nil")
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	reg := o.Registerer
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	m, err := newMetrics(reg)
	if err != nil {
		return nil, err
	}
	h := &handler{ops: ops, logger: o.Logger, timeout: o.Timeout, m: m}
	mux := http.NewServeMux()
	routes := map[string]func(*http.Request) (int, any){
		opapi.PatternPutOperator: h.putOperator,
		opapi.PatternOnline:      h.presence("online", ops.Online),
		opapi.PatternOffline:     h.presence("offline", ops.Offline),
		opapi.PatternAccept:      h.offer("accept", ops.Accept),
		opapi.PatternRefuse:      h.offer("refuse", ops.Refuse),
		opapi.PatternReply:       h.reply,
		opapi.PatternResolve:     h.offer("resolve", ops.Resolve),
		opapi.PatternSettings:    h.settings,
	}
	for _, p := range opapi.Patterns {
		fn, ok := routes[p]
		if !ok {
			return nil, fmt.Errorf("opserver: no handler for %q", p)
		}
		mux.Handle(p, h.route(p, fn))
	}
	return mux, nil
}

// route wraps one handler function: it limits the body, bounds the work,
// writes the answer and counts the request.
func (h *handler) route(pattern string, fn func(*http.Request) (int, any)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		r.Body = http.MaxBytesReader(w, r.Body, opapi.MaxBodyBytes)
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()
		status, body := fn(r.WithContext(ctx))
		writeJSON(w, status, body)
		h.m.requests.WithLabelValues(pattern, strconv.Itoa(status)).Inc()
		h.m.duration.WithLabelValues(pattern).Observe(time.Since(start).Seconds())
	})
}

func (h *handler) putOperator(r *http.Request) (int, any) {
	id := r.PathValue(opapi.ParamOperatorID)
	if !opapi.ValidID(id) {
		return invalid("operator ID %q is not valid", id)
	}
	var req opapi.OperatorRequest
	if err := decodeBody(r.Body, &req); err != nil {
		return invalid("%v", err)
	}
	if err := req.Validate(); err != nil {
		return invalid("%v", err)
	}
	resp, err := h.ops.PutOperator(r.Context(), id, req)
	if err != nil {
		return h.fail(r, "put_operator", err)
	}
	h.m.actions.WithLabelValues("put_operator", string(resp.Result)).Inc()
	return http.StatusOK, resp
}

func (h *handler) presence(action string, fn func(context.Context, string) (opapi.Result, error)) func(*http.Request) (int, any) {
	return func(r *http.Request) (int, any) {
		id := r.PathValue(opapi.ParamOperatorID)
		if !opapi.ValidID(id) {
			return invalid("operator ID %q is not valid", id)
		}
		res, err := fn(r.Context(), id)
		return h.result(r, action, res, err)
	}
}

func (h *handler) offer(action string, fn func(context.Context, string, string) (opapi.Result, error)) func(*http.Request) (int, any) {
	return func(r *http.Request) (int, any) {
		op, offer, ok := offerIDs(r)
		if !ok {
			return invalid("operator ID %q or offer ID %q is not valid", op, offer)
		}
		res, err := fn(r.Context(), op, offer)
		return h.result(r, action, res, err)
	}
}

func (h *handler) reply(r *http.Request) (int, any) {
	op, offer, ok := offerIDs(r)
	if !ok {
		return invalid("operator ID %q or offer ID %q is not valid", op, offer)
	}
	var req opapi.ReplyRequest
	if err := decodeBody(r.Body, &req); err != nil {
		return invalid("%v", err)
	}
	if err := req.Validate(); err != nil {
		return invalid("%v", err)
	}
	res, err := h.ops.Reply(r.Context(), op, offer, req.ReplyID)
	return h.result(r, "reply", res, err)
}

func (h *handler) settings(*http.Request) (int, any) {
	return http.StatusOK, h.ops.Settings()
}

func offerIDs(r *http.Request) (op, offer string, ok bool) {
	op, offer = r.PathValue(opapi.ParamOperatorID), r.PathValue(opapi.ParamOfferID)
	return op, offer, opapi.ValidID(op) && opapi.ValidID(offer)
}

// result turns the answer of an action into a response.
func (h *handler) result(r *http.Request, action string, res opapi.Result, err error) (int, any) {
	if err != nil {
		return h.fail(r, action, err)
	}
	if res != opapi.ResultDone && res != opapi.ResultAlreadyDone {
		return h.fail(r, action, fmt.Errorf("unknown result %q", res))
	}
	h.m.actions.WithLabelValues(action, string(res)).Inc()
	return http.StatusOK, opapi.ActionResponse{Result: res}
}

// fail maps an error of the core to the HTTP answer. This is the only
// place that does it:
//
//   - *router.ActionError: its code, with the status of the code.
//   - an error that a later call may not get (the database is not
//     reachable, a timeout, a serialization failure or a deadlock): 503
//     CodeUnavailable, so the caller retries later.
//   - anything else: 500 CodeInternal.
//
// The message of a database error is logged and never sent to the caller.
func (h *handler) fail(r *http.Request, action string, err error) (int, any) {
	resp := errorAnswer(err)
	h.m.actions.WithLabelValues(action, string(resp.Code)).Inc()
	if resp.Code == opapi.CodeUnavailable || resp.Code == opapi.CodeInternal {
		h.logger.Warn("operator action failed", "action", action, "path", r.URL.Path, "code", resp.Code, "error", err)
	}
	return resp.Code.Status(), resp
}

func errorAnswer(err error) opapi.ErrorResponse {
	if ae, ok := errors.AsType[*router.ActionError](err); ok {
		return opapi.ErrorResponse{Code: ae.Code, Message: ae.Message}
	}
	if unavailable(err) {
		return opapi.ErrorResponse{Code: opapi.CodeUnavailable, Message: "the router cannot answer now, retry later"}
	}
	return opapi.ErrorResponse{Code: opapi.CodeInternal, Message: "internal error"}
}

// unavailable reports an error that a later call may not get.
func unavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || pgconn.Timeout(err) {
		return true
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch {
		case len(pgErr.Code) < 2:
			return false
		case pgErr.Code[:2] == "08", // connection exception
			pgErr.Code[:2] == "40", // serialization failure, deadlock
			pgErr.Code[:2] == "53", // insufficient resources
			pgErr.Code[:2] == "57": // operator intervention, for example a shutdown
			return true
		}
		return false
	}
	if _, ok := errors.AsType[*pgconn.ConnectError](err); ok {
		return true
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return true
	}
	return pgconn.SafeToRetry(err)
}

// invalid is the answer to a request that is not valid.
func invalid(format string, args ...any) (int, any) {
	return http.StatusBadRequest, opapi.ErrorResponse{Code: opapi.CodeInvalidRequest, Message: fmt.Sprintf(format, args...)}
}

// decodeBody reads one JSON object. Unknown fields are ignored, like in
// the event package, so a newer caller can add fields. A body larger than
// opapi.MaxBodyBytes, an empty body, or data after the object is an error.
func decodeBody(body io.Reader, v any) error {
	dec := json.NewDecoder(body)
	if err := dec.Decode(v); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return fmt.Errorf("body is larger than %d bytes", opapi.MaxBodyBytes)
		}
		if errors.Is(err, io.EOF) {
			return errors.New("body is empty, want a JSON object")
		}
		return errors.New("body is not a valid JSON object of the expected shape")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return fmt.Errorf("body is larger than %d bytes", opapi.MaxBodyBytes)
		}
		return errors.New("body has data after the JSON object")
	}
	return nil
}

// writeJSON writes one JSON body. Bodies are small structs that always
// encode.
func writeJSON(w http.ResponseWriter, status int, body any) {
	b, err := json.Marshal(body)
	if err != nil {
		status = http.StatusInternalServerError
		b, _ = json.Marshal(opapi.ErrorResponse{Code: opapi.CodeInternal, Message: "internal error"})
	}
	h := w.Header()
	h.Set("Content-Type", opapi.ContentType)
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
