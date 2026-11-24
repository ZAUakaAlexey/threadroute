// Package opclient is the HTTP client of the operator API (decision 0010)
// for the simulator and the gateway. It has one method per action of
// internal/opapi. Each returns the opapi result, or an error:
//
//   - *opapi.Error when the router answered with an error (read the code
//     with errors.As, or with Code). A code for which ErrorCode.Rejected is
//     true means the state moved on: repeating the call gives the same
//     answer. CodeUnavailable means retry later.
//   - *opapi.SettingsError instead of *opapi.Error for PutSettings and
//     PutSimDefaults, with the details of the code (fields, the current
//     settings of a conflict, the wait). A router from before decision 0014
//     gives CodeSettingsOff there.
//   - any other error when no answer came: the router is not reachable,
//     the call timed out, or the answer was not valid.
//
// # Stand settings
//
// Settings and SettingsWithChanges check the base settings and the stand
// block apart (opapi.Settings.Validate and ValidateStand). Broken base
// settings are an error. A broken stand block, one that does not decode or
// does not pass ValidateStand, counts as "stand settings are off": the
// call returns the base settings with Stand == nil and logs a warning on
// the logger of WithLogger. It logs once when a block becomes broken, again
// when the reason changes, and once at Info level when the block is fine
// again, so a caller that polls every opapi.SettingsPollInterval does not
// fill the log.
//
// The client does not retry: the caller decides, because only the caller
// knows whether a late call still makes sense.
package opclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// DefaultTimeout bounds one call, including the wait for the answer.
const DefaultTimeout = 10 * time.Second

// Client calls the operator API. It is safe for concurrent use.
type Client struct {
	base    string
	hc      *http.Client
	timeout time.Duration
	logger  *slog.Logger

	// standMu guards standErr.
	standMu sync.Mutex
	// standErr holds, by path, the reason of the last broken stand block
	// that was logged. A path is absent when its last block was fine.
	standErr map[string]string
}

// Option changes a Client.
type Option func(*Client)

// WithHTTPClient sets the HTTP client. The default is a client of its own
// with the default transport settings.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.hc = hc }
}

// WithTimeout sets the limit of one call. DefaultTimeout by default.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = d }
}

// WithLogger sets the logger that reports a broken stand block. nil, the
// default, discards the reports.
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) { c.logger = l }
}

// New returns a client of the API at baseURL, for example
// "http://127.0.0.1:9470".
func New(baseURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("opclient: base URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("opclient: base URL %q is not http(s)://host[:port][/path]", baseURL)
	}
	c := &Client{
		base:    strings.TrimSuffix(u.String(), "/"),
		hc:      &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()},
		timeout: DefaultTimeout,
	}
	for _, o := range opts {
		o(c)
	}
	if c.logger == nil {
		c.logger = slog.New(slog.DiscardHandler)
	}
	c.standErr = make(map[string]string)
	if c.hc == nil || c.timeout <= 0 {
		return nil, errors.New("opclient: the HTTP client is nil or the timeout is not positive")
	}
	return c, nil
}

// PutOperator creates an operator or sets its languages and capacity.
func (c *Client) PutOperator(ctx context.Context, operatorID string, req opapi.OperatorRequest) (opapi.OperatorResponse, error) {
	var resp opapi.OperatorResponse
	if err := c.do(ctx, http.MethodPut, opapi.OperatorPath(operatorID), req, &resp); err != nil {
		return opapi.OperatorResponse{}, err
	}
	if err := checkResult(resp.Result); err != nil {
		return opapi.OperatorResponse{}, err
	}
	return resp, nil
}

// Online puts the operator online.
func (c *Client) Online(ctx context.Context, operatorID string) (opapi.Result, error) {
	return c.action(ctx, opapi.OnlinePath(operatorID), nil)
}

// Offline puts the operator offline.
func (c *Client) Offline(ctx context.Context, operatorID string) (opapi.Result, error) {
	return c.action(ctx, opapi.OfflinePath(operatorID), nil)
}

// Accept accepts an offer.
func (c *Client) Accept(ctx context.Context, operatorID, offerID string) (opapi.Result, error) {
	return c.action(ctx, opapi.AcceptPath(operatorID, offerID), nil)
}

// Refuse refuses an offer.
func (c *Client) Refuse(ctx context.Context, operatorID, offerID string) (opapi.Result, error) {
	return c.action(ctx, opapi.RefusePath(operatorID, offerID), nil)
}

// Reply records a reply marker with an ID chosen by the caller. A repeat
// with the same ID gives opapi.ResultAlreadyDone.
func (c *Client) Reply(ctx context.Context, operatorID, offerID, replyID string) (opapi.Result, error) {
	return c.action(ctx, opapi.ReplyPath(operatorID, offerID), opapi.ReplyRequest{ReplyID: replyID})
}

// Resolve resolves an assignment.
func (c *Client) Resolve(ctx context.Context, operatorID, offerID string) (opapi.Result, error) {
	return c.action(ctx, opapi.ResolvePath(operatorID, offerID), nil)
}

// Settings reads the router settings and validates them. The simulator
// does not start without valid base settings (decision 0010). The answer
// has no change log. A broken stand block gives Stand == nil, see the
// package comment.
func (c *Client) Settings(ctx context.Context) (opapi.Settings, error) {
	return c.settings(ctx, opapi.SettingsPath(), opapi.MaxBodyBytes)
}

// SettingsWithChanges reads the router settings with the change log of the
// stand settings (StandState.Changes). It checks the answer like Settings.
func (c *Client) SettingsWithChanges(ctx context.Context) (opapi.Settings, error) {
	return c.settings(ctx, opapi.SettingsWithChangesPath(), opapi.MaxSettingsBodyBytes)
}

// PutSettings changes the stand settings. An error answer is an
// *opapi.SettingsError: CodeInvalidRequest with Fields, CodeSettingsConflict
// with the current Settings, CodeSettingsTooSoon with RetryAfterMS,
// CodeSettingsOff (also for a router from before decision 0014), or
// CodeUnavailable or CodeInternal.
func (c *Client) PutSettings(ctx context.Context, req opapi.SettingsChangeRequest) (opapi.SettingsChangeResponse, error) {
	path := opapi.SettingsPath()
	var resp opapi.SettingsChangeResponse
	if err := c.doSettings(ctx, path, opapi.MaxSettingsBodyBytes, req, &resp); err != nil {
		return opapi.SettingsChangeResponse{}, err
	}
	if err := checkStandAnswer(resp.Result, resp.Settings); err != nil {
		return opapi.SettingsChangeResponse{}, fmt.Errorf("opclient: PUT %s: %w", path, err)
	}
	return resp, nil
}

// PutSimDefaults reports the values of the simulator environment. A
// successful answer always has Settings.Stand.Current.Sim. An error
// answer is an *opapi.SettingsError: CodeInvalidRequest (with Fields when
// the values break the bounds), CodeSettingsOff (also for a router from
// before decision 0014), or CodeUnavailable or CodeInternal.
func (c *Client) PutSimDefaults(ctx context.Context, req opapi.SimDefaultsRequest) (opapi.SimDefaultsResponse, error) {
	path := opapi.SimDefaultsPath()
	var resp opapi.SimDefaultsResponse
	if err := c.doSettings(ctx, path, opapi.MaxBodyBytes, req, &resp); err != nil {
		return opapi.SimDefaultsResponse{}, err
	}
	err := checkStandAnswer(resp.Result, resp.Settings)
	if err == nil && resp.Settings.Stand.Current.Sim == nil {
		// After a report the stored set always has a sim part.
		err = errors.New("answer settings have no sim part")
	}
	if err != nil {
		return opapi.SimDefaultsResponse{}, fmt.Errorf("opclient: PUT %s: %w", path, err)
	}
	return resp, nil
}

// settingsWire decodes the stand block apart from the base settings, so
// that a block of the wrong shape does not fail the whole answer. Its Stand
// hides the Stand of the embedded Settings.
type settingsWire struct {
	opapi.Settings
	Stand json.RawMessage `json:"stand"`
}

func (c *Client) settings(ctx context.Context, path string, limit int) (opapi.Settings, error) {
	status, b, err := c.send(ctx, http.MethodGet, path, nil, limit)
	if err != nil {
		return opapi.Settings{}, err
	}
	if status != http.StatusOK {
		return opapi.Settings{}, answerError(status, b)
	}
	var w settingsWire
	if err := json.Unmarshal(b, &w); err != nil {
		return opapi.Settings{}, fmt.Errorf("opclient: GET %s: decode answer: %w", path, err)
	}
	s := w.Settings
	if err := s.Validate(); err != nil {
		return opapi.Settings{}, fmt.Errorf("opclient: settings: %w", err)
	}
	s.Stand = c.stand(path, w.Stand)
	return s, nil
}

// stand decodes and checks a stand block. It returns nil when the block is
// absent or broken, and reports a broken block on the logger.
func (c *Client) stand(path string, raw json.RawMessage) *opapi.StandState {
	if len(raw) == 0 || string(raw) == "null" {
		c.noteStand(path, nil)
		return nil
	}
	var st opapi.StandState
	err := json.Unmarshal(raw, &st)
	if err != nil {
		err = fmt.Errorf("stand: %w", err)
	} else {
		err = opapi.Settings{Stand: &st}.ValidateStand()
	}
	c.noteStand(path, err)
	if err != nil {
		return nil
	}
	return &st
}

// noteStand logs a broken stand block once per path and reason, and once
// more when the block of the path is fine again. err is nil for a block
// that is fine or absent.
func (c *Client) noteStand(path string, err error) {
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	c.standMu.Lock()
	last := c.standErr[path]
	if reason == "" {
		delete(c.standErr, path)
	} else {
		c.standErr[path] = reason
	}
	c.standMu.Unlock()
	switch {
	case reason != "" && reason != last:
		c.logger.Warn("stand settings block of the router is not valid, stand settings are off",
			"path", path, "error", reason)
	case reason == "" && last != "":
		c.logger.Info("stand settings block of the router is no longer broken", "path", path)
	}
}

// checkStandAnswer checks a successful answer of PutSettings or
// PutSimDefaults: a known result, valid base settings and a valid stand
// block, which such an answer always has.
func checkStandAnswer(r opapi.Result, s opapi.Settings) error {
	if err := checkResult(r); err != nil {
		return err
	}
	if err := s.Validate(); err != nil {
		return fmt.Errorf("answer settings: %w", err)
	}
	if s.Stand == nil {
		return errors.New("answer settings have no stand block")
	}
	if err := s.ValidateStand(); err != nil {
		return fmt.Errorf("answer settings: %w", err)
	}
	return nil
}

// Code returns the code of an *opapi.Error or an *opapi.SettingsError in
// err, or "" when err is not an answer of the router.
func Code(err error) opapi.ErrorCode {
	if e, ok := errors.AsType[*opapi.Error](err); ok {
		return e.Code
	}
	if e, ok := errors.AsType[*opapi.SettingsError](err); ok {
		return e.Code
	}
	return ""
}

func (c *Client) action(ctx context.Context, path string, body any) (opapi.Result, error) {
	var resp opapi.ActionResponse
	if err := c.do(ctx, http.MethodPost, path, body, &resp); err != nil {
		return "", err
	}
	if err := checkResult(resp.Result); err != nil {
		return "", err
	}
	return resp.Result, nil
}

func checkResult(r opapi.Result) error {
	if r != opapi.ResultDone && r != opapi.ResultAlreadyDone {
		return fmt.Errorf("opclient: answer has an unknown result %q", r)
	}
	return nil
}

// do sends one request and decodes the answer into out. The answer is at
// most opapi.MaxBodyBytes, and an error answer is an *opapi.Error.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	status, b, err := c.send(ctx, method, path, body, opapi.MaxBodyBytes)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return answerError(status, b)
	}
	return decode(method, path, b, out)
}

// doSettings sends one PUT of the stand settings and decodes the answer
// into out. The answer is at most limit bytes, and an error answer is an
// *opapi.SettingsError.
func (c *Client) doSettings(ctx context.Context, path string, limit int, body, out any) error {
	status, b, err := c.send(ctx, http.MethodPut, path, body, limit)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return settingsAnswerError(http.MethodPut, path, status, b)
	}
	return decode(http.MethodPut, path, b, out)
}

// send sends one request and returns the status and the body of the
// answer, which must be at most limit bytes.
func (c *Client) send(ctx context.Context, method, path string, body any, limit int) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("opclient: %s %s: encode body: %w", method, path, err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return 0, nil, fmt.Errorf("opclient: %s %s: %w", method, path, err)
	}
	req.Header.Set("Accept", opapi.ContentType)
	if body != nil {
		req.Header.Set("Content-Type", opapi.ContentType)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("opclient: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	// One byte more than the limit shows an answer that is too large.
	b, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
	if err != nil {
		return 0, nil, fmt.Errorf("opclient: %s %s: read answer: %w", method, path, err)
	}
	if len(b) > limit {
		return 0, nil, fmt.Errorf("opclient: %s %s: answer is larger than %d bytes", method, path, limit)
	}
	return resp.StatusCode, b, nil
}

func decode(method, path string, b []byte, out any) error {
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("opclient: %s %s: decode answer: %w", method, path, err)
	}
	return nil
}

// answerError turns an error answer into an *opapi.Error. An answer
// without a valid ErrorResponse (for example from a proxy) gets a code
// from its status.
func answerError(status int, body []byte) error {
	var er opapi.ErrorResponse
	if err := json.Unmarshal(body, &er); err != nil || er.Code == "" {
		er = bareError(status, opapi.CodeInternal)
	}
	return &opapi.Error{Status: status, ErrorResponse: er}
}

// settingsAnswerError turns an error answer of a stand settings PUT into an
// *opapi.SettingsError. An answer without an ErrorResponse gets a code from
// its status, like in answerError, except that a bare 404 or 405 comes from
// a router from before decision 0014 (see opapi) and gets CodeSettingsOff.
// An answer that has a code but whose details do not decode or do not fit
// the code is not a valid answer, and gives a plain error.
func settingsAnswerError(method, path string, status int, body []byte) error {
	var plain opapi.ErrorResponse
	if err := json.Unmarshal(body, &plain); err != nil || plain.Code == "" {
		code := opapi.CodeInternal
		if status == http.StatusNotFound || status == http.StatusMethodNotAllowed {
			code = opapi.CodeSettingsOff
		}
		return &opapi.SettingsError{Status: status, SettingsErrorResponse: opapi.SettingsErrorResponse{
			ErrorResponse: bareError(status, code),
		}}
	}
	var er opapi.SettingsErrorResponse
	if err := json.Unmarshal(body, &er); err != nil {
		return fmt.Errorf("opclient: %s %s: decode error answer %d: %w", method, path, status, err)
	}
	if err := er.Validate(); err != nil {
		return fmt.Errorf("opclient: %s %s: error answer %d is not valid: %w", method, path, status, err)
	}
	return &opapi.SettingsError{Status: status, SettingsErrorResponse: er}
}

// bareError is the ErrorResponse of an answer without one: code, or
// CodeUnavailable for the statuses of a proxy that cannot reach the
// router.
func bareError(status int, code opapi.ErrorCode) opapi.ErrorResponse {
	if status == http.StatusServiceUnavailable || status == http.StatusBadGateway || status == http.StatusGatewayTimeout {
		code = opapi.CodeUnavailable
	}
	return opapi.ErrorResponse{Code: code, Message: http.StatusText(status)}
}
