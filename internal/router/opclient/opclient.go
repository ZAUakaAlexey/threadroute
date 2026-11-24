// Package opclient is the HTTP client of the operator API (decision 0010)
// for the simulator and, later, the gateway. It has one method per action
// of internal/opapi. Each returns the opapi result, or an error:
//
//   - *opapi.Error when the router answered with an error (read the code
//     with errors.As, or with Code). A code for which ErrorCode.Rejected is
//     true means the state moved on: repeating the call gives the same
//     answer. CodeUnavailable means retry later.
//   - any other error when no answer came: the router is not reachable,
//     the call timed out, or the answer was not valid.
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
	"net/http"
	"net/url"
	"strings"
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
// does not start without valid settings (decision 0010).
func (c *Client) Settings(ctx context.Context) (opapi.Settings, error) {
	var s opapi.Settings
	if err := c.do(ctx, http.MethodGet, opapi.SettingsPath(), nil, &s); err != nil {
		return opapi.Settings{}, err
	}
	if err := s.Validate(); err != nil {
		return opapi.Settings{}, fmt.Errorf("opclient: settings: %w", err)
	}
	return s, nil
}

// Code returns the code of an *opapi.Error in err, or "" when err is not
// an answer of the router.
func Code(err error) opapi.ErrorCode {
	if e, ok := errors.AsType[*opapi.Error](err); ok {
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

// do sends one request and decodes the answer into out.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("opclient: %s %s: encode body: %w", method, path, err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return fmt.Errorf("opclient: %s %s: %w", method, path, err)
	}
	req.Header.Set("Accept", opapi.ContentType)
	if body != nil {
		req.Header.Set("Content-Type", opapi.ContentType)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("opclient: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	// One byte more than the limit shows an answer that is too large.
	b, err := io.ReadAll(io.LimitReader(resp.Body, opapi.MaxBodyBytes+1))
	if err != nil {
		return fmt.Errorf("opclient: %s %s: read answer: %w", method, path, err)
	}
	if len(b) > opapi.MaxBodyBytes {
		return fmt.Errorf("opclient: %s %s: answer is larger than %d bytes", method, path, opapi.MaxBodyBytes)
	}
	if resp.StatusCode != http.StatusOK {
		return answerError(resp.StatusCode, b)
	}
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
		er = opapi.ErrorResponse{Code: opapi.CodeInternal, Message: http.StatusText(status)}
		if status == http.StatusServiceUnavailable || status == http.StatusBadGateway || status == http.StatusGatewayTimeout {
			er.Code = opapi.CodeUnavailable
		}
	}
	return &opapi.Error{Status: status, ErrorResponse: er}
}
