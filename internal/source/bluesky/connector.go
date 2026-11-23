// Package bluesky implements source.Connector for the Bluesky Jetstream v2,
// on top of the official SDK github.com/bluesky-social/jetstream (decisions
// 0002 and 0003). No other package imports the SDK.
package bluesky

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

// minTimeCursorUS is the smallest cursor value the SDK reads as a time
// cursor. A smaller value is read as a host-local seq.
const minTimeCursorUS = 1_000_000_000_000_000

// errStreamEnded is returned when the SDK iterator stops on its own. A live
// tail should only stop on context cancellation or a fatal error.
var errStreamEnded = errors.New("bluesky: stream ended without an error")

// Config configures a Connector. Every field is required.
type Config struct {
	// PrimaryHost is the Jetstream host to connect to first, for example
	// "jetstream.us-east.bsky.network".
	PrimaryHost string
	// FailoverHost is used when the primary host cannot be reached.
	FailoverHost string
	// Collection is the record collection to receive, for example
	// "app.bsky.feed.post". Account and sync events are not filtered by it.
	Collection string
	// Logger receives connector and SDK logs.
	Logger *slog.Logger
	// Registerer receives the connector metrics.
	Registerer prometheus.Registerer
}

// validate checks the fields this package can check by itself. The SDK checks
// the host and collection formats in New.
func (c Config) validate() error {
	var errs []error
	if strings.TrimSpace(c.PrimaryHost) == "" {
		errs = append(errs, errors.New("primary host is empty"))
	}
	if strings.TrimSpace(c.FailoverHost) == "" {
		errs = append(errs, errors.New("failover host is empty"))
	}
	if c.PrimaryHost != "" && c.PrimaryHost == c.FailoverHost {
		errs = append(errs, errors.New("failover host is the same as the primary host"))
	}
	if strings.TrimSpace(c.Collection) == "" {
		errs = append(errs, errors.New("collection is empty"))
	}
	if c.Logger == nil {
		errs = append(errs, errors.New("logger is nil"))
	}
	if c.Registerer == nil {
		errs = append(errs, errors.New("registerer is nil"))
	}
	return errors.Join(errs...)
}

// eventStream is the part of the SDK client the read loop uses. It yields
// the events of each SDK batch, or a recoverable or fatal error.
type eventStream interface {
	Batches(ctx context.Context) iter.Seq2[[]jetstream.Event, error]
	Close() error
}

// Connector reads the Bluesky Jetstream and hands batches to a sink.
type Connector struct {
	cfg     Config
	logger  *slog.Logger
	metrics *metrics
	now     func() time.Time
	// open builds the stream for one Run. Tests replace it with a fake.
	open func(from source.Cursor) (eventStream, error)
	// sdkExtra holds additional SDK options. Tests use it to turn off
	// compression against a local fake server.
	sdkExtra []jetstream.Option
}

var _ source.Connector = (*Connector)(nil)

// New validates cfg, registers the metrics and returns a Connector. It does
// not connect; Run does.
func New(cfg Config) (*Connector, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("bluesky: invalid config: %w", err)
	}
	c := &Connector{
		cfg:    cfg,
		logger: cfg.Logger.With("component", "source.bluesky"),
		now:    time.Now,
	}
	c.open = c.openSDK
	// Let the SDK check the hosts and the collection now, so a bad config
	// fails at start. Subscribe does not connect.
	probe, err := jetstream.Subscribe(cfg.PrimaryHost, c.sdkOptions(0)...)
	if err != nil {
		return nil, fmt.Errorf("bluesky: invalid config: %w", err)
	}
	if err := probe.Close(); err != nil {
		return nil, fmt.Errorf("bluesky: close config probe: %w", err)
	}
	m, err := newMetrics(cfg.Registerer, func() time.Time { return c.now() })
	if err != nil {
		return nil, fmt.Errorf("bluesky: %w", err)
	}
	c.metrics = m
	return c, nil
}

// sdkOptions returns the subscription options of decision 0003. cursorUS is
// the witnessed-time cursor in Unix microseconds, 0 for live.
func (c *Connector) sdkOptions(cursorUS uint64) []jetstream.Option {
	opts := []jetstream.Option{
		jetstream.WithKinds([]jetstream.Kind{jetstream.KindCommit, jetstream.KindAccount, jetstream.KindSync}),
		jetstream.WithCollections([]string{c.cfg.Collection}),
		jetstream.WithCursorMode(jetstream.CursorTime),
		jetstream.WithFailoverHosts(c.cfg.FailoverHost),
		jetstream.WithLogger(c.cfg.Logger.With("component", "jetstream-sdk")),
	}
	if cursorUS > 0 {
		opts = append(opts, jetstream.WithLiveCursor(cursorUS))
	}
	return append(opts, c.sdkExtra...)
}

// cursorMicros converts a resume position to the SDK time cursor. An unset
// cursor gives 0, which means live.
func cursorMicros(from source.Cursor) (uint64, error) {
	if from.IsZero() {
		return 0, nil
	}
	us := from.Witnessed.UnixMicro()
	if us < minTimeCursorUS {
		return 0, fmt.Errorf("cursor time %s is too old to be a time cursor", from.Witnessed.UTC().Format(time.RFC3339))
	}
	return uint64(us), nil
}

// openSDK subscribes with the real SDK client.
func (c *Connector) openSDK(from source.Cursor) (eventStream, error) {
	us, err := cursorMicros(from)
	if err != nil {
		return nil, err
	}
	client, err := jetstream.Subscribe(c.cfg.PrimaryHost, c.sdkOptions(us)...)
	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}
	return sdkStream{client: client}, nil
}

// sdkStream adapts *jetstream.Client to eventStream.
type sdkStream struct {
	client *jetstream.Client
}

func (s sdkStream) Batches(ctx context.Context) iter.Seq2[[]jetstream.Event, error] {
	return func(yield func([]jetstream.Event, error) bool) {
		for b, err := range s.client.Events(ctx) {
			var events []jetstream.Event
			if b != nil {
				events = b.Events()
			}
			if !yield(events, err) {
				return
			}
		}
	}
}

func (s sdkStream) Close() error { return s.client.Close() }

// Run implements source.Connector. It calls sink synchronously for every
// batch, so a blocked sink stops the reads.
//
// Run returns nil when ctx is cancelled, also when the sink returns the
// context error because of that cancellation. It returns a wrapped error for
// a fatal SDK error, any other sink error, or a stream that ends on its own.
func (c *Connector) Run(ctx context.Context, from source.Cursor, sink source.Sink) error {
	if sink == nil {
		return errors.New("bluesky: sink is nil")
	}
	if ctx.Err() != nil {
		return nil
	}
	stream, err := c.open(from)
	if err != nil {
		return fmt.Errorf("bluesky: open stream: %w", err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			c.logger.Warn("close stream", "err", err)
		}
	}()
	if from.IsZero() {
		c.logger.Info("starting from live")
	} else {
		c.logger.Info("resuming", "witnessed", from.Witnessed, "seq", from.Seq, "host", from.Host)
	}
	return c.consume(ctx, stream, sink)
}

// consume is the read loop. There is no goroutine or queue between the SDK
// iterator and the sink.
func (c *Connector) consume(ctx context.Context, stream eventStream, sink source.Sink) error {
	for events, err := range stream.Batches(ctx) {
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if ferr := c.handleStreamError(err); ferr != nil {
				return ferr
			}
			continue
		}
		if err := c.deliver(ctx, events, sink); err != nil {
			if ctx.Err() != nil && isContextError(err) {
				return nil
			}
			return err
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	return errStreamEnded
}

// deliver maps one SDK batch and passes it to the sink.
func (c *Connector) deliver(ctx context.Context, events []jetstream.Event, sink source.Sink) error {
	res := mapBatch(events)
	c.metrics.observeSkipped(res)
	if len(res.batch.Events) == 0 {
		return nil
	}
	if err := sink(ctx, res.batch); err != nil {
		return fmt.Errorf("bluesky: sink: %w", err)
	}
	c.metrics.observeDelivered(res)
	return nil
}

// handleStreamError counts and logs an error from the SDK iterator. It
// returns a non-nil error only when the stream cannot continue.
func (c *Connector) handleStreamError(err error) error {
	switch {
	case errors.Is(err, jetstream.ErrFatal):
		return fmt.Errorf("bluesky: stream failed: %w", err)
	case errors.Is(err, jetstream.ErrCursorClamped):
		c.metrics.cursorClamped.Inc()
		c.logger.Warn("resume cursor was older than the source keeps, events may be lost", "err", err)
		return nil
	}
	typ := classifyError(err)
	c.metrics.errors.WithLabelValues(typ).Inc()
	if typ == errTypeReconnect || typ == errTypeConsumerTooSlow {
		c.logger.Warn("recoverable source error", "type", typ, "err", err)
		return nil
	}
	// Other errors can quote parts of a frame. Log only the type, so post
	// text never reaches the logs.
	c.logger.Warn("recoverable source error", "type", typ)
	return nil
}

// classifyError maps a recoverable SDK error to a metric label. The SDK
// exports no error types for these cases, so this matches on its stable
// message prefixes. Unknown errors are "other".
func classifyError(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "live tail reconnecting"):
		if strings.Contains(msg, "ConsumerTooSlow") {
			return errTypeConsumerTooSlow
		}
		return errTypeReconnect
	case strings.Contains(msg, "zstd"):
		return errTypeZstd
	case strings.Contains(msg, "decode") || strings.Contains(msg, "frame"):
		return errTypeDecode
	default:
		return errTypeOther
	}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
