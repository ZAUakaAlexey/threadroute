// Package source defines the contract between a channel connector and the
// rest of the pipeline.
package source

import (
	"context"

	"github.com/ZAUakaAlexey/threadroute/internal/model"
)

// Sink receives events from a connector, in source order.
//
// A Sink may block. A connector must then stop reading from the source, so
// pressure reaches the source instead of growing a queue in memory. A Sink
// error is fatal for the connector.
type Sink func(ctx context.Context, ev model.Event) error

// Connector streams events from one channel.
type Connector interface {
	// Run delivers events to sink until ctx is done or a fatal error happens.
	// It reconnects on its own and resumes after the last event it delivered.
	//
	// from is the position to start at. A zero cursor means "start from live".
	// Delivery is at-least-once: after a reconnect the connector can deliver an
	// event again, and the consumer drops it by Event.DedupID.
	//
	// Run returns nil when ctx is done.
	Run(ctx context.Context, from model.Cursor, sink Sink) error
}
