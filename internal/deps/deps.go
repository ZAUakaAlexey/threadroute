//go:build deps

// Package deps pins the third-party modules of the project before the code
// that uses them exists. It is never built.
package deps

import (
	_ "github.com/coder/websocket"
	_ "github.com/klauspost/compress/zstd"
	_ "github.com/nats-io/nats-server/v2/server"
	_ "github.com/nats-io/nats.go"
	_ "github.com/nats-io/nats.go/jetstream"
	_ "github.com/prometheus/client_golang/prometheus"
	_ "github.com/prometheus/client_golang/prometheus/promhttp"
	_ "github.com/prometheus/client_golang/prometheus/testutil"
)
