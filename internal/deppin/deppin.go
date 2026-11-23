//go:build deppin

// This file is temporary. It blank-imports the packages that the ingest,
// normalize and test code will use, so that go mod tidy keeps them in go.mod
// and go.sum before that code exists. The build tag keeps it out of normal
// builds. Remove each import once a real package imports it.
package deppin

import (
	_ "github.com/bluesky-social/jetstream"
	_ "github.com/klauspost/compress/zstd"
	_ "github.com/prometheus/client_golang/prometheus"
	_ "github.com/prometheus/client_golang/prometheus/promhttp"
	_ "github.com/prometheus/client_golang/prometheus/testutil"
	_ "github.com/twmb/franz-go/pkg/kadm"
	_ "github.com/twmb/franz-go/pkg/kfake"
	_ "github.com/twmb/franz-go/pkg/kgo"
)
