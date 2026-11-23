//go:build deppin

// This file is temporary, see doc.go. It is only built with -tags deppin, so
// no binary links these packages, but go mod tidy keeps their modules.

package deppin

import (
	_ "github.com/alicebob/miniredis/v2"
	_ "github.com/redis/go-redis/v9"
)
