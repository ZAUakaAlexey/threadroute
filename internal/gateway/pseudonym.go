package gateway

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

// pseudonymCacheMax bounds the cache of pseudonyms. When it is full the
// cache is emptied; the values are made again on demand.
const pseudonymCacheMax = 300_000

// Pseudonyms makes the pseudonyms of the demo level (decision 0012): a
// keyed hash of an ID under a key of the UTC day. The key is random, lives
// only in this process and is never logged or sent. It is safe for
// concurrent use.
type Pseudonyms struct {
	mu    sync.Mutex
	day   int64
	key   []byte
	cache map[string]string
}

// NewPseudonyms returns pseudonyms with a key for the day of now.
func NewPseudonyms(now time.Time) *Pseudonyms {
	p := &Pseudonyms{}
	p.rotate(now)
	return p
}

func dayOf(t time.Time) int64 { return t.UTC().Unix() / 86400 }

// Rotate makes a new key when the UTC day of now differs from the day of
// the current key, and reports whether it did.
func (p *Pseudonyms) Rotate(now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if dayOf(now) == p.day {
		return false
	}
	p.rotate(now)
	return true
}

func (p *Pseudonyms) rotate(now time.Time) {
	key := make([]byte, 32)
	// crypto/rand.Read never returns an error (Go 1.24 and later).
	_, _ = rand.Read(key)
	p.day = dayOf(now)
	p.key = key
	p.cache = map[string]string{}
}

// Of returns the pseudonym of id under the current key.
func (p *Pseudonyms) Of(id string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.cache[id]; ok {
		return s
	}
	s := gwapi.Pseudonym(p.key, id)
	if len(p.cache) >= pseudonymCacheMax {
		p.cache = map[string]string{}
	}
	p.cache[id] = s
	return s
}
