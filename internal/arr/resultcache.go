package arr

import (
	"context"
	"sync"
	"time"
)

// The apps page through a Torznab answer: Radarr and Sonarr (through
// Prowlarr) ask for offset=0, 100, 200... while a page comes back full.
// Every page used to repeat the whole eD2K search (one search per title,
// one after another), so a search of a minute became six. resultCache
// keeps the full answer of a search for a few minutes, so the pages are
// cut from it, and searches that arrive while the same one is running
// wait for it instead of starting another.

const (
	resultTTL        = 10 * time.Minute
	resultCacheLimit = 200 // entries; the cache is emptied when it grows past this
)

type cacheEntry struct {
	items []tzItem
	at    time.Time
}

type flight struct {
	done  chan struct{}
	items []tzItem
	ok    bool // the answer is complete and may be reused
	err   error
}

type resultCache struct {
	mu      sync.Mutex
	data    map[string]cacheEntry
	flights map[string]*flight
	now     func() time.Time // replaceable in tests
}

func (c *resultCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// do returns the items stored under key, or runs fn and stores what it
// returns. fn reports whether its answer is complete (a search that lost
// one of its titles on the way is answered but not remembered). The
// returned slice is shared: callers must not change it.
func (c *resultCache) do(ctx context.Context, key string, fn func() (items []tzItem, complete bool, err error)) ([]tzItem, error) {
	for {
		c.mu.Lock()
		if e, ok := c.data[key]; ok {
			if c.clock().Sub(e.at) < resultTTL {
				c.mu.Unlock()
				return e.items, nil
			}
			delete(c.data, key)
		}
		if f, ok := c.flights[key]; ok {
			c.mu.Unlock()
			select {
			case <-f.done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if f.err == nil {
				return f.items, nil
			}
			// The search we waited for failed (maybe its app gave up):
			// try again, this time running it ourselves.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		f := &flight{done: make(chan struct{})}
		if c.flights == nil {
			c.flights = map[string]*flight{}
		}
		c.flights[key] = f
		c.mu.Unlock()

		f.items, f.ok, f.err = fn()

		c.mu.Lock()
		delete(c.flights, key)
		if f.err == nil && f.ok {
			if c.data == nil || len(c.data) >= resultCacheLimit {
				c.data = map[string]cacheEntry{}
			}
			c.data[key] = cacheEntry{items: f.items, at: c.clock()}
		}
		c.mu.Unlock()
		close(f.done)
		return f.items, f.err
	}
}
