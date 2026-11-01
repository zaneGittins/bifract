package ingesttokens

import (
	"sync"
	"time"

	"bifract/pkg/pgnotify"
)

// TokenCache provides an in-memory cache for validated ingest tokens
// to avoid a DB lookup on every ingest request.
type TokenCache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	ttl     time.Duration
	// gen counts invalidations, so a lookup that began before one is not cached after it.
	gen uint64
	bus *pgnotify.Bus
}

type cacheEntry struct {
	token     *ValidatedToken
	expiresAt time.Time
}

func NewTokenCache(ttl time.Duration) *TokenCache {
	c := &TokenCache{
		entries: make(map[string]*cacheEntry),
		ttl:     ttl,
	}
	go c.cleanupLoop()
	return c
}

// Watch shares invalidation with every other process on bus: InvalidateAll
// announces it, and an announcement from anywhere clears this cache.
func (c *TokenCache) Watch(bus *pgnotify.Bus) {
	c.mu.Lock()
	c.bus = bus
	c.mu.Unlock()
	bus.Subscribe(pgnotify.TopicIngestTokens, c.clear)
}

func (c *TokenCache) Get(tokenHash string) (*ValidatedToken, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[tokenHash]
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.token, true
}

// Generation is read before looking a token up and passed to Set.
func (c *TokenCache) Generation() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.gen
}

// Set caches a lookup unless the cache was invalidated since gen was read.
func (c *TokenCache) Set(tokenHash string, token *ValidatedToken, gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen != c.gen {
		return
	}
	c.entries[tokenHash] = &cacheEntry{
		token:     token,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// InvalidateAll drops every cached token here and in every watching process.
func (c *TokenCache) InvalidateAll() {
	c.clear()
	c.mu.RLock()
	bus := c.bus
	c.mu.RUnlock()
	bus.Publish(pgnotify.TopicIngestTokens)
}

func (c *TokenCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*cacheEntry)
	c.gen++
}

func (c *TokenCache) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for hash, e := range c.entries {
			if now.After(e.expiresAt) {
				delete(c.entries, hash)
			}
		}
		c.mu.Unlock()
	}
}
