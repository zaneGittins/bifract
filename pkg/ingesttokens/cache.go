package ingesttokens

import (
	"sync"
	"time"
)

// TokenCache provides an in-memory cache for validated ingest tokens
// to avoid a DB lookup on every ingest request.
type TokenCache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	ttl     time.Duration
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

func (c *TokenCache) Get(tokenHash string) (*ValidatedToken, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[tokenHash]
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.token, true
}

func (c *TokenCache) Set(tokenHash string, token *ValidatedToken) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[tokenHash] = &cacheEntry{
		token:     token,
		expiresAt: time.Now().Add(c.ttl),
	}
}

func (c *TokenCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*cacheEntry)
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
