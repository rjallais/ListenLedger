//go:build goexperiment.jsonv2

// Package correlation provides request ID tracking for command-event correlation.
package correlation

import (
	"sync"
	"time"
)

var (
	mu       sync.RWMutex
	registry = make(map[string]entry)
)

type entry struct {
	expiresAt time.Time
	requestID string
}

const ttl = 5 * time.Minute

func Associate(artistID, requestID string) {
	mu.Lock()
	defer mu.Unlock()
	registry[artistID] = entry{
		requestID: requestID,
		expiresAt: time.Now().Add(ttl),
	}
}

func Get(artistID string) string {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := registry[artistID]
	if !ok || time.Now().After(e.expiresAt) {
		return ""
	}
	return e.requestID
}

func Pop(artistID string) string {
	mu.Lock()
	defer mu.Unlock()

	e, ok := registry[artistID]
	if !ok {
		return ""
	}

	delete(registry, artistID)
	if time.Now().After(e.expiresAt) {
		return ""
	}

	return e.requestID
}

func Clear(artistID string) {
	mu.Lock()
	defer mu.Unlock()
	delete(registry, artistID)
}

func PurgeExpired() {
	mu.Lock()
	defer mu.Unlock()
	now := time.Now()
	for k, e := range registry {
		if now.After(e.expiresAt) {
			delete(registry, k)
		}
	}
}
