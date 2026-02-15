//go:build goexperiment.jsonv2

package correlation

import (
	"testing"
	"time"
)

func TestAssociateAndGet(t *testing.T) {
	artistID := "artist-123"
	requestID := "req-456"

	Associate(artistID, requestID)

	got := Get(artistID)
	if got != requestID {
		t.Errorf("Get() = %q, want %q", got, requestID)
	}
}

func TestGetExpired(t *testing.T) {
	artistID := "artist-expired"
	requestID := "req-expired"

	mu.Lock()
	registry[artistID] = entry{
		requestID: requestID,
		expiresAt: time.Now().Add(-1 * time.Minute),
	}
	mu.Unlock()

	got := Get(artistID)
	if got != "" {
		t.Errorf("Get() of expired entry = %q, want empty", got)
	}
}

func TestClear(t *testing.T) {
	artistID := "artist-clear"
	requestID := "req-clear"

	Associate(artistID, requestID)
	Clear(artistID)

	got := Get(artistID)
	if got != "" {
		t.Errorf("Get() after Clear() = %q, want empty", got)
	}
}

func TestPurgeExpired(t *testing.T) {
	expiredID := "artist-expired-purge"
	activeID := "artist-active-purge"

	mu.Lock()
	registry[expiredID] = entry{
		requestID: "req-expired",
		expiresAt: time.Now().Add(-1 * time.Minute),
	}
	registry[activeID] = entry{
		requestID: "req-active",
		expiresAt: time.Now().Add(5 * time.Minute),
	}
	mu.Unlock()

	PurgeExpired()

	if Get(expiredID) != "" {
		t.Error("expired entry should be purged")
	}
	if Get(activeID) == "" {
		t.Error("active entry should remain")
	}
}
