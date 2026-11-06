package eventsourcing

import (
	"sync"
	"time"
)

// Clock abstracts the wall clock used to stamp events. Aggregates read
// domain time through BaseAggregate.Now so tests can inject a FakeClock
// and pin payload timestamps deterministically.
type Clock interface {
	Now() time.Time
}

// WallClock is the default Clock backed by the system clock.
type WallClock struct{}

// Now returns the current UTC time.
func (WallClock) Now() time.Time { return time.Now().UTC() }

// FakeClock is a deterministic Clock for tests. Now returns the current
// fake time; Set and Advance mutate it and are safe for concurrent use.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock creates a FakeClock starting at the given time.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Set moves the fake time to t.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

// Advance moves the fake time forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}
