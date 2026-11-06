package eventsourcing

import (
	"fmt"
	"sync"
	"uuid"
)

// IDGen generates event identifiers. The default produces UUIDs; tests can
// inject a sequence generator to pin MsgID dedup keys and stream ordering.
type IDGen interface {
	NewEventID() string
}

// UUIDIDGen is the default IDGen.
type UUIDIDGen struct{}

// NewEventID returns an evt_-prefixed random UUID.
func (UUIDIDGen) NewEventID() string { return "evt_" + uuid.New().String() }

// SeqIDGen produces evt_1, evt_2, ... for deterministic tests. Safe for
// concurrent use.
type SeqIDGen struct {
	mu sync.Mutex
	n  int
}

// NewEventID returns the next sequential event ID.
func (g *SeqIDGen) NewEventID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n++
	return fmt.Sprintf("evt_%d", g.n)
}
