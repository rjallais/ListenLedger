package eventsourcing

import (
	"cmp"
	"fmt"
	"time"
)

// Aggregate defines the contract for domain aggregates.
type Aggregate interface {
	AggregateID() string
	AggregateType() string
	Version() int64
	UncommittedEvents() []Event
	ClearUncommittedEvents()
	Apply(event Event) error
}

// AggregateOption configures the injected edges of a BaseAggregate.
type AggregateOption func(*BaseAggregate)

// WithClock injects the clock used to stamp recorded events and domain
// payload timestamps. A nil clock leaves the default wall clock in place.
func WithClock(clock Clock) AggregateOption {
	return func(b *BaseAggregate) {
		if clock != nil {
			b.clock = clock
		}
	}
}

// WithIDGen injects the generator used to assign event IDs. A nil generator
// leaves the default UUID generator in place.
func WithIDGen(idgen IDGen) AggregateOption {
	return func(b *BaseAggregate) {
		if idgen != nil {
			b.idgen = idgen
		}
	}
}

// SequencedAggregate is an optional Aggregate capability that tracks the
// underlying stream sequence (GlobalPosition) of the latest folded event,
// allowing JetStream writes to anchor OCC without an extra read round-trip.
type SequencedAggregate interface {
	LastSeq() uint64
	SetLastSeq(seq uint64)
}

// BaseAggregate provides common state and tracking for domain aggregates.
type BaseAggregate struct {
	id          string
	streamType  string
	version     int64
	lastSeq     uint64
	uncommitted []Event
	clock       Clock
	idgen       IDGen
}

// NewBaseAggregate initializes a new BaseAggregate. Clock and ID generation
// default to the wall clock and UUIDs; pass WithClock/WithIDGen to inject
// deterministic edges in tests.
func NewBaseAggregate(id, streamType string, opts ...AggregateOption) BaseAggregate {
	b := BaseAggregate{
		id:          id,
		streamType:  streamType,
		version:     0,
		uncommitted: make([]Event, 0),
		clock:       WallClock{},
		idgen:       UUIDIDGen{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&b)
		}
	}
	return b
}

func (b *BaseAggregate) AggregateID() string {
	return b.id
}

func (b *BaseAggregate) AggregateType() string {
	return b.streamType
}

func (b *BaseAggregate) Version() int64 {
	return b.version
}

func (b *BaseAggregate) SetVersion(v int64) {
	b.version = v
}

// LastSeq returns the last known stream sequence number (GlobalPosition) for
// this aggregate's persisted history, or 0 when unknown.
func (b *BaseAggregate) LastSeq() uint64 {
	return b.lastSeq
}

// SetLastSeq records the stream sequence number of the latest persisted event.
func (b *BaseAggregate) SetLastSeq(seq uint64) {
	b.lastSeq = seq
}

// Now returns the aggregate's current domain time. Domain methods use it for
// payload timestamps so an injected clock pins them. A zero-value
// BaseAggregate (never passed through NewBaseAggregate) falls back to the
// wall clock instead of panicking on a nil interface.
func (b *BaseAggregate) Now() time.Time {
	return cmp.Or[Clock](b.clock, WallClock{}).Now()
}

func (b *BaseAggregate) UncommittedEvents() []Event {
	return b.uncommitted
}

func (b *BaseAggregate) ClearUncommittedEvents() {
	b.uncommitted = nil
}

// RecordThat records a new uncommitted event and increments version. When
// eventType is empty, it is inferred from the registered payload struct type.
// A zero-value BaseAggregate falls back to UUID IDs and the wall clock.
func (b *BaseAggregate) RecordThat(eventType string, payload any, metadata any) (Event, error) {
	evt, err := newEvent(cmp.Or[IDGen](b.idgen, UUIDIDGen{}), cmp.Or[Clock](b.clock, WallClock{}), b.id, b.streamType, b.version+1, eventType, payload, metadata)
	if err != nil {
		return Event{}, fmt.Errorf("recording event %s: %w", eventType, err)
	}

	b.version = evt.Version
	b.uncommitted = append(b.uncommitted, evt)
	return evt, nil
}

// RecordPayload records a new uncommitted event whose event type is inferred
// from the registered payload struct type (Rita-style auto-type inference).
func (b *BaseAggregate) RecordPayload(payload any, metadata any) (Event, error) {
	return b.RecordThat("", payload, metadata)
}
