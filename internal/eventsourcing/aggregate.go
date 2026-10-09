package eventsourcing

import "fmt"

// Aggregate defines the contract for domain aggregates.
type Aggregate interface {
	AggregateID() string
	AggregateType() string
	Version() int64
	UncommittedEvents() []Event
	ClearUncommittedEvents()
	Apply(event Event) error
}

// BaseAggregate provides common state and tracking for domain aggregates.
type BaseAggregate struct {
	id          string
	streamType  string
	version     int64
	uncommitted []Event
}

// NewBaseAggregate initializes a new BaseAggregate.
func NewBaseAggregate(id, streamType string) BaseAggregate {
	return BaseAggregate{
		id:          id,
		streamType:  streamType,
		version:     0,
		uncommitted: make([]Event, 0),
	}
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

func (b *BaseAggregate) UncommittedEvents() []Event {
	return b.uncommitted
}

func (b *BaseAggregate) ClearUncommittedEvents() {
	b.uncommitted = nil
}

// RecordThat records a new uncommitted event and increments version.
func (b *BaseAggregate) RecordThat(eventType string, payload any, metadata any) (Event, error) {
	evt, err := NewEvent(b.id, b.streamType, b.version+1, eventType, payload, metadata)
	if err != nil {
		return Event{}, fmt.Errorf("recording event %s: %w", eventType, err)
	}

	b.version = evt.Version
	b.uncommitted = append(b.uncommitted, evt)
	return evt, nil
}
