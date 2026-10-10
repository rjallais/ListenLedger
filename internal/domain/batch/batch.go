// Package batch models the Batch aggregate: a bounded-lifecycle aggregate
// for batch refresh workflows (Started -> ArtistCompleted x N -> Completed).
//
// Per the talk's aggregate guidance, streams stay bounded: one stream per
// batch with a beginning and an end, instead of accumulating events on a
// long-lived aggregate. Event names are past-tense facts; the imperative
// command side (start batch, redrive) lives in handlers and internal/commands.
package batch

import (
	"fmt"

	"ListenLedger/internal/eventsourcing"
)

// StreamTypeBatch identifies batch event streams.
const StreamTypeBatch = "batch"

const (
	EventTypeBatchStarted         = "BatchStarted"
	EventTypeBatchArtistCompleted = "BatchArtistCompleted"
	EventTypeBatchCompleted       = "BatchCompleted"
)

// StartedPayload captures the full member list at batch birth.
type StartedPayload struct {
	BatchID   string         `json:"batch_id"`
	ArtistIDs []string       `json:"artist_ids"`
	Stats     map[string]int `json:"stats"`
}

// ArtistCompletedPayload records one member's terminal completion.
type ArtistCompletedPayload struct {
	BatchID  string `json:"batch_id"`
	ArtistID string `json:"artist_id"`
}

// CompletedPayload marks the bounded lifecycle's end.
type CompletedPayload struct {
	BatchID   string `json:"batch_id"`
	Total     int    `json:"total"`
	Completed int    `json:"completed"`
}

// Batch is the aggregate root for one batch refresh run.
type Batch struct {
	eventsourcing.BaseAggregate

	members map[string]bool
	stats   map[string]int
	closed  bool
}

// NewBatch creates a Batch and records BatchStarted.
func NewBatch(id string, artistIDs []string, stats map[string]int) (*Batch, error) {
	if id == "" {
		return nil, fmt.Errorf("batch: id cannot be empty")
	}
	agg := &Batch{
		BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeBatch),
		members:       make(map[string]bool, len(artistIDs)),
		stats:         stats,
	}
	seen := make(map[string]struct{}, len(artistIDs))
	unique := make([]string, 0, len(artistIDs))
	for _, artistID := range artistIDs {
		if artistID == "" {
			continue
		}
		if _, dup := seen[artistID]; dup {
			continue
		}
		seen[artistID] = struct{}{}
		unique = append(unique, artistID)
		agg.members[artistID] = false
	}

	evt, err := agg.RecordThat(EventTypeBatchStarted, StartedPayload{
		BatchID:   id,
		ArtistIDs: unique,
		Stats:     stats,
	}, nil)
	if err != nil {
		return nil, err
	}
	if err := agg.apply(evt); err != nil {
		return nil, err
	}
	// An empty batch ends at birth so replay observes the same Done state
	// the projection stores.
	if len(unique) == 0 {
		if _, _, err := agg.RecordClosed(); err != nil {
			return nil, err
		}
	}
	return agg, nil
}

// Total returns the member count.
func (b *Batch) Total() int {
	return len(b.members)
}

// Completed counts finished members.
func (b *Batch) Completed() int {
	n := 0
	for _, done := range b.members {
		if done {
			n++
		}
	}
	return n
}

// Done reports whether the bounded lifecycle ended. An empty batch is done
// at birth (beginning and end coincide); otherwise all members must finish.
func (b *Batch) Done() bool {
	return b.Total() == 0 || b.Completed() >= b.Total()
}

// Stats returns the creation-time priority stats.
func (b *Batch) Stats() map[string]int {
	out := make(map[string]int, len(b.stats))
	for k, v := range b.stats {
		out[k] = v
	}
	return out
}

// Members returns artistID -> done.
func (b *Batch) Members() map[string]bool {
	out := make(map[string]bool, len(b.members))
	for k, v := range b.members {
		out[k] = v
	}
	return out
}

// RecordCompletion records BatchArtistCompleted, or reports false when there
// is no new fact (unknown artist or duplicate) so callers append nothing.
// The log stays free of non-facts; the projection is already converged.
func (b *Batch) RecordCompletion(artistID string) (eventsourcing.Event, bool, error) {
	done, ok := b.members[artistID]
	if !ok || done {
		return eventsourcing.Event{}, false, nil
	}
	evt, err := b.RecordThat(EventTypeBatchArtistCompleted, ArtistCompletedPayload{
		BatchID:  b.AggregateID(),
		ArtistID: artistID,
	}, nil)
	if err != nil {
		return eventsourcing.Event{}, false, err
	}
	if err := b.apply(evt); err != nil {
		return eventsourcing.Event{}, false, err
	}
	return evt, true, nil
}

// RecordClosed records BatchCompleted when the lifecycle ended, or false.
// Idempotent: a closed batch records nothing further.
func (b *Batch) RecordClosed() (eventsourcing.Event, bool, error) {
	if !b.Done() || b.closed {
		return eventsourcing.Event{}, false, nil
	}
	evt, err := b.RecordThat(EventTypeBatchCompleted, CompletedPayload{
		BatchID:   b.AggregateID(),
		Total:     b.Total(),
		Completed: b.Completed(),
	}, nil)
	if err != nil {
		return eventsourcing.Event{}, false, err
	}
	if err := b.apply(evt); err != nil {
		return eventsourcing.Event{}, false, err
	}
	return evt, true, nil
}

// Apply reconstitutes state from one event (also used by Replay).
func (b *Batch) Apply(evt eventsourcing.Event) error {
	return b.apply(evt)
}

func (b *Batch) apply(evt eventsourcing.Event) error {
	b.SetVersion(evt.Version)
	switch evt.EventType {
	case EventTypeBatchStarted:
		var pl StartedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		if b.members == nil {
			b.members = make(map[string]bool, len(pl.ArtistIDs))
		}
		for _, id := range pl.ArtistIDs {
			if _, ok := b.members[id]; !ok {
				b.members[id] = false
			}
		}
		b.stats = pl.Stats
	case EventTypeBatchArtistCompleted:
		var pl ArtistCompletedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		if _, ok := b.members[pl.ArtistID]; ok {
			b.members[pl.ArtistID] = true
		}
	case EventTypeBatchCompleted:
		// Lifecycle marker; state already follows from completions.
		var pl CompletedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		b.closed = true
	}
	return nil
}

// Replay folds a batch stream into its aggregate.
func Replay(id string, events []eventsourcing.Event) (*Batch, error) {
	if len(events) == 0 {
		return nil, eventsourcing.ErrStreamNotFound
	}
	agg := &Batch{
		BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeBatch),
		members:       make(map[string]bool),
	}
	for _, evt := range events {
		if err := agg.apply(evt); err != nil {
			return nil, fmt.Errorf("replaying batch event %s v%d: %w", evt.EventType, evt.Version, err)
		}
	}
	return agg, nil
}
