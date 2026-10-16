package correctionproposal

import (
	"context"
	"fmt"

	"ListenLedger/internal/eventsourcing"
)

// Repository loads and saves correctionproposal aggregates.
type Repository struct {
	store eventsourcing.Store
}

// NewRepository creates a new correctionproposal repository.
func NewRepository(store eventsourcing.Store) *Repository {
	return &Repository{store: store}
}

// Load loads and replays a correctionproposal aggregate by ID.
func (r *Repository) Load(ctx context.Context, id string) (*Proposal, error) {
	events, err := r.store.Load(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("loading correctionproposal stream %s: %w", id, err)
	}
	if len(events) == 0 {
		return nil, eventsourcing.ErrStreamNotFound
	}
	return Replay(id, events)
}

// Save appends the aggregate's uncommitted events to the event store.
func (r *Repository) Save(ctx context.Context, agg *Proposal) ([]eventsourcing.Event, error) {
	uncommitted := agg.UncommittedEvents()
	if len(uncommitted) == 0 {
		return nil, nil
	}
	expectedVersion := agg.Version() - int64(len(uncommitted))
	if err := eventsourcing.AppendWithRetry(ctx, r.store, agg.AggregateID(), expectedVersion, uncommitted...); err != nil {
		return nil, fmt.Errorf("saving correctionproposal stream %s: %w", agg.AggregateID(), err)
	}
	agg.ClearUncommittedEvents()
	return uncommitted, nil
}
