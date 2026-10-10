package scrapejob

import (
	"context"
	"fmt"

	"ListenLedger/internal/eventsourcing"
)

// Repository loads and persists Job aggregates.
type Repository struct {
	store eventsourcing.Store
}

// NewRepository creates a Repository.
func NewRepository(store eventsourcing.Store) *Repository {
	return &Repository{store: store}
}

// Load replays a request stream or returns ErrStreamNotFound.
func (r *Repository) Load(ctx context.Context, requestID string) (*Job, error) {
	events, err := r.store.Load(ctx, requestID)
	if err != nil {
		return nil, fmt.Errorf("loading scrapejob stream %s: %w", requestID, err)
	}
	if len(events) == 0 {
		return nil, eventsourcing.ErrStreamNotFound
	}
	return Replay(requestID, events)
}

// Save commits uncommitted events with optimistic concurrency.
func (r *Repository) Save(ctx context.Context, agg *Job) ([]eventsourcing.Event, error) {
	uncommitted := agg.UncommittedEvents()
	if len(uncommitted) == 0 {
		return nil, nil
	}
	expectedVersion := agg.Version() - int64(len(uncommitted))
	if err := r.store.Append(ctx, agg.AggregateID(), expectedVersion, uncommitted...); err != nil {
		return nil, fmt.Errorf("saving scrapejob stream %s: %w", agg.AggregateID(), err)
	}
	agg.ClearUncommittedEvents()
	return uncommitted, nil
}
