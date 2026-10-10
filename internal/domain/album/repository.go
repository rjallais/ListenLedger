package album

import (
	"context"
	"fmt"

	"ListenLedger/internal/eventsourcing"
)

// Repository loads and persists album aggregates.
type Repository struct {
	store eventsourcing.Store
}

// NewRepository creates an album repository.
func NewRepository(store eventsourcing.Store) *Repository {
	return &Repository{store: store}
}

// Load replays the album stream.
func (r *Repository) Load(ctx context.Context, id string) (*Album, error) {
	events, err := r.store.Load(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("loading album stream %s: %w", id, err)
	}
	if len(events) == 0 {
		return nil, eventsourcing.ErrStreamNotFound
	}
	return Replay(id, events)
}

// Save appends the aggregate's uncommitted events.
func (r *Repository) Save(ctx context.Context, agg *Album) ([]eventsourcing.Event, error) {
	uncommitted := agg.UncommittedEvents()
	if len(uncommitted) == 0 {
		return nil, nil
	}
	expectedVersion := agg.Version() - int64(len(uncommitted))
	if err := eventsourcing.AppendWithRetry(ctx, r.store, agg.AggregateID(), expectedVersion, uncommitted...); err != nil {
		return nil, fmt.Errorf("saving album stream %s: %w", agg.AggregateID(), err)
	}
	agg.ClearUncommittedEvents()
	return uncommitted, nil
}
