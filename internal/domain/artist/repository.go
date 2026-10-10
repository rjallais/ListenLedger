package artist

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"ListenLedger/internal/eventsourcing"
)

// snapshotInterval bounds artist stream replay: every Nth version stores a
// freeze-frame so loaders replay at most N-1 events after it. Artist streams
// are the only long-lived aggregates (one listener/status fact per scrape);
// scrapejob/batch/album/song streams stay tiny and replay directly.
const snapshotInterval = 200

// Repository handles loading and persisting Artist aggregates.
type Repository struct {
	store eventsourcing.Store
}

// NewRepository creates a new Artist repository.
func NewRepository(store eventsourcing.Store) *Repository {
	return &Repository{store: store}
}

// Load replays historical events for an artist, skipping history before the
// latest snapshot when the store supports snapshots. Snapshots are a load
// optimization: the event log stays the source of truth and every event
// after the snapshot version is still applied. Falls back to full replay
// when no snapshot exists or the snapshot path fails.
func (r *Repository) Load(ctx context.Context, id string) (*Artist, error) {
	if snapStore, ok := r.store.(eventsourcing.SnapshotStore); ok {
		agg, err := r.loadFromSnapshot(ctx, snapStore, id)
		if err == nil {
			return agg, nil
		}
		if !errors.Is(err, eventsourcing.ErrSnapshotNotFound) {
			slog.Warn("artist snapshot path failed, falling back to full replay", "artist_id", id, "error", err)
		}
		// No snapshot yet, or the snapshot path failed: fall through to
		// full replay rather than failing the read. The event log stays
		// the source of truth either way.
	}

	events, err := r.store.Load(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("loading artist stream %s: %w", id, err)
	}

	if len(events) == 0 {
		return nil, eventsourcing.ErrStreamNotFound
	}

	return Replay(id, events)
}

// loadFromSnapshot restores the freeze-frame then applies the tail.
// Returns ErrSnapshotNotFound when no snapshot exists, ErrStreamNotFound
// when neither snapshot nor events exist.
func (r *Repository) loadFromSnapshot(ctx context.Context, snapStore eventsourcing.SnapshotStore, id string) (*Artist, error) {
	snap, err := snapStore.LoadSnapshot(ctx, id)
	if err != nil {
		if errors.Is(err, eventsourcing.ErrSnapshotNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("loading artist snapshot %s: %w", id, err)
	}
	if snap.StreamType != "" && snap.StreamType != StreamTypeArtist {
		return nil, fmt.Errorf("snapshot for %s has type %q, want %q", id, snap.StreamType, StreamTypeArtist)
	}

	agg := &Artist{
		BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeArtist),
	}
	if err := agg.restoreFromSnapshot(snap.Version, snap.Payload); err != nil {
		return nil, err
	}

	tail, err := snapStore.LoadAfter(ctx, id, snap.Version)
	if err != nil {
		return nil, fmt.Errorf("loading artist tail %s after v%d: %w", id, snap.Version, err)
	}
	for _, evt := range tail {
		if err := agg.apply(evt); err != nil {
			return nil, fmt.Errorf("applying artist tail %s v%d: %w", id, evt.Version, err)
		}
	}
	return agg, nil
}

// Save commits any uncommitted events on the aggregate to the event store.
// Every snapshotInterval-th version also stores a freeze-frame (best-effort:
// a snapshot failure never fails the save; the next interval retries).
func (r *Repository) Save(ctx context.Context, agg *Artist) ([]eventsourcing.Event, error) {
	uncommitted := agg.UncommittedEvents()
	if len(uncommitted) == 0 {
		return nil, nil
	}

	expectedVersion := agg.Version() - int64(len(uncommitted))
	if err := eventsourcing.AppendWithRetry(ctx, r.store, agg.AggregateID(), expectedVersion, uncommitted...); err != nil {
		return nil, fmt.Errorf("saving artist stream %s: %w", agg.AggregateID(), err)
	}

	agg.ClearUncommittedEvents()
	r.maybeSnapshot(ctx, agg, int64(len(uncommitted)))
	return uncommitted, nil
}

// maybeSnapshot stores a freeze-frame whenever a save crosses a
// snapshotInterval boundary, even when the final version does not land
// exactly on one (multi-event saves would otherwise skip snapshots).
func (r *Repository) maybeSnapshot(ctx context.Context, agg *Artist, committed int64) {
	if committed <= 0 || agg.Version() <= 0 {
		return
	}
	if agg.Version()/snapshotInterval == (agg.Version()-committed)/snapshotInterval {
		return
	}
	snapStore, ok := r.store.(eventsourcing.SnapshotStore)
	if !ok {
		return
	}
	payload, err := agg.marshalSnapshot()
	if err != nil {
		slog.Warn("artist snapshot marshal failed", "artist_id", agg.AggregateID(), "error", err)
		return
	}
	if err := snapStore.SaveSnapshot(ctx, eventsourcing.Snapshot{
		StreamID:   agg.AggregateID(),
		StreamType: StreamTypeArtist,
		Version:    agg.Version(),
		Payload:    payload,
		CreatedAt:  time.Now().UTC(),
	}); err != nil {
		slog.Warn("artist snapshot save failed", "artist_id", agg.AggregateID(), "error", err)
	}
}
