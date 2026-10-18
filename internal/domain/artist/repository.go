package artist

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"ListenLedger/internal/eventsourcing"
)

// SnapshotInterval bounds artist stream replay: every Nth version stores a
// freeze-frame so loaders replay at most N-1 events after it. Artist streams
// are the only long-lived aggregates (one listener/status fact per scrape);
// scrapejob/batch/album/song streams stay tiny and replay directly.
// Exported for the audit tool's long-stream warning: snapshot degradation
// matters to the event-store flip (JetStream reads replay full streams).
const SnapshotInterval = 200

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
	missingSnapshot := false
	if snapStore, ok := r.store.(eventsourcing.SnapshotStore); ok {
		agg, err := r.loadFromSnapshot(ctx, snapStore, id)
		if err == nil {
			return agg, nil
		}
		if errors.Is(err, eventsourcing.ErrSnapshotNotFound) {
			missingSnapshot = true
		} else {
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

	agg, err := Replay(id, events)
	if err != nil {
		return nil, err
	}
	// Heal: a stream already past the interval with no snapshot (it grew
	// before snapshots existed for this store, or the anchor was invalidated)
	// would replay in full on every command until its next boundary
	// crossing. The state is in hand — persist it once, best-effort.
	if missingSnapshot && agg.Version() >= SnapshotInterval {
		r.saveSnapshot(ctx, agg)
	}
	return agg, nil
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
// Every SnapshotInterval-th version also stores a freeze-frame (best-effort:
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

// SaveBatch commits uncommitted events across multiple Artist aggregates.
// When the store implements eventsourcing.BatchAppender (SQLite), all streams
// commit in a single atomic transaction; on conflict or when the store does
// not support batch appends, it falls back to concurrent per-aggregate Save.
func (r *Repository) SaveBatch(ctx context.Context, aggs []*Artist) error {
	dirty := make([]*Artist, 0, len(aggs))
	for _, agg := range aggs {
		if agg != nil && len(agg.UncommittedEvents()) > 0 {
			dirty = append(dirty, agg)
		}
	}
	if len(dirty) == 0 {
		return nil
	}

	if batcher, ok := r.store.(eventsourcing.BatchAppender); ok {
		batches := make([]eventsourcing.StreamAppend, 0, len(dirty))
		for _, agg := range dirty {
			uncommitted := agg.UncommittedEvents()
			batches = append(batches, eventsourcing.StreamAppend{
				StreamID:        agg.AggregateID(),
				ExpectedVersion: agg.Version() - int64(len(uncommitted)),
				Events:          uncommitted,
			})
		}
		if err := batcher.AppendBatch(ctx, batches); err == nil {
			for _, agg := range dirty {
				committed := int64(len(agg.UncommittedEvents()))
				agg.ClearUncommittedEvents()
				r.maybeSnapshot(ctx, agg, committed)
			}
			return nil
		} else {
			slog.Warn("batch append failed, falling back to per-stream saves", "error", err)
		}
	}

	var (
		wg       sync.WaitGroup
		sem      = make(chan struct{}, 16)
		mu       sync.Mutex
		firstErr error
	)
	for _, agg := range dirty {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			if _, err := r.Save(ctx, agg); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return firstErr
}

// maybeSnapshot stores a freeze-frame whenever a save crosses a
// snapshot-interval boundary, even when the final version does not land
// exactly on one (multi-event saves would otherwise skip snapshots).
func (r *Repository) maybeSnapshot(ctx context.Context, agg *Artist, committed int64) {
	if committed <= 0 || agg.Version() <= 0 {
		return
	}
	if agg.Version()/SnapshotInterval == (agg.Version()-committed)/SnapshotInterval {
		return
	}
	r.saveSnapshot(ctx, agg)
}

// saveSnapshot stores a freeze-frame of agg's current state, best-effort: a
// failure is logged and never fails the caller.
func (r *Repository) saveSnapshot(ctx context.Context, agg *Artist) {
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
