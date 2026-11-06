package eventsourcing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"
)

// SnapshotCodec configures periodic freeze-frame snapshotting for a
// Repository[T].
type SnapshotCodec[T Aggregate] struct {
	Interval int64
	NewEmpty func(id string) T
	Marshal  func(agg T) ([]byte, error)
	Restore  func(agg T, version int64, payload []byte) error
}

// RepositoryOption configures optional behavior on a generic Repository[T].
type RepositoryOption[T Aggregate] func(*Repository[T])

// WithSnapshotCodec enables snapshot-accelerated loads and interval snapshot
// writes on a Repository[T].
func WithSnapshotCodec[T Aggregate](codec SnapshotCodec[T]) RepositoryOption[T] {
	return func(r *Repository[T]) {
		r.snapshot = &codec
	}
}

// Repository is a generic event-sourced aggregate repository supporting Load,
// Save, SaveBatch, Decide (Rita-style DecideAndEvolve), and optional snapshots.
type Repository[T Aggregate] struct {
	store      Store
	streamType string
	replay     func(id string, events []Event) (T, error)
	snapshot   *SnapshotCodec[T]
}

// NewRepository constructs a generic Repository[T] for a domain aggregate type.
func NewRepository[T Aggregate](store Store, streamType string, replay func(id string, events []Event) (T, error), opts ...RepositoryOption[T]) *Repository[T] {
	r := &Repository[T]{
		store:      store,
		streamType: streamType,
		replay:     replay,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
}

// Store returns the underlying event store.
func (r *Repository[T]) Store() Store {
	return r.store
}

// Load reconstructs an aggregate from its event stream, using snapshots when
// both the repository and the underlying store support them.
func (r *Repository[T]) Load(ctx context.Context, id string) (T, error) {
	var zero T
	missingSnapshot := false
	if r.snapshot != nil && r.snapshot.Interval > 0 {
		if snapStore, ok := r.store.(SnapshotStore); ok {
			agg, err := r.loadFromSnapshot(ctx, snapStore, id)
			if err == nil {
				return agg, nil
			}
			if errors.Is(err, ErrSnapshotNotFound) {
				missingSnapshot = true
			} else {
				slog.Warn(r.streamType+" snapshot path failed, falling back to full replay",
					r.streamType+"_id", id, "error", err)
			}
		}
	}

	events, err := r.store.Load(ctx, id)
	if err != nil {
		return zero, fmt.Errorf("loading %s stream %s: %w", r.streamType, id, err)
	}
	if len(events) == 0 {
		return zero, ErrStreamNotFound
	}

	agg, err := r.replay(id, events)
	if err != nil {
		return zero, err
	}
	recordAggregateSeq(agg, events)

	if missingSnapshot && r.snapshot != nil && r.snapshot.Interval > 0 && agg.Version() >= r.snapshot.Interval {
		r.saveSnapshot(ctx, agg)
	}
	return agg, nil
}

func (r *Repository[T]) loadFromSnapshot(ctx context.Context, snapStore SnapshotStore, id string) (T, error) {
	var zero T
	snap, err := snapStore.LoadSnapshot(ctx, id)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return zero, err
		}
		return zero, fmt.Errorf("loading %s snapshot %s: %w", r.streamType, id, err)
	}
	if snap.StreamType != "" && snap.StreamType != r.streamType {
		return zero, fmt.Errorf("snapshot for %s has type %q, want %q", id, snap.StreamType, r.streamType)
	}

	agg := r.snapshot.NewEmpty(id)
	if err := r.snapshot.Restore(agg, snap.Version, snap.Payload); err != nil {
		return zero, err
	}
	if seqAgg, ok := any(agg).(SequencedAggregate); ok && snap.GlobalPosition > 0 {
		seqAgg.SetLastSeq(uint64(snap.GlobalPosition))
	}

	tail, err := snapStore.LoadAfter(ctx, id, snap.Version)
	if err != nil {
		return zero, fmt.Errorf("loading %s tail %s after v%d: %w", r.streamType, id, snap.Version, err)
	}
	for _, evt := range tail {
		if err := agg.Apply(evt); err != nil {
			return zero, fmt.Errorf("applying %s tail %s v%d: %w", r.streamType, id, evt.Version, err)
		}
	}
	recordAggregateSeq(agg, tail)
	return agg, nil
}

// Save commits any uncommitted events on the aggregate to the event store.
func (r *Repository[T]) Save(ctx context.Context, agg T) ([]Event, error) {
	uncommitted := agg.UncommittedEvents()
	if len(uncommitted) == 0 {
		return nil, nil
	}

	expectedVersion := agg.Version() - int64(len(uncommitted))
	var expectedSeq uint64
	if seqAgg, ok := any(agg).(SequencedAggregate); ok {
		expectedSeq = seqAgg.LastSeq()
	}
	if err := appendWithRetrySequenced(ctx, r.store, agg.AggregateID(), expectedVersion, expectedSeq, uncommitted...); err != nil {
		return nil, fmt.Errorf("saving %s stream %s: %w", r.streamType, agg.AggregateID(), err)
	}
	if seqAgg, ok := any(agg).(SequencedAggregate); ok {
		// Clear any stale sequence until next Load unless updated by store.
		seqAgg.SetLastSeq(0)
	}

	agg.ClearUncommittedEvents()
	r.maybeSnapshot(ctx, agg, int64(len(uncommitted)))
	return uncommitted, nil
}

// SaveBatch commits uncommitted events across multiple aggregates in a single
// atomic batch when supported by the store, falling back to concurrent
// per-aggregate Save on conflict.
func (r *Repository[T]) SaveBatch(ctx context.Context, aggs []T) ([]Event, error) {
	dirty := make([]T, 0, len(aggs))
	for _, agg := range aggs {
		if !isNilAggregate(agg) && len(agg.UncommittedEvents()) > 0 {
			dirty = append(dirty, agg)
		}
	}
	if len(dirty) == 0 {
		return nil, nil
	}

	if batcher, ok := r.store.(BatchAppender); ok {
		batches := make([]StreamAppend, 0, len(dirty))
		for _, agg := range dirty {
			uncommitted := agg.UncommittedEvents()
			var expectedSeq uint64
			if seqAgg, ok := any(agg).(SequencedAggregate); ok {
				expectedSeq = seqAgg.LastSeq()
			}
			batches = append(batches, StreamAppend{
				StreamID:        agg.AggregateID(),
				ExpectedVersion: agg.Version() - int64(len(uncommitted)),
				ExpectedLastSeq: expectedSeq,
				Events:          uncommitted,
			})
		}
		if err := batcher.AppendBatch(ctx, batches); err == nil {
			saved := make([]Event, 0, len(batches))
			for _, agg := range dirty {
				uncommitted := agg.UncommittedEvents()
				saved = append(saved, uncommitted...)
				committed := int64(len(uncommitted))
				if seqAgg, ok := any(agg).(SequencedAggregate); ok {
					seqAgg.SetLastSeq(0)
				}
				agg.ClearUncommittedEvents()
				r.maybeSnapshot(ctx, agg, committed)
			}
			return saved, nil
		} else {
			slog.Warn("batch append failed, falling back to per-stream saves", "stream_type", r.streamType, "error", err)
		}
	}

	var (
		wg       sync.WaitGroup
		sem      = make(chan struct{}, 16)
		mu       sync.Mutex
		saved    []Event
		firstErr error
	)
	for _, agg := range dirty {
		if err := ctx.Err(); err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = fmt.Errorf("saving %s batch: %w", r.streamType, err)
			}
			mu.Unlock()
			break
		}
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			events, err := r.Save(ctx, agg)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			saved = append(saved, events...)
		})
	}
	wg.Wait()
	return saved, firstErr
}

// Decide loads the aggregate for id, executes fn to record new domain events,
// and commits them atomically (Rita-style DecideAndEvolve orchestration).
func (r *Repository[T]) Decide(ctx context.Context, id string, fn func(agg T) error) (T, []Event, error) {
	var zero T
	agg, err := r.Load(ctx, id)
	if err != nil {
		return zero, nil, err
	}
	if err := fn(agg); err != nil {
		return zero, nil, err
	}
	events, err := r.Save(ctx, agg)
	if err != nil {
		return zero, nil, err
	}
	return agg, events, nil
}

func (r *Repository[T]) maybeSnapshot(ctx context.Context, agg T, committed int64) {
	if r.snapshot == nil || r.snapshot.Interval <= 0 || committed <= 0 || agg.Version() <= 0 {
		return
	}
	if agg.Version()/r.snapshot.Interval == (agg.Version()-committed)/r.snapshot.Interval {
		return
	}
	r.saveSnapshot(ctx, agg)
}

func (r *Repository[T]) saveSnapshot(ctx context.Context, agg T) {
	if r.snapshot == nil {
		return
	}
	snapStore, ok := r.store.(SnapshotStore)
	if !ok {
		return
	}
	payload, err := r.snapshot.Marshal(agg)
	if err != nil {
		slog.Warn(r.streamType+" snapshot marshal failed", r.streamType+"_id", agg.AggregateID(), "error", err)
		return
	}
	var createdAt time.Time
	if nowProvider, ok := any(agg).(interface{ Now() time.Time }); ok {
		createdAt = nowProvider.Now()
	}
	if err := snapStore.SaveSnapshot(ctx, Snapshot{
		StreamID:   agg.AggregateID(),
		StreamType: r.streamType,
		Version:    agg.Version(),
		Payload:    payload,
		CreatedAt:  createdAt,
	}); err != nil {
		slog.Warn(r.streamType+" snapshot save failed", r.streamType+"_id", agg.AggregateID(), "error", err)
	}
}

func recordAggregateSeq(agg Aggregate, events []Event) {
	if len(events) == 0 {
		return
	}
	seqAgg, ok := any(agg).(SequencedAggregate)
	if !ok {
		return
	}
	if last := events[len(events)-1]; last.GlobalPosition > 0 {
		seqAgg.SetLastSeq(uint64(last.GlobalPosition))
	}
}

func isNilAggregate(agg Aggregate) bool {
	if agg == nil {
		return true
	}
	v := reflect.ValueOf(agg)
	return v.Kind() == reflect.Pointer && v.IsNil()
}
