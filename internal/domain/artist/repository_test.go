package artist

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"ListenLedger/internal/eventsourcing"
)

// fakeSnapshotStore is a hermetic SnapshotStore for repository tests.
type fakeSnapshotStore struct {
	mu         sync.Mutex
	events     map[string][]eventsourcing.Event
	snaps      map[string]eventsourcing.Snapshot
	loads      int
	loadAfters int
}

func newFakeSnapshotStore() *fakeSnapshotStore {
	return &fakeSnapshotStore{
		events: make(map[string][]eventsourcing.Event),
		snaps:  make(map[string]eventsourcing.Snapshot),
	}
}

func (f *fakeSnapshotStore) Append(_ context.Context, streamID string, expectedVersion int64, events ...eventsourcing.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if int64(len(f.events[streamID])) != expectedVersion {
		return fmt.Errorf("%w: stream %s expected %d, found %d",
			eventsourcing.ErrConcurrencyConflict, streamID, expectedVersion, len(f.events[streamID]))
	}
	f.events[streamID] = append(f.events[streamID], events...)
	return nil
}

func (f *fakeSnapshotStore) Load(_ context.Context, streamID string) ([]eventsourcing.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	return append([]eventsourcing.Event(nil), f.events[streamID]...), nil
}

func (f *fakeSnapshotStore) LoadSnapshot(_ context.Context, streamID string) (eventsourcing.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	snap, ok := f.snaps[streamID]
	if !ok {
		return eventsourcing.Snapshot{}, fmt.Errorf("stream %s: %w", streamID, eventsourcing.ErrSnapshotNotFound)
	}
	return snap, nil
}

func (f *fakeSnapshotStore) SaveSnapshot(_ context.Context, snap eventsourcing.Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snaps[snap.StreamID] = snap
	return nil
}

func (f *fakeSnapshotStore) LoadAfter(_ context.Context, streamID string, afterVersion int64) ([]eventsourcing.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadAfters++
	var tail []eventsourcing.Event
	for _, evt := range f.events[streamID] {
		if evt.Version > afterVersion {
			tail = append(tail, evt)
		}
	}
	return tail, nil
}

func TestRepository_LoadWithoutSnapshotFallsBackToFullReplay(t *testing.T) {
	ctx := t.Context()
	store := newFakeSnapshotStore()
	repo := NewRepository(store)

	agg, err := NewArtist("ar_snap_1", "Tool", "sp1", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if err := agg.RecordMonthlyListeners(1000, "test", 1); err != nil {
		t.Fatalf("RecordMonthlyListeners: %v", err)
	}
	if _, err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := repo.Load(ctx, "ar_snap_1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.MonthlyListeners != 1000 || loaded.Version() != 2 {
		t.Fatalf("loaded = listeners %d v%d, want 1000 v2", loaded.MonthlyListeners, loaded.Version())
	}
	if store.loads != 1 {
		t.Fatalf("full Load calls = %d, want 1", store.loads)
	}
}

func TestRepository_LoadFromSnapshotSkipsHistory(t *testing.T) {
	ctx := t.Context()
	store := newFakeSnapshotStore()
	repo := NewRepository(store)

	agg, err := NewArtist("ar_snap_2", "Nirvana", "sp2", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if err := agg.RecordMonthlyListeners(100, "test", 1); err != nil {
		t.Fatalf("RecordMonthlyListeners: %v", err)
	}
	if _, err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Freeze v2, then append v3 (a later scrape).
	mid, err := repo.Load(ctx, "ar_snap_2")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	payload, err := mid.marshalSnapshot()
	if err != nil {
		t.Fatalf("marshalSnapshot: %v", err)
	}
	if err := store.SaveSnapshot(ctx, eventsourcing.Snapshot{
		StreamID:   "ar_snap_2",
		StreamType: StreamTypeArtist,
		Version:    mid.Version(),
		Payload:    payload,
		CreatedAt:  time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	live, err := repo.Load(ctx, "ar_snap_2")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := live.RecordMonthlyListeners(200, "test", 1); err != nil {
		t.Fatalf("RecordMonthlyListeners: %v", err)
	}
	if _, err := repo.Save(ctx, live); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loadsBefore := store.loads
	final, err := repo.Load(ctx, "ar_snap_2")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if final.MonthlyListeners != 200 || final.Version() != 3 {
		t.Fatalf("final = listeners %d v%d, want 200 v3", final.MonthlyListeners, final.Version())
	}
	if store.loadAfters == 0 {
		t.Fatal("snapshot path should call LoadAfter")
	}
	if store.loads != loadsBefore {
		t.Fatalf("snapshot path must not call full Load (loads %d -> %d)", loadsBefore, store.loads)
	}
}

func TestRepository_MissingStream(t *testing.T) {
	ctx := t.Context()
	repo := NewRepository(newFakeSnapshotStore())
	if _, err := repo.Load(ctx, "ghost"); err == nil {
		t.Fatal("Load(ghost) should error")
	}
}

func TestRepository_SaveSnapshotsAtInterval(t *testing.T) {
	ctx := t.Context()
	store := newFakeSnapshotStore()
	repo := NewRepository(store)

	agg, err := NewArtist("ar_snap_n", "Meshuggah", "sp3", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if _, err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	for agg.Version() < snapshotInterval {
		if err := agg.RecordMonthlyListeners(int64(1000+agg.Version()), "test", 1); err != nil {
			t.Fatalf("RecordMonthlyListeners: %v", err)
		}
		if _, err := repo.Save(ctx, agg); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	snap, err := store.LoadSnapshot(ctx, "ar_snap_n")
	if err != nil {
		t.Fatalf("snapshot should exist at v%d: %v", snapshotInterval, err)
	}
	if snap.Version != snapshotInterval {
		t.Fatalf("snapshot version = %d, want %d", snap.Version, snapshotInterval)
	}

	loaded, err := repo.Load(ctx, "ar_snap_n")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Version() != snapshotInterval || loaded.MonthlyListeners != int64(1000+snapshotInterval-1) {
		t.Fatalf("loaded v%d listeners %d, want v%d", loaded.Version(), loaded.MonthlyListeners, snapshotInterval)
	}
}

func TestRepository_SaveSnapshotsOnBoundaryCrossing(t *testing.T) {
	ctx := t.Context()
	store := newFakeSnapshotStore()
	repo := NewRepository(store)

	// Drive to just before the boundary with single-event saves (no snapshot).
	agg, err := NewArtist("ar_snap_x", "Bathory", "sp4", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if _, err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	for agg.Version() < snapshotInterval-1 {
		if err := agg.RecordMonthlyListeners(int64(agg.Version()), "test", 1); err != nil {
			t.Fatalf("RecordMonthlyListeners: %v", err)
		}
		if _, err := repo.Save(ctx, agg); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	if _, err := store.LoadSnapshot(ctx, "ar_snap_x"); err == nil {
		t.Fatal("no snapshot should exist before the boundary")
	}

	// One multi-event save crossing the boundary must still snapshot.
	if err := agg.RecordMonthlyListeners(9000, "test", 1); err != nil {
		t.Fatalf("RecordMonthlyListeners: %v", err)
	}
	if err := agg.RecordMonthlyListeners(9001, "test", 1); err != nil {
		t.Fatalf("RecordMonthlyListeners: %v", err)
	}
	if _, err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	snap, err := store.LoadSnapshot(ctx, "ar_snap_x")
	if err != nil {
		t.Fatalf("boundary-crossing save should snapshot: %v", err)
	}
	if snap.Version != agg.Version() {
		t.Fatalf("snapshot version = %d, want %d", snap.Version, agg.Version())
	}
}
