package artist_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/eventsourcing"
)

// TestReplayRejectsUnknownEventType pins the strict fold contract: a fact
// this binary cannot fold must fail Replay instead of silently vanishing
// from the row. cmd/audit's census (undecodable_events) is the deployment
// gate; retiring an event type requires explicit no-op cases in the folds.
func TestReplayRejectsUnknownEventType(t *testing.T) {
	unknown, err := eventsourcing.NewEvent("ar_unknown", artist.StreamTypeArtist, 1, "ArtistNotAnEvent", map[string]any{}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if _, err := artist.Replay("ar_unknown", []eventsourcing.Event{unknown}); err == nil {
		t.Fatal("Replay(unknown event type) = nil, want an error")
	}
}

func TestArtist_Lifecycle(t *testing.T) {
	artistID := "artist_123"

	// 1. Create Artist (v1)
	agg, err := artist.NewArtist(artistID, "Tool", "2yEwvVSSOPueGk6Mtu2FeT", "rock_metal", "included")
	if err != nil {
		t.Fatalf("unexpected error creating artist: %v", err)
	}

	if agg.Version() != 1 {
		t.Fatalf("expected version 1, got %d", agg.Version())
	}
	if agg.Name != "Tool" || agg.MonthlyListeners != 0 {
		t.Fatalf("unexpected state after creation: %+v", agg)
	}

	// 2. Scrape Monthly Listeners (v2)
	if err := agg.RecordMonthlyListeners(4200000, "local_headless", 450, eventsourcing.Correlation{RequestID: "req-1"}); err != nil {
		t.Fatalf("failed recording listeners: %v", err)
	}
	if agg.Version() != 2 {
		t.Fatalf("expected version 2, got %d", agg.Version())
	}
	if agg.MonthlyListeners != 4200000 {
		t.Fatalf("expected 4200000 listeners, got %d", agg.MonthlyListeners)
	}
	if !agg.HasRecordedListenersForRequest("req-1") {
		t.Fatal("expected HasRecordedListenersForRequest(req-1) == true")
	}
	if agg.HasRecordedListenersForRequest("req-2") {
		t.Fatal("expected HasRecordedListenersForRequest(req-2) == false")
	}

	// 3. Second Scrape with delta (v3)
	if err := agg.RecordMonthlyListeners(4350000, "scrapingant", 320, eventsourcing.Correlation{RequestID: "req-2"}); err != nil {
		t.Fatalf("failed recording second scrape: %v", err)
	}
	if agg.Version() != 3 {
		t.Fatalf("expected version 3, got %d", agg.Version())
	}
	if agg.MonthlyListeners != 4350000 {
		t.Fatalf("expected 4350000 listeners, got %d", agg.MonthlyListeners)
	}
	if !agg.HasRecordedListenersForRequest("req-2") {
		t.Fatal("expected HasRecordedListenersForRequest(req-2) == true")
	}

	// 4. Change Status (v4)
	if err := agg.ChangeListStatus("recently_added", "promoted by review"); err != nil {
		t.Fatalf("failed changing status: %v", err)
	}
	if agg.Version() != 4 {
		t.Fatalf("expected version 4, got %d", agg.Version())
	}
	if agg.ListStatus != "recently_added" {
		t.Fatalf("expected list_status recently_added, got %s", agg.ListStatus)
	}

	// 5. Test Replay from events
	events := agg.UncommittedEvents()
	if len(events) != 4 {
		t.Fatalf("expected 4 uncommitted events, got %d", len(events))
	}

	replayed, err := artist.Replay(artistID, events)
	if err != nil {
		t.Fatalf("failed replaying aggregate: %v", err)
	}

	if replayed.Version() != 4 {
		t.Fatalf("replayed version mismatch: %d", replayed.Version())
	}
	if replayed.MonthlyListeners != 4350000 || replayed.ListStatus != "recently_added" {
		t.Fatalf("replayed state mismatch: %+v", replayed)
	}
	if !replayed.HasRecordedListenersForRequest("req-2") {
		t.Fatal("expected replayed HasRecordedListenersForRequest(req-2) == true")
	}
}

func TestRepository_SaveBatch(t *testing.T) {
	ctx := context.Background()
	sqlDB, err := db.SetupDB(ctx, slog.Default(), t.TempDir(), false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = sqlDB.Close() }()

	store := eventsourcing.NewSQLiteStore(sqlDB)
	repo := artist.NewRepository(store)

	a1, err := artist.NewArtist("art-1", "Artist One", "sp-1", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist a1: %v", err)
	}
	if err := a1.SetFetchStatus("pending", "req-a1"); err != nil {
		t.Fatalf("SetFetchStatus a1: %v", err)
	}

	a2, err := artist.NewArtist("art-2", "Artist Two", "sp-2", "pop_other", "included")
	if err != nil {
		t.Fatalf("NewArtist a2: %v", err)
	}
	if err := a2.SetFetchStatus("pending", "req-a2"); err != nil {
		t.Fatalf("SetFetchStatus a2: %v", err)
	}

	saved, err := repo.SaveBatch(ctx, []*artist.Artist{a1, a2})
	if err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}
	if len(saved) != 4 {
		t.Fatalf("expected 4 saved events (2 per artist), got %d", len(saved))
	}
	if len(a1.UncommittedEvents()) != 0 || len(a2.UncommittedEvents()) != 0 {
		t.Fatal("expected UncommittedEvents to be cleared after SaveBatch")
	}

	loaded1, err := repo.Load(ctx, "art-1")
	if err != nil {
		t.Fatalf("Load art-1: %v", err)
	}
	if loaded1.FetchStatus != "pending" || loaded1.Version() != 2 {
		t.Fatalf("loaded1 = %+v (v=%d), want pending v2", loaded1, loaded1.Version())
	}
}

func TestRepository_LoadHealsMissingSnapshotOnLongStream(t *testing.T) {
	ctx := context.Background()
	sqlDB, err := db.SetupDB(ctx, slog.Default(), t.TempDir(), false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = sqlDB.Close() }()

	store := eventsourcing.NewSQLiteStore(sqlDB)
	repo := artist.NewRepository(store)

	// Build a stream past the snapshot interval behind the repository's back,
	// as if it grew before snapshots existed for this store.
	agg, err := artist.NewArtist("art-long", "Long Stream", "sp-long", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	for i := range artist.SnapshotInterval + 10 {
		if err := agg.RecordMonthlyListeners(int64(1000+i), "local_headless", 10, eventsourcing.Correlation{}); err != nil {
			t.Fatalf("RecordMonthlyListeners %d: %v", i, err)
		}
	}
	version := agg.Version()
	if err := store.Append(ctx, "art-long", 0, agg.UncommittedEvents()...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := store.LoadSnapshot(ctx, "art-long"); !errors.Is(err, eventsourcing.ErrSnapshotNotFound) {
		t.Fatalf("precondition: LoadSnapshot = %v, want ErrSnapshotNotFound", err)
	}

	loaded, err := repo.Load(ctx, "art-long")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Version() != version {
		t.Fatalf("loaded version = %d, want %d", loaded.Version(), version)
	}
	snap, err := store.LoadSnapshot(ctx, "art-long")
	if err != nil {
		t.Fatalf("Load should have healed the snapshot: %v", err)
	}
	if snap.Version != version {
		t.Fatalf("healed snapshot version = %d, want %d", snap.Version, version)
	}

	// The healed snapshot must produce an identical aggregate.
	again, err := repo.Load(ctx, "art-long")
	if err != nil {
		t.Fatalf("Load from snapshot: %v", err)
	}
	if again.Version() != version || again.MonthlyListeners != loaded.MonthlyListeners {
		t.Fatalf("snapshot load = v%d/%d, want v%d/%d", again.Version(), again.MonthlyListeners, version, loaded.MonthlyListeners)
	}
}

// TestArtistInjectedClockPinsPayloadTime proves domain constructors forward
// aggregate options and that domain payload timestamps read the injected
// clock: both the envelope and the ScrapedAt payload stay deterministic.
func TestArtistInjectedClockPinsPayloadTime(t *testing.T) {
	fixed := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	clock := eventsourcing.NewFakeClock(fixed)

	agg, err := artist.NewArtist("artist-clock", "Tool", "2yEwvVSSOPueGk6Mtu2FeT", "rock_metal", "included",
		eventsourcing.WithClock(clock), eventsourcing.WithIDGen(&eventsourcing.SeqIDGen{}))
	if err != nil {
		t.Fatalf("NewArtist() error = %v", err)
	}

	created := agg.UncommittedEvents()[0]
	if created.ID != "evt_1" || !created.CreatedAt.Equal(fixed) {
		t.Fatalf("created event = %s at %s, want evt_1 at %s", created.ID, created.CreatedAt, fixed)
	}

	clock.Advance(30 * time.Minute)
	if err := agg.RecordMonthlyListeners(4200000, "local_headless", 450); err != nil {
		t.Fatalf("RecordMonthlyListeners() error = %v", err)
	}

	scraped := agg.UncommittedEvents()[1]
	if !scraped.CreatedAt.Equal(fixed.Add(30 * time.Minute)) {
		t.Errorf("scrape envelope CreatedAt = %s, want %s", scraped.CreatedAt, fixed.Add(30*time.Minute))
	}
	var payload artist.ListenersScrapedPayload
	if err := eventsourcing.DecodeEventPayload(scraped, &payload); err != nil {
		t.Fatalf("decoding payload: %v", err)
	}
	if !payload.ScrapedAt.Equal(fixed.Add(30 * time.Minute)) {
		t.Errorf("payload ScrapedAt = %s, want %s", payload.ScrapedAt, fixed.Add(30*time.Minute))
	}
}
