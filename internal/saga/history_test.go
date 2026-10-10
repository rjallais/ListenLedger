package saga

import (
	"context"
	"testing"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"

	"ListenLedger/internal/commands"
	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/eventsourcing"
)

func setupTestDB(t *testing.T) *toolbeltdb.Database {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dataDir := t.TempDir()
	sqliteDB, err := db.SetupDB(ctx, nil, dataDir, false)
	if err != nil {
		// SetupDB needs a logger; retry with default via test helper path.
		t.Fatalf("SetupDB() error = %v", err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })
	return sqliteDB
}

func TestCorrelationMetadataRoundTrip(t *testing.T) {
	agg, err := artist.NewArtist("ar_1", "Name", "sp_1", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist() error = %v", err)
	}
	// Drain the creation event so only the correlated fact remains uncommitted.
	agg.ClearUncommittedEvents()
	if err := agg.SetFetchStatus("pending", "queue refresh",
		eventsourcing.Correlation{RequestID: "req_1"}); err != nil {
		t.Fatalf("SetFetchStatus() error = %v", err)
	}
	uncommitted := agg.UncommittedEvents()
	if len(uncommitted) != 1 {
		t.Fatalf("uncommitted events = %d, want 1", len(uncommitted))
	}
	meta, err := eventsourcing.DecodeMetadata(uncommitted[0].Metadata)
	if err != nil {
		t.Fatalf("DecodeMetadata() error = %v", err)
	}
	if got := eventsourcing.RequestIDFromMetadata(meta); got != "req_1" {
		t.Fatalf("request_id = %q, want req_1", got)
	}
}

func TestFirstCorrelationEmpty(t *testing.T) {
	corr := eventsourcing.FirstCorrelation(nil)
	if got := corr.Metadata(nil); got != nil {
		t.Fatalf("empty correlation metadata = %v, want nil", got)
	}
	// Legacy callers without correlation keep nil metadata (no behavior change).
	agg, err := artist.NewArtist("ar_2", "Name", "sp_2", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist() error = %v", err)
	}
	agg.ClearUncommittedEvents()
	if err := agg.RecordMonthlyListeners(100, "test", 5); err != nil {
		t.Fatalf("RecordMonthlyListeners() error = %v", err)
	}
	evt := agg.UncommittedEvents()[0]
	meta, err := eventsourcing.DecodeMetadata(evt.Metadata)
	if err != nil {
		t.Fatalf("DecodeMetadata() error = %v", err)
	}
	if got := eventsourcing.RequestIDFromMetadata(meta); got != "" {
		t.Fatalf("legacy request_id = %q, want empty", got)
	}
	// Provider metadata still present alongside (merge, not overwrite).
	if meta["provider"] != "test" {
		t.Fatalf("provider metadata = %v, want test", meta["provider"])
	}
}

func TestLoadByRequestLinksCommandJobArtist(t *testing.T) {
	sqliteDB := setupTestDB(t)
	ctx := context.Background()
	store := eventsourcing.NewSQLiteStore(sqliteDB)

	// Seed artist stream with a correlated pending fact.
	a, err := artist.NewArtist("ar_9", "Saga Artist", "sp_9", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist() error = %v", err)
	}
	artistBase := int64(0)
	if uncommitted := a.UncommittedEvents(); len(uncommitted) > 0 {
		if err := store.Append(ctx, a.AggregateID(), artistBase, uncommitted...); err != nil {
			t.Fatalf("Append artist created error = %v", err)
		}
		a.ClearUncommittedEvents()
		artistBase = a.Version()
	}
	if err := a.SetFetchStatus("pending", "queue refresh",
		eventsourcing.Correlation{RequestID: "req_9"}); err != nil {
		t.Fatalf("SetFetchStatus() error = %v", err)
	}
	if uncommitted := a.UncommittedEvents(); len(uncommitted) > 0 {
		if err := store.Append(ctx, a.AggregateID(), artistBase, uncommitted...); err != nil {
			t.Fatalf("Append artist pending error = %v", err)
		}
		a.ClearUncommittedEvents()
	}

	// Seed job stream.
	j, err := scrapejob.NewScrapeJob("req_9", "ar_9", commands.TypeRefresh)
	if err != nil {
		t.Fatalf("NewScrapeJob() error = %v", err)
	}
	if uncommitted := j.UncommittedEvents(); len(uncommitted) > 0 {
		if err := store.Append(ctx, "req_9", 0, uncommitted...); err != nil {
			t.Fatalf("Append job requested error = %v", err)
		}
		// Clear via replay state: NewScrapeJob leaves version 1 uncommitted;
		// reload to get a clean aggregate at version 1.
	}
	evts, err := store.Load(ctx, "req_9")
	if err != nil {
		t.Fatalf("Load job error = %v", err)
	}
	j2, err := scrapejob.Replay("req_9", evts)
	if err != nil {
		t.Fatalf("Replay job error = %v", err)
	}
	if _, _, err := j2.RecordStarted(eventsourcing.Correlation{RequestID: "req_9"}); err != nil {
		t.Fatalf("RecordStarted() error = %v", err)
	}
	if uncommitted := j2.UncommittedEvents(); len(uncommitted) > 0 {
		if err := store.Append(ctx, "req_9", j2.Version()-int64(len(uncommitted)), uncommitted...); err != nil {
			t.Fatalf("Append job started error = %v", err)
		}
	}

	if err := commands.Log(ctx, sqliteDB, commands.Command{
		RequestID: "req_9", Type: commands.TypeRefresh,
		ArtistID: "ar_9", SpotifyID: "sp_9", ArtistName: "Saga Artist",
	}); err != nil {
		t.Fatalf("commands.Log() error = %v", err)
	}

	loader := NewLoader(sqliteDB, store)
	h, err := loader.LoadByRequest(ctx, "req_9")
	if err != nil {
		t.Fatalf("LoadByRequest() error = %v", err)
	}
	if h.ArtistID != "ar_9" {
		t.Fatalf("ArtistID = %q, want ar_9", h.ArtistID)
	}
	if h.Command == nil || h.Command.RequestID != "req_9" {
		t.Fatalf("Command = %+v, want req_9", h.Command)
	}
	if len(h.JobEvents) != 2 {
		t.Fatalf("JobEvents = %d, want 2 (Requested+Started)", len(h.JobEvents))
	}
	if len(h.ArtistEvents) != 1 {
		t.Fatalf("ArtistEvents = %d, want 1 (correlated pending)", len(h.ArtistEvents))
	}

	recent, err := loader.RecentRequestIDs(ctx, "ar_9", 10)
	if err != nil {
		t.Fatalf("RecentRequestIDs() error = %v", err)
	}
	if len(recent) != 1 || recent[0] != "req_9" {
		t.Fatalf("RecentRequestIDs = %v, want [req_9]", recent)
	}
}
