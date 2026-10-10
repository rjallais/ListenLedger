package artist_test

import (
	"testing"

	"ListenLedger/internal/domain/artist"
)

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
	if err := agg.RecordMonthlyListeners(4200000, "local_headless", 450); err != nil {
		t.Fatalf("failed recording listeners: %v", err)
	}
	if agg.Version() != 2 {
		t.Fatalf("expected version 2, got %d", agg.Version())
	}
	if agg.MonthlyListeners != 4200000 {
		t.Fatalf("expected 4200000 listeners, got %d", agg.MonthlyListeners)
	}

	// 3. Second Scrape with delta (v3)
	if err := agg.RecordMonthlyListeners(4350000, "scrapingant", 320); err != nil {
		t.Fatalf("failed recording second scrape: %v", err)
	}
	if agg.Version() != 3 {
		t.Fatalf("expected version 3, got %d", agg.Version())
	}
	if agg.MonthlyListeners != 4350000 {
		t.Fatalf("expected 4350000 listeners, got %d", agg.MonthlyListeners)
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
}
