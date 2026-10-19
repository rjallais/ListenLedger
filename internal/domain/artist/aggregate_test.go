package artist

import (
	"testing"

	"ListenLedger/internal/eventsourcing"
)

func TestArtist_HasRecordedListenersForRequestAnyScrape(t *testing.T) {
	agg, err := NewArtist("artist_dedup", "Dedup", "sp-dedup", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if err := agg.RecordMonthlyListeners(100, "local_headless", 10, eventsourcing.Correlation{RequestID: "req-1"}); err != nil {
		t.Fatalf("RecordMonthlyListeners req-1: %v", err)
	}
	if err := agg.RecordMonthlyListeners(200, "local_headless", 10, eventsourcing.Correlation{RequestID: "req-2"}); err != nil {
		t.Fatalf("RecordMonthlyListeners req-2: %v", err)
	}
	if !agg.HasRecordedListenersForRequest("req-1") {
		t.Fatal("expected HasRecordedListenersForRequest(req-1) == true after a later req-2 scrape")
	}
	if agg.HasRecordedListenersForRequest("req-3") {
		t.Fatal("expected HasRecordedListenersForRequest(req-3) == false")
	}

	replayed, err := Replay("artist_dedup", agg.UncommittedEvents())
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !replayed.HasRecordedListenersForRequest("req-1") {
		t.Fatal("expected replayed HasRecordedListenersForRequest(req-1) == true")
	}
	if !replayed.HasRecordedListenersForRequest("req-2") {
		t.Fatal("expected replayed HasRecordedListenersForRequest(req-2) == true")
	}
}
