package artist

import (
	"fmt"
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

func TestArtist_RecordedListenerRequestIDsBounded(t *testing.T) {
	const artistID = "artist_dedup_bound"
	agg, err := NewArtist(artistID, "Bounded Dedup", "sp-dedup-bound", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	for i := 1; i <= 70; i++ {
		requestID := fmt.Sprintf("req-%02d", i)
		if err := agg.RecordMonthlyListeners(int64(i), "local_headless", 10, eventsourcing.Correlation{RequestID: requestID}); err != nil {
			t.Fatalf("RecordMonthlyListeners(%s): %v", requestID, err)
		}
	}
	if got := len(agg.recordedListenerRequestIDs); got != recordedListenerRequestIDsLimit {
		t.Fatalf("recorded request IDs = %d, want %d", got, recordedListenerRequestIDsLimit)
	}
	for i := 1; i <= 6; i++ {
		if agg.HasRecordedListenersForRequest(fmt.Sprintf("req-%02d", i)) {
			t.Fatalf("expected trimmed request req-%02d to be unrecognized", i)
		}
	}
	for i := 7; i <= 70; i++ {
		if !agg.HasRecordedListenersForRequest(fmt.Sprintf("req-%02d", i)) {
			t.Fatalf("expected recent request req-%02d to be recognized", i)
		}
	}

	// A duplicate request ID must not consume a slot or change insertion order.
	if err := agg.RecordMonthlyListeners(999, "local_headless", 10, eventsourcing.Correlation{RequestID: "req-70"}); err != nil {
		t.Fatalf("RecordMonthlyListeners(duplicate): %v", err)
	}
	if got := len(agg.recordedListenerRequestIDs); got != recordedListenerRequestIDsLimit {
		t.Fatalf("recorded request IDs after duplicate = %d, want %d", got, recordedListenerRequestIDsLimit)
	}
	if got := agg.recordedListenerRequestIDs[len(agg.recordedListenerRequestIDs)-1]; got != "req-70" {
		t.Fatalf("last recorded request ID = %q, want req-70", got)
	}

	payload, err := agg.marshalSnapshot()
	if err != nil {
		t.Fatalf("marshalSnapshot: %v", err)
	}
	restored := &Artist{BaseAggregate: eventsourcing.NewBaseAggregate(artistID, StreamTypeArtist)}
	if err := restored.restoreFromSnapshot(agg.Version(), payload); err != nil {
		t.Fatalf("restoreFromSnapshot: %v", err)
	}
	if got := len(restored.recordedListenerRequestIDs); got != recordedListenerRequestIDsLimit {
		t.Fatalf("restored request IDs = %d, want %d", got, recordedListenerRequestIDsLimit)
	}
	if restored.HasRecordedListenersForRequest("req-06") {
		t.Fatal("expected trimmed request req-06 to stay unrecognized after restore")
	}
	if !restored.HasRecordedListenersForRequest("req-07") {
		t.Fatal("expected oldest retained request req-07 after restore")
	}
	if !restored.HasRecordedListenersForRequest("req-70") {
		t.Fatal("expected newest request req-70 after restore")
	}
}
