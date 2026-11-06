package artist

import (
	"reflect"
	"slices"
	"testing"

	"ListenLedger/internal/eventsourcing"
)

// TestEventPayloadsRegistered keeps the payload registry in step with the
// event type constants this package declares: an event type added without a
// registered payload would make stored events unreadable.
func TestEventPayloadsRegistered(t *testing.T) {
	want := map[string]any{
		EventTypeArtistCreated:                 CreatedPayload{},
		EventTypeArtistMonthlyListenersScraped: ListenersScrapedPayload{},
		EventTypeArtistStatusChanged:           StatusChangedPayload{},
		EventTypeArtistFetchStatusChanged:      FetchStatusChangedPayload{},
		EventTypeArtistCollectionSongsAdjusted: CollectionSongsAdjustedPayload{},
	}

	for eventType, payload := range want {
		got, ok := eventsourcing.PayloadTypeOf(eventType)
		if !ok {
			t.Errorf("event type %q has no registered payload", eventType)
			continue
		}
		if wantType := reflect.PointerTo(reflect.TypeOf(payload)); got != wantType {
			t.Errorf("event type %q registered to %s, want %s", eventType, got, wantType)
		}
	}
}

// TestFoldCoversAllEventTypes walks every command once and asserts the walk
// records exactly the declared event types. A command recording an event the
// fold cannot apply fails the walk; a declared type no command records would
// leave the read model without facts to project.
func TestFoldCoversAllEventTypes(t *testing.T) {
	agg, err := NewArtist("ar_walk", "Walk", "sp_walk", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if err := agg.RecordMonthlyListeners(1000, "walk", 1); err != nil {
		t.Fatalf("RecordMonthlyListeners: %v", err)
	}
	if err := agg.ChangeListStatus("recently_added", "walk"); err != nil {
		t.Fatalf("ChangeListStatus: %v", err)
	}
	if err := agg.SetFetchStatus("pending", ""); err != nil {
		t.Fatalf("SetFetchStatus: %v", err)
	}
	if err := agg.AdjustCollectionSongs(1, 2); err != nil {
		t.Fatalf("AdjustCollectionSongs: %v", err)
	}

	want := []string{
		EventTypeArtistCollectionSongsAdjusted,
		EventTypeArtistCreated,
		EventTypeArtistFetchStatusChanged,
		EventTypeArtistMonthlyListenersScraped,
		EventTypeArtistStatusChanged,
	}
	if got := eventsourcing.EventTypesOf(agg.UncommittedEvents()); !slices.Equal(got, want) {
		t.Errorf("walk recorded %v, want %v", got, want)
	}
}
