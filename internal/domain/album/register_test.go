package album

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
		EventTypeAlbumCreated:            CreatedPayload{},
		EventTypeAlbumStatusChanged:      StatusChangedPayload{},
		EventTypeAlbumSongCountsAdjusted: SongCountsAdjustedPayload{},
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
	agg, err := NewAlbum("al_walk", "Walk", "Artist", "waiting", 0, 0)
	if err != nil {
		t.Fatalf("NewAlbum: %v", err)
	}
	if err := agg.ChangeStatus("full"); err != nil {
		t.Fatalf("ChangeStatus: %v", err)
	}
	if err := agg.AdjustSongCounts(1, 2); err != nil {
		t.Fatalf("AdjustSongCounts: %v", err)
	}

	want := []string{
		EventTypeAlbumCreated,
		EventTypeAlbumSongCountsAdjusted,
		EventTypeAlbumStatusChanged,
	}
	if got := eventsourcing.EventTypesOf(agg.UncommittedEvents()); !slices.Equal(got, want) {
		t.Errorf("walk recorded %v, want %v", got, want)
	}
}
