package song

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
		EventTypeSongCreated:                CreatedPayload{},
		EventTypeRecentFlagChanged:          RecentFlagChangedPayload{},
		EventTypeSongMetadataEnriched:       MetadataEnrichedPayload{},
		EventTypeTidalIDLinked:              TidalIDLinkedPayload{},
		EventTypeSongMetadataCorrected:      MetadataCorrectedPayload{},
		EventTypeSongArtistCreditsCorrected: ArtistCreditsCorrectedPayload{},
		EventTypeSongMarkedDuplicate:        MarkedDuplicatePayload{},
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
	// Metadata starts empty so enrichment has something to fill.
	agg, err := NewSong("sg_walk", "Walk", "Artist", "", "", "", "sp_walk", 0, 0, 0, false)
	if err != nil {
		t.Fatalf("NewSong: %v", err)
	}
	if err := agg.SetRecent(true, 1, 1); err != nil {
		t.Fatalf("SetRecent: %v", err)
	}
	if recorded, err := agg.EnrichMetadata("Walk Album", "2020-06-06", "album", "walk"); err != nil || !recorded {
		t.Fatalf("EnrichMetadata: recorded=%v err=%v", recorded, err)
	}
	if recorded, err := agg.LinkTidalID("tidal_walk", "exact", 0.9); err != nil || !recorded {
		t.Fatalf("LinkTidalID: recorded=%v err=%v", recorded, err)
	}
	if recorded, err := agg.CorrectMetadata("Corrected Album", "2021-07-07", "single", "walk"); err != nil || !recorded {
		t.Fatalf("CorrectMetadata: recorded=%v err=%v", recorded, err)
	}
	if recorded, err := agg.CorrectArtistCredits("Other Artist", "walk"); err != nil || !recorded {
		t.Fatalf("CorrectArtistCredits: recorded=%v err=%v", recorded, err)
	}
	if recorded, err := agg.MarkDuplicate("sg_canonical_walk", "walk"); err != nil || !recorded {
		t.Fatalf("MarkDuplicate: recorded=%v err=%v", recorded, err)
	}

	want := []string{
		EventTypeRecentFlagChanged,
		EventTypeSongArtistCreditsCorrected,
		EventTypeSongCreated,
		EventTypeSongMarkedDuplicate,
		EventTypeSongMetadataCorrected,
		EventTypeSongMetadataEnriched,
		EventTypeTidalIDLinked,
	}
	if got := eventsourcing.EventTypesOf(agg.UncommittedEvents()); !slices.Equal(got, want) {
		t.Errorf("walk recorded %v, want %v", got, want)
	}
}
