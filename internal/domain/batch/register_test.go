package batch

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
		EventTypeBatchStarted:         StartedPayload{},
		EventTypeBatchArtistCompleted: ArtistCompletedPayload{},
		EventTypeBatchCompleted:       CompletedPayload{},
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
	agg, err := NewBatch("bt_walk", []string{"ar_walk"}, map[string]int{"waiting": 1})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if _, recorded, err := agg.RecordCompletion("ar_walk"); err != nil || !recorded {
		t.Fatalf("RecordCompletion: recorded=%v err=%v", recorded, err)
	}
	if _, recorded, err := agg.RecordClosed(); err != nil || !recorded {
		t.Fatalf("RecordClosed: recorded=%v err=%v", recorded, err)
	}

	want := []string{
		EventTypeBatchArtistCompleted,
		EventTypeBatchCompleted,
		EventTypeBatchStarted,
	}
	if got := eventsourcing.EventTypesOf(agg.UncommittedEvents()); !slices.Equal(got, want) {
		t.Errorf("walk recorded %v, want %v", got, want)
	}
}
