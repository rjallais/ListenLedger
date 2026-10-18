package tidalsync

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"ListenLedger/internal/eventsourcing"
)

// TestEventPayloadsRegistered keeps the payload registry in step with the
// event type constants this package declares: an event type added without a
// registered payload would make stored events unreadable.
func TestEventPayloadsRegistered(t *testing.T) {
	want := map[string]any{
		EventTypeTidalSyncStarted:       StartedPayload{},
		EventTypeTidalFavoritesObserved: FavoritesObservedPayload{},
		EventTypeTidalSyncCompleted:     CompletedPayload{},
		EventTypeTidalSyncFailed:        FailedPayload{},
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

// TestFoldCoversAllEventTypes walks both run outcomes and asserts the walks
// record exactly the declared event types. A command recording an event the
// fold cannot apply fails the walk; a declared type no command records would
// leave the run report without facts to fold.
func TestFoldCoversAllEventTypes(t *testing.T) {
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	completed, err := NewSync("run_walk_ok", "user_1", "US", at)
	if err != nil {
		t.Fatalf("NewSync: %v", err)
	}
	if err := completed.RecordFavoritesObserved(1, nil, nil, nil); err != nil {
		t.Fatalf("RecordFavoritesObserved: %v", err)
	}
	if err := completed.RecordCompleted(at); err != nil {
		t.Fatalf("RecordCompleted: %v", err)
	}

	failed, err := NewSync("run_walk_fail", "user_1", "US", at)
	if err != nil {
		t.Fatalf("NewSync: %v", err)
	}
	if err := failed.RecordFailed(at, "boom"); err != nil {
		t.Fatalf("RecordFailed: %v", err)
	}

	events := slices.Concat(completed.UncommittedEvents(), failed.UncommittedEvents())
	want := []string{
		EventTypeTidalFavoritesObserved,
		EventTypeTidalSyncCompleted,
		EventTypeTidalSyncFailed,
		EventTypeTidalSyncStarted,
	}
	if got := eventsourcing.EventTypesOf(events); !slices.Equal(got, want) {
		t.Errorf("walks recorded %v, want %v", got, want)
	}
}
