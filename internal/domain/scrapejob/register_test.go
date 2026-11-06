package scrapejob

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
		EventTypeScrapeRequested:    RequestedPayload{},
		EventTypeScrapeStarted:      StartedPayload{},
		EventTypeScrapeSucceeded:    SucceededPayload{},
		EventTypeScrapeFailed:       FailedPayload{},
		EventTypeScrapeDeadLettered: DeadLetteredPayload{},
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

// TestFoldCoversAllEventTypes walks both lifecycle paths and asserts the
// walks record exactly the declared event types. A command recording an event
// the fold cannot apply fails the walk; a declared type no command records
// would leave the read model without facts to project.
func TestFoldCoversAllEventTypes(t *testing.T) {
	succeeded, err := NewScrapeJob("req_walk_ok", "ar_walk", "refresh")
	if err != nil {
		t.Fatalf("NewScrapeJob: %v", err)
	}
	if _, recorded, err := succeeded.RecordStarted(); err != nil || !recorded {
		t.Fatalf("RecordStarted: recorded=%v err=%v", recorded, err)
	}
	if _, recorded, err := succeeded.RecordSucceeded("walk", 1); err != nil || !recorded {
		t.Fatalf("RecordSucceeded: recorded=%v err=%v", recorded, err)
	}

	dead, err := NewScrapeJob("req_walk_dead", "ar_walk", "refresh")
	if err != nil {
		t.Fatalf("NewScrapeJob: %v", err)
	}
	if _, recorded, err := dead.RecordFailed("boom"); err != nil || !recorded {
		t.Fatalf("RecordFailed: recorded=%v err=%v", recorded, err)
	}
	if _, recorded, err := dead.RecordDeadLettered("poison"); err != nil || !recorded {
		t.Fatalf("RecordDeadLettered: recorded=%v err=%v", recorded, err)
	}

	events := slices.Concat(succeeded.UncommittedEvents(), dead.UncommittedEvents())
	want := []string{
		EventTypeScrapeDeadLettered,
		EventTypeScrapeFailed,
		EventTypeScrapeRequested,
		EventTypeScrapeStarted,
		EventTypeScrapeSucceeded,
	}
	if got := eventsourcing.EventTypesOf(events); !slices.Equal(got, want) {
		t.Errorf("walks recorded %v, want %v", got, want)
	}
}
