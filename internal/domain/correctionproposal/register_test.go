package correctionproposal

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
		EventTypeCorrectionProposed: ProposedPayload{},
		EventTypeCorrectionApproved: ApprovedPayload{},
		EventTypeCorrectionRejected: RejectedPayload{},
		EventTypeCorrectionApplied:  AppliedPayload{},
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

// TestFoldCoversAllEventTypes walks both review outcomes and asserts the
// walks record exactly the declared event types. A command recording an event
// the fold cannot apply fails the walk; a declared type no command records
// would leave the review queue without facts to fold.
func TestFoldCoversAllEventTypes(t *testing.T) {
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	approved, err := NewProposal("cp_walk_ok", "run_1", "song", "sg_1",
		"metadata", map[string]any{"album": "Old"}, map[string]any{"album": "New"}, "tidal", 0.9, "track_1", at)
	if err != nil {
		t.Fatalf("NewProposal: %v", err)
	}
	if err := approved.Approve(at, "walk"); err != nil {
		t.Fatalf("Approve: %v", err)
	}
	if err := approved.MarkApplied(at); err != nil {
		t.Fatalf("MarkApplied: %v", err)
	}

	rejected, err := NewProposal("cp_walk_no", "run_1", "song", "sg_2",
		"metadata", nil, nil, "tidal", 0.5, "", at)
	if err != nil {
		t.Fatalf("NewProposal: %v", err)
	}
	if err := rejected.Reject(at, "walk"); err != nil {
		t.Fatalf("Reject: %v", err)
	}

	events := slices.Concat(approved.UncommittedEvents(), rejected.UncommittedEvents())
	want := []string{
		EventTypeCorrectionApplied,
		EventTypeCorrectionApproved,
		EventTypeCorrectionProposed,
		EventTypeCorrectionRejected,
	}
	if got := eventsourcing.EventTypesOf(events); !slices.Equal(got, want) {
		t.Errorf("walks recorded %v, want %v", got, want)
	}
}
