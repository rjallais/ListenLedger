package correctionproposal

import (
	"testing"
	"time"
)

func TestProposalLifecycleAndReplay(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	prop, err := NewProposal(
		"prop-1",
		"run-101",
		"song",
		"song-123",
		"credit",
		map[string]any{"artist_name": "Tom Jobim, ..."},
		map[string]any{"artist_name": "Tom Jobim, Elis Regina"},
		"TIDAL credits match",
		0.95,
		"tidal-999",
		now,
	)
	if err != nil {
		t.Fatalf("NewProposal() error = %v", err)
	}

	if prop.Status() != StatusProposed {
		t.Errorf("expected status 'proposed', got %q", prop.Status())
	}

	if err := prop.Approve(now.Add(time.Second), "verified match"); err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	if prop.Status() != StatusApproved {
		t.Errorf("expected status 'approved', got %q", prop.Status())
	}

	if err := prop.MarkApplied(now.Add(2 * time.Second)); err != nil {
		t.Fatalf("MarkApplied() error = %v", err)
	}
	if prop.Status() != StatusApplied {
		t.Errorf("expected status 'applied', got %q", prop.Status())
	}

	events := prop.UncommittedEvents()
	if len(events) != 3 {
		t.Fatalf("expected 3 uncommitted events, got %d", len(events))
	}

	replayed, err := Replay("prop-1", events)
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if replayed.Status() != StatusApplied {
		t.Errorf("expected replayed status 'applied', got %q", replayed.Status())
	}
	if replayed.Kind() != "credit" {
		t.Errorf("expected kind 'credit', got %q", replayed.Kind())
	}
	if replayed.TargetID() != "song-123" {
		t.Errorf("expected targetID 'song-123', got %q", replayed.TargetID())
	}
}
