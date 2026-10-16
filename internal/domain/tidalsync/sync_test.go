package tidalsync

import (
	"testing"
	"time"

	"ListenLedger/internal/tidal"
)

func TestTidalSyncLifecycleAndReplay(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	runID := "run-101"
	userID := "user-42"
	country := "BR"

	sync, err := NewSync(runID, userID, country, now)
	if err != nil {
		t.Fatalf("NewSync() error = %v", err)
	}

	tracks := []tidal.ObservedTrack{
		{ID: "track-1", Title: "Águas de Março", ArtistNames: []string{"Tom Jobim", "Elis Regina"}},
	}
	if err := sync.RecordFavoritesObserved(1, tracks, nil, nil); err != nil {
		t.Fatalf("RecordFavoritesObserved() error = %v", err)
	}

	if err := sync.RecordCompleted(now.Add(time.Minute)); err != nil {
		t.Fatalf("RecordCompleted() error = %v", err)
	}

	if !sync.IsClosed() {
		t.Errorf("expected sync to be closed")
	}

	events := sync.UncommittedEvents()
	if len(events) != 3 {
		t.Fatalf("expected 3 uncommitted events, got %d", len(events))
	}

	// Replay test
	replayed, err := Replay(runID, events)
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	if !replayed.IsClosed() {
		t.Errorf("expected replayed sync to be closed")
	}
	if len(replayed.Tracks()) != 1 {
		t.Fatalf("expected 1 replayed track, got %d", len(replayed.Tracks()))
	}
	if replayed.Tracks()[0].Title != "Águas de Março" {
		t.Errorf("expected 'Águas de Março', got %q", replayed.Tracks()[0].Title)
	}
}
