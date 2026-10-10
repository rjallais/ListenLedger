package song_test

import (
	"errors"
	"testing"

	"ListenLedger/internal/domain/song"
)

func TestSongReplayRoundTrip(t *testing.T) {
	agg, err := song.NewSong("sg_1", "Rooster", "Alice in Chains", "Dirt", "1992-09-29", "album", "", 1992, 3, 1, true)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := agg.SetRecent(false, 0, 0); err != nil {
		t.Fatalf("recent: %v", err)
	}

	replayed, err := song.Replay("sg_1", agg.UncommittedEvents())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed.Version() != 2 || replayed.IsRecent || replayed.Title != "Rooster" {
		t.Fatalf("replayed state: %+v", replayed)
	}
}

func TestSongRejectsEmptyTitle(t *testing.T) {
	if _, err := song.NewSong("sg_1", " ", "x", "", "", "", "", 0, 0, 0, false); !errors.Is(err, song.ErrEmptyTitle) {
		t.Fatalf("expected empty title, got %v", err)
	}
}
