package album_test

import (
	"errors"
	"testing"

	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/eventsourcing"
)

func TestAlbumReplayRoundTrip(t *testing.T) {
	agg, err := album.NewAlbum("al_1", "Dirt", "Alice in Chains", "waiting", 0, 13)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := agg.ChangeStatus("full"); err != nil {
		t.Fatalf("status: %v", err)
	}
	if err := agg.AdjustSongCounts(13, 13); err != nil {
		t.Fatalf("counts: %v", err)
	}

	events := agg.UncommittedEvents()
	replayed, err := album.Replay("al_1", events)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed.Version() != 3 || replayed.Status != "full" || replayed.CollectionSongs != 13 {
		t.Fatalf("replayed state: %+v", replayed)
	}
}

func TestAlbumRejectsBadInput(t *testing.T) {
	if _, err := album.NewAlbum("al_1", "  ", "x", "waiting", 0, 0); err == nil {
		t.Fatal("expected empty title error")
	}
	agg, err := album.NewAlbum("al_1", "Dirt", "Alice in Chains", "waiting", 0, 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := agg.ChangeStatus("nope"); !errors.Is(err, album.ErrInvalidStatus) {
		t.Fatalf("expected invalid status, got %v", err)
	}
}

// Editing history means replacing one event's payload and replaying.
func TestAlbumHistoryEditReplays(t *testing.T) {
	agg, err := album.NewAlbum("al_1", "Dirt", "Alice in Chains", "waiting", 0, 13)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := agg.ChangeStatus("full"); err != nil {
		t.Fatalf("status: %v", err)
	}
	events := agg.UncommittedEvents()

	// The status change was wrong; correct that event's payload in place.
	corrected, err := eventsourcing.NewEvent(events[1].StreamID, events[1].StreamType, events[1].Version,
		album.EventTypeAlbumStatusChanged, album.StatusChangedPayload{
			AlbumID: "al_1", OldStatus: "waiting", NewStatus: "processed_once",
		}, nil)
	if err != nil {
		t.Fatalf("corrected event: %v", err)
	}
	events[1] = corrected

	replayed, err := album.Replay("al_1", events)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed.Status != "processed_once" {
		t.Fatalf("corrected status: %s", replayed.Status)
	}
}
