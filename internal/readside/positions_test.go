package readside

import (
	"context"
	"log/slog"
	"testing"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/db"
	"ListenLedger/internal/eventsourcing"
)

// TestAdvancePositionsSkipsGaps pins the contiguity contract: a batch whose
// lowest version is beyond position+1 must not advance the position, or a
// later write would claim the gap's fact was folded and hide it from the
// healer. A contiguous batch (or a full re-fold starting at v1) advances.
func TestAdvancePositionsSkipsGaps(t *testing.T) {
	ctx := context.Background()
	database, err := db.SetupDB(ctx, slog.Default(), t.TempDir(), false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = database.Close() }()

	events := func(versions ...int64) []eventsourcing.Event {
		t.Helper()
		out := make([]eventsourcing.Event, 0, len(versions))
		for _, v := range versions {
			for _, st := range []struct{ id, typ string }{{"ar_gap_a", "artist"}, {"ar_gap_b", "artist"}} {
				out = append(out, eventsourcing.Event{
					ID: "evt", StreamID: st.id, StreamType: st.typ, Version: v,
				})
			}
		}
		return out
	}

	if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		return AdvancePositions(tx, []eventsourcing.Event{
			{ID: "e1", StreamID: "ar_gap_a", StreamType: "artist", Version: 1},
			{ID: "e1", StreamID: "ar_gap_b", StreamType: "artist", Version: 1},
			{ID: "e2", StreamID: "ar_gap_a", StreamType: "artist", Version: 2},
			{ID: "e2", StreamID: "ar_gap_b", StreamType: "artist", Version: 2},
		})
	}); err != nil {
		t.Fatalf("seed positions: %v", err)
	}
	if got, _ := Position(ctx, database, "ar_gap_a"); got != 2 {
		t.Fatalf("contiguous batch position = %d, want 2", got)
	}

	// Gap: inline projection folded only v4 while the position sits at 2 (v3
	// failed). The position must not jump to 4 and hide the v3 lag.
	if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		return AdvancePositions(tx, events(4))
	}); err != nil {
		t.Fatalf("gap batch: %v", err)
	}
	if got, _ := Position(ctx, database, "ar_gap_a"); got != 2 {
		t.Fatalf("position after gap = %d, want 2 (gap must stay visible)", got)
	}

	// A contiguous batch closes the gap normally.
	if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		return AdvancePositions(tx, events(3, 4))
	}); err != nil {
		t.Fatalf("contiguous catch-up: %v", err)
	}
	if got, _ := Position(ctx, database, "ar_gap_a"); got != 4 {
		t.Fatalf("contiguous catch-up position = %d, want 4", got)
	}
}

func TestAdvancePositionMonotonic(t *testing.T) {
	ctx := context.Background()
	database, err := db.SetupDB(ctx, slog.Default(), t.TempDir(), false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = database.Close() }()

	advance := func(version int64) {
		t.Helper()
		if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
			return AdvancePosition(tx, "ar_pos", "artist", version)
		}); err != nil {
			t.Fatalf("AdvancePosition(%d): %v", version, err)
		}
	}

	advance(3)
	advance(2) // A stale redelivery must not regress the recorded tip.
	got, err := Position(ctx, database, "ar_pos")
	if err != nil {
		t.Fatalf("Position: %v", err)
	}
	if got != 3 {
		t.Fatalf("Position() = %d, want 3 (upsert must be monotonic)", got)
	}
}
