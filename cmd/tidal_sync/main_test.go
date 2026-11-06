package main

import (
	"context"
	"encoding/json/v2"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
)

// TestRunApplySkipsNoOpProposals asserts a proposal the stream already
// reflects records no proposal fact and stays unapplied, while a real
// change in the same report still applies.
func TestRunApplySkipsNoOpProposals(t *testing.T) {
	t.Setenv("LISTENLEDGER_EVENT_STORE", "sqlite")
	ctx := context.Background()
	dataDir := t.TempDir()

	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store := eventsourcing.NewSQLiteStore(database)
	songRepo := song.NewRepository(store)
	for streamID, tidalID := range map[string]string{"sg_noop": "tidal-1", "sg_change": ""} {
		agg, err := song.NewSong(streamID, "Song "+streamID, "Artist", "", "", "", "", 0, 0, 0, false)
		if err != nil {
			t.Fatalf("NewSong(%s): %v", streamID, err)
		}
		if tidalID != "" {
			if _, err := agg.LinkTidalID(tidalID, "seed", 1.0); err != nil {
				t.Fatalf("LinkTidalID(%s): %v", streamID, err)
			}
		}
		if _, err := songRepo.Save(ctx, agg); err != nil {
			t.Fatalf("saving song stream %s: %v", streamID, err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatalf("closing setup database: %v", err)
	}

	report := `{
		"timestamp": "2026-10-16T00:00:00Z",
		"run_id": "tidalsync-test",
		"user_id": "user-1",
		"country_code": "BR",
		"dry_run": true,
		"tracks_count": 2,
		"summary": {"link": 2},
		"proposals": [
			{
				"kind": "link",
				"target_type": "song",
				"target_id": "sg_noop",
				"title": "Song sg_noop",
				"artist": "Artist",
				"before": {"tidal_id": ""},
				"after": {"tidal_id": "tidal-1"},
				"evidence": "exact title and artist",
				"confidence": 0.99,
				"tidal_track_id": "tr-1",
				"auto_approve": true,
				"approved": true
			},
			{
				"kind": "link",
				"target_type": "song",
				"target_id": "sg_change",
				"title": "Song sg_change",
				"artist": "Artist",
				"before": {"tidal_id": ""},
				"after": {"tidal_id": "tidal-2"},
				"evidence": "exact title and artist",
				"confidence": 0.99,
				"tidal_track_id": "tr-2",
				"auto_approve": true,
				"approved": true
			}
		]
	}`
	reportPath := filepath.Join(dataDir, "report.json")
	if err := os.WriteFile(reportPath, []byte(report), 0644); err != nil {
		t.Fatalf("writing report: %v", err)
	}

	if err := runApply(ctx, []string{"-data-dir", dataDir, "-report", reportPath}); err != nil {
		t.Fatalf("runApply: %v", err)
	}

	database, err = db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB for assertions: %v", err)
	}
	defer func() { _ = database.Close() }()
	countEvents := func(where string) int64 {
		t.Helper()
		var count int64
		if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT count(*) FROM events WHERE " + where + ";")
			defer func() { _ = stmt.Reset() }()
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if hasRow {
				count = stmt.ColumnInt64(0)
			}
			return nil
		}); err != nil {
			t.Fatalf("counting events (%s): %v", where, err)
		}
		return count
	}
	if got := countEvents("stream_id = 'sg_noop'"); got != 2 {
		t.Fatalf("no-op song stream has %d events, want 2 (seed + link, unchanged)", got)
	}
	if got := countEvents("stream_id = 'sg_change'"); got != 2 {
		t.Fatalf("changed song stream has %d events, want 2 (created + applied link)", got)
	}
	if got := countEvents("stream_id LIKE 'prop-%'"); got != 3 {
		t.Fatalf("proposal streams hold %d events, want 3 (one fact chain for the real change only)", got)
	}

	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("reading report: %v", err)
	}
	var rep syncReport
	if err := json.Unmarshal(data, &rep); err != nil {
		t.Fatalf("decoding report: %v", err)
	}
	if len(rep.Proposals) != 2 {
		t.Fatalf("report has %d proposals, want 2", len(rep.Proposals))
	}
	if rep.Proposals[0].Applied || rep.Proposals[0].Note == "" {
		t.Fatalf("no-op proposal: applied=%t note=%q, want unapplied with a note",
			rep.Proposals[0].Applied, rep.Proposals[0].Note)
	}
	if !rep.Proposals[1].Applied {
		t.Fatal("changed proposal was not marked applied")
	}
}

// TestRunApplySkipsNoOpProposalOnSeededLegacySong asserts the no-op guard
// fires on a seeded legacy song too: the row already carries the proposal's
// tidal ID, so neither the pending seed facts nor a proposal fact chain are
// written for the no-op.
func TestRunApplySkipsNoOpProposalOnSeededLegacySong(t *testing.T) {
	t.Setenv("LISTENLEDGER_EVENT_STORE", "sqlite")
	ctx := context.Background()
	dataDir := t.TempDir()

	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`INSERT INTO songs (id, title, artist_name, tidal_id)
			VALUES ('sg_legacy', 'Legacy Song', 'Artist', 'tidal-9');`)
		defer func() { _ = stmt.Reset() }()
		_, err := stmt.Step()
		return err
	}); err != nil {
		t.Fatalf("inserting legacy song row: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("closing setup database: %v", err)
	}

	report := `{
		"timestamp": "2026-10-16T00:00:00Z",
		"run_id": "tidalsync-test",
		"user_id": "user-1",
		"country_code": "BR",
		"dry_run": true,
		"tracks_count": 1,
		"summary": {"link": 1},
		"proposals": [
			{
				"kind": "link",
				"target_type": "song",
				"target_id": "sg_legacy",
				"title": "Legacy Song",
				"artist": "Artist",
				"before": {"tidal_id": ""},
				"after": {"tidal_id": "tidal-9"},
				"evidence": "exact title and artist",
				"confidence": 0.99,
				"tidal_track_id": "tr-9",
				"auto_approve": true,
				"approved": true
			}
		]
	}`
	reportPath := filepath.Join(dataDir, "report.json")
	if err := os.WriteFile(reportPath, []byte(report), 0644); err != nil {
		t.Fatalf("writing report: %v", err)
	}

	if err := runApply(ctx, []string{"-data-dir", dataDir, "-report", reportPath}); err != nil {
		t.Fatalf("runApply: %v", err)
	}

	database, err = db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB for assertions: %v", err)
	}
	defer func() { _ = database.Close() }()
	countEvents := func(where string) int64 {
		t.Helper()
		var count int64
		if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT count(*) FROM events WHERE " + where + ";")
			defer func() { _ = stmt.Reset() }()
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if hasRow {
				count = stmt.ColumnInt64(0)
			}
			return nil
		}); err != nil {
			t.Fatalf("counting events (%s): %v", where, err)
		}
		return count
	}
	if got := countEvents("stream_id = 'sg_legacy'"); got != 0 {
		t.Fatalf("legacy song stream holds %d events, want 0 (seed not saved for a no-op)", got)
	}
	if got := countEvents("stream_id LIKE 'prop-%'"); got != 0 {
		t.Fatalf("proposal streams hold %d events, want 0 (no fact chain for a no-op)", got)
	}

	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("reading report: %v", err)
	}
	var rep syncReport
	if err := json.Unmarshal(data, &rep); err != nil {
		t.Fatalf("decoding report: %v", err)
	}
	if len(rep.Proposals) != 1 {
		t.Fatalf("report has %d proposals, want 1", len(rep.Proposals))
	}
	if rep.Proposals[0].Applied || rep.Proposals[0].Note == "" {
		t.Fatalf("no-op proposal: applied=%t note=%q, want unapplied with a note",
			rep.Proposals[0].Applied, rep.Proposals[0].Note)
	}
}

func TestRunApplyRequiresReviewAndStaleLink(t *testing.T) {
	t.Setenv("LISTENLEDGER_EVENT_STORE", "sqlite")
	ctx := context.Background()
	dataDir := t.TempDir()

	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store := eventsourcing.NewSQLiteStore(database)
	songRepo := song.NewRepository(store)

	// sg_review: unlinked, high confidence proposal, but has requires_review=true
	aggReview, err := song.NewSong("sg_review", "Review Song", "Artist", "", "", "", "", 0, 0, 0, false)
	if err != nil {
		t.Fatalf("NewSong: %v", err)
	}
	if _, err := songRepo.Save(ctx, aggReview); err != nil {
		t.Fatalf("saving sg_review: %v", err)
	}

	// sg_stale: already linked to tidal-existing, proposal expects before.tidal_id == ""
	aggStale, err := song.NewSong("sg_stale", "Stale Song", "Artist", "", "", "", "", 0, 0, 0, false)
	if err != nil {
		t.Fatalf("NewSong: %v", err)
	}
	if _, err := aggStale.LinkTidalID("tidal-existing", "test", 1.0); err != nil {
		t.Fatalf("LinkTidalID: %v", err)
	}
	if _, err := songRepo.Save(ctx, aggStale); err != nil {
		t.Fatalf("saving sg_stale: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("closing setup database: %v", err)
	}

	report := `{
		"timestamp": "2026-10-16T00:00:00Z",
		"run_id": "tidalsync-test",
		"user_id": "user-1",
		"country_code": "BR",
		"dry_run": true,
		"tracks_count": 2,
		"summary": {"link": 2},
		"proposals": [
			{
				"kind": "link",
				"target_type": "song",
				"target_id": "sg_review",
				"title": "Review Song",
				"artist": "Artist",
				"before": {"tidal_id": ""},
				"after": {"tidal_id": "tidal-target"},
				"evidence": "matched title but claimed elsewhere",
				"confidence": 0.95,
				"tidal_track_id": "tidal-target",
				"auto_approve": false,
				"requires_review": true,
				"approved": false
			},
			{
				"kind": "link",
				"target_type": "song",
				"target_id": "sg_stale",
				"title": "Stale Song",
				"artist": "Artist",
				"before": {"tidal_id": ""},
				"after": {"tidal_id": "tidal-new"},
				"evidence": "stale proposal",
				"confidence": 0.99,
				"tidal_track_id": "tidal-new",
				"auto_approve": false,
				"approved": true
			}
		]
	}`
	reportPath := filepath.Join(dataDir, "report.json")
	if err := os.WriteFile(reportPath, []byte(report), 0644); err != nil {
		t.Fatalf("writing report: %v", err)
	}

	if err := runApply(ctx, []string{"-data-dir", dataDir, "-report", reportPath, "-approve-all-above", "0.90"}); err != nil {
		t.Fatalf("runApply: %v", err)
	}

	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("reading report: %v", err)
	}
	var rep syncReport
	if err := json.Unmarshal(data, &rep); err != nil {
		t.Fatalf("decoding report: %v", err)
	}

	// sg_review should NOT have been applied by --approve-all-above 0.90 because requires_review is true
	if len(rep.Proposals) != 2 {
		t.Fatalf("report has %d proposals, want 2", len(rep.Proposals))
	}
	if rep.Proposals[0].Applied {
		t.Fatalf("proposal with requires_review=true must not be applied by threshold approval")
	}

	// sg_stale was approved=true, but song is already linked to tidal-existing (not ""), so it should be skipped as stale
	if rep.Proposals[1].Applied {
		t.Fatalf("stale link proposal must not be applied")
	}
	if !strings.Contains(rep.Proposals[1].Note, "stale proposal: song now linked to tidal-existing") {
		t.Fatalf("expected stale proposal note, got %q", rep.Proposals[1].Note)
	}
}
