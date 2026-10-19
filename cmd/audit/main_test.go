package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/domain/batch"
	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
)

func seedAuditDB(ctx context.Context, t *testing.T, dataDir string) {
	t.Helper()
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = database.Close() }()
	store := eventsourcing.NewSQLiteStore(database)

	appendAgg := func(streamID string, base int64, events ...eventsourcing.Event) {
		t.Helper()
		if err := store.Append(ctx, streamID, base, events...); err != nil {
			t.Fatalf("Append %s: %v", streamID, err)
		}
	}
	exec := func(query string, bind func(*sqlite.Stmt)) {
		t.Helper()
		if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep(query)
			defer func() { _ = stmt.Reset() }()
			if bind != nil {
				bind(stmt)
			}
			_, err := stmt.Step()
			return err
		}); err != nil {
			t.Fatalf("exec %s: %v", query, err)
		}
	}

	// Artists: match, mismatch (row stale), missing row, unlogged row.
	newArtist := func(id string, listeners int64) *artist.Artist {
		agg, err := artist.NewArtist(id, "Name "+id, "sp_"+id, "rock_metal", "included")
		if err != nil {
			t.Fatalf("NewArtist: %v", err)
		}
		if err := agg.RecordMonthlyListeners(listeners, "test", 1); err != nil {
			t.Fatalf("RecordMonthlyListeners: %v", err)
		}
		return agg
	}
	artistRow := func(id string, listeners int64) {
		exec("INSERT INTO artists (id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs) VALUES (?, ?, ?, ?, 'rock_metal', 'included', 'idle', 0, 0);",
			func(s *sqlite.Stmt) {
				s.BindText(1, id)
				s.BindText(2, "Name "+id)
				s.BindText(3, "sp_"+id)
				s.BindInt64(4, listeners)
			})
	}
	agg := newArtist("ar_match", 1000)
	appendAgg("ar_match", 0, agg.UncommittedEvents()...)
	artistRow("ar_match", 1000)

	agg = newArtist("ar_stale", 2000)
	appendAgg("ar_stale", 0, agg.UncommittedEvents()...)
	artistRow("ar_stale", 1000)

	agg = newArtist("ar_norow", 500)
	appendAgg("ar_norow", 0, agg.UncommittedEvents()...)

	artistRow("ar_nostream", 100)

	// Albums: match, mismatch.
	newAlbum := func(id, status string) *album.Album {
		agg, err := album.NewAlbum(id, "Title "+id, "Artist", status, 1, 8)
		if err != nil {
			t.Fatalf("NewAlbum: %v", err)
		}
		return agg
	}
	albumRow := func(id, status string) {
		exec("INSERT INTO albums (id, title, artist_name, collection_songs, total_songs, status) VALUES (?, ?, ?, 1, 8, ?);",
			func(s *sqlite.Stmt) {
				s.BindText(1, id)
				s.BindText(2, "Title "+id)
				s.BindText(3, "Artist")
				s.BindText(4, status)
			})
	}
	aggAl := newAlbum("al_match", "waiting")
	appendAgg("al_match", 0, aggAl.UncommittedEvents()...)
	albumRow("al_match", "waiting")

	aggAl = newAlbum("al_stale", "waiting")
	appendAgg("al_stale", 0, aggAl.UncommittedEvents()...)
	albumRow("al_stale", "full")

	// Songs: match.
	aggSong, err := song.NewSong("sg_match", "Battery", "Metallica", "Master", "1986-03-03", "", "sp_sg", 1986, 0, 0, false)
	if err != nil {
		t.Fatalf("NewSong: %v", err)
	}
	appendAgg("sg_match", 0, aggSong.UncommittedEvents()...)
	exec("INSERT INTO songs (id, title, artist_name, is_recent) VALUES ('sg_match', 'Battery', 'Metallica', 0);", nil)

	// Jobs: match (succeeded), mismatch (succeeded stream, failed row),
	// purged info (terminal stream, no row), open missing row, unlogged row.
	newJob := func(reqID string) *scrapejob.Job {
		job, err := scrapejob.NewScrapeJob(reqID, "ar_match", "refresh")
		if err != nil {
			t.Fatalf("NewScrapeJob: %v", err)
		}
		return job
	}
	jobRow := func(reqID, status string) {
		exec("INSERT INTO scrape_jobs (id, request_id, artist_id, status, attempts) VALUES (?, ?, 'ar_match', ?, 1);",
			func(s *sqlite.Stmt) {
				s.BindText(1, "row_"+reqID)
				s.BindText(2, reqID)
				s.BindText(3, status)
			})
	}
	job := newJob("req_match")
	if _, _, err := job.RecordStarted(); err != nil {
		t.Fatalf("RecordStarted: %v", err)
	}
	if _, _, err := job.RecordSucceeded("test", 1); err != nil {
		t.Fatalf("RecordSucceeded: %v", err)
	}
	appendAgg("req_match", 0, job.UncommittedEvents()...)
	jobRow("req_match", "succeeded")

	job = newJob("req_stale")
	if _, _, err := job.RecordStarted(); err != nil {
		t.Fatalf("RecordStarted: %v", err)
	}
	if _, _, err := job.RecordSucceeded("test", 1); err != nil {
		t.Fatalf("RecordSucceeded: %v", err)
	}
	appendAgg("req_stale", 0, job.UncommittedEvents()...)
	jobRow("req_stale", "failed")

	job = newJob("req_purged")
	if _, _, err := job.RecordStarted(); err != nil {
		t.Fatalf("RecordStarted: %v", err)
	}
	if _, _, err := job.RecordSucceeded("test", 1); err != nil {
		t.Fatalf("RecordSucceeded: %v", err)
	}
	appendAgg("req_purged", 0, job.UncommittedEvents()...)

	job = newJob("req_open_norow")
	appendAgg("req_open_norow", 0, job.UncommittedEvents()...)

	jobRow("req_nostream", "queued")

	// Batches: match, mismatch (completed count off), missing row, unlogged row.
	newBatch := func(id string, artists []string) *batch.Batch {
		b, err := batch.NewBatch(id, artists, map[string]int{"priority": 1})
		if err != nil {
			t.Fatalf("NewBatch: %v", err)
		}
		return b
	}
	batchRowStats := func(id string, total, completed, done int64, stats string) {
		exec("INSERT INTO batches (id, total, completed, done, stats) VALUES (?, ?, ?, ?, ?);",
			func(s *sqlite.Stmt) {
				s.BindText(1, id)
				s.BindInt64(2, total)
				s.BindInt64(3, completed)
				s.BindInt64(4, done)
				s.BindText(5, stats)
			})
	}
	batchRow := func(id string, total, completed, done int64) {
		batchRowStats(id, total, completed, done, `{"priority":1}`)
	}
	memberRow := func(batchID, artistID string, done int64) {
		exec("INSERT INTO batch_members (batch_id, artist_id, done) VALUES (?, ?, ?);",
			func(s *sqlite.Stmt) {
				s.BindText(1, batchID)
				s.BindText(2, artistID)
				s.BindInt64(3, done)
			})
	}

	bMatch := newBatch("bt_match", []string{"ar_1", "ar_2"})
	if _, _, err := bMatch.RecordCompletion("ar_1"); err != nil {
		t.Fatalf("RecordCompletion: %v", err)
	}
	appendAgg("bt_match", 0, bMatch.UncommittedEvents()...)
	batchRow("bt_match", 2, 1, 0)
	memberRow("bt_match", "ar_1", 1)
	memberRow("bt_match", "ar_2", 0)

	bStale := newBatch("bt_stale", []string{"ar_1", "ar_2"})
	if _, _, err := bStale.RecordCompletion("ar_1"); err != nil {
		t.Fatalf("RecordCompletion: %v", err)
	}
	appendAgg("bt_stale", 0, bStale.UncommittedEvents()...)
	batchRow("bt_stale", 2, 0, 0)
	memberRow("bt_stale", "ar_1", 0)
	memberRow("bt_stale", "ar_2", 0)

	bNoRow := newBatch("bt_norow", []string{"ar_1"})
	appendAgg("bt_norow", 0, bNoRow.UncommittedEvents()...)

	bMemberDrift := newBatch("bt_member_drift", []string{"ar_1", "ar_2"})
	if _, _, err := bMemberDrift.RecordCompletion("ar_1"); err != nil {
		t.Fatalf("RecordCompletion: %v", err)
	}
	appendAgg("bt_member_drift", 0, bMemberDrift.UncommittedEvents()...)
	batchRow("bt_member_drift", 2, 1, 0)
	memberRow("bt_member_drift", "ar_1", 0)
	memberRow("bt_member_drift", "ar_2", 0)

	bStats := newBatch("bt_stats", []string{"ar_1"})
	appendAgg("bt_stats", 0, bStats.UncommittedEvents()...)
	batchRowStats("bt_stats", 1, 0, 0, `{"priority":2}`)
	memberRow("bt_stats", "ar_1", 0)

	bPruned := newBatch("bt_pruned", []string{"ar_1"})
	if _, _, err := bPruned.RecordCompletion("ar_1"); err != nil {
		t.Fatalf("RecordCompletion: %v", err)
	}
	appendAgg("bt_pruned", 0, bPruned.UncommittedEvents()...)
	exec("UPDATE events SET created_at = ? WHERE stream_id = 'bt_pruned';",
		func(s *sqlite.Stmt) {
			s.BindText(1, time.Now().Add(-3*time.Hour).UTC().Format(time.RFC3339Nano))
		})

	batchRow("bt_nostream", 1, 1, 1)
}

func TestAuditFindsDivergence(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	seedAuditDB(ctx, t, dataDir)

	selected := map[string]struct{}{
		artist.StreamTypeArtist:       {},
		album.StreamTypeAlbum:         {},
		song.StreamTypeSong:           {},
		scrapejob.StreamTypeScrapeJob: {},
		batch.StreamTypeBatch:         {},
	}
	rep, err := audit(ctx, dataDir, selected, 20)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}

	if rep.Artists.Checked != 3 {
		t.Fatalf("artists checked = %d, want 3", rep.Artists.Checked)
	}
	if rep.Artists.Mismatched != 1 || rep.Artists.MissingRows != 1 || rep.Artists.UnloggedRows != 1 {
		t.Fatalf("artists = %+v, want 1 mismatch + 1 missing + 1 unlogged", rep.Artists)
	}
	if rep.Albums.Checked != 2 || rep.Albums.Mismatched != 1 {
		t.Fatalf("albums = %+v, want 2 checked 1 mismatched", rep.Albums)
	}
	if rep.Songs.Checked != 1 || rep.Songs.Mismatched != 0 {
		t.Fatalf("songs = %+v, want 1 checked clean", rep.Songs)
	}
	if rep.Jobs.Checked != 4 {
		t.Fatalf("jobs checked = %d, want 4", rep.Jobs.Checked)
	}
	if rep.Jobs.Mismatched != 1 || rep.Jobs.MissingRows != 1 || rep.Jobs.UnloggedRows != 1 {
		t.Fatalf("jobs = %+v, want 1 mismatch + 1 missing + 1 unlogged", rep.Jobs)
	}
	if rep.Batches.Checked != 6 {
		t.Fatalf("batches checked = %d, want 6", rep.Batches.Checked)
	}
	if rep.Batches.Mismatched != 3 || rep.Batches.MissingRows != 1 || rep.Batches.UnloggedRows != 1 {
		t.Fatalf("batches = %+v, want 3 mismatches + 1 missing + 1 unlogged", rep.Batches)
	}
	if rep.PurgedInfos != 1 {
		t.Fatalf("purged infos = %d, want 1", rep.PurgedInfos)
	}
	if !rep.hasMismatch() {
		t.Fatal("hasMismatch should be true")
	}
}

func TestAuditClean(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()

	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store := eventsourcing.NewSQLiteStore(database)
	agg, err := artist.NewArtist("ar_clean", "Clean", "sp_clean", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if err := store.Append(ctx, "ar_clean", 0, agg.UncommittedEvents()...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO artists (id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs) VALUES ('ar_clean', 'Clean', 'sp_clean', 0, 'rock_metal', 'included', 'idle', 0, 0);")
		defer func() { _ = stmt.Reset() }()
		_, err := stmt.Step()
		return err
	}); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	_ = database.Close()

	selected := map[string]struct{}{artist.StreamTypeArtist: {}}
	rep, err := audit(ctx, dataDir, selected, 20)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if rep.hasMismatch() {
		t.Fatalf("clean audit should have no mismatch: %+v", rep.Artists)
	}
}

func TestAuditReportsStreamSizes(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()

	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store := eventsourcing.NewSQLiteStore(database)
	var evts []eventsourcing.Event
	for v := int64(1); v <= artist.SnapshotInterval+5; v++ {
		e, err := eventsourcing.NewEvent("ar_long", artist.StreamTypeArtist, v, "ArtistMonthlyListenersScraped",
			map[string]any{"monthly_listeners": v}, nil)
		if err != nil {
			t.Fatalf("NewEvent v%d: %v", v, err)
		}
		evts = append(evts, e)
	}
	if err := store.Append(ctx, "ar_long", 0, evts...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	_ = database.Close()

	selected := map[string]struct{}{artist.StreamTypeArtist: {}}
	rep, err := audit(ctx, dataDir, selected, 20)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if got := rep.MaxStreamVersions[artist.StreamTypeArtist]; got != artist.SnapshotInterval+5 {
		t.Fatalf("max artist version = %d, want %d", got, artist.SnapshotInterval+5)
	}
	want := fmt.Sprintf("longest artist stream is v%d (snapshot interval %d)", artist.SnapshotInterval+5, artist.SnapshotInterval)
	found := false
	for _, w := range rep.Warnings {
		if strings.Contains(w, want) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("want warning containing %q, got warnings=%v", want, rep.Warnings)
	}
}
