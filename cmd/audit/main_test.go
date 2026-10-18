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
	batchRow := func(id string, total, completed, done int64) {
		exec("INSERT INTO batches (id, total, completed, done) VALUES (?, ?, ?, ?);",
			func(s *sqlite.Stmt) {
				s.BindText(1, id)
				s.BindInt64(2, total)
				s.BindInt64(3, completed)
				s.BindInt64(4, done)
			})
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
	if rep.Batches.Checked != 3 {
		t.Fatalf("batches checked = %d, want 3", rep.Batches.Checked)
	}
	if rep.Batches.Mismatched != 1 || rep.Batches.MissingRows != 1 || rep.Batches.UnloggedRows != 1 {
		t.Fatalf("batches = %+v, want 1 mismatch + 1 missing + 1 unlogged", rep.Batches)
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

func TestAuditEventTypesCensus(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()

	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store := eventsourcing.NewSQLiteStore(database)

	agg, err := artist.NewArtist("ar_census", "Census", "sp_census", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if err := store.Append(ctx, "ar_census", 0, agg.UncommittedEvents()...); err != nil {
		t.Fatalf("Append: %v", err)
	}

	unknown, err := eventsourcing.NewEvent("ar_census", artist.StreamTypeArtist, 2, "UnknownEventType", map[string]string{"x": "1"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if err := store.Append(ctx, "ar_census", 1, unknown); err != nil {
		t.Fatalf("Append unknown: %v", err)
	}

	broken, err := eventsourcing.NewEvent("ar_census", artist.StreamTypeArtist, 3, artist.EventTypeArtistCreated, map[string]any{"id": 42}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if err := store.Append(ctx, "ar_census", 2, broken); err != nil {
		t.Fatalf("Append broken: %v", err)
	}
	_ = database.Close()

	rep, err := audit(ctx, dataDir, map[string]struct{}{}, 20)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}

	census := make(map[string]eventTypeCensus, len(rep.EventTypes))
	for _, entry := range rep.EventTypes {
		census[entry.EventType] = entry
	}
	if got := census[artist.EventTypeArtistCreated]; !got.Registered || got.Count != 2 || got.DecodeFails != 1 {
		t.Errorf("ArtistCreated census = %+v, want registered, count 2, 1 decode failure", got)
	}
	if got := census["UnknownEventType"]; got.Registered || got.Count != 1 {
		t.Errorf("UnknownEventType census = %+v, want unregistered with count 1", got)
	}
	if want := int64(2); rep.UndecodableEvents != want {
		t.Errorf("UndecodableEvents = %d, want %d", rep.UndecodableEvents, want)
	}
	if !rep.hasMismatch() {
		t.Error("undecodable events should count as divergence")
	}
}

// TestAuditCensusSamplesDespiteZeroMaxMismatches pins the gate floor:
// --max-mismatches 0 only silences mismatch samples, so the census must
// still sample payloads and report undecodable events.
func TestAuditCensusSamplesDespiteZeroMaxMismatches(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()

	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store := eventsourcing.NewSQLiteStore(database)

	broken, err := eventsourcing.NewEvent("ar_quiet", artist.StreamTypeArtist, 1, artist.EventTypeArtistCreated, map[string]any{"id": 42}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if err := store.Append(ctx, "ar_quiet", 0, broken); err != nil {
		t.Fatalf("Append broken: %v", err)
	}
	_ = database.Close()

	rep, err := audit(ctx, dataDir, map[string]struct{}{}, 0)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}

	for _, entry := range rep.EventTypes {
		if entry.EventType != artist.EventTypeArtistCreated {
			continue
		}
		if entry.DecodeFails != 1 {
			t.Errorf("ArtistCreated DecodeFails = %d, want 1 (census sampling must survive --max-mismatches 0)", entry.DecodeFails)
		}
	}
	if rep.UndecodableEvents != 1 {
		t.Errorf("UndecodableEvents = %d, want 1", rep.UndecodableEvents)
	}
	if !rep.hasMismatch() {
		t.Error("undecodable events should count as divergence")
	}
}

// TestAuditCensusWarnsInJetstreamMode checks that the jetstream-mode census
// flags its blind spot: SQLite freezes at the flip, so JetStream-only appends
// go unchecked and the report must say so instead of reporting clean.
func TestAuditCensusWarnsInJetstreamMode(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	t.Setenv("LISTENLEDGER_EVENT_STORE", "jetstream")

	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store := eventsourcing.NewSQLiteStore(database)
	agg, err := artist.NewArtist("ar_census_js", "Census", "sp_census_js", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if err := store.Append(ctx, "ar_census_js", 0, agg.UncommittedEvents()...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	_ = database.Close()

	rep, err := audit(ctx, dataDir, map[string]struct{}{}, 20)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	found := false
	for _, w := range rep.Warnings {
		if strings.Contains(w, "JetStream-only appends are not checked") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("want JetStream census blind-spot warning, got %v", rep.Warnings)
	}
}

func TestAuditPurgedDoneBatch(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store := eventsourcing.NewSQLiteStore(database)
	b, err := batch.NewBatch("bt_purged_done", []string{"ar_1"}, nil)
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if _, _, err := b.RecordCompletion("ar_1"); err != nil {
		t.Fatalf("RecordCompletion: %v", err)
	}
	if !b.Done() {
		t.Fatalf("fixture batch should be done")
	}
	events := b.UncommittedEvents()
	for i := range events {
		events[i].CreatedAt = time.Now().Add(-3 * time.Hour)
	}
	if err := store.Append(ctx, "bt_purged_done", 0, events...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	_ = database.Close()
	selected := map[string]struct{}{batch.StreamTypeBatch: {}}
	rep, err := audit(ctx, dataDir, selected, 20)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if rep.Batches.MissingRows != 0 {
		t.Fatalf("purged done batch should not count as missing: %+v", rep.Batches)
	}
	if rep.Batches.Checked != 1 {
		t.Fatalf("batches checked = %d, want 1", rep.Batches.Checked)
	}
	if rep.PurgedInfos != 1 {
		t.Fatalf("purged infos = %d, want 1", rep.PurgedInfos)
	}
	if rep.hasMismatch() {
		t.Fatalf("purged done batch should be clean: %+v", rep.Batches)
	}
}

func TestAuditRecentDoneBatchMissingRow(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store := eventsourcing.NewSQLiteStore(database)
	b, err := batch.NewBatch("bt_recent_done", []string{"ar_1"}, nil)
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if _, _, err := b.RecordCompletion("ar_1"); err != nil {
		t.Fatalf("RecordCompletion: %v", err)
	}
	if !b.Done() {
		t.Fatalf("fixture batch should be done")
	}
	// Recent events (< 2h) without a row should be flagged as missing row, not purged.
	if err := store.Append(ctx, "bt_recent_done", 0, b.UncommittedEvents()...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	_ = database.Close()
	selected := map[string]struct{}{batch.StreamTypeBatch: {}}
	rep, err := audit(ctx, dataDir, selected, 20)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if rep.Batches.MissingRows != 1 {
		t.Fatalf("recent done batch without row must count as missing: %+v", rep.Batches)
	}
	if !rep.hasMismatch() {
		t.Fatalf("recent done batch missing row should trigger hasMismatch")
	}
}
