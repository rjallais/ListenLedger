// Command audit reports divergence between the event log (source of truth)
// and SQLite read models (derived projections and operational mirrors).
//
// Read-only: it never writes. Run with the app stopped or against a backup
// copy (same guidance as cmd/replay): live event-first writes mean a stream
// briefly leads its row, which reads as a false mismatch under load.
//
// Usage:
//
//	go run ./cmd/audit [--data-dir pb_data] [--streams artist,album,song,scrapejob] [--max-mismatches 20] [--pb-mirror]
//
// Exit code is 1 when value mismatches, missing projection rows, or unlogged
// rows are found; purged scrape-job rows (retention) and outbox/checkpoint
// lag are informational only.
//
// --pb-mirror additionally compares PocketBase collections against their
// SQLite projections (PB boot required; run against a backup copy with the
// app stopped — same rule as cmd/backfill_song_artists).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"strings"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"github.com/pocketbase/pocketbase"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/appdir"
	"ListenLedger/internal/batchprogress"
	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/domain/batch"
	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	_ "ListenLedger/migrations"
)

// streamReport tallies one stream type's convergence check.
type streamReport struct {
	Checked      int      `json:"checked"`
	Mismatched   int      `json:"mismatched"`
	MissingRows  int      `json:"missing_rows"`
	UnloggedRows int      `json:"unlogged_rows"`
	Samples      []string `json:"samples,omitempty"`
}

// report is the full audit outcome.
type report struct {
	Artists     streamReport     `json:"artists"`
	Albums      streamReport     `json:"albums"`
	Songs       streamReport     `json:"songs"`
	Jobs        streamReport     `json:"jobs"`
	Batches     streamReport     `json:"batches"`
	PurgedInfos int              `json:"purged_infos"`
	OutboxLag   int64            `json:"outbox_lag"`
	Checkpoints map[string]int64 `json:"checkpoints,omitempty"`
	// MaxStreamVersions records the longest stream per type (stream type ->
	// max tip version). Informational only: long artist streams lose their
	// snapshot shortcut under JetStream reads, so flip-day operators need
	// this number, not a red/green verdict.
	MaxStreamVersions map[string]int64 `json:"max_stream_versions,omitempty"`
	// Warnings records non-fatal observability failures (outbox/checkpoint
	// reads) that leave a field at its zero value instead of failing the run.
	Warnings []string `json:"warnings,omitempty"`
}

// hasMismatch reports whether the audit found real divergence.
func (r *report) hasMismatch() bool {
	for _, s := range []streamReport{r.Artists, r.Albums, r.Songs, r.Jobs, r.Batches} {
		if s.Mismatched > 0 || s.MissingRows > 0 || s.UnloggedRows > 0 {
			return true
		}
	}
	return false
}

func (r *streamReport) addSample(cap int, sample string) {
	if len(r.Samples) < cap {
		r.Samples = append(r.Samples, sample)
	}
}

func main() {
	var (
		dataDir       string
		streams       string
		maxMismatches int
		pbMirror      bool
	)
	flag.StringVar(&dataDir, "data-dir", appdir.ResolveDataDir(), "Directory holding sqlite/ledger.sqlite")
	flag.StringVar(&streams, "streams", "artist,album,song,scrapejob,batch", "Comma-separated stream types to audit")
	flag.IntVar(&maxMismatches, "max-mismatches", 20, "Maximum mismatch samples kept per stream type")
	flag.BoolVar(&pbMirror, "pb-mirror", false, "Also compare PocketBase collections against SQLite projections (requires stopped app or backup copy)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	selected, err := parseStreamTypes(streams)
	if err != nil {
		logger.Error("invalid stream selection", "error", err)
		os.Exit(2)
	}

	rep, err := audit(context.Background(), dataDir, selected, maxMismatches)
	if err != nil {
		logger.Error("audit failed", "error", err)
		os.Exit(1)
	}
	logger.Info("audit complete",
		"artists", rep.Artists,
		"albums", rep.Albums,
		"songs", rep.Songs,
		"jobs", rep.Jobs,
		"batches", rep.Batches,
		"purged_infos", rep.PurgedInfos,
		"outbox_lag", rep.OutboxLag,
		"checkpoints", rep.Checkpoints,
		"max_stream_versions", rep.MaxStreamVersions,
		"warnings", rep.Warnings,
	)
	diverged := rep.hasMismatch()
	if diverged {
		logger.Error("audit found divergence: streams and read models disagree (run quiescent to rule out in-flight writes)")
	}
	if pbMirror {
		if err := runPBMirror(context.Background(), dataDir, maxMismatches, logger); err != nil {
			logger.Error("pb-mirror audit failed", "error", err)
			diverged = true
		}
	}
	if diverged {
		os.Exit(1)
	}
	logger.Info("audit clean: read models converge with the event log")
}

// runPBMirror bootstraps PocketBase against dataDir and compares its
// collections with the SQLite projections. It returns an error listing every
// drifted collection (deterministic order) so main can exit through the
// returned error with deferred cleanup intact.
func runPBMirror(ctx context.Context, dataDir string, maxSamples int, logger *slog.Logger) error {
	app := pocketbase.NewWithConfig(pocketbase.Config{DefaultDataDir: dataDir})
	if err := app.Bootstrap(); err != nil {
		return fmt.Errorf("bootstrapping PocketBase: %w", err)
	}
	// Release PB handles (db connections, cron) when the mirror audit ends.
	defer func() {
		if err := app.ClearBootstrap(); err != nil {
			logger.Warn("clearing PocketBase bootstrap state", "error", err)
		}
	}()
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer func() { _ = database.Close() }()

	mirrorOut, err := auditPBMirror(ctx, app, database, maxSamples)
	if err != nil {
		return err
	}
	logger.Info("pb-mirror audit complete",
		"artists", mirrorOut["artists"],
		"albums", mirrorOut["albums"],
		"songs", mirrorOut["songs"],
	)
	var drifted []string
	for _, name := range []string{"albums", "artists", "songs"} {
		if rep := mirrorOut[name]; rep != nil && rep.hasMismatch() {
			logger.Error("pb-mirror found drift: PocketBase and SQLite disagree (run quiescent to rule out in-flight writes)",
				"collection", name, "report", rep)
			drifted = append(drifted, name)
		}
	}
	if len(drifted) > 0 {
		return fmt.Errorf("pb-mirror drift in collections: %s", strings.Join(drifted, ", "))
	}
	logger.Info("pb-mirror clean: PocketBase converges with SQLite projections")
	return nil
}

func parseStreamTypes(value string) (map[string]struct{}, error) {
	selected := make(map[string]struct{})
	for streamType := range strings.SplitSeq(value, ",") {
		streamType = strings.TrimSpace(streamType)
		if streamType == "" {
			continue
		}
		switch streamType {
		case artist.StreamTypeArtist, album.StreamTypeAlbum, song.StreamTypeSong, scrapejob.StreamTypeScrapeJob, batch.StreamTypeBatch:
			selected[streamType] = struct{}{}
		default:
			return nil, fmt.Errorf("unknown stream type %q", streamType)
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("at least one stream type is required")
	}
	return selected, nil
}

func audit(ctx context.Context, dataDir string, selected map[string]struct{}, maxSamples int) (*report, error) {
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	defer func() { _ = database.Close() }()

	store := eventsourcing.NewSQLiteStore(database)
	rep := &report{Checkpoints: make(map[string]int64)}

	streamInfos, err := store.Streams(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing streams: %w", err)
	}
	byType := make(map[string][]string)
	for _, info := range streamInfos {
		if _, ok := selected[info.Type]; ok {
			byType[info.Type] = append(byType[info.Type], info.ID)
		}
	}

	// Stream-size stats for flip-day operators: longest stream per selected
	// type from one tips query. Warn-only (never a mismatch): long artist
	// streams lose the snapshot shortcut under JetStream reads.
	rep.MaxStreamVersions = make(map[string]int64)
	if tips, err := store.StreamTips(ctx); err != nil {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf("stream tips unavailable: %v", err))
	} else {
		idToType := make(map[string]string, len(streamInfos))
		for _, info := range streamInfos {
			idToType[info.ID] = info.Type
		}
		for _, tip := range tips {
			typ, ok := idToType[tip.StreamID]
			if !ok {
				continue
			}
			if _, ok := selected[typ]; !ok {
				continue
			}
			rep.MaxStreamVersions[typ] = max(rep.MaxStreamVersions[typ], tip.Version)
		}
		if maxArtist, ok := rep.MaxStreamVersions[artist.StreamTypeArtist]; ok && maxArtist > artist.SnapshotInterval {
			rep.Warnings = append(rep.Warnings, fmt.Sprintf("longest artist stream is v%d (snapshot interval %d): JetStream reads replay it fully until a KV snapshot is stored, then only the tail", maxArtist, artist.SnapshotInterval))
		}
	}

	if _, ok := selected[artist.StreamTypeArtist]; ok {
		if err := auditArtists(ctx, database, store, byType[artist.StreamTypeArtist], maxSamples, &rep.Artists); err != nil {
			return nil, err
		}
	}
	if _, ok := selected[album.StreamTypeAlbum]; ok {
		if err := auditAlbums(ctx, database, store, byType[album.StreamTypeAlbum], maxSamples, &rep.Albums); err != nil {
			return nil, err
		}
	}
	if _, ok := selected[song.StreamTypeSong]; ok {
		if err := auditSongs(ctx, database, store, byType[song.StreamTypeSong], maxSamples, &rep.Songs); err != nil {
			return nil, err
		}
	}
	if _, ok := selected[scrapejob.StreamTypeScrapeJob]; ok {
		if err := auditJobs(ctx, database, store, byType[scrapejob.StreamTypeScrapeJob], maxSamples, &rep.Jobs, &rep.PurgedInfos); err != nil {
			return nil, err
		}
	}
	if _, ok := selected[batch.StreamTypeBatch]; ok {
		if err := auditBatches(ctx, database, store, byType[batch.StreamTypeBatch], maxSamples, &rep.Batches); err != nil {
			return nil, err
		}
	}

	if lag, err := store.UnpublishedCount(ctx); err != nil {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf("outbox lag unavailable: %v", err))
	} else {
		rep.OutboxLag = lag
	}
	for _, name := range []string{
		eventsourcing.CheckpointArtistProjection,
		eventsourcing.CheckpointAlbumProjection,
		eventsourcing.CheckpointSongProjection,
		eventsourcing.CheckpointBatchProjection,
		eventsourcing.CheckpointDomainEventsRelay,
	} {
		pos, err := store.GetCheckpoint(ctx, name)
		if err != nil {
			rep.Warnings = append(rep.Warnings, fmt.Sprintf("checkpoint %s unreadable: %v", name, err))
			continue
		}
		if pos > 0 {
			rep.Checkpoints[name] = pos
		}
	}
	return rep, nil
}

type artistRow struct {
	found            bool
	monthlyListeners int64
	listStatus       string
	fetchStatus      string
}

func readArtistRow(ctx context.Context, database *toolbeltdb.Database, id string) (artistRow, error) {
	var row artistRow
	err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT monthly_listeners, list_status, fetch_status FROM artists WHERE id = ? LIMIT 1;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, id)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		row = artistRow{found: true, monthlyListeners: stmt.ColumnInt64(0), listStatus: stmt.ColumnText(1), fetchStatus: stmt.ColumnText(2)}
		return nil
	})
	return row, err
}

func auditArtists(ctx context.Context, database *toolbeltdb.Database, store *eventsourcing.SQLiteStore, ids []string, maxSamples int, rep *streamReport) error {
	for _, id := range ids {
		rep.Checked++
		events, err := store.Load(ctx, id)
		if err != nil {
			return fmt.Errorf("loading artist stream %s: %w", id, err)
		}
		if len(events) == 0 {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("artist %s: stream listed but has no events", id))
			continue
		}
		agg, err := artist.Replay(id, events)
		if err != nil {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("artist %s: unreplayable: %v", id, err))
			continue
		}
		row, err := readArtistRow(ctx, database, id)
		if err != nil {
			return fmt.Errorf("reading artist row %s: %w", id, err)
		}
		if !row.found {
			rep.MissingRows++
			rep.addSample(maxSamples, fmt.Sprintf("artist %s: stream v%d has no artists row", id, agg.Version()))
			continue
		}
		if row.monthlyListeners != agg.MonthlyListeners || row.listStatus != agg.ListStatus || row.fetchStatus != agg.FetchStatus {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("artist %s: row(listeners=%d list=%s fetch=%s) != stream(listeners=%d list=%s fetch=%s)",
				id, row.monthlyListeners, row.listStatus, row.fetchStatus, agg.MonthlyListeners, agg.ListStatus, agg.FetchStatus))
		}
	}
	// Rows without streams: bypass writes that never logged a fact.
	return auditUnloggedRows(ctx, database, "artists", "id", ids, maxSamples, rep, "artist")
}

type albumRow struct {
	found           bool
	status          string
	collectionSongs int64
	totalSongs      int64
}

func auditAlbums(ctx context.Context, database *toolbeltdb.Database, store *eventsourcing.SQLiteStore, ids []string, maxSamples int, rep *streamReport) error {
	for _, id := range ids {
		rep.Checked++
		events, err := store.Load(ctx, id)
		if err != nil {
			return fmt.Errorf("loading album stream %s: %w", id, err)
		}
		if len(events) == 0 {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("album %s: stream listed but has no events", id))
			continue
		}
		agg, err := album.Replay(id, events)
		if err != nil {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("album %s: unreplayable: %v", id, err))
			continue
		}
		var row albumRow
		err = database.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT status, collection_songs, total_songs FROM albums WHERE id = ? LIMIT 1;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, id)
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				return nil
			}
			row = albumRow{found: true, status: stmt.ColumnText(0), collectionSongs: stmt.ColumnInt64(1), totalSongs: stmt.ColumnInt64(2)}
			return nil
		})
		if err != nil {
			return fmt.Errorf("reading album row %s: %w", id, err)
		}
		if !row.found {
			rep.MissingRows++
			rep.addSample(maxSamples, fmt.Sprintf("album %s: stream v%d has no albums row", id, agg.Version()))
			continue
		}
		if row.status != agg.Status || row.collectionSongs != agg.CollectionSongs || row.totalSongs != agg.TotalSongs {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("album %s: row(status=%s coll=%d total=%d) != stream(status=%s coll=%d total=%d)",
				id, row.status, row.collectionSongs, row.totalSongs, agg.Status, agg.CollectionSongs, agg.TotalSongs))
		}
	}
	return auditUnloggedRows(ctx, database, "albums", "id", ids, maxSamples, rep, "album")
}

func auditUnloggedRows(ctx context.Context, database *toolbeltdb.Database, table, idColumn string, streamIDs []string, maxSamples int, rep *streamReport, label string) error {
	streams := make(map[string]struct{}, len(streamIDs))
	for _, id := range streamIDs {
		streams[id] = struct{}{}
	}
	return database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT " + idColumn + " FROM " + table + ";")
		defer func() { _ = stmt.Reset() }()
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return fmt.Errorf("listing %s rows: %w", table, err)
			}
			if !hasRow {
				return nil
			}
			id := stmt.ColumnText(0)
			if _, ok := streams[id]; !ok {
				rep.UnloggedRows++
				rep.addSample(maxSamples, fmt.Sprintf("%s %s: row has no event stream", label, id))
			}
		}
	})
}

func auditSongs(ctx context.Context, database *toolbeltdb.Database, store *eventsourcing.SQLiteStore, ids []string, maxSamples int, rep *streamReport) error {
	for _, id := range ids {
		rep.Checked++
		events, err := store.Load(ctx, id)
		if err != nil {
			return fmt.Errorf("loading song stream %s: %w", id, err)
		}
		if len(events) == 0 {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("song %s: stream listed but has no events", id))
			continue
		}
		agg, err := song.Replay(id, events)
		if err != nil {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("song %s: unreplayable: %v", id, err))
			continue
		}
		var found bool
		var isRecent int64
		var title, artistName, tidalID, duplicateOf string
		err = database.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT title, artist_name, is_recent, tidal_id, duplicate_of FROM songs WHERE id = ? LIMIT 1;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, id)
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				return nil
			}
			found = true
			title, artistName, isRecent = stmt.ColumnText(0), stmt.ColumnText(1), stmt.ColumnInt64(2)
			tidalID, duplicateOf = stmt.ColumnText(3), stmt.ColumnText(4)
			return nil
		})
		if err != nil {
			return fmt.Errorf("reading song row %s: %w", id, err)
		}
		if !found {
			rep.MissingRows++
			rep.addSample(maxSamples, fmt.Sprintf("song %s: stream v%d has no songs row", id, agg.Version()))
			continue
		}
		wantRecent := int64(0)
		if agg.IsRecent {
			wantRecent = 1
		}
		if title != agg.Title || artistName != agg.ArtistName || isRecent != wantRecent || tidalID != agg.TidalID || duplicateOf != agg.DuplicateOf {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("song %s: row(title=%q artist=%q recent=%d tidal=%q dup=%q) != stream(title=%q artist=%q recent=%d tidal=%q dup=%q)",
				id, title, artistName, isRecent, tidalID, duplicateOf, agg.Title, agg.ArtistName, wantRecent, agg.TidalID, agg.DuplicateOf))
		}
	}
	return auditUnloggedRows(ctx, database, "songs", "id", ids, maxSamples, rep, "song")
}

// jobRowStatus maps a scrapejob stream to its acceptable row statuses.
// Event-first ordering means the stream briefly leads the row, so open
// lifecycle states accept their in-flight neighbors; only terminal states
// are strict. Dead-lettered streams project to failed rows (the row CHECK
// constraint has no dead_lettered value).
func jobRowStatus(status string) []string {
	switch status {
	case scrapejob.StatusSucceeded:
		return []string{scrapejob.StatusSucceeded}
	case scrapejob.StatusDeadLettered:
		return []string{scrapejob.StatusFailed}
	case scrapejob.StatusFailed:
		return []string{scrapejob.StatusFailed, scrapejob.StatusQueued, scrapejob.StatusProcessing}
	case scrapejob.StatusProcessing:
		return []string{scrapejob.StatusProcessing, scrapejob.StatusFailed, scrapejob.StatusQueued}
	default:
		return []string{scrapejob.StatusQueued, scrapejob.StatusProcessing}
	}
}

func auditJobs(ctx context.Context, database *toolbeltdb.Database, store *eventsourcing.SQLiteStore, ids []string, maxSamples int, rep *streamReport, purgedInfos *int) error {
	for _, id := range ids {
		rep.Checked++
		events, err := store.Load(ctx, id)
		if err != nil {
			return fmt.Errorf("loading job stream %s: %w", id, err)
		}
		if len(events) == 0 {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("job %s: stream listed but has no events", id))
			continue
		}
		agg, err := scrapejob.Replay(id, events)
		if err != nil {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("job %s: unreplayable: %v", id, err))
			continue
		}
		var found bool
		var rowStatus string
		err = database.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT status FROM scrape_jobs WHERE request_id = ? LIMIT 1;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, id)
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				return nil
			}
			found = true
			rowStatus = stmt.ColumnText(0)
			return nil
		})
		if err != nil {
			return fmt.Errorf("reading job row %s: %w", id, err)
		}
		if !found {
			// Terminal streams without rows are retention purges: the audit
			// log survives by design. Open streams without rows lost the row.
			if agg.IsTerminal() {
				*purgedInfos++
				continue
			}
			rep.MissingRows++
			rep.addSample(maxSamples, fmt.Sprintf("job %s: open stream (status=%s) has no scrape_jobs row", id, agg.Status()))
			continue
		}
		allowed := jobRowStatus(agg.Status())
		ok := false
		for _, want := range allowed {
			if rowStatus == want {
				ok = true
				break
			}
		}
		if !ok {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("job %s: row(status=%s) not in stream(status=%s) projection %v",
				id, rowStatus, agg.Status(), allowed))
		}
	}
	// Rows without streams: row-first crash windows that never logged the fact.
	return auditUnloggedRows(ctx, database, "scrape_jobs", "request_id", ids, maxSamples, rep, "job")
}

type batchRow struct {
	found     bool
	total     int64
	completed int64
	done      int64
	stats     map[string]int
	statsOK   bool
}

func readBatchRow(ctx context.Context, database *toolbeltdb.Database, id string) (batchRow, error) {
	var row batchRow
	err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT total, completed, done, stats FROM batches WHERE id = ? LIMIT 1;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, id)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		rawStats := stmt.ColumnText(3)
		stats := make(map[string]int)
		statsOK := true
		if strings.TrimSpace(rawStats) != "" {
			if err := json.Unmarshal([]byte(rawStats), &stats); err != nil {
				statsOK = false
			}
		}
		row = batchRow{
			found:     true,
			total:     stmt.ColumnInt64(0),
			completed: stmt.ColumnInt64(1),
			done:      stmt.ColumnInt64(2),
			stats:     stats,
			statsOK:   statsOK,
		}
		return nil
	})
	return row, err
}

func readBatchMembers(ctx context.Context, database *toolbeltdb.Database, batchID string) (map[string]bool, error) {
	members := make(map[string]bool)
	err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT artist_id, done FROM batch_members WHERE batch_id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, batchID)
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				return nil
			}
			artistID := stmt.ColumnText(0)
			isDone := stmt.ColumnInt64(1) == 1
			members[artistID] = isDone
		}
	})
	return members, err
}

func auditBatches(ctx context.Context, database *toolbeltdb.Database, store *eventsourcing.SQLiteStore, ids []string, maxSamples int, rep *streamReport) error {
	type batchAuditStream struct {
		id     string
		events []eventsourcing.Event
	}
	streams := make([]batchAuditStream, 0, len(ids))
	starts := make(map[string]time.Time, len(ids))
	for _, id := range ids {
		rep.Checked++
		events, err := store.Load(ctx, id)
		if err != nil {
			return fmt.Errorf("loading batch stream %s: %w", id, err)
		}
		if len(events) == 0 {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("batch %s: stream listed but has no events", id))
			continue
		}
		streams = append(streams, batchAuditStream{id: id, events: events})
		starts[id] = events[0].CreatedAt
	}
	for _, stream := range streams {
		id, events := stream.id, stream.events
		agg, err := batch.Replay(id, events)
		if err != nil {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("batch %s: unreplayable: %v", id, err))
			continue
		}
		row, err := readBatchRow(ctx, database, id)
		if err != nil {
			return fmt.Errorf("reading batch row %s: %w", id, err)
		}
		if !row.found {
			lastEvent := events[len(events)-1]
			if agg.Done() && batchCouldHaveBeenPruned(id, lastEvent.CreatedAt, starts) {
				continue
			}
			rep.MissingRows++
			rep.addSample(maxSamples, fmt.Sprintf("batch %s: stream v%d has no batches row", id, agg.Version()))
			continue
		}
		wantDone := int64(0)
		if agg.Done() {
			wantDone = 1
		}
		wantTotal := int64(agg.Total())
		wantCompleted := int64(agg.Completed())
		rowStats := fmt.Sprintf("%v", row.stats)
		if !row.statsOK {
			rowStats = "<invalid>"
		}
		if row.total != wantTotal || row.completed != wantCompleted || row.done != wantDone ||
			!row.statsOK || !maps.Equal(row.stats, agg.Stats()) {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("batch %s: row(total=%d completed=%d done=%d stats=%s) != stream(total=%d completed=%d done=%d stats=%v)",
				id, row.total, row.completed, row.done, rowStats, wantTotal, wantCompleted, wantDone, agg.Stats()))
			continue
		}

		members, err := readBatchMembers(ctx, database, id)
		if err != nil {
			return fmt.Errorf("reading batch members %s: %w", id, err)
		}
		aggMembers := agg.Members()
		if len(members) != len(aggMembers) {
			rep.Mismatched++
			rep.addSample(maxSamples, fmt.Sprintf("batch %s: members count row=%d != stream=%d", id, len(members), len(aggMembers)))
			continue
		}
		for artistID, wantMemberDone := range aggMembers {
			gotMemberDone, ok := members[artistID]
			if !ok || gotMemberDone != wantMemberDone {
				rep.Mismatched++
				rep.addSample(maxSamples, fmt.Sprintf("batch %s artist %s: row(found=%v done=%v) != stream(done=%v)",
					id, artistID, ok, gotMemberDone, wantMemberDone))
				break
			}
		}
	}
	return auditUnloggedRows(ctx, database, "batches", "id", ids, maxSamples, rep, "batch")
}

// batchCouldHaveBeenPruned reports whether a later batch start could have run
// the pruning that removed the missing row. Without a later start, an old done
// stream still needs its row; exempting it would hide projection loss. The
// missing row's updated_at is gone, so the final event time is the latest
// surviving timestamp. A later start must clear it by the full retention.
func batchCouldHaveBeenPruned(id string, lastEventAt time.Time, starts map[string]time.Time) bool {
	for otherID, startedAt := range starts {
		if otherID == id {
			continue
		}
		if startedAt.Sub(lastEventAt) > batchprogress.PruneRetention {
			return true
		}
	}
	return false
}
