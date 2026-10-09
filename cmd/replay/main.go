// Command replay rebuilds event-sourced read models from stored event streams.
//
// Safety: rebuild in staging first, then production. Dry run is the default;
// nothing is deleted or rewritten unless --apply is passed. The event log
// itself is never mutated — only disposable read models (artists history,
// albums/songs, batches/members) are cleared and refolded.
//
// Stop the live app first or point --data-dir at a backup copy (see
// cmd/safebackup, cmd/backfill_song_artists pattern). Applying against the
// live default data dir requires --live-ok or LISTENLEDGER_REPLAY_LIVE_OK=1.
//
// Usage:
//
//	go run ./cmd/replay [--data-dir pb_data] [--streams artist,album,song,batch] [--apply] [--live-ok] [--since-checkpoint]
//
// --since-checkpoint skips the reset and re-folds idempotently from saved
// projection checkpoints (recorded per projection on every --apply).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"ListenLedger/internal/appdir"
	"ListenLedger/internal/batchprogress"
	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/domain/batch"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/projections"
)

func main() {
	var (
		dataDir         string
		streams         string
		apply           bool
		liveOK          bool
		sinceCheckpoint bool
	)
	flag.StringVar(&dataDir, "data-dir", appdir.ResolveDataDir(), "Directory holding sqlite/ledger.sqlite")
	flag.StringVar(&streams, "streams", "artist,album,song,batch", "Comma-separated stream types to replay")
	flag.BoolVar(&apply, "apply", false, "Actually clear and rebuild read models (default is dry run)")
	flag.BoolVar(&liveOK, "live-ok", false, "Allow --apply against the live default data dir")
	flag.BoolVar(&sinceCheckpoint, "since-checkpoint", false, "Resume from saved projection checkpoints (no reset; idempotent re-fold)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	selectedStreams, err := parseStreamTypes(streams)
	if err != nil {
		logger.Error("invalid stream selection", "error", err)
		os.Exit(2)
	}

	if !apply {
		logger.Info("dry run: no changes will be made (pass --apply to rebuild)",
			"data_dir", dataDir, "streams", streams)
		if err := dryRun(context.Background(), logger, dataDir, selectedStreams); err != nil {
			logger.Error("dry run failed", "error", err)
			os.Exit(1)
		}
		return
	}

	if !liveOK && os.Getenv("LISTENLEDGER_REPLAY_LIVE_OK") != "1" && isLiveDataDir(dataDir) {
		logger.Error("refusing --apply against the live data dir: stop the app or replay a backup copy",
			"data_dir", dataDir, "hint", "go run ./cmd/safebackup, then go run ./cmd/replay --data-dir <backup> --apply; or pass --live-ok")
		os.Exit(2)
	}

	if err := replay(context.Background(), logger, dataDir, selectedStreams, sinceCheckpoint); err != nil {
		logger.Error("replay failed", "error", err)
		os.Exit(1)
	}
}

// isLiveDataDir reports whether dataDir is the live default (pb_data via
// PB_DATA_DIR or repo root) rather than an explicit staging/backup copy.
// Paths are cleaned to absolute form so relative paths and trailing slashes
// cannot bypass the --apply guard.
func isLiveDataDir(dataDir string) bool {
	abs, err := filepath.Abs(filepath.Clean(dataDir))
	if err != nil {
		return true
	}
	// Resolve symlinks (e.g. /tmp on some systems) before comparing, so
	// equivalent paths cannot bypass the --apply guard. Unresolvable
	// paths (e.g. not-yet-created staging dirs) fall back to lexical form.
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	liveDefault := appdir.ResolveDataDir()
	liveAbs, err := filepath.Abs(filepath.Clean(liveDefault))
	if err != nil {
		return true
	}
	if resolved, err := filepath.EvalSymlinks(liveAbs); err == nil {
		liveAbs = resolved
	}
	if abs == liveAbs {
		return true
	}
	// PB_DATA_DIR unset + a bare pb_data path also means live.
	if os.Getenv("PB_DATA_DIR") == "" && filepath.Base(abs) == "pb_data" {
		return true
	}
	return false
}

// dryRun lists matching streams and event counts without touching read models.
func dryRun(ctx context.Context, logger *slog.Logger, dataDir string, selectedStreams map[string]struct{}) error {
	database, err := db.SetupDB(ctx, logger, dataDir, false)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			logger.Warn("failed closing database", "error", err)
		}
	}()

	store := eventsourcing.NewSQLiteStore(database)
	streamInfos, err := store.Streams(ctx)
	if err != nil {
		return fmt.Errorf("listing streams: %w", err)
	}
	var matched, totalEvents int
	counts := make(map[string]int)
	for _, stream := range streamInfos {
		if _, ok := selectedStreams[stream.Type]; !ok {
			continue
		}
		events, err := store.Load(ctx, stream.ID)
		if err != nil {
			return fmt.Errorf("loading stream %s: %w", stream.ID, err)
		}
		matched++
		totalEvents += len(events)
		counts[stream.Type] += len(events)
	}
	checkpoints := make(map[string]int64)
	for _, name := range []string{
		eventsourcing.CheckpointArtistProjection,
		eventsourcing.CheckpointAlbumProjection,
		eventsourcing.CheckpointSongProjection,
		eventsourcing.CheckpointBatchProjection,
		eventsourcing.CheckpointDomainEventsRelay,
	} {
		if pos, err := store.GetCheckpoint(ctx, name); err == nil && pos > 0 {
			checkpoints[name] = pos
		}
	}
	tails, err := tailCounts(ctx, store, selectedStreams)
	if err != nil {
		return fmt.Errorf("counting pending tail: %w", err)
	}
	logger.Info("dry run plan", "streams", matched, "events", totalEvents, "by_type", counts,
		"checkpoints", checkpoints, "pending_tail", tails,
		"next", "verify in staging, then re-run with --apply (add --since-checkpoint to resume)")
	return nil
}

// tailCounts reports per-type events after each projection's checkpoint: what
// --since-checkpoint would re-project. Missing checkpoints count everything.
func tailCounts(ctx context.Context, store *eventsourcing.SQLiteStore, selectedStreams map[string]struct{}) (map[string]int, error) {
	tails := make(map[string]int, len(selectedStreams))
	for _, streamType := range slices.Sorted(maps.Keys(selectedStreams)) {
		var floor int64
		if pos, err := store.GetCheckpoint(ctx, checkpointFor(streamType)); err == nil {
			floor = pos
		}
		count := 0
		pos := floor
		for {
			events, err := store.LoadFromGlobalPosition(ctx, pos, 500)
			if err != nil {
				return nil, fmt.Errorf("loading events after position %d: %w", pos, err)
			}
			if len(events) == 0 {
				break
			}
			for _, evt := range events {
				if evt.StreamType == streamType {
					count++
				}
				if evt.GlobalPosition > pos {
					pos = evt.GlobalPosition
				}
			}
		}
		tails[streamType] = count
	}
	return tails, nil
}

func parseStreamTypes(value string) (map[string]struct{}, error) {
	selected := make(map[string]struct{})
	for streamType := range strings.SplitSeq(value, ",") {
		streamType = strings.TrimSpace(streamType)
		if streamType == "" {
			continue
		}

		switch streamType {
		case artist.StreamTypeArtist, album.StreamTypeAlbum, song.StreamTypeSong, batch.StreamTypeBatch:
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

// checkpointFor maps a replayed stream type to its projection checkpoint.
func checkpointFor(streamType string) string {
	switch streamType {
	case artist.StreamTypeArtist:
		return eventsourcing.CheckpointArtistProjection
	case album.StreamTypeAlbum:
		return eventsourcing.CheckpointAlbumProjection
	case song.StreamTypeSong:
		return eventsourcing.CheckpointSongProjection
	case batch.StreamTypeBatch:
		return eventsourcing.CheckpointBatchProjection
	default:
		return ""
	}
}

// logResumeCheckpoint reports the saved resume position for a projection.
// Lookup failures warn (fail-open: replay proceeds from zero) since a
// checkpoint read must never block a rebuild.
func logResumeCheckpoint(ctx context.Context, logger *slog.Logger, store *eventsourcing.SQLiteStore, projection string) {
	pos, err := store.GetCheckpoint(ctx, projection)
	if err != nil {
		logger.Warn("checkpoint lookup failed, resuming from zero", "projection", projection, "error", err)
		return
	}
	logger.Info("resuming from checkpoint (no reset)", "projection", projection, "position", pos)
}

func replay(ctx context.Context, logger *slog.Logger, dataDir string, selectedStreams map[string]struct{}, sinceCheckpoint bool) error {
	database, err := db.SetupDB(ctx, logger, dataDir, false)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			logger.Warn("failed closing database", "error", err)
		}
	}()

	store := eventsourcing.NewSQLiteStore(database)
	artistProj := projections.NewArtistProjection(logger, database, nil)
	catalogProj := projections.NewCatalogProjection(logger, database)
	batchProj := batchprogress.NewStore(database)

	streamInfos, err := store.Streams(ctx)
	if err != nil {
		return fmt.Errorf("listing streams: %w", err)
	}
	streamsToReplay := make([]eventsourcing.StreamInfo, 0, len(streamInfos))
	for _, stream := range streamInfos {
		if _, ok := selectedStreams[stream.Type]; ok {
			streamsToReplay = append(streamsToReplay, stream)
		}
	}

	if _, ok := selectedStreams[artist.StreamTypeArtist]; ok {
		if sinceCheckpoint {
			logResumeCheckpoint(ctx, logger, store, eventsourcing.CheckpointArtistProjection)
		} else if err := artistProj.ResetForReplay(ctx); err != nil {
			return fmt.Errorf("resetting artist projection for replay: %w", err)
		}
	}
	var catalogStreams []string
	for _, streamType := range []string{album.StreamTypeAlbum, song.StreamTypeSong} {
		if _, ok := selectedStreams[streamType]; !ok {
			continue
		}
		catalogStreams = append(catalogStreams, streamType)
	}
	if sinceCheckpoint {
		for _, streamType := range catalogStreams {
			logResumeCheckpoint(ctx, logger, store, checkpointFor(streamType))
		}
	} else if err := catalogProj.ResetForReplay(ctx, catalogStreams...); err != nil {
		return fmt.Errorf("resetting catalog projection for replay: %w", err)
	}
	if _, ok := selectedStreams[batch.StreamTypeBatch]; ok {
		if sinceCheckpoint {
			logResumeCheckpoint(ctx, logger, store, eventsourcing.CheckpointBatchProjection)
		} else if err := batchProj.ResetForReplay(ctx); err != nil {
			return fmt.Errorf("resetting batch projection for replay: %w", err)
		}
	}

	replayedEvents := 0
	replayedStreams := 0
	frontiers := make(map[string]int64)
	project := func(stream eventsourcing.StreamInfo) error {
		n, err := projectStream(ctx, logger, store, artistProj, catalogProj, batchProj, stream, frontiers)
		replayedEvents += n
		if n > 0 {
			replayedStreams++
		}
		return err
	}

	if sinceCheckpoint {
		if err := replayIncremental(ctx, logger, store, streamInfos, selectedStreams, project); err != nil {
			return err
		}
		return saveFrontiers(ctx, logger, store, frontiers, replayedEvents, replayedStreams)
	}
	for _, stream := range streamsToReplay {
		if err := project(stream); err != nil {
			return err
		}
	}

	return saveFrontiers(ctx, logger, store, frontiers, replayedEvents, replayedStreams)
}

// projectStream folds one stream through its projector and records its
// frontier. It returns the number of events folded (0 when the stream is
// empty). Projectors are idempotent, so re-folding is always safe.
func projectStream(ctx context.Context, logger *slog.Logger, store *eventsourcing.SQLiteStore, artistProj *projections.ArtistProjection, catalogProj *projections.CatalogProjection, batchProj *batchprogress.Store, stream eventsourcing.StreamInfo, frontiers map[string]int64) (int, error) {
	streamID := stream.ID
	events, err := store.Load(ctx, streamID)
	if err != nil {
		return 0, fmt.Errorf("loading stream %s: %w", streamID, err)
	}
	if len(events) == 0 {
		return 0, nil
	}

	streamType := stream.Type
	if events[0].StreamType != streamType {
		return 0, fmt.Errorf("stream %s changed type from %q to %q during replay", streamID, streamType, events[0].StreamType)
	}

	switch streamType {
	case artist.StreamTypeArtist:
		agg, err := artist.Replay(streamID, events)
		if err != nil {
			return 0, fmt.Errorf("replaying artist stream %s: %w", streamID, err)
		}
		if err := artistProj.Project(ctx, agg, events); err != nil {
			return 0, fmt.Errorf("projecting artist stream %s: %w", streamID, err)
		}
	case album.StreamTypeAlbum, song.StreamTypeSong:
		if err := catalogProj.Project(ctx, streamType, events); err != nil {
			return 0, fmt.Errorf("projecting %s stream %s: %w", streamType, streamID, err)
		}
	case batch.StreamTypeBatch:
		agg, err := batch.Replay(streamID, events)
		if err != nil {
			return 0, fmt.Errorf("replaying batch stream %s: %w", streamID, err)
		}
		if err := batchProj.ProjectAggregate(ctx, agg); err != nil {
			return 0, fmt.Errorf("projecting batch stream %s: %w", streamID, err)
		}
	default:
		return 0, fmt.Errorf("stream %s has unsupported type %q", streamID, streamType)
	}
	for _, evt := range events {
		if evt.GlobalPosition > frontiers[checkpointFor(streamType)] {
			frontiers[checkpointFor(streamType)] = evt.GlobalPosition
		}
	}
	logger.Info("replayed stream", "stream", streamID, "type", streamType, "events", len(events))
	return len(events), nil
}

// replayIncremental re-projects only streams touched after the saved resume
// floor (the minimum checkpoint across selected projections), without
// resetting anything. Idempotent projectors make re-folding safe; untouched
// streams are skipped entirely, so resume cost scales with new facts.
func replayIncremental(ctx context.Context, logger *slog.Logger, store *eventsourcing.SQLiteStore, streamInfos []eventsourcing.StreamInfo, selectedStreams map[string]struct{}, project func(eventsourcing.StreamInfo) error) error {
	infoByID := make(map[string]eventsourcing.StreamInfo, len(streamInfos))
	for _, info := range streamInfos {
		if _, ok := selectedStreams[info.Type]; ok {
			infoByID[info.ID] = info
		}
	}

	floor := int64(0)
	floorSet := false
	for streamType := range selectedStreams {
		pos, err := store.GetCheckpoint(ctx, checkpointFor(streamType))
		if err != nil {
			logger.Warn("checkpoint lookup failed, resuming from zero for projection", "projection", checkpointFor(streamType), "error", err)
			floor, floorSet = 0, true
			break
		}
		if !floorSet || pos < floor {
			floor, floorSet = pos, true
		}
	}

	touched := make(map[string]eventsourcing.StreamInfo)
	pos := floor
	for {
		events, err := store.LoadFromGlobalPosition(ctx, pos, 500)
		if err != nil {
			return fmt.Errorf("loading events after position %d: %w", pos, err)
		}
		if len(events) == 0 {
			break
		}
		for _, evt := range events {
			if info, ok := infoByID[evt.StreamID]; ok {
				touched[evt.StreamID] = info
			}
			if evt.GlobalPosition > pos {
				pos = evt.GlobalPosition
			}
		}
	}
	logger.Info("incremental resume plan", "floor", floor, "touched_streams", len(touched))

	for _, id := range slices.Sorted(maps.Keys(touched)) {
		if err := project(touched[id]); err != nil {
			return err
		}
	}
	return nil
}

// saveFrontiers records per-projection frontiers so the next
// --since-checkpoint resume (and lag checks) observe replay progress.
// Positions only advance; a lookup failure proceeds to save (fail-open) with
// a warning.
func saveFrontiers(ctx context.Context, logger *slog.Logger, store *eventsourcing.SQLiteStore, frontiers map[string]int64, replayedEvents, replayedStreams int) error {
	for name, pos := range frontiers {
		if pos <= 0 {
			continue
		}
		prev, err := store.GetCheckpoint(ctx, name)
		if err != nil {
			logger.Warn("checkpoint lookup failed, saving frontier anyway", "projection", name, "error", err)
		} else if prev >= pos {
			continue
		}
		if err := store.SaveCheckpoint(ctx, name, pos); err != nil {
			return fmt.Errorf("saving %s checkpoint: %w", name, err)
		}
	}

	logger.Info("replay complete", "events", replayedEvents, "streams", replayedStreams, "checkpoints", frontiers)
	return nil
}
