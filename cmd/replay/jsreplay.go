// JetStream-sourced replay for jetstream mode.
//
// In sqlite mode replay folds the SQLite log (see replay in main.go). In
// jetstream mode the SQLite log is frozen pre-flip history, so rebuilding
// from it would erase post-flip facts — replay here enumerates streams from
// DOMAIN_EVENTS subject details and folds each stream through the same
// projectors via JetStreamStore.Load (cutoff-bounded: the rebuild sees a
// consistent prefix, never a moving tail).
//
// Differences from the SQLite path, all deliberate:
//   - Enumeration comes from stream subject details, not Streams(); the
//     single-type-per-ID invariant is enforced the same way.
//   - No --since-checkpoint: checkpoints are SQLite rowids, meaningless for
//     JetStream positions. Replays are always full (idempotent projectors
//     make re-folding safe).
//   - No frontier saving: replayed events carry no GlobalPosition, so the
//     frontiers map stays empty and saveFrontiers only logs completion.
//   - Tooling never ensures stream config: CreateOrUpdate from here could
//     downgrade the authority stream's retention. The stream must exist
//     (the app ensures it at every boot).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/batchprogress"
	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/domain/batch"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
	"ListenLedger/internal/projections"
)

// openReplayJetStream boots an embedded NATS server on the data dir's
// existing NATS store and returns a JetStream context over DOMAIN_EVENTS.
// The app must be stopped first (same rule as --apply on SQLite): two
// servers on one store dir corrupt it. Never ensures stream config.
func openReplayJetStream(ctx context.Context, dataDir string) (jetstream.JetStream, func(), error) {
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true,
		JetStream: true, StoreDir: filepath.Join(dataDir, "nats"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("starting embedded NATS: %w", err)
	}
	go ns.Start()
	cleanup := func() { ns.Shutdown() }
	if !ns.ReadyForConnections(10 * time.Second) {
		cleanup()
		return nil, nil, fmt.Errorf("embedded NATS not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("connecting embedded NATS: %w", err)
	}
	cleanupBoth := func() { nc.Close(); ns.Shutdown() }
	js, err := messaging.NewJetStream(nc)
	if err != nil {
		cleanupBoth()
		return nil, nil, fmt.Errorf("creating JetStream context: %w", err)
	}
	if _, err := js.Stream(ctx, messaging.DomainEventsStreamName); err != nil {
		cleanupBoth()
		return nil, nil, fmt.Errorf("DOMAIN_EVENTS stream: %w", err)
	}
	return js, cleanupBoth, nil
}

// jsStreamInfos enumerates replayable streams from DOMAIN_EVENTS subject
// details (subject -> count), parsing each subject back to (type, ID).
// Streams are sorted by ID for a deterministic replay order. A subject that
// does not parse, or an ID claimed by two types, fails the enumeration —
// the log's shape invariant must hold before a rebuild.
//
// Gotcha: plain stream.Info omits subject details; the SubjectFilter option
// is what asks the server for them (the client pages the full map).
func jsStreamInfos(ctx context.Context, js jetstream.JetStream, selectedStreams map[string]struct{}) ([]eventsourcing.StreamInfo, error) {
	stream, err := js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		return nil, fmt.Errorf("opening stream: %w", err)
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(messaging.SubjectDomainEventsWildcard))
	if err != nil {
		return nil, fmt.Errorf("reading stream info: %w", err)
	}
	byID := make(map[string]*eventsourcing.StreamInfo)
	for subject := range info.State.Subjects {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		streamType, streamID, _, err := messaging.ParseSubjectDomainEvent(subject)
		if err != nil {
			return nil, fmt.Errorf("enumerating streams: %w", err)
		}
		existing, ok := byID[streamID]
		if !ok {
			byID[streamID] = &eventsourcing.StreamInfo{ID: streamID, Type: streamType}
			continue
		}
		if existing.Type != streamType {
			return nil, fmt.Errorf("stream %s has aggregate types %q and %q", streamID, existing.Type, streamType)
		}
	}
	infos := make([]eventsourcing.StreamInfo, 0, len(byID))
	for _, info := range byID {
		if _, ok := selectedStreams[info.Type]; ok {
			infos = append(infos, *info)
		}
	}
	slices.SortFunc(infos, func(a, b eventsourcing.StreamInfo) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		default:
			return 0
		}
	})
	return infos, nil
}

// runJetStreamReplay serves both dry-run and apply in jetstream mode, where
// the SQLite log is frozen pre-flip history and DOMAIN_EVENTS is the source.
// Full replay only: rowid checkpoints are meaningless for JetStream
// positions, so --since-checkpoint is refused. Both paths boot an embedded
// server on the data dir's NATS store, so the live-dir stop-first discipline
// applies to dry-run too (a second server on a live store corrupts it).
func runJetStreamReplay(logger *slog.Logger, dataDir string, selectedStreams map[string]struct{}, apply, liveOK, sinceCheckpoint bool) {
	if sinceCheckpoint {
		logger.Error("refusing --since-checkpoint in jetstream mode: checkpoints are SQLite rowids",
			"hint", "re-run without --since-checkpoint for a full JetStream rebuild")
		os.Exit(2)
	}
	if !liveOK && os.Getenv("LISTENLEDGER_REPLAY_LIVE_OK") != "1" && isLiveDataDir(dataDir) {
		logger.Error("refusing jetstream replay against the live data dir: stop the app first (it boots an embedded NATS server on it)",
			"data_dir", dataDir, "hint", "stop the app, or replay a copy containing the nats/ store; or pass --live-ok")
		os.Exit(2)
	}
	if !apply {
		if err := dryRunJS(context.Background(), logger, dataDir, selectedStreams); err != nil {
			logger.Error("dry run failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if err := replayFromJetStream(context.Background(), logger, dataDir, selectedStreams); err != nil {
		logger.Error("replay failed", "error", err)
		os.Exit(1)
	}
}

// dryRunJS reports what a JetStream-sourced rebuild would fold, without
// touching read models: stream and event counts per selected type straight
// from subject details.
func dryRunJS(ctx context.Context, logger *slog.Logger, dataDir string, selectedStreams map[string]struct{}) error {
	js, cleanup, err := openReplayJetStream(ctx, dataDir)
	if err != nil {
		return err
	}
	defer cleanup()
	infos, err := jsStreamInfos(ctx, js, selectedStreams)
	if err != nil {
		return err
	}
	streamsByType := make(map[string]int)
	for _, info := range infos {
		streamsByType[info.Type]++
	}
	stream, err := js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		return fmt.Errorf("opening stream: %w", err)
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(messaging.SubjectDomainEventsWildcard))
	if err != nil {
		return fmt.Errorf("reading stream info: %w", err)
	}
	eventsByType := make(map[string]uint64)
	for subject, count := range info.State.Subjects {
		streamType, _, _, err := messaging.ParseSubjectDomainEvent(subject)
		if err != nil {
			return fmt.Errorf("dry run: %w", err)
		}
		if _, ok := selectedStreams[streamType]; ok {
			eventsByType[streamType] += count
		}
	}
	logger.Info("dry run plan (jetstream source)",
		"streams", len(infos), "streams_by_type", streamsByType, "events_by_type", eventsByType,
		"next", "verify in staging, then re-run with --apply (full replay; no checkpoints in jetstream mode)")
	return nil
}

// replayFromJetStream rebuilds read models from the JetStream log: reset
// projections, fold every selected stream, log completion. Always a full
// replay — --since-checkpoint is refused by the caller in jetstream mode.
func replayFromJetStream(ctx context.Context, logger *slog.Logger, dataDir string, selectedStreams map[string]struct{}) error {
	database, err := db.SetupDB(ctx, logger, dataDir, false)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			logger.Warn("failed closing database", "error", err)
		}
	}()

	js, cleanup, err := openReplayJetStream(ctx, dataDir)
	if err != nil {
		return err
	}
	defer cleanup()
	jsStore := eventsourcing.NewJetStreamStore(js)

	// The SQLite store still backs checkpoints/frontiers helpers and the
	// batch projector's tables; the event source is JetStream.
	sqliteStore := eventsourcing.NewSQLiteStore(database)
	artistProj := projections.NewArtistProjection(logger, database, nil)
	catalogProj := projections.NewCatalogProjection(logger, database)
	batchProj := batchprogress.NewStore(database, jsStore)

	infos, err := jsStreamInfos(ctx, js, selectedStreams)
	if err != nil {
		return err
	}

	if _, ok := selectedStreams[artist.StreamTypeArtist]; ok {
		if err := artistProj.ResetForReplay(ctx); err != nil {
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
	if err := catalogProj.ResetForReplay(ctx, catalogStreams...); err != nil {
		return fmt.Errorf("resetting catalog projection for replay: %w", err)
	}
	if _, ok := selectedStreams[batch.StreamTypeBatch]; ok {
		if err := batchProj.ResetForReplay(ctx); err != nil {
			return fmt.Errorf("resetting batch projection for replay: %w", err)
		}
	}

	replayedEvents := 0
	replayedStreams := 0
	frontiers := make(map[string]int64)
	for _, stream := range infos {
		n, err := projectStream(ctx, logger, jsStore, artistProj, catalogProj, batchProj, stream, frontiers)
		replayedEvents += n
		if n > 0 {
			replayedStreams++
		}
		if err != nil {
			return err
		}
	}

	return saveFrontiers(ctx, logger, sqliteStore, frontiers, replayedEvents, replayedStreams)
}
