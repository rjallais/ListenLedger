// Package heal converges SQLite read models with the authoritative event log.
//
// The write path projects synchronously so the UI reads fresh rows, which
// leaves a narrow dual-write window: an append can commit while its
// projection fails (a crash between the two, or a write that errored after
// the fact was durable). This package runs a durable reactor over
// DOMAIN_EVENTS and re-projects any stream whose recorded position lags the
// delivered event, so drift repairs itself instead of waiting for cmd/replay.
//
// Stream types with no SQLite read model here (scrapejob, batch, tidalsync,
// correctionproposal) are acked untouched: scrape jobs and batches have their
// own reconcilers, and the rest are tooling-only aggregates.
package heal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"

	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/projections"
	"ListenLedger/internal/reactor"
	"ListenLedger/internal/readside"
)

// healGrace bounds the window in which a lagging fact is assumed to be an
// in-flight inline projection rather than drift: in jetstream mode the
// publish is visible to this consumer before the write path projects, so a
// young lag is NAKed untouched for one redelivery. The reactor's NakDelay
// (default 5s, equal to the grace) spaces that redelivery, so on the next
// delivery the oldest unprojected fact has aged past the grace and the gap
// heals, even for a stream that never publishes again. The grace ages the
// oldest unprojected fact, not the delivered event: a caught-up consumer
// delivers every event young, so only the gap's age tells an in-flight
// write from a crash. Cost: one extra delivery per young-lag event.
const healGrace = 5 * time.Second

// errYoungLag sends a young gap back for one delayed re-check instead of
// acking it: acking would drop the crash case until another event happens
// to land on the stream.
var errYoungLag = fmt.Errorf("%w: young lag, re-check after grace", reactor.ErrRetryLater)

// Healer re-projects streams whose read models lag the event log.
type Healer struct {
	log     *slog.Logger
	db      *toolbeltdb.Database
	store   eventsourcing.Store
	artists *projections.ArtistProjection
	catalog *projections.CatalogProjection
	// healed maps the stream types this healer can rebuild. Keeping it
	// explicit means a new aggregate type is a visible decision, not a silent
	// fallthrough.
	healed map[string]struct{}
}

// NewHealer builds the read-model healer for the projections this app owns.
func NewHealer(log *slog.Logger, db *toolbeltdb.Database, store eventsourcing.Store,
	artists *projections.ArtistProjection, catalog *projections.CatalogProjection) *Healer {
	if log == nil {
		log = slog.Default()
	}
	return &Healer{
		log:     log,
		db:      db,
		store:   store,
		artists: artists,
		catalog: catalog,
		healed: map[string]struct{}{
			artist.StreamTypeArtist: {},
			album.StreamTypeAlbum:   {},
			song.StreamTypeSong:     {},
		},
	}
}

// HandleEvent implements reactor.Handler: ack when the read model already
// reflects the event, repair it when it does not, and never spin on facts
// that cannot be repaired.
func (h *Healer) HandleEvent(ctx context.Context, evt eventsourcing.Event) error {
	if _, ok := h.healed[evt.StreamType]; !ok {
		return nil
	}

	position, err := readside.Position(ctx, h.db, evt.StreamID)
	if err != nil {
		return err
	}
	if position >= evt.Version {
		// The inline projection committed this event (or a later one) already.
		return nil
	}
	// The delivered event is the only unprojected fact: grace-check it
	// without loading the stream (common jetstream-mode first delivery).
	if position+1 == evt.Version {
		if age := time.Since(evt.CreatedAt); age < healGrace {
			h.log.Debug("healer: young lag, nak for re-check after grace",
				"stream_id", evt.StreamID, "age", age)
			return errYoungLag
		}
	}
	events, err := h.store.Load(ctx, evt.StreamID)
	if err != nil {
		if errors.Is(err, eventsourcing.ErrStreamNotFound) {
			// The fact is not readable from the authoritative log, so no retry
			// can rebuild the row. Term the delivery and let audit/replay speak
			// for the missing stream instead of a stuck consumer.
			return fmt.Errorf("%w: stream %s has no readable events", reactor.ErrUnprocessable, evt.StreamID)
		}
		return fmt.Errorf("healer: loading stream %s: %w", evt.StreamID, err)
	}
	// Grace-check the oldest unprojected fact, not the delivered one: a
	// caught-up consumer delivers every event young, so only the gap's age
	// tells an in-flight write from a crash.
	for _, e := range events {
		if e.Version <= position {
			continue
		}
		if age := time.Since(e.CreatedAt); age < healGrace {
			// The write path may still be projecting this fact; re-playing the
			// stream now would race it. NAK once: the delayed redelivery sees an
			// aged gap (or a caught-up position) and converges either way.
			h.log.Debug("healer: young lag, nak for re-check after grace",
				"stream_id", evt.StreamID, "age", age)
			return errYoungLag
		}
		break
	}

	h.log.Info("healer: read model lags the event log, re-projecting stream",
		"stream_id", evt.StreamID, "stream_type", evt.StreamType,
		"projected_position", position, "event_version", evt.Version, "event_id", evt.ID)

	if err := h.healStream(ctx, evt.StreamType, evt.StreamID, events); err != nil {
		if errors.Is(err, eventsourcing.ErrStreamNotFound) {
			// The fact is not readable from the authoritative log, so no retry
			// can rebuild the row. Term the delivery and let audit/replay speak
			// for the missing stream instead of a stuck consumer.
			return fmt.Errorf("%w: stream %s has no readable events", reactor.ErrUnprocessable, evt.StreamID)
		}
		return err
	}
	return nil
}

// healStream rebuilds one stream's read model from the authoritative log.
// HandleEvent loads the events (and owns the not-found term). The fold runs
// outside the projection transaction, so Project re-checks
// read_model_positions inside its own WriteTX and skips events a
// concurrent (inline) projection already committed: re-applying older
// events would regress the row while the monotonic position stays ahead.
func (h *Healer) healStream(ctx context.Context, streamType, streamID string, events []eventsourcing.Event) error {
	switch streamType {
	case artist.StreamTypeArtist:
		agg, err := artist.Replay(streamID, events)
		if err != nil {
			if errors.Is(err, eventsourcing.ErrStreamNotFound) {
				return fmt.Errorf("healer: folding artist %s: %w", streamID, err)
			}
			// Deterministic: the same events fold the same way on every retry.
			return fmt.Errorf("%w: healer: folding artist %s: %w", reactor.ErrUnprocessable, streamID, err)
		}
		return h.artists.Project(ctx, agg, events)
	case album.StreamTypeAlbum:
		// The fold gates; the catalog projection consumes the events itself.
		if _, err := album.Replay(streamID, events); err != nil {
			if errors.Is(err, eventsourcing.ErrStreamNotFound) {
				return fmt.Errorf("healer: folding album %s: %w", streamID, err)
			}
			// Deterministic: the same events fold the same way on every retry.
			return fmt.Errorf("%w: healer: folding album %s: %w", reactor.ErrUnprocessable, streamID, err)
		}
		return h.catalog.Project(ctx, streamType, events)
	case song.StreamTypeSong:
		if _, err := song.Replay(streamID, events); err != nil {
			if errors.Is(err, eventsourcing.ErrStreamNotFound) {
				return fmt.Errorf("healer: folding song %s: %w", streamID, err)
			}
			// Deterministic: the same events fold the same way on every retry.
			return fmt.Errorf("%w: healer: folding song %s: %w", reactor.ErrUnprocessable, streamID, err)
		}
		return h.catalog.Project(ctx, streamType, events)
	default:
		return nil
	}
}
