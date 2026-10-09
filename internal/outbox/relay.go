// Package outbox relays SQLite outbox rows to JetStream DOMAIN_EVENTS.
//
// Write path: SQLiteStore.Append writes events + outbox rows in one
// transaction. This relay publishes unpublished rows (MsgID = event ID) and
// marks them published. Projections keep their synchronous publish for
// low-latency SSE; the relay is the durability backstop that closes the
// dual-write loss window (commit succeeded, sync publish crashed).
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
)

const (
	// PollInterval paces the relay loop. Crash catch-up happens at most one
	// interval after boot; steady-state lag stays under it.
	PollInterval = 5 * time.Second
	// BatchSize bounds one relay pass so a huge backlog (e.g. migration
	// backfill) drains over several ticks instead of one giant publish burst.
	BatchSize = 100
	// PublishTimeout bounds a single JetStream publish.
	PublishTimeout = 5 * time.Second
)

// Relay drains unpublished outbox events into JetStream.
type Relay struct {
	log   *slog.Logger
	db    *toolbeltdb.Database
	store *eventsourcing.SQLiteStore
	js    jetstream.JetStream
}

// NewRelay creates a Relay that drains unpublished outbox rows from db into js.
func NewRelay(log *slog.Logger, db *toolbeltdb.Database, js jetstream.JetStream) *Relay {
	if log == nil {
		log = slog.Default()
	}
	return &Relay{
		log:   log,
		db:    db,
		store: eventsourcing.NewSQLiteStore(db),
		js:    js,
	}
}

// RelayOnce publishes one batch of unpublished events and marks them.
// A crash between publish and mark republishes next tick; MsgID dedups.
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	if r.js == nil {
		return 0, fmt.Errorf("outbox relay: JetStream not configured")
	}
	events, err := r.store.UnpublishedEvents(ctx, BatchSize)
	if err != nil {
		return 0, fmt.Errorf("outbox relay: load unpublished: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}

	published := make([]string, 0, len(events))
	var frontier int64
	frontierContiguous := true
	for _, evt := range events {
		if err := ctx.Err(); err != nil {
			break
		}
		subject := messaging.SubjectDomainEvent(evt.StreamType, evt.StreamID, evt.EventType)
		data, err := evt.Bytes()
		if err != nil {
			// Poison row: deterministic encode failure retries forever and
			// pins UnpublishedCount. The SQLite event log keeps the fact and
			// projections use the synchronous path, so JetStream catch-up may
			// skip it: mark published to advance past it.
			r.log.Error("outbox relay: encode event, dropping from queue", "event_id", evt.ID, "error", err)
			published = append(published, evt.ID)
			frontierContiguous = false
			continue
		}
		pubCtx, cancel := context.WithTimeout(ctx, PublishTimeout)
		_, pubErr := messaging.PublishDomainEvent(pubCtx, r.js, subject, evt.ID, data)
		cancel()
		if pubErr != nil {
			r.log.Error("outbox relay: publish event", "subject", subject, "event_id", evt.ID, "error", pubErr)
			frontierContiguous = false
			continue
		}
		published = append(published, evt.ID)
		// The frontier advances only through the contiguous published prefix
		// (events arrive ordered by GlobalPosition): a gap means an earlier
		// event is still unpublished, so later positions must not advance it.
		if frontierContiguous && evt.GlobalPosition > frontier {
			frontier = evt.GlobalPosition
		}
	}

	if len(published) > 0 {
		// Persist on a detached context: if the caller cancelled mid-batch,
		// already-published events must still be marked (otherwise they
		// redeliver as duplicates next tick) and the frontier checkpoint
		// must still advance. MsgID dedups any redelivery regardless.
		persistCtx, cancel := context.WithTimeout(context.Background(), PublishTimeout)
		defer cancel()
		if err := r.store.MarkPublished(persistCtx, published); err != nil {
			return len(published), fmt.Errorf("outbox relay: mark published: %w", err)
		}
		// Publish frontier: record the highest global position drained so
		// replay/resume tooling and lag checks observe relay progress.
		// Best-effort: outbox rows stay the resumption source of truth, and
		// only actually-published positions advance the frontier.
		if frontier > 0 {
			if err := r.store.SaveCheckpoint(persistCtx, eventsourcing.CheckpointDomainEventsRelay, frontier); err != nil {
				r.log.Warn("outbox relay: save checkpoint failed", "error", err)
			}
		}
	}
	if lag := len(events) - len(published); lag > 0 {
		r.log.Warn("outbox relay: publish failures, will retry", "failed", lag, "published", len(published))
	}
	return len(published), nil
}

// Run drains the outbox every PollInterval until ctx is done. Startup
// catch-up (migration backfill, crash backlog) happens on the first tick.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(PollInterval)
	defer ticker.Stop()

	if n, err := r.RelayOnce(ctx); err != nil {
		r.log.Warn("outbox relay: initial pass failed", "error", err)
	} else if n > 0 {
		r.log.Info("outbox relay: initial catch-up published", "events", n)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := r.RelayOnce(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				r.log.Warn("outbox relay: pass failed", "error", err)
				continue
			}
			if n > 0 {
				r.log.Info("outbox relay: published batch", "events", n)
			}
		}
	}
}
