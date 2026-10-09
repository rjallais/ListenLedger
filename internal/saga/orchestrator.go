// Package saga tracks scrapejob→artist sagas as durable instances.
//
// The job event stream stays the authority for lifecycle facts; the
// saga_instances table is the queryable state index this orchestrator
// refreshes from those streams. No handler or worker calls into it — Tick
// derives everything from scrape_jobs rows + job streams, so tracking is
// eventually consistent and adds no write-path coupling. Timeout enforcement
// stays with the worker stale sweeper; the orchestrator only makes saga
// states (and staleness) observable, and History joins them per request.
package saga

import (
	"context"
	"log"
	"strings"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/eventsourcing"
)

// Saga states. Lowercase to match row/status conventions elsewhere.
const (
	StateRequested  = "requested"
	StateProcessing = "processing"
	StateFailed     = "failed"
	StateDone       = "done"
	StateDead       = "dead"
)

// TerminalStates are never refreshed once reached (except by prune).
func TerminalStates() []string { return []string{StateDone, StateDead} }

const (
	// liveScanLimit bounds one Tick's discovery of live saga rows.
	liveScanLimit = 200
	// refreshLimit bounds one Tick's re-derivation of open instances.
	refreshLimit = 500
	// terminalRetention mirrors the scrape_jobs succeeded retention: terminal
	// instances older than this are pruned by Tick.
	terminalRetention = 7 * 24 * time.Hour
	// timestampFormat matches scrape_jobs timestamps so lexicographic
	// comparison stays chronological.
	timestampFormat = "2006-01-02 15:04:05.000Z"
)

// Orchestrator refreshes saga_instances from job streams.
type Orchestrator struct {
	db    *toolbeltdb.Database
	store *eventsourcing.SQLiteStore
}

// NewOrchestrator creates an Orchestrator. A nil db disables Tick (fail-open).
func NewOrchestrator(db *toolbeltdb.Database) *Orchestrator {
	var store *eventsourcing.SQLiteStore
	if db != nil {
		store = eventsourcing.NewSQLiteStore(db)
	}
	return &Orchestrator{db: db, store: store}
}

// DeriveState maps a job aggregate to its saga state. A nil job (row exists
// but no stream yet — crash between row create and event append) reads as
// requested, never terminal.
func DeriveState(job *scrapejob.Job) string {
	if job == nil {
		return StateRequested
	}
	switch {
	case job.IsTerminal() && job.Status() == scrapejob.StatusSucceeded:
		return StateDone
	case job.IsTerminal():
		return StateDead
	case job.Status() == scrapejob.StatusFailed:
		return StateFailed
	case job.Status() == scrapejob.StatusProcessing:
		return StateProcessing
	default:
		return StateRequested
	}
}

// Tick refreshes saga instances: discovers live rows, re-derives open
// instances (catching terminal flips like reconciled successes), and prunes
// old terminal instances. Returns refreshed instance count.
func (o *Orchestrator) Tick(ctx context.Context) (int, error) {
	if o.db == nil || o.store == nil {
		return 0, nil
	}
	refreshed, err := o.refreshLiveRows(ctx)
	if err != nil {
		return refreshed, err
	}
	n, err := o.refreshOpenInstances(ctx)
	refreshed += n
	if err != nil {
		return refreshed, err
	}
	o.pruneTerminal(ctx)
	return refreshed, nil
}

// Run ticks immediately, then every interval until ctx ends. Startup catch-up
// converges instances created while the app was down.
func (o *Orchestrator) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if n, err := o.Tick(ctx); err != nil {
		log.Printf("[saga] orchestrator initial tick failed: %v", err)
	} else if n > 0 {
		log.Printf("[saga] orchestrator initial tick refreshed %d instance(s)", n)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := o.Tick(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[saga] orchestrator tick failed: %v", err)
			} else if n > 0 {
				log.Printf("[saga] orchestrator tick refreshed %d instance(s)", n)
			}
		}
	}
}

// refreshLiveRows ensures instances for live saga rows, deriving state from
// each request stream.
func (o *Orchestrator) refreshLiveRows(ctx context.Context) (int, error) {
	type liveRow struct{ requestID, artistID string }
	var rows []liveRow
	err := o.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`SELECT request_id, artist_id FROM scrape_jobs
			WHERE status IN ('queued', 'processing', 'failed')
			ORDER BY queued_at DESC LIMIT ?;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindInt64(1, liveScanLimit)
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			rows = append(rows, liveRow{requestID: stmt.ColumnText(0), artistID: stmt.ColumnText(1)})
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	refreshed := 0
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return refreshed, err
		}
		if err := o.refreshInstance(ctx, row.requestID, row.artistID); err != nil {
			log.Printf("[saga] refresh %s failed: %v", row.requestID, err)
			continue
		}
		refreshed++
	}
	return refreshed, nil
}

// refreshOpenInstances re-derives non-terminal instances so terminal flips
// that bypass live rows (reconciled successes, dead letters) converge.
func (o *Orchestrator) refreshOpenInstances(ctx context.Context) (int, error) {
	type openRow struct{ requestID, artistID string }
	var rows []openRow
	err := o.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`SELECT request_id, artist_id FROM saga_instances
			WHERE state NOT IN ('done', 'dead') LIMIT ?;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindInt64(1, refreshLimit)
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			rows = append(rows, openRow{requestID: stmt.ColumnText(0), artistID: stmt.ColumnText(1)})
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	refreshed := 0
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return refreshed, err
		}
		if err := o.refreshInstance(ctx, row.requestID, row.artistID); err != nil {
			log.Printf("[saga] refresh %s failed: %v", row.requestID, err)
			continue
		}
		refreshed++
	}
	return refreshed, nil
}

// refreshInstance derives one saga's state from its job stream and upserts it.
// Missing streams read as requested (row-ahead-of-stream crash window).
func (o *Orchestrator) refreshInstance(ctx context.Context, requestID, artistID string) error {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return nil
	}
	var job *scrapejob.Job
	var lastEvent, failure string
	var attempts int
	if events, err := o.store.Load(ctx, requestID); err == nil && len(events) > 0 {
		if agg, err := scrapejob.Replay(requestID, events); err == nil {
			job = agg
			attempts = agg.Attempts()
			lastEvent = events[len(events)-1].EventType
			if lastEvent == scrapejob.EventTypeScrapeFailed {
				failure = failedReason(events[len(events)-1])
			}
		}
	}
	state := DeriveState(job)
	now := time.Now().UTC().Format(timestampFormat)
	return o.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`INSERT INTO saga_instances (request_id, artist_id, state, attempts, last_event, error, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(request_id) DO UPDATE SET
				artist_id = excluded.artist_id,
				state = excluded.state,
				attempts = excluded.attempts,
				last_event = excluded.last_event,
				error = excluded.error,
				updated_at = excluded.updated_at;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, requestID)
		stmt.BindText(2, artistID)
		stmt.BindText(3, state)
		stmt.BindInt64(4, int64(attempts))
		stmt.BindText(5, lastEvent)
		stmt.BindText(6, failure)
		stmt.BindText(7, now)
		_, err := stmt.Step()
		return err
	})
}

// pruneTerminal deletes terminal instances past retention. Best-effort: a
// failed prune is retried next Tick instead of failing it.
// Audit guarantee: saga_instances is a queryable index only. Job event
// streams in events are never deleted here and stay the complete audit log.
func (o *Orchestrator) pruneTerminal(ctx context.Context) {
	cutoff := time.Now().UTC().Add(-terminalRetention).Format(timestampFormat)
	if err := o.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`DELETE FROM saga_instances WHERE state IN ('done', 'dead') AND updated_at < ?;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, cutoff)
		_, err := stmt.Step()
		return err
	}); err != nil {
		log.Printf("[saga] prune terminal instances failed: %v", err)
	}
}

// failedReason extracts the error from a ScrapeFailed event, best-effort.
func failedReason(evt eventsourcing.Event) string {
	var pl scrapejob.FailedPayload
	if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
		return ""
	}
	return pl.Error
}
