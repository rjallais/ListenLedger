package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/eventsourcing"
)

func (w *Worker) scrapeJobByRequestID(requestID string) (*core.Record, error) {
	if requestID == "" {
		return nil, nil
	}

	records, err := w.app.FindRecordsByFilter(
		"scrape_jobs",
		"request_id = {:request_id}",
		"",
		1,
		0,
		dbx.Params{"request_id": requestID},
	)
	if err != nil || len(records) == 0 {
		return nil, err
	}
	return records[0], nil
}

func (w *Worker) scrapeJobByRequestIDWithContext(ctx context.Context, requestID string) (*core.Record, error) {
	if requestID == "" {
		return nil, nil
	}

	var records []*core.Record
	err := w.app.RecordQuery("scrape_jobs").
		WithContext(ctx).
		AndWhere(dbx.NewExp("request_id = {:request_id}", dbx.Params{"request_id": requestID})).
		Limit(1).
		All(&records)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], nil
}

const jobTimestampFormat = "2006-01-02 15:04:05.000Z"

// recordJobEvent appends job lifecycle facts to the request's bounded stream
// (one stream per request_id). Missing streams seed from (requestID,
// artistID) so pre-log jobs converge; seed and transition commit atomically.
// No-ops without a job store. Callers warn-and-continue on error: row updates
// below still happen, and the fact can be reconciled later.
func (w *Worker) recordJobEvent(ctx context.Context, requestID, artistID string, record func(*scrapejob.Job) error) error {
	if w.jobStore == nil || strings.TrimSpace(requestID) == "" {
		return nil
	}
	evts, err := w.jobStore.Load(ctx, requestID)
	if err != nil {
		return fmt.Errorf("load job stream %s: %w", requestID, err)
	}
	var agg *scrapejob.Job
	var base int64
	if len(evts) == 0 {
		agg, err = scrapejob.NewScrapeJob(requestID, artistID, "")
		if err != nil {
			return fmt.Errorf("seed job stream %s: %w", requestID, err)
		}
		base = 0
	} else {
		agg, err = scrapejob.Replay(requestID, evts)
		if err != nil {
			return fmt.Errorf("replay job stream %s: %w", requestID, err)
		}
		base = agg.Version()
	}
	if err := record(agg); err != nil {
		return err
	}
	uncommitted := agg.UncommittedEvents()
	if len(uncommitted) == 0 {
		return nil
	}
	if err := eventsourcing.AppendWithRetry(ctx, w.jobStore, requestID, base, uncommitted...); err != nil {
		return fmt.Errorf("append job stream %s: %w", requestID, err)
	}
	return nil
}

// recordJobEventWarn is recordJobEvent for call sites that must not fail the
// scrape flow on log errors.
func (w *Worker) recordJobEventWarn(ctx context.Context, requestID, artistID, what string, record func(*scrapejob.Job) error) {
	if err := w.recordJobEvent(ctx, requestID, artistID, record); err != nil {
		log.Printf("[worker] Warning: job event %s for request %s not logged: %v", what, requestID, err)
	}
}

func (w *Worker) setScrapeJobProcessing(ctx context.Context, requestID string) {
	if requestID == "" {
		return
	}

	if w.db != nil {
		now := time.Now().UTC().Format(jobTimestampFormat)
		if err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("UPDATE scrape_jobs SET status = 'processing', attempts = attempts + 1, started_at = ?, error = '' WHERE request_id = ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, now)
			stmt.BindText(2, requestID)
			_, err := stmt.Step()
			return err
		}); err != nil {
			log.Printf("[worker] Warning: failed to update scrape job %s to processing in SQLite: %v", requestID, err)
		}
	}

	if w.app != nil {
		job, err := w.scrapeJobByRequestID(requestID)
		if err == nil && job != nil {
			job.Set("status", "processing")
			job.Set("attempts", job.GetInt("attempts")+1)
			job.Set("started_at", time.Now())
			job.Set("error", "")
			if err := w.app.Save(job); err != nil {
				log.Printf("[worker] Warning: failed to update scrape job to processing: %v", err)
			}
		}
	}
}

func (w *Worker) setScrapeJobFinished(ctx context.Context, requestID, status, errMsg string) {
	if requestID == "" {
		return
	}

	if w.db != nil {
		now := time.Now().UTC().Format(jobTimestampFormat)
		if err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("UPDATE scrape_jobs SET status = ?, finished_at = ?, error = ? WHERE request_id = ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, status)
			stmt.BindText(2, now)
			stmt.BindText(3, errMsg)
			stmt.BindText(4, requestID)
			_, err := stmt.Step()
			return err
		}); err != nil {
			log.Printf("[worker] Warning: failed to update scrape job %s to %s in SQLite: %v", requestID, status, err)
		}
	}

	if w.app != nil {
		job, err := w.scrapeJobByRequestID(requestID)
		if err == nil && job != nil {
			job.Set("status", status)
			job.Set("finished_at", time.Now())
			job.Set("error", errMsg)
			if err := w.app.Save(job); err != nil {
				log.Printf("[worker] Warning: failed to update scrape job to %s: %v", status, err)
			}
		}
	}
}

func (w *Worker) setScrapeJobFinishedWithContext(ctx context.Context, requestID, status, errMsg string) error {
	if requestID == "" {
		return nil
	}

	if w.db != nil {
		now := time.Now().UTC().Format(jobTimestampFormat)
		if err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("UPDATE scrape_jobs SET status = ?, finished_at = ?, error = ? WHERE request_id = ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, status)
			stmt.BindText(2, now)
			stmt.BindText(3, errMsg)
			stmt.BindText(4, requestID)
			_, err := stmt.Step()
			return err
		}); err != nil {
			log.Printf("[worker] Warning: failed to update scrape job %s to %s in SQLite: %v", requestID, status, err)
		}
	}

	if w.app != nil {
		job, err := w.scrapeJobByRequestIDWithContext(ctx, requestID)
		if err != nil {
			return fmt.Errorf("find scrape job %s: %w", requestID, err)
		}
		if job != nil {
			job.Set("status", status)
			job.Set("finished_at", time.Now())
			job.Set("error", errMsg)
			if err := w.app.SaveWithContext(ctx, job); err != nil {
				return fmt.Errorf("save scrape job %s: %w", requestID, err)
			}
		}
	}
	return nil
}

// isRequestAlreadySucceeded reports whether the request stream or row records
// a terminal success.
func (w *Worker) isRequestAlreadySucceeded(ctx context.Context, requestID string) bool {
	succeeded, _ := w.requestStreamVerdict(ctx, requestID)
	return succeeded
}

// requestStreamVerdict derives the preflight skip disposition from one job
// stream load. The returned orphanedAt is non-zero when the stream contains
// the sweep's orphaned_no_dispatch verdict; callers compare it with the
// delivery timestamp so a retry published after the verdict is processed while
// the original late delivery is skipped.
func (w *Worker) requestStreamVerdict(ctx context.Context, requestID string) (succeeded bool, orphanedAt time.Time) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return false, time.Time{}
	}

	now := time.Now()
	w.succeededMu.Lock()
	w.pruneSucceededLocked(now)
	if _, ok := w.succeededRequests[requestID]; ok {
		w.succeededMu.Unlock()
		return true, time.Time{}
	}
	w.succeededMu.Unlock()

	// Domain-level dedup: the request stream's terminal state survives
	// restarts and retention purges that the cache and scrape_jobs row do not.
	// Terminal streams are definitive — return without querying PocketBase on
	// every queued job. Non-terminal streams (queued or processing) fall
	// through to the row lookup: recordJobEventWarn can drop the
	// ScrapeSucceeded fact while the PocketBase row already flipped. Keep the
	// orphan verdict time across that fallback for the delivery-time guard.
	if w.jobStore != nil {
		if evts, err := w.jobStore.Load(ctx, requestID); err == nil && len(evts) > 0 {
			if isOrphanedQueuedVerdict(evts) {
				return false, evts[1].CreatedAt
			}
			orphanedAt, _ = orphanedQueuedVerdictAt(evts)
			if agg, err := scrapejob.Replay(requestID, evts); err == nil {
				if agg.Status() == scrapejob.StatusSucceeded {
					w.markRequestSucceeded(requestID)
					return true, orphanedAt
				}
				if agg.IsTerminal() {
					return false, orphanedAt
				}
			}
		}
	}

	if w.app == nil {
		return false, orphanedAt
	}

	job, err := w.scrapeJobByRequestID(requestID)
	if err != nil {
		log.Printf("[worker] Warning: dedupe check failed for request_id=%s: %v", requestID, err)
		return false, orphanedAt
	}
	if job == nil || job.GetString("status") != "succeeded" {
		return false, orphanedAt
	}

	w.markRequestSucceeded(requestID)
	return true, orphanedAt
}

func (w *Worker) markRequestSucceeded(requestID string) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return
	}

	now := time.Now()
	w.succeededMu.Lock()
	w.pruneSucceededLocked(now)
	w.succeededRequests[requestID] = now
	w.succeededMu.Unlock()
}

func (w *Worker) pruneSucceededLocked(now time.Time) {
	for requestID, seenAt := range w.succeededRequests {
		if now.Sub(seenAt) > requestSuccessCacheTTL {
			delete(w.succeededRequests, requestID)
		}
	}
}

type failedJob struct{ requestID, artistID string }

// reconcileFailedJobsSQLite flips the selected jobs from failed to succeeded
// and appends their ScrapeSucceeded facts. The guarded UPDATE runs first, so a
// concurrent retry that moved a row to queued or processing after the SELECT
// is left alone and never gets a success fact on its live stream.
func (w *Worker) reconcileFailedJobsSQLite(ctx context.Context, artistID string, jobs []failedJob, note string) {
	if w.db == nil || len(jobs) == 0 {
		return
	}
	now := time.Now().UTC().Format(jobTimestampFormat)
	changed := make([]failedJob, 0, len(jobs))
	if err := w.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("UPDATE scrape_jobs SET status = 'succeeded', finished_at = ?, error = ? WHERE request_id = ? AND status = 'failed';")
		defer func() { _ = stmt.Reset() }()
		for _, job := range jobs {
			if ctx.Err() != nil {
				break
			}
			_ = stmt.Reset()
			stmt.BindText(1, now)
			stmt.BindText(2, note)
			stmt.BindText(3, job.requestID)
			if _, err := stmt.Step(); err != nil {
				return err
			}
			if tx.Changes() > 0 {
				changed = append(changed, job)
			}
		}
		return nil
	}); err != nil {
		log.Printf("[worker] Warning: failed to clear failed jobs for artist %s in SQLite: %v", artistID, err)
		return
	}

	for _, job := range changed {
		if ctx.Err() != nil {
			break
		}
		w.recordJobEventWarn(ctx, job.requestID, job.artistID, "recovered", func(j *scrapejob.Job) error {
			_, _, err := j.RecordSucceeded("reconcile", 0, eventsourcing.Correlation{RequestID: job.requestID})
			return err
		})
	}
}

func (w *Worker) clearFailedJobsForArtist(ctx context.Context, artistID, succeededRequestID string) {
	if artistID == "" {
		return
	}
	if err := ctx.Err(); err != nil {
		return
	}

	note := reconciliationNote(succeededRequestID)

	if w.db != nil {
		// Update only the rows the SELECT below actually returns: the SELECT
		// is capped at 500 and its errors are handled, while a predicate-wide
		// UPDATE would flip rows this run never audits (including rows a
		// concurrent retry moved out of failed after the SELECT ran).
		var failed []failedJob
		if err := w.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT request_id, artist_id FROM scrape_jobs WHERE artist_id = ? AND status = 'failed' AND request_id != ? LIMIT 500;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, artistID)
			stmt.BindText(2, succeededRequestID)
			for {
				hasRow, err := stmt.Step()
				if err != nil {
					return err
				}
				if !hasRow {
					break
				}
				failed = append(failed, failedJob{requestID: stmt.ColumnText(0), artistID: stmt.ColumnText(1)})
			}
			return nil
		}); err != nil {
			log.Printf("[worker] Warning: failed to load failed jobs for artist %s, skipping clear: %v", artistID, err)
			return
		}
		if len(failed) > 0 {
			w.reconcileFailedJobsSQLite(ctx, artistID, failed, note)
		}
	}

	if w.app != nil {
		records := make([]*core.Record, 0, 500)
		err := w.app.RecordQuery("scrape_jobs").
			WithContext(ctx).
			AndWhere(dbx.NewExp(
				"artist = {:artist} AND status = {:status} AND request_id != {:request_id}",
				dbx.Params{
					"artist":     artistID,
					"status":     "failed",
					"request_id": succeededRequestID,
				},
			)).
			Limit(500).
			All(&records)
		if err != nil {
			log.Printf("[worker] Warning: failed to load failed jobs for artist %s: %v", artistID, err)
			return
		}

		if len(records) > 0 {
			w.reconcileFailedJobs(ctx, records, note)
		}
	}
}

func reconciliationNote(succeededRequestID string) string {
	if strings.TrimSpace(succeededRequestID) != "" {
		return "recovered_by_retry:" + succeededRequestID
	}
	return "recovered_by_retry"
}

func (w *Worker) reconcileFailedJobs(ctx context.Context, records []*core.Record, note string) {
	for _, rec := range records {
		if ctx.Err() != nil {
			return
		}
		w.recordJobEventWarn(ctx, rec.GetString("request_id"), rec.GetString("artist"), "recovered", func(j *scrapejob.Job) error {
			_, _, err := j.RecordSucceeded("reconcile", 0, eventsourcing.Correlation{RequestID: rec.GetString("request_id")})
			return err
		})
		rec.Set("status", "succeeded")
		rec.Set("finished_at", time.Now())
		existingErr := rec.GetString("error")
		if existingErr == "" {
			rec.Set("error", note)
		} else {
			rec.Set("error", note+" | "+existingErr)
		}
		if saveErr := w.app.SaveWithContext(ctx, rec); saveErr != nil {
			log.Printf("[worker] Warning: failed to reconcile failed job %s: %v", rec.Id, saveErr)
		}
	}
}

const staleJobThreshold = 5 * time.Minute
const staleJobSweepInterval = 30 * time.Second
const staleJobSweepTimeout = 20 * time.Second

// orphanedQueuedGrace bounds how long a queued row may exist without its
// ScrapeRequested fact before the sweeper treats it as never dispatched.
// Dispatch opens the job stream before publishing (single and batch paths),
// so a queued row whose stream is missing was persisted by an enqueue that
// died before its fact/dispatch; the grace covers enqueue timeouts and
// retry republishes of rows whose original attempt never opened a stream.
const orphanedQueuedGrace = 15 * time.Minute

// orphanedQueuedReason is the failure reason recorded for swept rows.
const orphanedQueuedReason = "orphaned_no_dispatch"

// orphanedQueuedScanLimit bounds per-sweep stream existence checks. Each check
// is one event-store Load (a filtered consumer in JetStream mode), so the cap
// keeps a sweep inside its timeout during deep backlogs; oldest rows surface
// first and the remainder is caught on later ticks.
const orphanedQueuedScanLimit = 100

// jobPurgeInterval bounds how often succeeded-job retention is enforced.
// The first sweep after startup always purges (lastPurge is zero).
const jobPurgeInterval = time.Hour

// defaultScrapeJobRetention applies when ScrapeJobRetention is unset.
const defaultScrapeJobRetention = 7 * 24 * time.Hour

// sweepStaleJobs periodically marks scrape jobs that have been "processing"
// for longer than staleJobThreshold as "failed" with a "stale_timeout" error.
// It also updates the associated artist's fetch_status to "failed" so that
// batch progress tracking can count it as completed.
// Hourly it additionally purges succeeded jobs beyond the retention window so
// the scrape_jobs table (and every COUNT(*) over it) stays bounded.
func (w *Worker) sweepStaleJobs() {
	ticker := time.NewTicker(staleJobSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			func() {
				ctx, cancel := context.WithTimeout(w.ctx, staleJobSweepTimeout)
				defer cancel()
				w.markStaleJobs(ctx)
				w.maybePurgeOldJobs(ctx)
			}()
		}
	}
}

// maybePurgeOldJobs runs purgeOldSucceededJobs at most hourly (and once on
// the first sweep after startup). lastPurge advances only on success so a
// failed purge is retried on the next tick instead of waiting out the hour.
func (w *Worker) maybePurgeOldJobs(ctx context.Context) {
	w.purgeMu.Lock()
	sincePurge := time.Since(w.lastPurge)
	shouldPurge := w.lastPurge.IsZero() || sincePurge >= jobPurgeInterval
	w.purgeMu.Unlock()
	if !shouldPurge {
		return
	}

	purged, err := w.purgeOldSucceededJobs(ctx)
	if err != nil {
		log.Printf("[worker] Warning: succeeded-job retention purge failed: %v", err)
		return
	}
	w.purgeMu.Lock()
	w.lastPurge = time.Now()
	w.purgeMu.Unlock()
	if purged > 0 {
		log.Printf("[worker] Purged %d succeeded scrape job(s) beyond retention", purged)
	}
}

// purgeOldSucceededJobs deletes succeeded scrape jobs finished before the
// retention cutoff in a single statement (no per-row hooks by design —
// callers observing queue counters re-read them on their next poll).
// Finished timestamps are stored as UTC "2006-01-02 15:04:05..." strings, so
// a lexicographic comparison against the same format is chronological.
//
// Audit guarantee: this touches ONLY the scrape_jobs read-model rows. The
// job event streams in events (+ outbox) are never deleted here — they stay
// the complete log for dedup (isRequestAlreadySucceeded) and audit. Set
// SCRAPE_JOB_RETENTION=0 to keep rows forever (full audit mode).
func (w *Worker) purgeOldSucceededJobs(ctx context.Context) (int64, error) {
	retention := w.cfg.ScrapeJobRetention
	if retention == 0 {
		return 0, nil
	}
	if retention < 0 {
		retention = defaultScrapeJobRetention
	}
	cutoff := time.Now().UTC().Add(-retention).Format(jobTimestampFormat)

	var purged int64
	if w.db != nil {
		err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep(`DELETE FROM scrape_jobs WHERE status = 'succeeded' AND ((finished_at IS NOT NULL AND finished_at < ?) OR (finished_at IS NULL AND queued_at < ?));`)
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, cutoff)
			stmt.BindText(2, cutoff)
			_, err := stmt.Step()
			if err != nil {
				return err
			}
			purged += int64(tx.Changes())
			return nil
		})
		if err != nil {
			return purged, fmt.Errorf("purge old succeeded jobs in SQLite: %w", err)
		}
	}

	if w.app == nil {
		return purged, nil
	}

	res, err := w.app.DB().Delete("scrape_jobs", dbx.NewExp(
		`status = {:status} AND ((finished_at IS NOT NULL AND finished_at < {:cutoff}) OR
		 (finished_at IS NULL AND queued_at < {:cutoff}))`,
		dbx.Params{"status": "succeeded", "cutoff": cutoff},
	)).WithContext(ctx).Execute()
	if err != nil {
		return purged, fmt.Errorf("delete succeeded jobs before %s: %w", cutoff, err)
	}
	pbPurged, err := res.RowsAffected()
	if err != nil {
		return purged, fmt.Errorf("purge rows affected: %w", err)
	}
	return purged + pbPurged, nil
}

// markStaleJobsSQLite is the saga timeout transition for stale processing
// jobs. Event-first per job: the ScrapeFailed(stale_timeout) fact is the
// decision point, the row flip is its projection. A job whose stream already
// closed as succeeded (crash between event and row in persistListeners) is
// converged to succeeded, never failed.
func (w *Worker) markStaleJobsSQLite(ctx context.Context, cutoff string) {
	type staleJob struct{ requestID, artistID string }
	var stale []staleJob
	err := w.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT request_id, artist_id FROM scrape_jobs WHERE status = 'processing' AND started_at < ? LIMIT 500;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, cutoff)
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			stale = append(stale, staleJob{requestID: stmt.ColumnText(0), artistID: stmt.ColumnText(1)})
		}
		return nil
	})
	if err != nil {
		log.Printf("[worker] Warning: failed to query stale jobs in SQLite: %v", err)
		return
	}
	if len(stale) == 0 {
		return
	}
	marked := 0
	for _, job := range stale {
		if ctx.Err() != nil {
			return
		}
		if w.isRequestAlreadySucceeded(ctx, job.requestID) {
			w.convergeStaleRowToSucceeded(ctx, job.requestID)
			continue
		}
		if err := w.recordJobEvent(ctx, job.requestID, job.artistID, func(j *scrapejob.Job) error {
			_, _, err := j.RecordFailed("stale_timeout", eventsourcing.Correlation{RequestID: job.requestID})
			return err
		}); err != nil {
			// Lost race with a concurrent completion: converge, don't fail.
			if w.isRequestAlreadySucceeded(ctx, job.requestID) {
				w.convergeStaleRowToSucceeded(ctx, job.requestID)
				continue
			}
			log.Printf("[worker] Warning: job event stale-timeout for request %s not logged: %v", job.requestID, err)
			continue
		}
		w.markSingleStaleRowFailed(ctx, job.requestID, cutoff)
		w.recordArtistFetchStatusWarn(ctx, job.artistID, "failed", job.requestID)
		marked++
	}
	if marked > 0 {
		log.Printf("[worker] Marked %d stale scrape job(s) older than %s", marked, staleJobThreshold)
	}
}

// convergeStaleRowToSucceeded flips a phantom processing row whose stream
// already closed as succeeded. Crash-window convergence, not a state transition.
func (w *Worker) convergeStaleRowToSucceeded(ctx context.Context, requestID string) {
	if w.db == nil || strings.TrimSpace(requestID) == "" {
		return
	}
	now := time.Now().UTC().Format(jobTimestampFormat)
	if err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("UPDATE scrape_jobs SET status = 'succeeded', finished_at = ?, error = 'recovered_after_success' WHERE request_id = ? AND status = 'processing';")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, now)
		stmt.BindText(2, requestID)
		_, err := stmt.Step()
		return err
	}); err != nil {
		log.Printf("[worker] Warning: failed to converge stale succeeded job %s: %v", requestID, err)
	}
}

// markSingleStaleRowFailed flips one processing row to failed(stale_timeout).
// Conditional on status so a job that completed between event and row keeps
// its succeeded state.
func (w *Worker) markSingleStaleRowFailed(ctx context.Context, requestID, cutoff string) {
	if w.db == nil || strings.TrimSpace(requestID) == "" {
		return
	}
	now := time.Now().UTC().Format(jobTimestampFormat)
	if err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("UPDATE scrape_jobs SET status = 'failed', finished_at = ?, error = 'stale_timeout' WHERE request_id = ? AND status = 'processing' AND started_at < ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, now)
		stmt.BindText(2, requestID)
		stmt.BindText(3, cutoff)
		_, err := stmt.Step()
		return err
	}); err != nil {
		log.Printf("[worker] Warning: failed to mark stale job %s: %v", requestID, err)
	}
}

func (w *Worker) markStaleJobs(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}

	cutoff := time.Now().UTC().Add(-staleJobThreshold).Format(jobTimestampFormat)

	if w.db != nil {
		w.markStaleJobsSQLite(ctx, cutoff)
	}

	// Never-dispatched queued rows: dispatch arms the JobRequested fact before
	// publishing, so a queued row with an empty stream is a crash-window
	// orphan. Swept well before the 24h retention expiry so its artist and
	// batch member leave pending within minutes, not a day.
	w.markOrphanedQueuedJobs(ctx, time.Now().UTC().Add(-orphanedQueuedGrace).Format(jobTimestampFormat))

	if w.app == nil {
		return
	}

	records := make([]*core.Record, 0)
	err := w.app.RecordQuery("scrape_jobs").
		WithContext(ctx).
		AndWhere(dbx.NewExp("status = {:status} AND started_at < {:cutoff}", dbx.Params{"status": "processing", "cutoff": cutoff})).
		Limit(50).
		All(&records)
	if err != nil {
		log.Printf("[worker] Warning: failed to query stale scrape jobs: %v", err)
		return
	}

	if len(records) == 0 {
		return
	}

	log.Printf("[worker] Sweeping %d stale scrape job(s) older than %s", len(records), staleJobThreshold)

	for _, job := range records {
		if ctx.Err() != nil {
			return
		}
		artistID := job.GetString("artist")
		requestID := job.GetString("request_id")

		// Crash-window convergence: stream already succeeded, don't fail it.
		if w.isRequestAlreadySucceeded(ctx, requestID) {
			continue
		}
		// Event first so a crash leaves a fact, not a phantom row flip.
		// With SQLite configured, markStaleJobsSQLite already recorded this
		// fact; appending again would double-record one timeout, so the PB
		// pass only mirrors the row below.
		if w.db == nil {
			if err := w.recordJobEvent(ctx, requestID, artistID, func(j *scrapejob.Job) error {
				_, _, err := j.RecordFailed("stale_timeout", eventsourcing.Correlation{RequestID: requestID})
				return err
			}); err != nil {
				if w.isRequestAlreadySucceeded(ctx, requestID) {
					continue
				}
				log.Printf("[worker] Warning: job event stale-timeout for request %s not logged: %v", requestID, err)
				continue
			}
		}

		updated, err := w.markSingleStaleJob(ctx, job, cutoff)
		if err != nil {
			log.Printf("[worker] Warning: failed to mark stale job %s (request_id=%s) as failed: %v", job.Id, requestID, err)
			continue
		}
		if updated {
			log.Printf("[worker] Marked stale scrape job %s (artist=%s, request_id=%s) as failed", job.Id, artistID, requestID)
			w.recordArtistFetchStatusWarn(ctx, artistID, "failed", requestID)
		}
	}
}

// markOrphanedQueuedJobs fails queued rows that never opened a job stream.
// The row-first crash window (persist commits, the process dies before the
// request's ScrapeRequested fact is appended) leaves rows that no redelivery
// can resolve; without this sweep they wedge the queue and their artists stay
// pending until the 24h queue_expired retention pass. Event-first per row on
// the first pass: the ScrapeFailed(orphaned_no_dispatch) fact seeds and closes
// the request's stream before any row flips; a stream that already contains
// more than that seed pair was dispatched (or is being) and is skipped.
func (w *Worker) markOrphanedQueuedJobs(ctx context.Context, cutoff string) {
	if w.jobStore == nil {
		return
	}
	type orphanJob struct{ requestID, artistID, queuedAt string }
	var candidates []orphanJob
	if w.db != nil {
		err := w.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			query := "SELECT request_id, artist_id, queued_at FROM scrape_jobs WHERE status = 'queued' AND queued_at < ?"
			if w.orphanScanQueuedAt != "" {
				query += " AND (queued_at > ? OR (queued_at = ? AND request_id > ?))"
			}
			query += " ORDER BY queued_at ASC, request_id ASC LIMIT ?;"
			stmt := tx.Prep(query)
			defer func() { _ = stmt.Reset() }()
			bind := 1
			stmt.BindText(bind, cutoff)
			bind++
			if w.orphanScanQueuedAt != "" {
				stmt.BindText(bind, w.orphanScanQueuedAt)
				bind++
				stmt.BindText(bind, w.orphanScanQueuedAt)
				bind++
				stmt.BindText(bind, w.orphanScanRequestID)
				bind++
			}
			stmt.BindInt64(bind, orphanedQueuedScanLimit)
			for {
				hasRow, err := stmt.Step()
				if err != nil {
					return err
				}
				if !hasRow {
					break
				}
				candidates = append(candidates, orphanJob{
					requestID: stmt.ColumnText(0),
					artistID:  stmt.ColumnText(1),
					queuedAt:  stmt.ColumnText(2),
				})
			}
			return nil
		})
		if err != nil {
			log.Printf("[worker] Warning: failed to query orphaned queued jobs (SQLite): %v", err)
			return
		}
	} else if w.app != nil {
		query := w.app.RecordQuery("scrape_jobs").
			WithContext(ctx).
			AndWhere(dbx.NewExp("status = {:status} AND queued_at < {:cutoff}", dbx.Params{"status": "queued", "cutoff": cutoff}))
		if w.orphanScanQueuedAt != "" {
			query = query.AndWhere(dbx.NewExp("(queued_at > {:cursor_at} OR (queued_at = {:cursor_at} AND request_id > {:cursor_id}))",
				dbx.Params{"cursor_at": w.orphanScanQueuedAt, "cursor_id": w.orphanScanRequestID}))
		}
		records := make([]*core.Record, 0)
		err := query.
			OrderBy("queued_at ASC", "request_id ASC").
			Limit(orphanedQueuedScanLimit).
			All(&records)
		if err != nil {
			log.Printf("[worker] Warning: failed to query orphaned queued jobs: %v", err)
			return
		}
		for _, rec := range records {
			candidates = append(candidates, orphanJob{
				requestID: rec.GetString("request_id"),
				artistID:  rec.GetString("artist"),
				queuedAt:  rec.GetString("queued_at"),
			})
		}
	} else {
		return
	}
	if len(candidates) == 0 {
		w.orphanScanQueuedAt, w.orphanScanRequestID = "", ""
		return
	}
	// A short page means the scan reached the end; restart from the oldest row
	// next tick. A full page advances the keyset cursor past the rows already
	// checked (including dispatched rows that stay queued), so older dispatched
	// rows cannot starve real orphans behind them.
	if len(candidates) < orphanedQueuedScanLimit {
		w.orphanScanQueuedAt, w.orphanScanRequestID = "", ""
	} else {
		last := candidates[len(candidates)-1]
		w.orphanScanQueuedAt, w.orphanScanRequestID = last.queuedAt, last.requestID
	}
	marked := 0
	for _, job := range candidates {
		if ctx.Err() != nil {
			// Timed out mid-page: restart from the oldest rows so nothing the
			// cursor already passed is skipped until the next reset.
			w.orphanScanQueuedAt, w.orphanScanRequestID = "", ""
			return
		}
		if strings.TrimSpace(job.requestID) == "" {
			continue
		}
		// Dispatch opens the stream before publishing, so a non-empty stream
		// means this row was (or is being) dispatched — never an orphan. The
		// one exception is the sweep's own seed pair, which a partial run may
		// have recorded without landing the row flips: finish that repair
		// without appending the fact again.
		events, err := w.jobStore.Load(ctx, job.requestID)
		if err != nil {
			log.Printf("[worker] Warning: orphaned-job stream check %s failed: %v", job.requestID, err)
			continue
		}
		if len(events) > 0 && !isOrphanedQueuedVerdict(events) {
			continue
		}
		if len(events) == 2 {
			// A row queued after the verdict is a retry waiting for dispatch,
			// not an unfinished repair from the sweep's original pass.
			if queuedAt, err := time.Parse(jobTimestampFormat, job.queuedAt); err == nil && queuedAt.After(events[1].CreatedAt) {
				continue
			}
		}
		if len(events) == 0 {
			if err := w.recordJobEvent(ctx, job.requestID, job.artistID, func(j *scrapejob.Job) error {
				_, _, err := j.RecordFailed(orphanedQueuedReason, eventsourcing.Correlation{RequestID: job.requestID})
				return err
			}); err != nil {
				log.Printf("[worker] Warning: orphaned-job fact for %s not recorded: %v", job.requestID, err)
				continue
			}
		}
		flipped := false
		artistFlipped := false
		if w.db != nil {
			flipped = w.markOrphanedQueuedRowFailedSQLite(ctx, job.requestID, cutoff)
			if flipped {
				// Direct read-model flip: the aggregate fold below converges
				// artists that have streams; this covers artists whose first
				// stream never opened, so batch members still leave pending.
				artistFlipped = w.markArtistFetchStatusFailedSQLite(ctx, job.artistID)
			}
			// PocketBase stays a best-effort mirror when SQLite is configured.
			_, _ = w.markOrphanedQueuedJobFailedPB(ctx, job.requestID, cutoff)
		} else {
			// PocketBase-only mode: the guarded save is the authoritative flip,
			// so its job and artist results decide whether the artist failed
			// fact may be recorded.
			flipped, artistFlipped = w.markOrphanedQueuedJobFailedPB(ctx, job.requestID, cutoff)
		}
		if !flipped {
			continue
		}
		// A newer queued/processing job owns the artist's fetch_status now: the
		// guarded flip refused, so skip the failed fact too — it would clobber
		// the live scrape's state.
		if artistFlipped {
			w.recordArtistFetchStatusWarn(ctx, job.artistID, "failed", job.requestID)
		}
		marked++
	}
	if marked > 0 {
		log.Printf("[worker] Marked %d orphaned queued job(s) without a dispatch fact", marked)
	}
}

// isOrphanedQueuedVerdict reports whether a stream holds exactly the sweep's
// own seed pair (ScrapeRequested + ScrapeFailed(orphaned_no_dispatch)), i.e.
// the verdict was recorded but the row repair below may not have landed yet.
func isOrphanedQueuedVerdict(events []eventsourcing.Event) bool {
	if len(events) != 2 ||
		events[0].EventType != scrapejob.EventTypeScrapeRequested ||
		events[1].EventType != scrapejob.EventTypeScrapeFailed {
		return false
	}
	var pl scrapejob.FailedPayload
	if err := eventsourcing.DecodeEventPayload(events[1], &pl); err != nil {
		return false
	}
	return pl.Error == orphanedQueuedReason
}

// orphanedQueuedVerdictAt returns when the sweep recorded its
// orphaned_no_dispatch verdict, if the stream contains one. The verdict stays
// relevant after a retry reopens the stream: a delivery stored before it is
// the original late message, while a delivery stored after it is the retry.
func orphanedQueuedVerdictAt(events []eventsourcing.Event) (time.Time, bool) {
	if len(events) == 0 || events[0].EventType != scrapejob.EventTypeScrapeRequested {
		return time.Time{}, false
	}
	for _, evt := range events {
		if evt.EventType != scrapejob.EventTypeScrapeFailed {
			continue
		}
		var pl scrapejob.FailedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			continue
		}
		if pl.Error == orphanedQueuedReason {
			return evt.CreatedAt, true
		}
	}
	return time.Time{}, false
}

// markOrphanedQueuedRowFailedSQLite flips one queued row to
// failed(orphaned_no_dispatch) and reports whether the row changed.
// Conditional on status and queued_at so a job that started processing
// between the scan and the flip keeps its state.
func (w *Worker) markOrphanedQueuedRowFailedSQLite(ctx context.Context, requestID, cutoff string) bool {
	if w.db == nil || strings.TrimSpace(requestID) == "" {
		return false
	}
	now := time.Now().UTC().Format(jobTimestampFormat)
	changed := false
	if err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("UPDATE scrape_jobs SET status = 'failed', finished_at = ?, error = ? WHERE request_id = ? AND status = 'queued' AND queued_at < ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, now)
		stmt.BindText(2, orphanedQueuedReason)
		stmt.BindText(3, requestID)
		stmt.BindText(4, cutoff)
		_, err := stmt.Step()
		if err != nil {
			return err
		}
		changed = tx.Changes() > 0
		return nil
	}); err != nil {
		log.Printf("[worker] Warning: failed to mark orphaned queued job %s failed (SQLite): %v", requestID, err)
		return false
	}
	return changed
}

// markArtistFetchStatusFailedSQLite flips the artist read model out of pending
// for a swept request. Direct guarded flip, mirroring the retry-path
// reconciliation: the aggregate fold (recordArtistFetchStatusWarn) covers
// artists with event streams, this covers artists whose first stream never
// opened. Only ever flips out of pending, and only when no newer job for the
// artist is still queued or processing.
func (w *Worker) markArtistFetchStatusFailedSQLite(ctx context.Context, artistID string) bool {
	if w.db == nil || strings.TrimSpace(artistID) == "" {
		return false
	}
	flipped := false
	if err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("UPDATE artists SET fetch_status = 'failed' WHERE id = ? AND fetch_status = 'pending' AND NOT EXISTS (SELECT 1 FROM scrape_jobs WHERE artist_id = artists.id AND status IN ('queued','processing'));")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		_, err := stmt.Step()
		if err != nil {
			return err
		}
		flipped = tx.Changes() > 0
		return nil
	}); err != nil {
		log.Printf("[worker] Warning: failed to mark artist %s failed after orphaned sweep: %v", artistID, err)
		return false
	}
	return flipped
}

// markOrphanedQueuedJobFailedPB mirrors the sweep to PocketBase: guarded job
// flip plus artist failed if still pending and no newer queued/processing job
// owns the artist (fires the artist.updated hook so batch progress converges
// without waiting for the reconciler). Best-effort: the SQLite read model stays
// authoritative when configured.
func (w *Worker) markOrphanedQueuedJobFailedPB(ctx context.Context, requestID, cutoff string) (jobFlipped, artistFlipped bool) {
	if w.app == nil || strings.TrimSpace(requestID) == "" {
		return false, false
	}
	err := w.app.RunInTransaction(func(txApp core.App) error {
		var guard []*core.Record
		guardErr := txApp.RecordQuery("scrape_jobs").
			WithContext(ctx).
			AndWhere(dbx.NewExp("request_id = {:request_id} AND status = {:status} AND queued_at < {:cutoff}",
				dbx.Params{"request_id": requestID, "status": "queued", "cutoff": cutoff})).
			Limit(1).
			All(&guard)
		if guardErr != nil {
			return fmt.Errorf("re-check orphaned job guard for %s: %w", requestID, guardErr)
		}
		if len(guard) == 0 {
			return nil
		}
		job := guard[0]
		job.Set("status", "failed")
		job.Set("finished_at", time.Now())
		job.Set("error", orphanedQueuedReason)
		if err := txApp.Save(job); err != nil {
			return fmt.Errorf("fail orphaned job %s: %w", requestID, err)
		}
		jobFlipped = true

		artistID := job.GetString("artist")
		var live []*core.Record
		if err := txApp.RecordQuery("scrape_jobs").
			WithContext(ctx).
			AndWhere(dbx.NewExp("artist = {:artist} AND status IN ('queued','processing')", dbx.Params{"artist": artistID})).
			Limit(1).
			All(&live); err != nil {
			return fmt.Errorf("check live jobs for artist %s: %w", artistID, err)
		}
		if len(live) > 0 {
			return nil
		}
		flipped, err := w.markStaleJobArtistFailed(ctx, txApp, job.Id, artistID)
		if err != nil {
			return err
		}
		artistFlipped = flipped
		return nil
	})
	if err != nil {
		log.Printf("[worker] Warning: failed to mirror orphaned queued job %s to PocketBase: %v", requestID, err)
		return false, false
	}
	return jobFlipped, artistFlipped
}

// recordArtistFetchStatusWarn folds a fetch-status fact into the artist stream
// and projection (best-effort). Reconcile paths that flip read-model status
// directly must call this to keep aggregate state converged — otherwise
// aggregate-based command validation decides on stale facts.
// Optional requestIDs[0] links the fact to its saga instance in metadata.
func (w *Worker) recordArtistFetchStatusWarn(ctx context.Context, artistID, status string, requestIDs ...string) {
	if w.artistRepo == nil || strings.TrimSpace(artistID) == "" {
		return
	}
	agg, err := w.artistRepo.Load(ctx, artistID)
	if err != nil {
		return
	}
	// Guard mirrors the read-model predicate (flip only out of pending): a
	// scrape that finished after the sweep selected the job must keep its
	// idle state in both log and projection.
	if agg.FetchStatus != "pending" {
		return
	}
	var corr eventsourcing.Correlation
	if len(requestIDs) > 0 {
		corr.RequestID = requestIDs[0]
	}
	if err := agg.SetFetchStatus(status, "stale sweep", corr); err != nil {
		return
	}
	events, err := w.artistRepo.Save(ctx, agg)
	if err != nil {
		log.Printf("[worker] Warning: artist fetch-status event for %s not saved: %v", artistID, err)
		return
	}
	if w.artistProjection != nil && len(events) > 0 {
		if err := w.artistProjection.Project(ctx, agg, events); err != nil {
			log.Printf("[worker] Warning: artist fetch-status projection for %s failed: %v", artistID, err)
		}
	}
}

// markSingleStaleJob runs a transaction that re-checks the guard condition and,
// if still valid, marks the job failed and sets the artist fetch_status to failed.
// Returns (true, nil) when the job was updated, (false, nil) when the guard
// prevented an update, or (false, err) on any DB error.
func (w *Worker) markSingleStaleJob(ctx context.Context, job *core.Record, cutoff string) (bool, error) {
	var jobUpdated bool
	err := w.app.RunInTransaction(func(txApp core.App) error {
		freshJob, err := txApp.FindRecordById("scrape_jobs", job.Id, func(q *dbx.SelectQuery) error {
			q.WithContext(ctx)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to reload job %s: %w", job.Id, err)
		}

		// Re-check status and started_at under the transaction to avoid clobbering
		// a job that was restarted after the initial scan.
		guard := make([]*core.Record, 0, 1)
		guardErr := txApp.RecordQuery("scrape_jobs").
			WithContext(ctx).
			AndWhere(dbx.NewExp("id = {:id} AND status = {:status} AND started_at < {:cutoff}",
				dbx.Params{"id": freshJob.Id, "status": "processing", "cutoff": cutoff})).
			Limit(1).
			All(&guard)
		if guardErr != nil {
			return fmt.Errorf("failed to re-check stale guard for job %s: %w", job.Id, guardErr)
		}
		if len(guard) == 0 {
			return nil
		}

		freshJob.Set("status", "failed")
		freshJob.Set("finished_at", time.Now())
		freshJob.Set("error", "stale_timeout")
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := txApp.Save(freshJob); err != nil {
			return fmt.Errorf("failed to save job %s: %w", job.Id, err)
		}
		jobUpdated = true

		_, err = w.markStaleJobArtistFailed(ctx, txApp, job.Id, freshJob.GetString("artist"))
		return err
	})
	return jobUpdated, err
}

// markStaleJobArtistFailed updates the artist's fetch_status to "failed" if it
// is currently "pending". It reports whether the artist row was flipped. It is
// called inside the same transaction as the job update.
func (w *Worker) markStaleJobArtistFailed(ctx context.Context, txApp core.App, jobID string, artistID string) (bool, error) {
	if artistID == "" {
		return false, nil
	}
	artist, err := txApp.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
		q.WithContext(ctx)
		return nil
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("[markStaleJobArtistFailed] artist %s not found for job %s; no-op — job already failed", artistID, jobID)
			return false, nil
		}
		return false, fmt.Errorf("failed to load artist %s for stale job %s: %w", artistID, jobID, err)
	}
	if artist.GetString("fetch_status") != "pending" {
		return false, nil
	}
	artist.Set("fetch_status", "failed")
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := txApp.Save(artist); err != nil {
		return false, fmt.Errorf("failed to save artist %s for stale job %s: %w", artistID, jobID, err)
	}
	return true, nil
}
