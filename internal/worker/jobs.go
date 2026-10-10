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
	if err := w.jobStore.Append(ctx, requestID, base, uncommitted...); err != nil {
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

func (w *Worker) isRequestAlreadySucceeded(ctx context.Context, requestID string) bool {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return false
	}

	now := time.Now()
	w.succeededMu.Lock()
	w.pruneSucceededLocked(now)
	if _, ok := w.succeededRequests[requestID]; ok {
		w.succeededMu.Unlock()
		return true
	}
	w.succeededMu.Unlock()

	// Domain-level dedup: the request stream's terminal state survives
	// restarts and retention purges that the cache and scrape_jobs row do not.
	if w.jobStore != nil {
		if evts, err := w.jobStore.Load(ctx, requestID); err == nil && len(evts) > 0 {
			if agg, err := scrapejob.Replay(requestID, evts); err == nil && agg.Status() == scrapejob.StatusSucceeded {
				w.succeededMu.Lock()
				w.pruneSucceededLocked(now)
				w.succeededRequests[requestID] = now
				w.succeededMu.Unlock()
				return true
			}
		}
	}

	if w.app == nil {
		return false
	}

	job, err := w.scrapeJobByRequestID(requestID)
	if err != nil {
		log.Printf("[worker] Warning: dedupe check failed for request_id=%s: %v", requestID, err)
		return false
	}
	if job == nil || job.GetString("status") != "succeeded" {
		return false
	}

	w.succeededMu.Lock()
	w.pruneSucceededLocked(now)
	w.succeededRequests[requestID] = now
	w.succeededMu.Unlock()
	return true
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
		type failedJob struct{ requestID, artistID string }
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
		for _, job := range failed {
			if ctx.Err() != nil {
				break
			}
			w.recordJobEventWarn(ctx, job.requestID, job.artistID, "recovered", func(j *scrapejob.Job) error {
				_, _, err := j.RecordSucceeded("reconcile", 0, eventsourcing.Correlation{RequestID: job.requestID})
				return err
			})
		}
		now := time.Now().UTC().Format(jobTimestampFormat)
		if err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("UPDATE scrape_jobs SET status = 'succeeded', finished_at = ?, error = ? WHERE request_id = ?;")
			defer func() { _ = stmt.Reset() }()
			for _, job := range failed {
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
			}
			return nil
		}); err != nil {
			log.Printf("[worker] Warning: failed to clear failed jobs for artist %s in SQLite: %v", artistID, err)
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

		return w.markStaleJobArtistFailed(ctx, txApp, job.Id, freshJob.GetString("artist"))
	})
	return jobUpdated, err
}

// markStaleJobArtistFailed updates the artist's fetch_status to "failed" if it
// is currently "pending". It is called inside the same transaction as the job update.
func (w *Worker) markStaleJobArtistFailed(ctx context.Context, txApp core.App, jobID string, artistID string) error {
	if artistID == "" {
		return nil
	}
	artist, err := txApp.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
		q.WithContext(ctx)
		return nil
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("[markStaleJobArtistFailed] artist %s not found for job %s; no-op — job already failed", artistID, jobID)
			return nil
		}
		return fmt.Errorf("failed to load artist %s for stale job %s: %w", artistID, jobID, err)
	}
	if artist.GetString("fetch_status") != "pending" {
		return nil
	}
	artist.Set("fetch_status", "failed")
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := txApp.Save(artist); err != nil {
		return fmt.Errorf("failed to save artist %s for stale job %s: %w", artistID, jobID, err)
	}
	return nil
}
