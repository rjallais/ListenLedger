package handlers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/commands"
	"ListenLedger/internal/correlation"
	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
	"ListenLedger/internal/priority"
)

type batchCandidate struct {
	job                 priority.Job
	requestID           string
	previousFetchStatus string
}

type batchPublishResult struct {
	candidate batchCandidate
	duplicate bool
	err       error
}

type scrapeJobRecordParams struct {
	requestID string
	artistID  string
	queuedAt  time.Time
}

func (h *Handler) queueArtistRefresh(ctx context.Context, record *core.Record) (string, bool, error) {
	return h.queueArtistRefreshWithType(ctx, record, commands.TypeRefresh)
}

// queueArtistRefreshWithType queues a refresh and logs the accepted command
// under the given type (refresh for single, batch for sequential batch path).
func (h *Handler) queueArtistRefreshWithType(ctx context.Context, record *core.Record, cmdType string) (string, bool, error) {
	// Command validation against reconstituted aggregate state: an artist
	// already pending has a live command in flight, so this intent is a
	// duplicate at the domain level — not just the transport level (JetStream
	// 30s dedup, 5min correlation TTL, neither of which survives restarts).
	// Fail-open: without the event store, transport dedup still guards.
	// Streamless artists (pre-event-log rows with no stream yet) fall back
	// to the PocketBase row state: MsgIDs are per-request now, so transport
	// dedup no longer blocks repeats for them.
	if duplicate, err := h.isArtistRefreshPending(ctx, record.Id); err != nil {
		log.Printf("[queueArtistRefresh] aggregate check failed for artist %s, proceeding: %v", record.Id, err)
	} else if duplicate || record.GetString("fetch_status") == "pending" {
		log.Printf("[handlers] Refresh already pending for artist %s (aggregate state)", record.Id)
		return "", true, nil
	}

	requestID := strconv.FormatInt(time.Now().UnixNano(), 10)
	previousFetchStatus := record.GetString("fetch_status")

	// Audit first: open the request's bounded job stream before any row or
	// NATS write, so a crash between steps leaves a ScrapeRequested fact to
	// reconcile instead of a phantom row. Conflicts (same request_id
	// re-queued) are not errors.
	h.appendJobRequested(ctx, requestID, record.Id, cmdType)

	if err := h.markArtistRefreshQueued(ctx, record, requestID); err != nil {
		h.appendJobTransition(ctx, requestID, record.Id, func(j *scrapejob.Job) error {
			_, _, err := j.RecordFailed("queue_mark_failed", eventsourcing.Correlation{RequestID: requestID})
			return err
		})
		return "", false, fmt.Errorf("queueArtistRefresh: mark queued: %w", err)
	}

	correlation.Associate(record.Id, requestID)
	if err := h.createScrapeJobRecord(ctx, requestID, record.Id); err != nil {
		h.appendJobTransition(ctx, requestID, record.Id, func(j *scrapejob.Job) error {
			_, _, err := j.RecordFailed("queue_row_failed", eventsourcing.Correlation{RequestID: requestID})
			return err
		})
		h.rollbackQueueArtistRefresh(record, requestID, previousFetchStatus)
		return "", false, fmt.Errorf("failed to create scrape job record: %w", err)
	}

	req := messaging.NewScrapeRequested(
		record.Id,
		record.GetString("spotify_id"),
		record.GetString("name"),
		requestID,
	)

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ack, err := h.publishScrapeRequest(pubCtx, req)
	if err != nil {
		// Compensating fact: the saga attempted dispatch but NATS failed.
		// Row rollback below restores queue counters; the stream keeps the
		// failure for audit and retry via /api/commands/redrive.
		h.appendJobTransition(ctx, requestID, record.Id, func(j *scrapejob.Job) error {
			_, _, err := j.RecordFailed("publish_failed", eventsourcing.Correlation{RequestID: requestID})
			return err
		})
		h.rollbackQueueArtistRefresh(record, requestID, previousFetchStatus)
		return "", false, fmt.Errorf("queueArtistRefresh: publish scrape request: %w", err)
	}

	if ack != nil && ack.Duplicate {
		h.handleDuplicateAck(record, requestID, previousFetchStatus)
		return requestID, true, nil
	}

	// Command sourcing: log the accepted command for reproducible history.
	// Duplicate-ack path skips logging (the original dispatch logged it);
	// retries/redrives re-execute this row, never add rows.
	h.logScrapeCommand(ctx, cmdType, requestID, record)

	return requestID, false, nil
}

// isArtistRefreshPending reports whether the artist aggregate's fetch state
// is pending (a command is already in flight). Missing aggregates or store
// errors return false with no error — callers fail open to transport dedup.
func (h *Handler) isArtistRefreshPending(ctx context.Context, artistID string) (bool, error) {
	if h.artistRepo == nil || strings.TrimSpace(artistID) == "" {
		return false, nil
	}
	agg, err := h.artistRepo.Load(ctx, artistID)
	if err != nil {
		return false, nil
	}
	return agg.FetchStatus == "pending", nil
}

// appendJobRequested opens the request's bounded job stream. Conflicts mean
// the stream already opened (re-queue/redrive of the same request_id) and
// are not errors. Warn-only: dispatch already succeeded.
func (h *Handler) appendJobRequested(ctx context.Context, requestID, artistID, cmdType string) {
	if h.events == nil || strings.TrimSpace(requestID) == "" || strings.TrimSpace(artistID) == "" {
		return
	}
	agg, err := scrapejob.NewScrapeJob(requestID, artistID, cmdType)
	if err != nil {
		log.Printf("[scrapejob] NewScrapeJob(%s) failed: %v", requestID, err)
		return
	}
	if err := eventsourcing.AppendWithRetry(ctx, h.events, requestID, 0, agg.UncommittedEvents()...); err != nil {
		if isJobStreamConflict(err) {
			return
		}
		log.Printf("[scrapejob] ScrapeRequested(%s) not logged: %v", requestID, err)
	}
}

// appendJobTransition folds a fact into a request stream, seeding pre-log
// streams from (requestID, artistID) first. Warn-only like appendJobRequested.
func (h *Handler) appendJobTransition(ctx context.Context, requestID, artistID string, record func(*scrapejob.Job) error) {
	if h.events == nil || strings.TrimSpace(requestID) == "" {
		return
	}
	evts, err := h.events.Load(ctx, requestID)
	if err != nil {
		log.Printf("[scrapejob] load %s failed: %v", requestID, err)
		return
	}
	var agg *scrapejob.Job
	var base int64
	if len(evts) == 0 {
		agg, err = scrapejob.NewScrapeJob(requestID, artistID, "")
		if err != nil {
			log.Printf("[scrapejob] seed %s failed: %v", requestID, err)
			return
		}
	} else {
		agg, err = scrapejob.Replay(requestID, evts)
		if err != nil {
			log.Printf("[scrapejob] replay %s failed: %v", requestID, err)
			return
		}
		base = agg.Version()
	}
	if err := record(agg); err != nil {
		log.Printf("[scrapejob] record %s failed: %v", requestID, err)
		return
	}
	uncommitted := agg.UncommittedEvents()
	if len(uncommitted) == 0 {
		return
	}
	if err := eventsourcing.AppendWithRetry(ctx, h.events, requestID, base, uncommitted...); err != nil {
		log.Printf("[scrapejob] append %s failed: %v", requestID, err)
	}
}

func isJobStreamConflict(err error) bool {
	return err != nil && (errors.Is(err, eventsourcing.ErrConcurrencyConflict) ||
		strings.Contains(err.Error(), "UNIQUE constraint failed"))
}

// logScrapeCommand appends an accepted scrape command to the durable log.
// Log failures warn only: the dispatch already succeeded and cannot be
// un-published, so failing the request here would lie about queue state.
func (h *Handler) logScrapeCommand(ctx context.Context, cmdType, requestID string, record *core.Record) {
	if h.db == nil || record == nil {
		return
	}
	cmd := commands.Command{
		RequestID:  requestID,
		Type:       cmdType,
		ArtistID:   record.Id,
		SpotifyID:  record.GetString("spotify_id"),
		ArtistName: record.GetString("name"),
	}
	if err := commands.Log(ctx, h.db, cmd); err != nil {
		log.Printf("[commands] failed logging %s command %s for artist %s: %v", cmdType, requestID, record.Id, err)
	}
}

func (h *Handler) rollbackQueueArtistRefresh(record *core.Record, requestID, previousFetchStatus string) {
	h.cleanupQueueArtistRefresh(record, requestID, previousFetchStatus, "rollback")
}

func (h *Handler) handleDuplicateAck(record *core.Record, requestID, previousFetchStatus string) {
	h.cleanupQueueArtistRefresh(record, requestID, previousFetchStatus, "duplicate acknowledgement")
}

func (h *Handler) cleanupQueueArtistRefresh(record *core.Record, requestID, previousFetchStatus, reason string) {
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCleanup()
	if rollbackErr := h.unmarkArtistRefreshQueued(cleanupCtx, record, previousFetchStatus, requestID); rollbackErr != nil {
		log.Printf("[queueArtistRefresh] %s failed to restore artist %s status: %v", reason, record.Id, rollbackErr)
	}
	correlation.Clear(record.Id)
	if delErr := h.deleteScrapeJobRecordByRequestID(cleanupCtx, requestID, record.Id); delErr != nil {
		log.Printf("[queueArtistRefresh] %s failed to delete scrape job %s for artist %s: %v", reason, requestID, record.Id, delErr)
	}
}

func createScrapeJobRecordTx(ctx context.Context, txApp core.App, jobsCol *core.Collection, params scrapeJobRecordParams) error {
	if params.requestID == "" || params.artistID == "" {
		return errors.New("requestID and artistID are required")
	}
	job := core.NewRecord(jobsCol)
	job.Set("request_id", params.requestID)
	job.Set("artist", params.artistID)
	job.Set("status", "queued")
	job.Set("attempts", 0)
	job.Set("queued_at", params.queuedAt)
	job.Set("error", "")
	job.Set("started_at", nil)
	job.Set("finished_at", nil)
	return txApp.SaveWithContext(ctx, job)
}

func (h *Handler) enqueueBatchRefreshJobs(ctx context.Context, jobs []priority.Job, count int) ([]string, map[string]int) {
	if len(jobs) == 0 || count <= 0 {
		return nil, make(map[string]int)
	}
	if h.app == nil {
		return h.enqueueBatchRefreshJobsSequential(ctx, jobs, count)
	}

	candidates := batchRefreshCandidates(jobs, count, time.Now())
	if len(candidates) == 0 {
		return nil, make(map[string]int)
	}
	if err := h.persistBatchRefreshCandidates(ctx, candidates); err != nil {
		log.Printf("[batch] Transaction failed to queue batch artists: %v", err)
		return nil, make(map[string]int)
	}

	results := h.publishBatchRefreshCandidates(ctx, candidates)
	return h.processBatchRefreshResults(ctx, results)
}

func (h *Handler) enqueueBatchRefreshJobsSequential(ctx context.Context, jobs []priority.Job, count int) ([]string, map[string]int) {
	queuedArtistIDs := make([]string, 0, min(len(jobs), count))
	stats := make(map[string]int)
	for _, job := range jobs {
		if len(queuedArtistIDs) >= count {
			break
		}
		record := job.Record
		_, duplicate, err := h.queueArtistRefreshWithType(ctx, record, commands.TypeBatch)
		if err != nil || duplicate {
			continue
		}
		queuedArtistIDs = append(queuedArtistIDs, record.Id)
		stats[job.Priority.String()]++
	}
	return queuedArtistIDs, stats
}

func batchRefreshCandidates(jobs []priority.Job, count int, now time.Time) []batchCandidate {
	targetCount := min(len(jobs), count)
	candidates := make([]batchCandidate, 0, targetCount)
	baseNano := now.UnixNano()
	for index, job := range jobs {
		if len(candidates) >= targetCount {
			break
		}
		record := job.Record
		candidates = append(candidates, batchCandidate{
			job:                 job,
			requestID:           strconv.FormatInt(baseNano+int64(index), 10),
			previousFetchStatus: record.GetString("fetch_status"),
		})
	}
	return candidates
}

func (h *Handler) persistBatchRefreshCandidates(ctx context.Context, candidates []batchCandidate) error {
	err := h.app.RunInTransaction(func(txApp core.App) error {
		jobsCol, err := txApp.FindCollectionByNameOrId("scrape_jobs")
		if err != nil {
			return fmt.Errorf("scrape_jobs collection not found: %w", err)
		}
		txNow := time.Now()
		for _, candidate := range candidates {
			record := candidate.job.Record
			record.Set("fetch_status", "pending")
			if err := txApp.SaveWithContext(ctx, record); err != nil {
				return fmt.Errorf("batch save artist %s: %w", record.Id, err)
			}
			if err := createScrapeJobRecordTx(ctx, txApp, jobsCol, scrapeJobRecordParams{
				requestID: candidate.requestID,
				artistID:  record.Id,
				queuedAt:  txNow,
			}); err != nil {
				return fmt.Errorf("batch create scrape job %s: %w", record.Id, err)
			}
			correlation.Associate(record.Id, candidate.requestID)
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Event side: fold pending facts with saga request_ids so aggregate state
	// converges with the rows above. Best-effort; rows are the compat mirror.
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			break
		}
		h.emitArtistFetchStatus(ctx, candidate.job.Record.Id, "pending", "batch queue", candidate.requestID)
	}
	// SQLite mirror: the PB transaction above created the scrape_jobs rows but
	// nothing mirrors them (unlike createScrapeJobRecord's single path), so
	// worker row transitions and SQLite queue counts would miss batch jobs.
	if h.db != nil {
		now := time.Now().UTC().Format("2006-01-02 15:04:05.000Z")
		if err := h.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep(`INSERT INTO scrape_jobs (id, request_id, artist_id, status, attempts, error, queued_at)
				VALUES (?, ?, ?, 'queued', 0, '', ?)
				ON CONFLICT(request_id) DO NOTHING;`)
			defer func() { _ = stmt.Reset() }()
			statusStmt := tx.Prep(`UPDATE artists SET fetch_status = 'pending' WHERE id = ?;`)
			defer func() { _ = statusStmt.Reset() }()
			for _, candidate := range candidates {
				if ctx.Err() != nil {
					break
				}
				_ = stmt.Reset()
				stmt.BindText(1, candidate.requestID)
				stmt.BindText(2, candidate.requestID)
				stmt.BindText(3, candidate.job.Record.Id)
				stmt.BindText(4, now)
				if _, err := stmt.Step(); err != nil {
					return err
				}
				// Artists mirror: without it SQLite keeps idle while PB and
				// scrape_jobs say queued, so the batch reconciler would
				// complete members instantly and queue counts under-report.
				_ = statusStmt.Reset()
				statusStmt.BindText(1, candidate.job.Record.Id)
				if _, err := statusStmt.Step(); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			log.Printf("[batch] Warning: failed to mirror %d scrape jobs to SQLite: %v", len(candidates), err)
		}
	}
	return nil
}

func (h *Handler) publishBatchRefreshCandidates(ctx context.Context, candidates []batchCandidate) []batchPublishResult {
	results := make([]batchPublishResult, len(candidates))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 16)
	for index, candidate := range candidates {
		wg.Go(func() {
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Audit first like the single-refresh path: open the job stream so a
			// crash between row persist and publish leaves a fact to reconcile.
			h.appendJobRequested(ctx, candidate.requestID, candidate.job.Record.Id, commands.TypeBatch)
			req := messaging.NewScrapeRequested(
				candidate.job.Record.Id,
				candidate.job.Record.GetString("spotify_id"),
				candidate.job.Record.GetString("name"),
				candidate.requestID,
			)
			pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			ack, err := h.publishScrapeRequest(pubCtx, req)
			results[index] = batchPublishResult{
				candidate: candidate,
				duplicate: ack != nil && ack.Duplicate,
				err:       err,
			}
		})
	}
	wg.Wait()
	return results
}

func (h *Handler) processBatchRefreshResults(ctx context.Context, results []batchPublishResult) ([]string, map[string]int) {
	queuedArtistIDs := make([]string, 0, len(results))
	stats := make(map[string]int)
	for _, result := range results {
		candidate := result.candidate
		record := candidate.job.Record
		switch {
		case result.err != nil:
			log.Printf("[batch] Failed to publish scrape request for %s: %v — rolling back", record.GetString("name"), result.err)
			h.appendJobTransition(ctx, candidate.requestID, record.Id, func(j *scrapejob.Job) error {
				_, _, err := j.RecordFailed("publish_failed", eventsourcing.Correlation{RequestID: candidate.requestID})
				return err
			})
			h.rollbackQueueArtistRefresh(record, candidate.requestID, candidate.previousFetchStatus)
		case result.duplicate:
			log.Printf("[batch] Duplicate request for %s skipped — cleaning up", record.GetString("name"))
			h.handleDuplicateAck(record, candidate.requestID, candidate.previousFetchStatus)
		default:
			queuedArtistIDs = append(queuedArtistIDs, record.Id)
			stats[candidate.job.Priority.String()]++
			h.logScrapeCommand(ctx, commands.TypeBatch, candidate.requestID, record)
			h.appendJobRequested(ctx, candidate.requestID, record.Id, commands.TypeBatch)
		}
	}
	return queuedArtistIDs, stats
}
