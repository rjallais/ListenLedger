// Package handlers provides HTTP request handlers and helpers for dashboard
// routes, queue management, and scrape refresh workflows.
package handlers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/router"
	"github.com/starfederation/datastar-go/datastar"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/correlation"
	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/messaging"
	"ListenLedger/internal/quota"
	"ListenLedger/templates"
)

func (h *Handler) publishScrapeRequest(ctx context.Context, req messaging.ScrapeRequested) (*jetstream.PubAck, error) {
	// MsgID = request_id (saga idempotency key). Falls back to artistID only
	// for legacy callers that left RequestID empty.
	msgID := messaging.ScrapeRequestMsgID(req.RequestID)
	if req.RequestID == "" {
		msgID = messaging.ScrapeRequestMsgID(req.ArtistID)
	}

	ack, err := messaging.PublishScrapeRequested(ctx, h.js, req, msgID)
	if err != nil {
		return nil, fmt.Errorf("failed to publish scrape request: %w", err)
	}
	return ack, nil
}

func (h *Handler) createScrapeJobRecord(ctx context.Context, requestID, artistID string) error {
	if requestID == "" || artistID == "" {
		return errors.New("requestID and artistID are required")
	}
	jobs, err := h.app.FindCollectionByNameOrId("scrape_jobs")
	if err != nil {
		log.Printf("[handlers] Warning: scrape_jobs collection not found: %v", err)
		return fmt.Errorf("scrape_jobs collection not found: %w", err)
	}

	job := core.NewRecord(jobs)
	job.Set("request_id", requestID)
	job.Set("artist", artistID)
	job.Set("status", "queued")
	job.Set("attempts", 0)
	job.Set("queued_at", time.Now())
	job.Set("error", "")
	job.Set("started_at", nil)
	job.Set("finished_at", nil)
	if err := h.app.SaveWithContext(ctx, job); err != nil {
		log.Printf("[handlers] Warning: failed to create scrape job record: %v", err)
		return fmt.Errorf("failed to create scrape job record: %w", err)
	}
	// SQLite-first: mirror the queue row into the SQLite read model so worker
	// status transitions (processing/succeeded/failed) have a row to update
	// even if PocketBase is retired. SQLite is the emerging source of truth;
	// PocketBase stays as compat until reads fully cut over.
	if h.db != nil {
		now := time.Now().UTC().Format("2006-01-02 15:04:05.000Z")
		if err := h.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep(`INSERT INTO scrape_jobs (id, request_id, artist_id, status, attempts, error, queued_at)
				VALUES (?, ?, ?, 'queued', 0, '', ?)
				ON CONFLICT(request_id) DO NOTHING;`)
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, requestID)
			stmt.BindText(2, requestID)
			stmt.BindText(3, artistID)
			stmt.BindText(4, now)
			_, err := stmt.Step()
			return err
		}); err != nil {
			log.Printf("[handlers] Warning: failed to mirror scrape job %s to SQLite: %v", requestID, err)
		}
	}
	return nil
}

type queueRetryStats struct {
	Candidates           int `json:"candidates"`
	Retried              int `json:"retried"`
	Duplicate            int `json:"duplicate"`
	PendingSkipped       int `json:"pending_skipped"`
	PublishFailed        int `json:"publish_failed"`
	InvalidArtist        int `json:"invalid_artist"`
	OrphanPendingReset   int `json:"orphan_pending_reset"`
	FailedArtistsMarked  int `json:"failed_artists_marked"`
	OrphanStreamMessages int `json:"orphan_stream_messages"`
	StaleQueuedExpired   int `json:"stale_queued_expired"`
}

type retryJobParams struct {
	Job                 *core.Record
	Artist              *core.Record
	ArtistID            string
	RequestID           string
	PreviousFetchStatus string
	PublishErr          error
}

func normalizeQueueRetryLimit(limit int) int {
	if limit <= 0 {
		return 250
	}
	return limit
}

func (h *Handler) reconcileOrphanQueueState(ctx context.Context) (queueRetryStats, error) {
	var stats queueRetryStats
	if err := h.expireStaleQueuedJobs(ctx, &stats); err != nil {
		return stats, err
	}
	if err := h.resetOrphanPendingArtists(ctx, &stats); err != nil {
		return stats, err
	}
	if err := h.markFailedJobArtists(ctx, &stats); err != nil {
		return stats, err
	}
	if err := h.purgeOrphanScrapeRequests(ctx, &stats); err != nil {
		return stats, err
	}
	return stats, nil
}

type reconcileArtistParams struct {
	Exp      dbx.Expression
	Status   string
	LogLabel string
	Counter  *int
}

// recordFetchStatusConvergence folds fetch-status facts for reconciled artists
// into their streams and projections (best-effort). The table flip above
// already happened; without the matching facts, aggregate-based command
// validation would decide on stale pending state. Guarded to pending-only so
// a scrape that finished mid-reconcile keeps its idle state everywhere.
func (h *Handler) recordFetchStatusConvergence(ctx context.Context, artistIDs []string, status string) {
	if h.artistRepo == nil {
		return
	}
	for _, artistID := range artistIDs {
		if err := ctx.Err(); err != nil {
			return
		}
		agg, err := h.artistRepo.Load(ctx, artistID)
		if err != nil {
			continue
		}
		if agg.FetchStatus != "pending" {
			continue
		}
		if err := agg.SetFetchStatus(status, "queue reconcile"); err != nil {
			continue
		}
		events, err := h.artistRepo.Save(ctx, agg)
		if err != nil {
			log.Printf("[queue-retry] fetch-status event for %s not saved: %v", artistID, err)
			continue
		}
		if h.artistProjection != nil && len(events) > 0 {
			if err := h.artistProjection.Project(ctx, agg, events); err != nil {
				log.Printf("[queue-retry] fetch-status projection for %s failed: %v", artistID, err)
			}
		}
	}
}

// sqliteReconcileParams drives the SQLite side of artist-status
// reconciliation: select up to 500 matching IDs, flip them in one UPDATE,
// clear their correlation entries, and bump the counter.
//
// Recheck re-applies the orphan predicate inside the UPDATE so a concurrent
// refresh that made an artist non-orphan between SELECT and UPDATE leaves it
// unchanged. RETURNING collects only rows actually flipped for convergence.
type sqliteReconcileParams struct {
	SelectIDs string
	// Recheck is the job-subquery condition from SelectIDs, re-evaluated at
	// write time (e.g. "id NOT IN (SELECT artist_id FROM ...)").
	Recheck  string
	Status   string
	LogLabel string
	Counter  *int
}

// reconcileArtistsWithStatusSQLite mirrors reconcileArtistsWithStatus against
// the SQLite read model. IDs are collected first so correlation entries clear
// only for rows actually reset. Returns the flipped IDs so callers can fold
// the matching fetch-status facts into the artist streams (aggregate/log
// convergence — the table flip alone would leave stale pending facts behind).
func (h *Handler) reconcileArtistsWithStatusSQLite(ctx context.Context, p sqliteReconcileParams) ([]string, error) {
	var ids []string
	err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(p.SelectIDs)
		defer func() { _ = stmt.Reset() }()
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			ids = append(ids, stmt.ColumnText(0))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("query %s artists (SQLite): %w", p.LogLabel, err)
	}
	if len(ids) == 500 {
		log.Printf("[queue-retry] %s reconciliation hit 500-record cap; remaining artists will be processed on next retry", p.LogLabel)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	placeholders := strings.Repeat("?,", len(ids))
	placeholders = strings.TrimSuffix(placeholders, ",")
	var updated []string
	err = h.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		query := "UPDATE artists SET fetch_status = ? WHERE id IN (" + placeholders + ") AND fetch_status = 'pending'"
		if recheck := strings.TrimSpace(p.Recheck); recheck != "" {
			query += " AND " + recheck
		}
		query += " RETURNING id;"
		stmt := tx.Prep(query)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, p.Status)
		for i, id := range ids {
			stmt.BindText(i+2, id)
		}
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			updated = append(updated, stmt.ColumnText(0))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("update %s artists (SQLite): %w", p.LogLabel, err)
	}
	for _, id := range updated {
		correlation.Clear(id)
	}
	*p.Counter += len(updated)
	return updated, nil
}

// queuedJobExpiry bounds how long a `queued` row may linger. The
// SCRAPE_REQUESTS stream retains messages for 24h, so a queued row older
// than that has no live JetStream message behind it (duplicate-ack cleanup
// miss or crash between row create and publish) and would otherwise block
// stream purges and inflate the queue widget forever. Expiring it to `failed`
// lets the normal retry path requeue the artist honestly.
const queuedJobExpiry = 24 * time.Hour

// ReconcileQueueOnStartup runs the orphan-queue reconciliation once at boot
// (stale queued rows, orphan pending artists, orphan stream messages) and
// notifies SSE subscribers so the UI converges without a manual retry.
// It is idempotent and safe to run on every startup.
func (h *Handler) ReconcileQueueOnStartup(ctx context.Context) error {
	stats, err := h.reconcileOrphanQueueState(ctx)
	if err != nil {
		return fmt.Errorf("startup queue reconcile: %w", err)
	}
	log.Printf("[queue-retry] startup reconcile: orphan_pending_reset=%d failed_artists_marked=%d orphan_stream_messages=%d stale_queued_expired=%d",
		stats.OrphanPendingReset, stats.FailedArtistsMarked, stats.OrphanStreamMessages, stats.StaleQueuedExpired)
	h.notifyQueueUpdated()
	return nil
}

// expireStaleQueuedJobs marks `queued` rows older than queuedJobExpiry as
// failed. Beyond stream retention no redelivery can occur, so the row is a
// phantom; failing it unblocks stream purges and surfaces the artist to the
// retry path instead of wedging the queue counters.
func (h *Handler) expireStaleQueuedJobs(ctx context.Context, stats *queueRetryStats) error {
	cutoff := time.Now().UTC().Add(-queuedJobExpiry).Format("2006-01-02 15:04:05.000Z")

	// Dual-write: repair the SQLite read model and the PocketBase compat
	// rows. Counters below sum per-store repairs (a job mirrored in both
	// stores counts once per store it was repaired in).
	if h.db != nil {
		if err := h.expireStaleQueuedJobsSQLite(ctx, cutoff, stats); err != nil {
			return err
		}
	}
	if h.app == nil {
		return nil
	}

	records := make([]*core.Record, 0)
	err := h.app.RecordQuery("scrape_jobs").
		WithContext(ctx).
		AndWhere(dbx.NewExp("status = {:status} AND queued_at < {:cutoff}",
			dbx.Params{"status": "queued", "cutoff": cutoff})).
		Limit(500).
		All(&records)
	if err != nil {
		return fmt.Errorf("query stale queued jobs: %w", err)
	}
	if len(records) == 500 {
		log.Printf("[queue-retry] stale queued expiry hit 500-record cap; remainder on next run")
	}

	for _, job := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		job.Set("status", "failed")
		job.Set("finished_at", time.Now())
		job.Set("error", "queue_expired")
		if err := h.app.SaveWithContext(ctx, job); err != nil {
			return fmt.Errorf("expire stale queued job %s: %w", job.Id, err)
		}
		stats.StaleQueuedExpired++
	}
	if stats.StaleQueuedExpired > 0 {
		log.Printf("[queue-retry] expired %d stale queued job(s) older than %s", stats.StaleQueuedExpired, queuedJobExpiry)
	}
	return nil
}

// expireStaleQueuedJobsSQLite expires phantom queued rows in one indexed
// UPDATE. Timestamps share the "2006-01-02 15:04:05.000Z" format on both
// sides, so the lexicographic comparison is chronological. Each expired
// request keeps its own ScrapeFailed(queue_expired) fact in its job stream.
func (h *Handler) expireStaleQueuedJobsSQLite(ctx context.Context, cutoff string, stats *queueRetryStats) error {
	type expiredJob struct{ requestID, artistID string }
	var expired []expiredJob
	err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT request_id, artist_id FROM scrape_jobs WHERE status = 'queued' AND queued_at < ? LIMIT 500;")
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
			expired = append(expired, expiredJob{requestID: stmt.ColumnText(0), artistID: stmt.ColumnText(1)})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("query stale queued jobs (SQLite): %w", err)
	}
	if len(expired) == 0 {
		return nil
	}
	now := time.Now().UTC().Format("2006-01-02 15:04:05.000Z")
	// Scope the expiry to the capped SELECT above: a blind mass UPDATE could
	// fail jobs beyond the 500-row cap that this run never emits facts for.
	placeholders := strings.Repeat("?,", len(expired))
	placeholders = strings.TrimSuffix(placeholders, ",")
	// Collect the rows the UPDATE actually changes: a worker can move a
	// selected job to processing before the UPDATE runs, and the status guard
	// then skips it. Facts below must cover only changed rows, never skipped
	// ones (which would record a false failure for a running job).
	var updated []expiredJob
	err = h.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("UPDATE scrape_jobs SET status = 'failed', finished_at = ?, error = 'queue_expired' WHERE status = 'queued' AND queued_at < ? AND request_id IN (" + placeholders + ") RETURNING request_id, artist_id;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, now)
		stmt.BindText(2, cutoff)
		for i, job := range expired {
			stmt.BindText(i+3, job.requestID)
		}
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			updated = append(updated, expiredJob{requestID: stmt.ColumnText(0), artistID: stmt.ColumnText(1)})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("expire stale queued jobs (SQLite): %w", err)
	}
	stats.StaleQueuedExpired = len(updated)
	if len(expired) == 500 {
		log.Printf("[queue-retry] stale queued expiry hit 500-record cap; remainder on next run")
	}
	log.Printf("[queue-retry] expired %d stale queued job(s) older than %s", len(updated), queuedJobExpiry)
	for _, job := range updated {
		if err := ctx.Err(); err != nil {
			return err
		}
		h.appendJobTransition(ctx, job.requestID, job.artistID, func(j *scrapejob.Job) error {
			_, _, err := j.RecordFailed("queue_expired")
			return err
		})
	}
	return nil
}

func (h *Handler) reconcileArtistsWithStatus(ctx context.Context, p reconcileArtistParams) error {
	records := make([]*core.Record, 0)
	err := h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(p.Exp).
		Limit(500).
		All(&records)
	if err != nil {
		return fmt.Errorf("query %s artists: %w", p.LogLabel, err)
	}
	if len(records) == 500 {
		log.Printf("[queue-retry] %s reconciliation hit 500-record cap; remaining artists will be processed on next retry", p.LogLabel)
	}

	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		record.Set("fetch_status", p.Status)
		if err := h.app.SaveWithContext(ctx, record); err != nil {
			return fmt.Errorf("update %s artist %s: %w", p.LogLabel, record.Id, err)
		}
		correlation.Clear(record.Id)
		*p.Counter++
	}

	return nil
}

func (h *Handler) resetOrphanPendingArtists(ctx context.Context, stats *queueRetryStats) error {
	if h.db != nil {
		ids, err := h.reconcileArtistsWithStatusSQLite(ctx, sqliteReconcileParams{
			SelectIDs: `SELECT id FROM artists WHERE fetch_status = 'pending'
				AND id NOT IN (SELECT artist_id FROM scrape_jobs WHERE status IN ('queued', 'processing', 'failed'))
				LIMIT 500;`,
			Recheck:  `id NOT IN (SELECT artist_id FROM scrape_jobs WHERE status IN ('queued', 'processing', 'failed'))`,
			Status:   "idle",
			LogLabel: "orphan pending",
			Counter:  &stats.OrphanPendingReset,
		})
		if err != nil {
			return err
		}
		h.recordFetchStatusConvergence(ctx, ids, "idle")
	}
	if h.app == nil {
		return nil
	}
	exp := dbx.NewExp(
		"fetch_status = {:pending} AND id NOT IN (SELECT artist FROM scrape_jobs WHERE status IN ({:queued}, {:processing}, {:failed}))",
		dbx.Params{
			"pending":    "pending",
			"queued":     "queued",
			"processing": "processing",
			"failed":     "failed",
		},
	)
	return h.reconcileArtistsWithStatus(ctx, reconcileArtistParams{
		Exp:      exp,
		Status:   "idle",
		LogLabel: "orphan pending",
		Counter:  &stats.OrphanPendingReset,
	})
}

func (h *Handler) markFailedJobArtists(ctx context.Context, stats *queueRetryStats) error {
	if h.db != nil {
		ids, err := h.reconcileArtistsWithStatusSQLite(ctx, sqliteReconcileParams{
			SelectIDs: `SELECT id FROM artists WHERE fetch_status = 'pending'
				AND id IN (SELECT artist_id FROM scrape_jobs WHERE status = 'failed')
				AND id NOT IN (SELECT artist_id FROM scrape_jobs WHERE status IN ('queued', 'processing'))
				LIMIT 500;`,
			Recheck: `id IN (SELECT artist_id FROM scrape_jobs WHERE status = 'failed')
				AND id NOT IN (SELECT artist_id FROM scrape_jobs WHERE status IN ('queued', 'processing'))`,
			Status:   "failed",
			LogLabel: "failed-job pending",
			Counter:  &stats.FailedArtistsMarked,
		})
		if err != nil {
			return err
		}
		h.recordFetchStatusConvergence(ctx, ids, "failed")
	}
	if h.app == nil {
		return nil
	}
	exp := dbx.NewExp(
		`fetch_status = {:pending}
		 AND id IN (SELECT artist FROM scrape_jobs WHERE status = {:failed})
		 AND id NOT IN (
		   SELECT artist FROM scrape_jobs WHERE status IN ({:queued}, {:processing})
		 )`,
		dbx.Params{
			"pending":    "pending",
			"failed":     "failed",
			"queued":     "queued",
			"processing": "processing",
		},
	)
	return h.reconcileArtistsWithStatus(ctx, reconcileArtistParams{
		Exp:      exp,
		Status:   "failed",
		LogLabel: "failed-job pending",
		Counter:  &stats.FailedArtistsMarked,
	})
}

func (h *Handler) purgeOrphanScrapeRequests(ctx context.Context, stats *queueRetryStats) error {
	streamInfo, consumerAvailable, queueAckPending, queueRedelivered := h.loadQueueConsumerState(ctx)
	if h.shouldSkipStreamPurge(streamInfo, consumerAvailable, queueAckPending, queueRedelivered) {
		return nil
	}

	jobsQueued, jobsProcessing, _, artistsPending := h.countQueueState(ctx)
	if h.hasPendingJobs(jobsQueued, jobsProcessing, artistsPending) {
		return nil
	}

	stream, err := h.js.Stream(ctx, messaging.ScrapeRequestsStreamName)
	if err != nil {
		return fmt.Errorf("load scrape request stream for purge: %w", err)
	}
	if err := stream.Purge(ctx, jetstream.WithPurgeSubject(messaging.SubjectScrapeRequest)); err != nil {
		return fmt.Errorf("purge orphan scrape requests: %w", err)
	}
	stats.OrphanStreamMessages = int(streamInfo.State.Msgs)
	return nil
}

func (h *Handler) shouldSkipStreamPurge(streamInfo *jetstream.StreamInfo, consumerAvailable bool, queueAckPending, queueRedelivered uint64) bool {
	return streamInfo == nil || !consumerAvailable || streamInfo.State.Msgs == 0 || queueAckPending > 0 || queueRedelivered > 0
}

func (h *Handler) hasPendingJobs(jobsQueued, jobsProcessing, artistsPending int64) bool {
	return jobsQueued > 0 || jobsProcessing > 0 || artistsPending > 0
}

func (h *Handler) scrapeJobsByStatus(ctx context.Context, status string, limit int) ([]*core.Record, error) {
	if strings.TrimSpace(status) == "" {
		return nil, nil
	}
	records := make([]*core.Record, 0)
	err := h.app.RecordQuery("scrape_jobs").
		WithContext(ctx).
		AndWhere(dbx.NewExp("status = {:status}", dbx.Params{"status": status})).
		OrderBy("queued_at DESC").
		Limit(int64(limit)).
		All(&records)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (h *Handler) collectRetryCandidates(ctx context.Context, limit int) ([]*core.Record, error) {
	failedRecords, err := h.scrapeJobsByStatus(ctx, "failed", limit)
	if err != nil {
		return nil, fmt.Errorf("query failed jobs: %w", err)
	}

	remaining := max(limit-len(failedRecords), 0)

	queuedRecords, err := h.scrapeJobsByStatus(ctx, "queued", remaining)
	if err != nil {
		return nil, fmt.Errorf("query queued jobs: %w", err)
	}

	records := make([]*core.Record, 0, len(failedRecords)+len(queuedRecords))
	records = append(records, failedRecords...)
	records = append(records, queuedRecords...)
	return records, nil
}

func (h *Handler) lookupRetryArtist(ctx context.Context, artistID string, stats *queueRetryStats) (*core.Record, bool, error) {
	artist, findErr := h.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
		q.WithContext(ctx)
		return nil
	})
	if findErr != nil {
		if router.ToApiError(findErr).Status == http.StatusNotFound {
			stats.InvalidArtist++
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to find artist %s: %w", artistID, findErr)
	}
	if artist == nil {
		stats.InvalidArtist++
		return nil, false, nil
	}
	if artist.GetString("fetch_status") == "pending" {
		stats.PendingSkipped++
		return nil, false, nil
	}
	return artist, true, nil
}

func (h *Handler) prepareRetryRequest(ctx context.Context, job *core.Record, seenArtist map[string]struct{}, stats *queueRetryStats) (messaging.ScrapeRequested, *core.Record, string, string, bool, error) {
	artistID := strings.TrimSpace(job.GetString("artist"))
	if artistID == "" {
		stats.InvalidArtist++
		return messaging.ScrapeRequested{}, nil, "", "", false, nil
	}
	if _, seen := seenArtist[artistID]; seen {
		return messaging.ScrapeRequested{}, nil, "", "", false, nil
	}
	seenArtist[artistID] = struct{}{}

	artist, ok, err := h.lookupRetryArtist(ctx, artistID, stats)
	if err != nil || !ok {
		return messaging.ScrapeRequested{}, nil, "", "", false, err
	}

	spotifyID := strings.TrimSpace(artist.GetString("spotify_id"))
	if spotifyID == "" {
		stats.InvalidArtist++
		return messaging.ScrapeRequested{}, nil, "", "", false, nil
	}

	requestID := strings.TrimSpace(job.GetString("request_id"))
	if requestID == "" {
		requestID = strconv.FormatInt(time.Now().UnixNano(), 10)
		job.Set("request_id", requestID)
	}

	req := messaging.NewScrapeRequested(
		artistID,
		spotifyID,
		artist.GetString("name"),
		requestID,
	)
	return req, artist, artistID, requestID, true, nil
}

func (h *Handler) publishRetryRequest(ctx context.Context, req messaging.ScrapeRequested) (*jetstream.PubAck, error) {
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return h.publishScrapeRequest(pubCtx, req)
}

func (h *Handler) saveRetryPublishFailure(ctx context.Context, job *core.Record, pubErr error) error {
	job.Set("status", "failed")
	job.Set("error", fmt.Sprintf("retry publish failed: %v", pubErr))
	job.Set("finished_at", time.Now())
	if saveErr := h.app.SaveWithContext(ctx, job); saveErr != nil {
		return fmt.Errorf("save publish error for job %s: %w", job.Id, saveErr)
	}
	return nil
}

func (h *Handler) saveRetryDeduped(ctx context.Context, job *core.Record) error {
	job.Set("status", "succeeded")
	job.Set("error", "deduped_existing_request")
	job.Set("finished_at", time.Now())
	if saveErr := h.app.SaveWithContext(ctx, job); saveErr != nil {
		return fmt.Errorf("save deduped status for job %s: %w", job.Id, saveErr)
	}
	return nil
}

func (h *Handler) saveRetryQueuedAndMarkPending(ctx context.Context, params retryJobParams) error {
	err := h.app.RunInTransaction(func(txApp core.App) error {
		params.Job.Set("status", "queued")
		params.Job.Set("queued_at", time.Now())
		params.Job.Set("error", "")
		params.Job.Set("started_at", nil)
		params.Job.Set("finished_at", nil)
		if saveErr := txApp.SaveWithContext(ctx, params.Job); saveErr != nil {
			return fmt.Errorf("save queued status for job %s: %w", params.Job.Id, saveErr)
		}

		params.Artist.Set("fetch_status", "pending")
		if saveErr := txApp.SaveWithContext(ctx, params.Artist); saveErr != nil {
			return fmt.Errorf("mark artist %s pending: %w", params.ArtistID, saveErr)
		}
		return nil
	})
	if err != nil {
		return err
	}
	correlation.Associate(params.ArtistID, params.RequestID)
	return nil
}

func (h *Handler) processRetryCandidate(ctx context.Context, job *core.Record, seenArtist map[string]struct{}, stats *queueRetryStats) error {
	req, artist, artistID, requestID, ok, err := h.prepareRetryRequest(ctx, job, seenArtist, stats)
	if err != nil {
		return fmt.Errorf("failed to prepare retry request: %w", err)
	}
	if !ok {
		return nil
	}

	previousFetchStatus := artist.GetString("fetch_status")
	params := retryJobParams{
		Job:                 job,
		Artist:              artist,
		ArtistID:            artistID,
		RequestID:           requestID,
		PreviousFetchStatus: previousFetchStatus,
	}
	if err := h.saveRetryQueuedAndMarkPending(ctx, params); err != nil {
		return err
	}

	ack, pubErr := h.publishRetryRequest(ctx, req)
	if pubErr != nil {
		params.PublishErr = pubErr
		return h.handleRetryPublishFailure(params, stats)
	}

	if ack != nil && ack.Duplicate {
		return h.handleRetryDuplicate(params, stats)
	}

	stats.Retried++
	return nil
}

func (h *Handler) retryFailedAndQueuedJobs(ctx context.Context, limit int, stats queueRetryStats) (queueRetryStats, error) {
	limit = normalizeQueueRetryLimit(limit)
	records, err := h.collectRetryCandidates(ctx, limit)
	if err != nil {
		return stats, err
	}

	stats.Candidates = len(records)
	seenArtist := make(map[string]struct{}, len(records))

	for _, job := range records {
		if err := h.processRetryCandidate(ctx, job, seenArtist, &stats); err != nil {
			return stats, err
		}
	}

	return stats, nil
}

func (h *Handler) rollbackAndSaveJobStatus(params retryJobParams, saveFn func(context.Context, *core.Record) error) error {
	if err := h.rollbackRetryQueuedState(params.Artist, params.ArtistID, params.PreviousFetchStatus); err != nil {
		return err
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cleanupCancel()
	return saveFn(cleanupCtx, params.Job)
}

func (h *Handler) handleRetryPublishFailure(params retryJobParams, stats *queueRetryStats) error {
	stats.PublishFailed++
	return h.rollbackAndSaveJobStatus(params, func(ctx context.Context, job *core.Record) error {
		if saveErr := h.saveRetryPublishFailure(ctx, job, params.PublishErr); saveErr != nil {
			return fmt.Errorf("failed to record publish failure: %w", saveErr)
		}
		return nil
	})
}

func (h *Handler) handleRetryDuplicate(params retryJobParams, stats *queueRetryStats) error {
	stats.Duplicate++
	err := h.rollbackAndSaveJobStatus(params, func(ctx context.Context, job *core.Record) error {
		if saveErr := h.saveRetryDeduped(ctx, job); saveErr != nil {
			return fmt.Errorf("failed to record deduped status: %w", saveErr)
		}
		return nil
	})
	if err != nil {
		return err
	}
	h.deleteRetryScrapeJob(params.RequestID, params.ArtistID)
	return nil
}

func (h *Handler) rollbackRetryQueuedState(artist *core.Record, artistID, previousFetchStatus string) error {
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status := strings.TrimSpace(previousFetchStatus)
	if status == "" {
		status = "idle"
	}
	artist.Set("fetch_status", status)
	if err := h.app.SaveWithContext(rollbackCtx, artist); err != nil {
		return fmt.Errorf("restore fetch_status for artist %s: %w", artistID, err)
	}
	correlation.Clear(artistID)
	return nil
}

func (h *Handler) deleteRetryScrapeJob(requestID, artistID string) {
	deleteCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.deleteScrapeJobRecordByRequestID(deleteCtx, requestID, artistID); err != nil {
		log.Printf("[queue-retry] warning: failed to delete scrape job for artist %s: %v", artistID, err)
	}
}

func (h *Handler) loadQueueConsumerState(ctx context.Context) (*jetstream.StreamInfo, bool, uint64, uint64) {
	stream, err := h.js.Stream(ctx, messaging.ScrapeRequestsStreamName)
	if err != nil {
		log.Printf("[queue] Warning: failed to load stream handle: %v", err)
		return nil, false, 0, 0
	}

	var (
		streamInfo       *jetstream.StreamInfo
		queueAckPending  uint64
		queueRedelivered uint64
	)
	if info, infoErr := stream.Info(ctx); infoErr != nil {
		log.Printf("[queue] Warning: failed to load stream info: %v", infoErr)
	} else {
		streamInfo = info
	}

	consumerAvailable := false
	for _, consumerName := range messaging.ScrapeWorkerConsumerNames() {
		consumer, consumerErr := stream.Consumer(ctx, consumerName)
		if consumerErr != nil {
			continue
		}
		info, infoErr := consumer.Info(ctx)
		if infoErr != nil {
			log.Printf("[queue] Warning: failed to load consumer info for %s: %v", consumerName, infoErr)
			continue
		}
		consumerAvailable = true
		queueAckPending += uint64(info.NumAckPending)
		queueRedelivered += uint64(info.NumRedelivered)
	}

	return streamInfo, consumerAvailable, queueAckPending, queueRedelivered
}

func (h *Handler) countQueueState(ctx context.Context) (int64, int64, int64, int64) {
	if h.db != nil {
		return h.countQueueStateSQLite(ctx)
	}
	var jobsQueued, jobsProcessing, jobsFailed, artistsPending int64
	for _, item := range []struct {
		collection string
		exp        dbx.Expression
		result     *int64
	}{
		{"scrape_jobs", dbx.HashExp{"status": "queued"}, &jobsQueued},
		{"scrape_jobs", dbx.HashExp{"status": "processing"}, &jobsProcessing},
		{"scrape_jobs", dbx.HashExp{"status": "failed"}, &jobsFailed},
		{"artists", dbx.HashExp{"fetch_status": "pending"}, &artistsPending},
	} {
		if err := ctx.Err(); err != nil {
			break
		}
		if qErr := h.app.RecordQuery(item.collection).
			WithContext(ctx).
			Select("COUNT(*)").
			AndWhere(item.exp).
			Limit(1).
			Row(item.result); qErr != nil {
			if isExpectedContextCancellation(qErr) {
				break
			}
			log.Printf("[queue] Warning: failed to count %s: %v", item.collection, qErr)
		}
	}
	return jobsQueued, jobsProcessing, jobsFailed, artistsPending
}

// countQueueStateSQLite serves the queue widget and SSE lane from the SQLite
// read model (queue-counters projection). Indexed COUNT(*)s replace four
// PocketBase queries; JetStream stream/consumer state still comes from NATS.
func (h *Handler) countQueueStateSQLite(ctx context.Context) (int64, int64, int64, int64) {
	var jobsQueued, jobsProcessing, jobsFailed, artistsPending int64
	_ = h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		counts := []struct {
			query  string
			result *int64
		}{
			{"SELECT COUNT(*) FROM scrape_jobs WHERE status = 'queued';", &jobsQueued},
			{"SELECT COUNT(*) FROM scrape_jobs WHERE status = 'processing';", &jobsProcessing},
			{"SELECT COUNT(*) FROM scrape_jobs WHERE status = 'failed';", &jobsFailed},
			{"SELECT COUNT(*) FROM artists WHERE fetch_status = 'pending';", &artistsPending},
		}
		for _, c := range counts {
			if err := ctx.Err(); err != nil {
				return err
			}
			stmt := tx.Prep(c.query)
			hasRow, err := stmt.Step()
			if err != nil {
				_ = stmt.Reset()
				log.Printf("[queue] Warning: SQLite count failed (%s): %v", c.query, err)
				continue
			}
			if hasRow {
				*c.result = stmt.ColumnInt64(0)
			}
			_ = stmt.Reset()
		}
		return nil
	})
	return jobsQueued, jobsProcessing, jobsFailed, artistsPending
}

func (h *Handler) applyActiveBatchProgress(ctx context.Context, artistsPending, jobsProcessing *int64) {
	activeBatchRemaining := int64(0)
	if snapshot, ok := h.getActiveBatchSnapshot(ctx); ok {
		remaining := snapshot.Total - snapshot.Completed
		if remaining > 0 {
			activeBatchRemaining = int64(remaining)
		}
	}

	if *artistsPending < activeBatchRemaining {
		*artistsPending = activeBatchRemaining
	}
	if *jobsProcessing < *artistsPending {
		*jobsProcessing = *artistsPending
	}
}

func queuePendingFromState(streamInfo *jetstream.StreamInfo, jobsQueued int64) uint64 {
	queuePending := uint64(0)
	if streamInfo != nil {
		queuePending = streamInfo.State.Msgs
	}
	if queuePending < uint64(jobsQueued) {
		queuePending = uint64(jobsQueued)
	}
	return queuePending
}

func (h *Handler) notifyQueueUpdated() {
	if h.nc != nil {
		_ = h.nc.Publish(messaging.SubjectQueueUpdated, []byte("{}"))
	}
}

func (h *Handler) getQueueStats(ctx context.Context) templates.QueueStats {
	h.ensureBatchProgressSubscriber()

	streamInfo, consumerAvailable, queueAckPending, queueRedelivered := h.loadQueueConsumerState(ctx)
	jobsQueued, jobsProcessing, jobsFailed, artistsPending := h.countQueueState(ctx)
	h.applyActiveBatchProgress(ctx, &artistsPending, &jobsProcessing)

	queuePending := queuePendingFromState(streamInfo, jobsQueued)

	return templates.QueueStats{
		QueuePending:      queuePending,
		QueueAckPending:   queueAckPending,
		QueueRedelivered:  queueRedelivered,
		JobsQueued:        jobsQueued,
		JobsProcessing:    jobsProcessing,
		JobsFailed:        jobsFailed,
		ArtistsPending:    artistsPending,
		StreamAvailable:   streamInfo != nil,
		ConsumerAvailable: consumerAvailable,
	}
}

// HandleQueue returns queue status as Datastar SSE fragment or JSON.
func (h *Handler) HandleQueue(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	stats := h.getQueueStats(ctx)

	if !wantsJSONResponse(r) {
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchElementTempl(templates.QueueJobsWidgetContent(stats))
		return
	}

	_ = writeJSON(w, http.StatusOK, map[string]any{
		"stream": map[string]any{
			"available": stats.StreamAvailable,
			"messages":  stats.QueuePending,
		},
		"consumer": map[string]any{
			"available":       stats.ConsumerAvailable,
			"num_ack_pending": stats.QueueAckPending,
			"num_redelivered": stats.QueueRedelivered,
		},
		"jobs": map[string]any{
			"queued":     stats.JobsQueued,
			"processing": stats.JobsProcessing,
			"failed":     stats.JobsFailed,
		},
		"artists_pending": stats.ArtistsPending,
	})
}

func (h *Handler) handleQueue(e *core.RequestEvent) error {
	h.HandleQueue(e.Response, e.Request)
	return nil
}

type queueRetryErrorParams struct {
	ResponseWriter http.ResponseWriter
	Request        *http.Request
	Err            error
	Stats          queueRetryStats
	IncludeStats   bool
}

func (h *Handler) queueRetryErrorResponseHTTP(p queueRetryErrorParams) {
	if wantsJSONResponse(p.Request) {
		body := map[string]any{
			"status": "error",
			"error":  p.Err.Error(),
		}
		if p.IncludeStats {
			body["stats"] = p.Stats
		}
		_ = writeJSON(p.ResponseWriter, http.StatusInternalServerError, body)
		return
	}
	h.notifyQueueUpdated()
	p.ResponseWriter.WriteHeader(http.StatusInternalServerError)
}

func (h *Handler) queueRetryErrorResponse(e *core.RequestEvent, err error, stats queueRetryStats, includeStats bool) error {
	h.queueRetryErrorResponseHTTP(queueRetryErrorParams{
		ResponseWriter: e.Response,
		Request:        e.Request,
		Err:            err,
		Stats:          stats,
		IncludeStats:   includeStats,
	})
	return nil
}

// HandleQueueRetry retries failed and queued jobs.
func (h *Handler) HandleQueueRetry(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	stats, err := h.reconcileOrphanQueueState(ctx)
	if err != nil {
		log.Printf("[queue-retry] reconciliation failed: %v", err)
		h.queueRetryErrorResponseHTTP(queueRetryErrorParams{
			ResponseWriter: w,
			Request:        r,
			Err:            err,
			Stats:          stats,
			IncludeStats:   true,
		})
		return
	}

	checker := quota.NewChecker(h.cfg)
	if !checker.HasAvailableQuota() {
		if wantsJSONResponse(r) {
			_ = writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"error": "No scraping quota available.",
				"stats": stats,
			})
			return
		}
		h.notifyQueueUpdated()
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	stats, err = h.retryFailedAndQueuedJobs(ctx, 250, stats)
	if err != nil {
		log.Printf("[queue-retry] retry loop failed: %v", err)
		h.queueRetryErrorResponseHTTP(queueRetryErrorParams{
			ResponseWriter: w,
			Request:        r,
			Err:            err,
			Stats:          stats,
			IncludeStats:   false,
		})
		return
	}

	log.Printf(
		"[queue-retry] candidates=%d retried=%d duplicate=%d pending_skipped=%d publish_failed=%d invalid_artist=%d orphan_pending_reset=%d failed_artists_marked=%d orphan_stream_messages=%d",
		stats.Candidates,
		stats.Retried,
		stats.Duplicate,
		stats.PendingSkipped,
		stats.PublishFailed,
		stats.InvalidArtist,
		stats.OrphanPendingReset,
		stats.FailedArtistsMarked,
		stats.OrphanStreamMessages,
	)

	h.notifyQueueUpdated()

	if wantsJSONResponse(r) {
		_ = writeJSON(w, http.StatusOK, map[string]any{
			"status": "ok",
			"stats":  stats,
		})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleQueueRetry(e *core.RequestEvent) error {
	h.HandleQueueRetry(e.Response, e.Request)
	return nil
}

func isExpectedContextCancellation(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}
