//go:build goexperiment.jsonv2

// Package handlers provides HTTP request handlers and helpers for dashboard
// routes, queue management, and scrape refresh workflows.
package handlers

import (
	"context"
	"encoding/json"
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

	"ListenLedger/internal/correlation"
	"ListenLedger/internal/messaging"
	"ListenLedger/internal/quota"
)

func (h *Handler) publishScrapeRequest(ctx context.Context, req messaging.ScrapeRequested) (*jetstream.PubAck, error) {
	msgID := messaging.ScrapeRequestMsgID(req.ArtistID)

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
}

type retryJobParams struct {
	Job                *core.Record
	Artist             *core.Record
	ArtistID           string
	RequestID          string
	PreviousFetchStatus string
	PublishErr         error
}

func normalizeQueueRetryLimit(limit int) int {
	if limit <= 0 {
		return 250
	}
	return limit
}

func (h *Handler) reconcileOrphanQueueState(ctx context.Context) (queueRetryStats, error) {
	var stats queueRetryStats
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

func (h *Handler) resetOrphanPendingArtists(ctx context.Context, stats *queueRetryStats) error {
	records := make([]*core.Record, 0)
	err := h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(dbx.NewExp(
			"fetch_status = {:pending} AND id NOT IN (SELECT artist FROM scrape_jobs WHERE status IN ({:queued}, {:processing}, {:failed}))",
			dbx.Params{
				"pending":    "pending",
				"queued":     "queued",
				"processing": "processing",
				"failed":     "failed",
			},
		)).
		Limit(500).
		All(&records)
	if err != nil {
		return fmt.Errorf("query orphan pending artists: %w", err)
	}

	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		record.Set("fetch_status", "idle")
		if err := h.app.SaveWithContext(ctx, record); err != nil {
			return fmt.Errorf("reset orphan pending artist %s: %w", record.Id, err)
		}
		correlation.Clear(record.Id)
		stats.OrphanPendingReset++
	}

	return nil
}

func (h *Handler) markFailedJobArtists(ctx context.Context, stats *queueRetryStats) error {
	records := make([]*core.Record, 0)
	err := h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(dbx.NewExp(
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
		)).
		Limit(500).
		All(&records)
	if err != nil {
		return fmt.Errorf("query failed-job pending artists: %w", err)
	}

	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		record.Set("fetch_status", "failed")
		if err := h.app.SaveWithContext(ctx, record); err != nil {
			return fmt.Errorf("mark failed-job artist %s failed: %w", record.Id, err)
		}
		correlation.Clear(record.Id)
		stats.FailedArtistsMarked++
	}

	return nil
}

func (h *Handler) purgeOrphanScrapeRequests(ctx context.Context, stats *queueRetryStats) error {
	streamInfo, consumerAvailable, queueAckPending, queueRedelivered := h.loadQueueConsumerState(ctx)
	if h.hasActiveQueueWork(streamInfo, consumerAvailable, queueAckPending, queueRedelivered) {
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

func (h *Handler) hasActiveQueueWork(streamInfo *jetstream.StreamInfo, consumerAvailable bool, queueAckPending, queueRedelivered uint64) bool {
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

	remaining := limit - len(failedRecords)
	if remaining < 0 {
		remaining = 0
	}

	queuedRecords, err := h.scrapeJobsByStatus(ctx, "queued", remaining)
	if err != nil {
		return nil, fmt.Errorf("query queued jobs: %w", err)
	}

	records := make([]*core.Record, 0, len(failedRecords)+len(queuedRecords))
	records = append(records, failedRecords...)
	records = append(records, queuedRecords...)
	return records, nil
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

	artist, findErr := h.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
		q.WithContext(ctx)
		return nil
	})
	if findErr != nil {
		if router.ToApiError(findErr).Status == http.StatusNotFound {
			stats.InvalidArtist++
			return messaging.ScrapeRequested{}, nil, "", "", false, nil
		}
		// Transient DB error - propagate to caller
		return messaging.ScrapeRequested{}, nil, "", "", false, fmt.Errorf("failed to find artist %s: %w", artistID, findErr)
	}
	if artist == nil {
		stats.InvalidArtist++
		return messaging.ScrapeRequested{}, nil, "", "", false, nil
	}
	if artist.GetString("fetch_status") == "pending" {
		stats.PendingSkipped++
		return messaging.ScrapeRequested{}, nil, "", "", false, nil
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

func (h *Handler) retryFailedAndQueuedJobs(ctx context.Context, limit int, stats queueRetryStats) (queueRetryStats, error) {
	limit = normalizeQueueRetryLimit(limit)
	records, err := h.collectRetryCandidates(ctx, limit)
	if err != nil {
		return stats, err
	}

	stats.Candidates = len(records)
	seenArtist := make(map[string]struct{}, len(records))

	for _, job := range records {
		req, artist, artistID, requestID, ok, err := h.prepareRetryRequest(ctx, job, seenArtist, &stats)
		if err != nil {
			return stats, fmt.Errorf("failed to prepare retry request: %w", err)
		}
		if !ok {
			continue
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
			return stats, err
		}

		ack, pubErr := h.publishRetryRequest(ctx, req)
		if pubErr != nil {
			params.PublishErr = pubErr
			return stats, h.handleRetryPublishFailure(ctx, params, &stats)
		}

		if ack != nil && ack.Duplicate {
			return stats, h.handleRetryDuplicate(ctx, params, &stats)
		}

		stats.Retried++
	}

	return stats, nil
}

func (h *Handler) handleRetryPublishFailure(ctx context.Context, params retryJobParams, stats *queueRetryStats) error {
	stats.PublishFailed++
	if err := h.rollbackRetryQueuedState(params.Artist, params.ArtistID, params.PreviousFetchStatus); err != nil {
		return err
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cleanupCancel()
	if saveErr := h.saveRetryPublishFailure(cleanupCtx, params.Job, params.PublishErr); saveErr != nil {
		return fmt.Errorf("failed to record publish failure: %w", saveErr)
	}
	return nil
}

func (h *Handler) handleRetryDuplicate(ctx context.Context, params retryJobParams, stats *queueRetryStats) error {
	stats.Duplicate++
	if err := h.rollbackRetryQueuedState(params.Artist, params.ArtistID, params.PreviousFetchStatus); err != nil {
		return err
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cleanupCancel()
	if saveErr := h.saveRetryDeduped(cleanupCtx, params.Job); saveErr != nil {
		return fmt.Errorf("failed to record deduped status: %w", saveErr)
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
			if isExpectedContextCancellation(ctx, qErr) {
				break
			}
			log.Printf("[queue] Warning: failed to count %s: %v", item.collection, qErr)
		}
	}
	return jobsQueued, jobsProcessing, jobsFailed, artistsPending
}

func (h *Handler) applyActiveBatchProgress(artistsPending, jobsProcessing *int64) {
	activeBatchRemaining := int64(0)
	if snapshot, ok := h.getActiveBatchSnapshot(); ok {
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

func (h *Handler) handleQueue(e *core.RequestEvent) error {
	h.ensureBatchProgressSubscriber()

	ctx, cancel := context.WithTimeout(e.Request.Context(), 3*time.Second)
	defer cancel()

	streamInfo, consumerAvailable, queueAckPending, queueRedelivered := h.loadQueueConsumerState(ctx)
	jobsQueued, jobsProcessing, jobsFailed, artistsPending := h.countQueueState(ctx)
	h.applyActiveBatchProgress(&artistsPending, &jobsProcessing)

	queuePending := uint64(0)
	if streamInfo != nil {
		queuePending = streamInfo.State.Msgs
	}

	if queuePending < uint64(jobsQueued) {
		queuePending = uint64(jobsQueued)
	}

	wantsJSON := wantsJSONResponse(e.Request)

	if !wantsJSON {
		signals := map[string]any{
			"queuePending":     queuePending,
			"queueAckPending":  queueAckPending,
			"queueRedelivered": queueRedelivered,
			"jobsQueued":       jobsQueued,
			"jobsProcessing":   jobsProcessing,
			"jobsFailed":       jobsFailed,
		}
		payload, err := json.Marshal(signals)
		if err != nil {
			return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to serialize queue signals"})
		}
		sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
		return sse.PatchSignals(payload)
	}

	return e.JSON(http.StatusOK, map[string]any{
		"stream": map[string]any{
			"available": streamInfo != nil,
			"messages":  queuePending,
		},
		"consumer": map[string]any{
			"available":       consumerAvailable,
			"num_ack_pending": queueAckPending,
			"num_redelivered": queueRedelivered,
		},
		"jobs": map[string]any{
			"queued":     jobsQueued,
			"processing": jobsProcessing,
			"failed":     jobsFailed,
		},
		"artists_pending": artistsPending,
	})
}

func (h *Handler) handleQueueRetry(e *core.RequestEvent) error {
	ctx, cancel := context.WithTimeout(e.Request.Context(), 30*time.Second)
	defer cancel()

	stats, err := h.reconcileOrphanQueueState(ctx)
	if err != nil {
		log.Printf("[queue-retry] reconciliation failed: %v", err)
		if wantsJSONResponse(e.Request) {
			return e.JSON(http.StatusInternalServerError, map[string]any{
				"status": "error",
				"error":  err.Error(),
				"stats":  stats,
			})
		}
		return h.handleQueue(e)
	}

	checker := quota.NewChecker(h.cfg)
	if !checker.HasAvailableQuota(e.Request.Context()) {
		return e.JSON(http.StatusTooManyRequests, map[string]any{
			"error": "No scraping quota available.",
			"stats": stats,
		})
	}

	stats, err = h.retryFailedAndQueuedJobs(ctx, 250, stats)
	if err != nil {
		log.Printf("[queue-retry] retry loop failed: %v", err)
		if wantsJSONResponse(e.Request) {
			return e.JSON(http.StatusInternalServerError, map[string]any{
				"status": "error",
				"error":  err.Error(),
			})
		}
		return h.handleQueue(e)
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

	if wantsJSONResponse(e.Request) {
		return e.JSON(http.StatusOK, map[string]any{
			"status": "ok",
			"stats":  stats,
		})
	}

	return h.handleQueue(e)
}

func isExpectedContextCancellation(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		(ctx != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()))
}
