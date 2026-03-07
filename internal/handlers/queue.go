//go:build goexperiment.jsonv2

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
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

func (h *Handler) createScrapeJobRecord(requestID, artistID string) {
	if requestID == "" || artistID == "" {
		return
	}
	jobs, err := h.app.FindCollectionByNameOrId("scrape_jobs")
	if err != nil {
		log.Printf("[handlers] Warning: scrape_jobs collection not found: %v", err)
		return
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
	if err := h.app.Save(job); err != nil {
		log.Printf("[handlers] Warning: failed to create scrape job record: %v", err)
	}
}

type queueRetryStats struct {
	Candidates     int `json:"candidates"`
	Retried        int `json:"retried"`
	Duplicate      int `json:"duplicate"`
	PendingSkipped int `json:"pending_skipped"`
	PublishFailed  int `json:"publish_failed"`
	InvalidArtist  int `json:"invalid_artist"`
}

func (h *Handler) scrapeJobsByStatus(status string, limit int) ([]*core.Record, error) {
	if strings.TrimSpace(status) == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 250
	}
	records, err := h.app.FindRecordsByFilter(
		"scrape_jobs",
		"status = {:status}",
		"-queued_at",
		limit,
		0,
		dbx.Params{"status": status},
	)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (h *Handler) retryFailedAndQueuedJobs(ctx context.Context, limit int) (queueRetryStats, error) {
	if limit <= 0 {
		limit = 250
	}

	failedRecords, err := h.scrapeJobsByStatus("failed", limit)
	if err != nil {
		return queueRetryStats{}, fmt.Errorf("query failed jobs: %w", err)
	}

	remaining := limit - len(failedRecords)
	if remaining < 0 {
		remaining = 0
	}

	queuedRecords, err := h.scrapeJobsByStatus("queued", remaining)
	if err != nil {
		return queueRetryStats{}, fmt.Errorf("query queued jobs: %w", err)
	}

	records := make([]*core.Record, 0, len(failedRecords)+len(queuedRecords))
	records = append(records, failedRecords...)
	records = append(records, queuedRecords...)

	stats := queueRetryStats{Candidates: len(records)}
	seenArtist := make(map[string]struct{}, len(records))

	for _, job := range records {
		artistID := strings.TrimSpace(job.GetString("artist"))
		if artistID == "" {
			stats.InvalidArtist++
			continue
		}
		if _, seen := seenArtist[artistID]; seen {
			continue
		}
		seenArtist[artistID] = struct{}{}

		artist, findErr := h.app.FindRecordById("artists", artistID)
		if findErr != nil || artist == nil {
			stats.InvalidArtist++
			continue
		}
		if artist.GetString("fetch_status") == "pending" {
			stats.PendingSkipped++
			continue
		}

		spotifyID := strings.TrimSpace(artist.GetString("spotify_id"))
		if spotifyID == "" {
			stats.InvalidArtist++
			continue
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

		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		ack, pubErr := h.publishScrapeRequest(pubCtx, req)
		cancel()
		if pubErr != nil {
			stats.PublishFailed++
			job.Set("status", "failed")
			job.Set("error", fmt.Sprintf("retry publish failed: %v", pubErr))
			job.Set("finished_at", time.Now())
			if saveErr := h.app.Save(job); saveErr != nil {
				log.Printf("[queue-retry] Warning: failed to save publish error for job %s: %v", job.Id, saveErr)
			}
			continue
		}

		if ack != nil && ack.Duplicate {
			stats.Duplicate++
			job.Set("status", "succeeded")
			job.Set("error", "deduped_existing_request")
			job.Set("finished_at", time.Now())
			if saveErr := h.app.Save(job); saveErr != nil {
				log.Printf("[queue-retry] Warning: failed to save deduped status for job %s: %v", job.Id, saveErr)
			}
			continue
		}

		job.Set("status", "queued")
		job.Set("queued_at", time.Now())
		job.Set("error", "")
		job.Set("started_at", nil)
		job.Set("finished_at", nil)
		if saveErr := h.app.Save(job); saveErr != nil {
			log.Printf("[queue-retry] Warning: failed to save queued status for job %s: %v", job.Id, saveErr)
		}

		correlation.Associate(artistID, requestID)
		artist.Set("fetch_status", "pending")
		if saveErr := h.app.Save(artist); saveErr != nil {
			log.Printf("[queue-retry] Warning: failed to mark artist %s pending: %v", artistID, saveErr)
		}
		stats.Retried++
	}

	return stats, nil
}

func (h *Handler) handleQueue(e *core.RequestEvent) error {
	h.ensureBatchProgressSubscriber()

	ctx, cancel := context.WithTimeout(e.Request.Context(), 3*time.Second)
	defer cancel()

	var (
		streamInfo        *jetstream.StreamInfo
		consumerAvailable bool
		queueAckPending   uint64
		queueRedelivered  uint64
	)
	stream, err := h.js.Stream(ctx, messaging.ScrapeRequestsStreamName)
	if err != nil {
		log.Printf("[queue] Warning: failed to load stream handle: %v", err)
	} else {
		if info, infoErr := stream.Info(ctx); infoErr != nil {
			log.Printf("[queue] Warning: failed to load stream info: %v", infoErr)
		} else {
			streamInfo = info
		}

		for _, consumerName := range messaging.ScrapeWorkerConsumerNames() {
			consumer, consumerErr := stream.Consumer(ctx, consumerName)
			if consumerErr != nil {
				continue
			}
			if info, infoErr := consumer.Info(ctx); infoErr != nil {
				log.Printf("[queue] Warning: failed to load consumer info for %s: %v", consumerName, infoErr)
			} else {
				consumerAvailable = true
				queueAckPending += uint64(info.NumAckPending)
				queueRedelivered += uint64(info.NumRedelivered)
			}
		}
	}

	jobsQueued, _ := h.app.CountRecords("scrape_jobs", dbx.HashExp{"status": "queued"})
	jobsProcessing, _ := h.app.CountRecords("scrape_jobs", dbx.HashExp{"status": "processing"})
	jobsFailed, _ := h.app.CountRecords("scrape_jobs", dbx.HashExp{"status": "failed"})
	artistsPending, _ := h.app.CountRecords("artists", dbx.HashExp{"fetch_status": "pending"})

	activeBatchRemaining := int64(0)
	if snapshot, ok := h.getActiveBatchSnapshot(); ok {
		remaining := snapshot.Total - snapshot.Completed
		if remaining > 0 {
			activeBatchRemaining = int64(remaining)
		}
	}

	if artistsPending < activeBatchRemaining {
		artistsPending = activeBatchRemaining
	}
	if jobsProcessing < artistsPending {
		jobsProcessing = artistsPending
	}

	queuePending := uint64(0)
	if streamInfo != nil {
		queuePending = streamInfo.State.Msgs
	}

	if queuePending < uint64(jobsQueued) {
		queuePending = uint64(jobsQueued)
	}
	if queueAckPending < uint64(artistsPending) {
		queueAckPending = uint64(artistsPending)
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
	checker := quota.NewChecker(h.cfg)
	if !checker.HasAvailableQuota(e.Request.Context()) {
		return e.JSON(http.StatusTooManyRequests, map[string]string{
			"error": "No scraping quota available.",
		})
	}

	stats, err := h.retryFailedAndQueuedJobs(e.Request.Context(), 250)
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
		"[queue-retry] candidates=%d retried=%d duplicate=%d pending_skipped=%d publish_failed=%d invalid_artist=%d",
		stats.Candidates,
		stats.Retried,
		stats.Duplicate,
		stats.PendingSkipped,
		stats.PublishFailed,
		stats.InvalidArtist,
	)

	if wantsJSONResponse(e.Request) {
		return e.JSON(http.StatusOK, map[string]any{
			"status": "ok",
			"stats":  stats,
		})
	}

	return h.handleQueue(e)
}
