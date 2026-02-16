//go:build goexperiment.jsonv2

// Package worker provides a NATS-based background worker for processing Spotify scrape requests.
package worker

import (
	"MonthlyListeners/config"
	"MonthlyListeners/internal/fetcher"
	"MonthlyListeners/internal/messaging"
	"MonthlyListeners/internal/spotify"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
)

// Worker handles background scraping jobs via NATS.
type Worker struct {
	app        *pocketbase.PocketBase
	nc         *nats.Conn
	js         jetstream.JetStream
	cfg        *config.Config
	fetcher    *fetcher.Service
	consume    jetstream.ConsumeContext
	maxDeliver int
	ackWait    time.Duration
	progress   time.Duration
	backoff    []time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a new worker instance.
func New(app *pocketbase.PocketBase, nc *nats.Conn, js jetstream.JetStream, cfg *config.Config) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		app:    app,
		nc:     nc,
		js:     js,
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins listening for scrape requests on NATS.
func (w *Worker) Start() {
	// Initialize the Spotify client and fetcher
	client, err := spotify.NewClient(w.cfg)
	if err != nil {
		log.Printf("[worker] Warning: Could not initialize Spotify client: %v", err)
		log.Printf("[worker] Worker will start but scraping will be disabled until tokens are configured")
	} else {
		w.fetcher = fetcher.NewService(client, w.cfg)
	}

	maxDeliver := w.cfg.ScrapeMaxDeliver
	if maxDeliver <= 0 {
		maxDeliver = 3
	}
	w.maxDeliver = maxDeliver

	backoff := w.cfg.ScrapeBackOff
	if len(backoff) == 0 {
		backoff = []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute}
	}
	w.backoff = backoff

	ackWait := w.cfg.ScrapeAckWait
	if ackWait <= 0 {
		ackWait = 2 * w.fetchTimeout()
		if ackWait < 2*time.Minute {
			ackWait = 2 * time.Minute
		}
		for _, d := range backoff {
			if d > ackWait {
				ackWait = d
			}
		}
	}
	w.ackWait = ackWait

	progress := w.cfg.ScrapeInProgressInterval
	if progress <= 0 {
		progress = 20 * time.Second
	}
	// Keep heartbeats frequent enough to prevent AckWait redelivery,
	// even if AckWait is tuned down via env.
	maxProgress := ackWait / 2
	if maxProgress < time.Second {
		maxProgress = time.Second
	}
	if progress > maxProgress {
		progress = maxProgress
	}
	w.progress = progress

	// Durable JetStream consumer (restart-safe)
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()
	consumer, err := messaging.EnsureScrapeWorkerConsumer(ctx, w.js, jetstream.ConsumerConfig{
		Durable:       messaging.ScrapeWorkerConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    maxDeliver,
		BackOff:       backoff,
		MaxAckPending: max(1, w.cfg.MaxConcurrency),
	})
	if err != nil {
		log.Printf("[worker] Failed to ensure scrape consumer: %v", err)
		return
	}

	// Align local behavior to the *actual* consumer config stored server-side.
	// This prevents redelivery duplicates if an existing durable consumer has a
	// smaller AckWait than we expect (e.g., from a prior run).
	if info, err := consumer.Info(ctx); err == nil && info != nil {
		if info.Config.MaxDeliver > 0 {
			w.maxDeliver = info.Config.MaxDeliver
		}
		if info.Config.AckWait > 0 {
			w.ackWait = info.Config.AckWait
			maxProgress := w.ackWait / 2
			if maxProgress < time.Second {
				maxProgress = time.Second
			}
			if w.progress > maxProgress {
				w.progress = maxProgress
			}
		}
		if len(info.Config.BackOff) > 0 {
			w.backoff = info.Config.BackOff
		}

		log.Printf(
			"[worker] Consumer config: durable=%s ack_wait=%s max_deliver=%d backoff=%v max_ack_pending=%d progress=%s",
			info.Config.Durable,
			info.Config.AckWait,
			info.Config.MaxDeliver,
			info.Config.BackOff,
			info.Config.MaxAckPending,
			w.progress,
		)
	} else if err != nil {
		log.Printf("[worker] Warning: failed to load consumer info: %v", err)
	}

	consume, err := consumer.Consume(w.handleJetStreamMsg)
	if err != nil {
		log.Printf("[worker] Failed to start consumer: %v", err)
		return
	}
	w.consume = consume

	log.Println("[worker] Started listening for scrape requests (JetStream)")
}

// Stop gracefully stops the worker.
func (w *Worker) Stop() {
	w.cancel()

	if w.consume != nil {
		w.consume.Drain()
	}

	// Wait for in-flight requests to complete (with timeout)
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[worker] Stopped gracefully")
	case <-time.After(30 * time.Second):
		log.Println("[worker] Stop timed out, forcing shutdown")
	}

	if w.fetcher != nil {
		if err := w.fetcher.Close(); err != nil {
			log.Printf("[worker] Warning: Failed to close fetcher: %v", err)
		}
	}
}

func (w *Worker) handleJetStreamMsg(msg jetstream.Msg) {
	w.wg.Add(1)
	defer w.wg.Done()

	meta, _ := msg.Metadata()
	req, err := messaging.UnmarshalScrapeRequested(msg.Data())
	if err != nil {
		log.Printf("[worker] Failed to unmarshal request (terminating): %v", err)
		w.publishScrapeDLQ(msg, meta, nil, "unmarshal_error")
		if err := msg.Term(); err != nil {
			log.Printf("[worker] Failed to terminate poison message: %v", err)
		}
		return
	}

	done := make(chan struct{})
	go w.inProgressLoop(msg, done)
	stopProgress := func() {
		select {
		case <-done:
		default:
			close(done)
		}
	}
	defer stopProgress()

	if err := w.processRequest(req, meta); err != nil {
		stopProgress()
		log.Printf("[worker] Retryable error processing %s: %v", req.ArtistID, err)

		// If we keep failing after MaxDeliver attempts, dead-letter the message.
		if meta != nil && int(meta.NumDelivered) >= w.maxDeliver {
			w.setScrapeJobFinished(req.RequestID, "failed", "retry_exhausted")
			w.publishScrapeDLQ(msg, meta, &req, "retry_exhausted: "+err.Error())
			if err := msg.Term(); err != nil {
				log.Printf("[worker] Failed to terminate retry-exhausted message: %v", err)
			}
			return
		}

		// Delay retries to avoid immediate reprocessing.
		if err := msg.NakWithDelay(w.retryDelay(meta)); err != nil {
			log.Printf("[worker] Failed to NAK message: %v", err)
		}
		return
	}

	stopProgress()
	ackCtx, cancel := context.WithTimeout(w.ctx, 2*time.Second)
	defer cancel()
	if err := msg.DoubleAck(ackCtx); err != nil {
		log.Printf("[worker] Failed to DoubleAck message: %v", err)
	}
}

func (w *Worker) retryDelay(meta *jetstream.MsgMetadata) time.Duration {
	if len(w.backoff) == 0 {
		return 10 * time.Second
	}
	if meta == nil || meta.NumDelivered == 0 {
		return w.backoff[0]
	}

	// NumDelivered=1 is first attempt; retry should use backoff[0].
	idx := int(meta.NumDelivered) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(w.backoff) {
		idx = len(w.backoff) - 1
	}
	return w.backoff[idx]
}

func (w *Worker) inProgressLoop(msg jetstream.Msg, done <-chan struct{}) {
	if w.progress <= 0 {
		return
	}
	t := time.NewTicker(w.progress)
	defer t.Stop()

	for {
		select {
		case <-done:
			return
		case <-w.ctx.Done():
			return
		case <-t.C:
			if err := msg.InProgress(); err != nil {
				log.Printf("[worker] Warning: InProgress failed: %v", err)
			}
		}
	}
}

func (w *Worker) publishScrapeDLQ(msg jetstream.Msg, meta *jetstream.MsgMetadata, req *messaging.ScrapeRequested, reason string) {
	env := map[string]any{
		"reason":      reason,
		"at":          time.Now().Format(time.RFC3339),
		"subject":     msg.Subject(),
		"payload_b64": base64.StdEncoding.EncodeToString(msg.Data()),
	}
	if meta != nil {
		env["num_delivered"] = meta.NumDelivered
		env["stream_seq"] = meta.Sequence.Stream
		env["consumer_seq"] = meta.Sequence.Consumer
		env["stream"] = meta.Stream
		env["consumer"] = meta.Consumer
	}
	if req != nil {
		env["request_id"] = req.RequestID
		env["artist_id"] = req.ArtistID
		env["spotify_id"] = req.SpotifyID
	}

	data, err := json.Marshal(env)
	if err != nil {
		log.Printf("[worker] Failed to marshal DLQ envelope: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(w.ctx, 2*time.Second)
	defer cancel()
	if _, err := w.js.Publish(ctx, messaging.SubjectScrapeDLQ, data); err != nil {
		log.Printf("[worker] Failed to publish DLQ message: %v", err)
	}
}

func (w *Worker) processRequest(req messaging.ScrapeRequested, meta *jetstream.MsgMetadata) error {
	numDelivered := uint64(0)
	streamSeq := uint64(0)
	if meta != nil {
		numDelivered = meta.NumDelivered
		streamSeq = meta.Sequence.Stream
	}
	log.Printf(
		"[worker] Processing scrape request (request_id=%s, delivered=%d, seq=%d) for artist: %s (spotify_id: %s)",
		req.RequestID,
		numDelivered,
		streamSeq,
		req.ArtistName,
		req.SpotifyID,
	)

	w.setScrapeJobProcessing(req.RequestID)

	// Update artist status to pending
	if err := w.updateArtistStatus(req.ArtistID, "pending"); err != nil {
		return fmt.Errorf("set pending: %w", err)
	}

	// Check if fetcher is available
	if w.fetcher == nil {
		log.Printf("[worker] Fetcher not available, marking as failed")
		if err := w.updateArtistStatus(req.ArtistID, "failed"); err != nil {
			return fmt.Errorf("set failed: %w", err)
		}
		w.setScrapeJobFinished(req.RequestID, "failed", "fetcher_unavailable")
		return nil
	}

	// Fetch the listener count
	ctx, cancel := context.WithTimeout(w.ctx, w.fetchTimeout())
	defer cancel()

	results, missed := w.fetcher.FetchAll(ctx, []string{req.SpotifyID})

	if len(missed) > 0 {
		log.Printf("[worker] Failed to fetch listeners for %s", req.SpotifyID)
		if err := w.updateArtistStatus(req.ArtistID, "failed"); err != nil {
			return fmt.Errorf("set failed: %w", err)
		}
		w.setScrapeJobFinished(req.RequestID, "failed", "fetch_failed")
		return fmt.Errorf("fetch failed for %s", req.SpotifyID)
	}

	listeners, ok := results[req.SpotifyID]
	if !ok {
		log.Printf("[worker] No result for %s", req.SpotifyID)
		if err := w.updateArtistStatus(req.ArtistID, "failed"); err != nil {
			return fmt.Errorf("set failed: %w", err)
		}
		w.setScrapeJobFinished(req.RequestID, "failed", "missing_result")
		return fmt.Errorf("no result for %s", req.SpotifyID)
	}

	// Update the artist record with new listener count
	if err := w.updateArtistListeners(req.ArtistID, listeners); err != nil {
		return fmt.Errorf("update listeners: %w", err)
	}

	log.Printf("[worker] Successfully updated %s with %d monthly listeners", req.ArtistName, listeners)
	w.setScrapeJobFinished(req.RequestID, "succeeded", "")
	return nil
}

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

func (w *Worker) setScrapeJobProcessing(requestID string) {
	job, err := w.scrapeJobByRequestID(requestID)
	if err != nil || job == nil {
		return
	}

	job.Set("status", "processing")
	job.Set("attempts", job.GetInt("attempts")+1)
	job.Set("started_at", time.Now())
	job.Set("error", "")
	if err := w.app.Save(job); err != nil {
		log.Printf("[worker] Warning: failed to update scrape job to processing: %v", err)
	}
}

func (w *Worker) setScrapeJobFinished(requestID, status, errMsg string) {
	job, err := w.scrapeJobByRequestID(requestID)
	if err != nil || job == nil {
		return
	}

	job.Set("status", status)
	job.Set("finished_at", time.Now())
	job.Set("error", errMsg)
	if err := w.app.Save(job); err != nil {
		log.Printf("[worker] Warning: failed to update scrape job to %s: %v", status, err)
	}
}

func (w *Worker) fetchTimeout() time.Duration {
	providerCount := 0
	if w.cfg.HasLocalHeadless() {
		providerCount++
	}
	if w.cfg.HasBrowserless() {
		providerCount++
	}
	if w.cfg.HasScrapingAnt() {
		providerCount++
	}
	if providerCount == 0 {
		providerCount = 1
	}

	timeout := time.Duration(providerCount) * w.cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if timeout < 30*time.Second {
		timeout = 30 * time.Second
	}

	return timeout
}

// updateArtistStatus updates the fetch_status field of an artist.
func (w *Worker) updateArtistStatus(artistID, status string) error {
	record, err := w.app.FindRecordById("artists", artistID)
	if err != nil {
		return err
	}

	record.Set("fetch_status", status)
	return w.app.Save(record)
}

// updateArtistListeners updates the monthly_listeners, last_updated, and fetch_status of an artist.
func (w *Worker) updateArtistListeners(artistID string, listeners int) error {
	record, err := w.app.FindRecordById("artists", artistID)
	if err != nil {
		return err
	}

	record.Set("monthly_listeners", listeners)
	record.Set("last_updated", time.Now())
	record.Set("fetch_status", "idle")
	return w.app.Save(record)
}
