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
	"sort"
	"strings"
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
	consumes   []jetstream.ConsumeContext
	maxDeliver int
	ackWait    time.Duration
	progress   time.Duration
	backoff    []time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	recalcMu      sync.Mutex
	recalcTimer   *time.Timer
	recalcPending map[string]struct{}

	providerStatsMu sync.Mutex
	providerStats   map[spotify.Provider]*providerRuntime
}

// New creates a new worker instance.
func New(app *pocketbase.PocketBase, nc *nats.Conn, js jetstream.JetStream, cfg *config.Config) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	w := &Worker{
		app:    app,
		nc:     nc,
		js:     js,
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,

		recalcPending: make(map[string]struct{}),
	}

	w.providerStats = make(map[spotify.Provider]*providerRuntime)
	for _, provider := range w.availableProviders() {
		w.providerStats[provider] = &providerRuntime{}
	}
	return w
}

type providerRuntime struct {
	inFlight     int
	completed    int
	totalLatency time.Duration
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
		ackWait = 2 * w.fetchTimeout(spotify.ProviderAny)
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

	// Single durable JetStream consumer (restart-safe) for shared queue.
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	consumer, err := messaging.EnsureScrapeWorkerConsumer(ctx, w.js, jetstream.ConsumerConfig{
		Durable:       messaging.ScrapeWorkerConsumerName,
		FilterSubject: messaging.SubjectScrapeRequest,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    maxDeliver,
		BackOff:       backoff,
		MaxAckPending: max(1, w.maxAckPending()),
	})
	if err != nil {
		log.Printf("[worker] Failed to ensure scrape consumer: %v", err)
		return
	}

	// Align local behavior to the actual consumer config stored server-side.
	if info, infoErr := consumer.Info(ctx); infoErr == nil && info != nil {
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
			"[worker] Consumer config: durable=%s subject=%s ack_wait=%s max_deliver=%d backoff=%v max_ack_pending=%d progress=%s",
			info.Config.Durable,
			info.Config.FilterSubject,
			info.Config.AckWait,
			info.Config.MaxDeliver,
			info.Config.BackOff,
			info.Config.MaxAckPending,
			w.progress,
		)
	} else if infoErr != nil {
		log.Printf("[worker] Warning: failed to load consumer info: %v", infoErr)
	}

	consume, err := consumer.Consume(w.handleJetStreamMsg)
	if err != nil {
		log.Printf("[worker] Failed to start consumer: %v", err)
		return
	}
	w.consumes = []jetstream.ConsumeContext{consume}

	log.Println("[worker] Started listening for scrape requests (shared JetStream queue)")
}

// Stop gracefully stops the worker.
func (w *Worker) Stop() {
	w.cancel()

	w.recalcMu.Lock()
	if w.recalcTimer != nil {
		w.recalcTimer.Stop()
		w.recalcTimer = nil
	}
	w.recalcMu.Unlock()

	for _, consume := range w.consumes {
		if consume != nil {
			consume.Drain()
		}
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
	w.wg.Go(func() {
		meta, _ := msg.Metadata()
		provider, subjectProviderLabel := providerForSubject(msg.Subject())
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

		if err := w.processRequest(req, meta, provider, subjectProviderLabel); err != nil {
			stopProgress()
			log.Printf("[worker] Retryable error processing %s (provider=%s): %v", req.ArtistID, subjectProviderLabel, err)

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
	})
}

func providerForSubject(subject string) (spotify.Provider, string) {
	switch messaging.ScrapeProviderFromSubject(subject) {
	case messaging.ScrapeProviderLocal:
		return spotify.ProviderLocalHeadless, messaging.ScrapeProviderLocal
	case messaging.ScrapeProviderBrowserless:
		return spotify.ProviderBrowserless, messaging.ScrapeProviderBrowserless
	case messaging.ScrapeProviderScrapingAnt:
		return spotify.ProviderScrapingAnt, messaging.ScrapeProviderScrapingAnt
	case messaging.ScrapeProviderScraperAPI:
		return spotify.ProviderScraperAPI, messaging.ScrapeProviderScraperAPI
	case messaging.ScrapeProviderApify:
		return spotify.ProviderApify, messaging.ScrapeProviderApify
	default:
		return spotify.ProviderAny, "adaptive"
	}
}

func (w *Worker) availableProviders() []spotify.Provider {
	providers := make([]spotify.Provider, 0, 5)
	if w.cfg.HasBrowserless() {
		providers = append(providers, spotify.ProviderBrowserless)
	}
	if w.cfg.HasScrapingAnt() {
		providers = append(providers, spotify.ProviderScrapingAnt)
	}
	if w.cfg.HasScraperAPI() {
		providers = append(providers, spotify.ProviderScraperAPI)
	}
	if w.cfg.HasApify() {
		providers = append(providers, spotify.ProviderApify)
	}
	if w.cfg.HasLocalHeadless() {
		providers = append(providers, spotify.ProviderLocalHeadless)
	}

	if len(providers) == 0 {
		return []spotify.Provider{spotify.ProviderAny}
	}
	return providers
}

func providerLabel(provider spotify.Provider) string {
	switch provider {
	case spotify.ProviderLocalHeadless:
		return messaging.ScrapeProviderLocal
	case spotify.ProviderBrowserless:
		return messaging.ScrapeProviderBrowserless
	case spotify.ProviderScrapingAnt:
		return messaging.ScrapeProviderScrapingAnt
	case spotify.ProviderScraperAPI:
		return messaging.ScrapeProviderScraperAPI
	case spotify.ProviderApify:
		return messaging.ScrapeProviderApify
	default:
		return messaging.ScrapeProviderAny
	}
}

func (w *Worker) rankedProviders() []spotify.Provider {
	providers := w.availableProviders()
	if len(providers) <= 1 {
		return providers
	}

	w.providerStatsMu.Lock()
	defer w.providerStatsMu.Unlock()

	sort.SliceStable(providers, func(i, j int) bool {
		left := w.providerStats[providers[i]]
		right := w.providerStats[providers[j]]
		leftLatency := w.fetchTimeout(providers[i])
		rightLatency := w.fetchTimeout(providers[j])
		leftInFlight := 0
		rightInFlight := 0

		if left != nil {
			leftInFlight = left.inFlight
			if left.completed > 0 && left.totalLatency > 0 {
				leftLatency = left.totalLatency / time.Duration(left.completed)
			}
		}
		if right != nil {
			rightInFlight = right.inFlight
			if right.completed > 0 && right.totalLatency > 0 {
				rightLatency = right.totalLatency / time.Duration(right.completed)
			}
		}

		leftScore := leftLatency * time.Duration(leftInFlight+1)
		rightScore := rightLatency * time.Duration(rightInFlight+1)
		if leftScore == rightScore {
			return providers[i] < providers[j]
		}
		return leftScore < rightScore
	})

	return providers
}

func (w *Worker) markProviderStart(provider spotify.Provider) {
	w.providerStatsMu.Lock()
	defer w.providerStatsMu.Unlock()
	stats, ok := w.providerStats[provider]
	if !ok {
		stats = &providerRuntime{}
		w.providerStats[provider] = stats
	}
	stats.inFlight++
}

func (w *Worker) markProviderFinish(provider spotify.Provider, duration time.Duration) {
	w.providerStatsMu.Lock()
	defer w.providerStatsMu.Unlock()
	stats, ok := w.providerStats[provider]
	if !ok {
		stats = &providerRuntime{}
		w.providerStats[provider] = stats
	}
	if stats.inFlight > 0 {
		stats.inFlight--
	}
	if duration > 0 {
		stats.completed++
		stats.totalLatency += duration
	}
}

func (w *Worker) fetchWithProvider(ctx context.Context, spotifyID string, provider spotify.Provider) (int, error) {
	startedAt := time.Now()
	w.markProviderStart(provider)
	defer func() {
		w.markProviderFinish(provider, time.Since(startedAt))
	}()
	return w.fetcher.FetchOne(ctx, spotifyID, provider)
}

func (w *Worker) fetchWithAdaptiveProviders(ctx context.Context, spotifyID string) (int, string, error) {
	providers := w.rankedProviders()
	if len(providers) == 1 && providers[0] == spotify.ProviderAny {
		listeners, err := w.fetchWithProvider(ctx, spotifyID, spotify.ProviderAny)
		return listeners, providerLabel(spotify.ProviderAny), err
	}

	errorsByProvider := make([]string, 0, len(providers))
	for _, provider := range providers {
		listeners, err := w.fetchWithProvider(ctx, spotifyID, provider)
		if err == nil {
			return listeners, providerLabel(provider), nil
		}
		errorsByProvider = append(errorsByProvider, fmt.Sprintf("%s: %v", providerLabel(provider), err))
		if ctx.Err() != nil {
			return 0, providerLabel(provider), ctx.Err()
		}
	}

	return 0, "adaptive", fmt.Errorf("all providers failed (%s)", strings.Join(errorsByProvider, "; "))
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

func (w *Worker) processRequest(req messaging.ScrapeRequested, meta *jetstream.MsgMetadata, provider spotify.Provider, subjectProviderLabel string) error {
	numDelivered := uint64(0)
	streamSeq := uint64(0)
	if meta != nil {
		numDelivered = meta.NumDelivered
		streamSeq = meta.Sequence.Stream
	}
	log.Printf(
		"[worker] Processing scrape request (request_id=%s, delivered=%d, seq=%d, provider=%s) for artist: %s (spotify_id: %s)",
		req.RequestID,
		numDelivered,
		streamSeq,
		subjectProviderLabel,
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
	ctx, cancel := context.WithTimeout(w.ctx, w.fetchTimeout(provider))
	defer cancel()

	listeners := 0
	usedProvider := providerLabel(provider)
	var err error
	if provider == spotify.ProviderAny {
		listeners, usedProvider, err = w.fetchWithAdaptiveProviders(ctx, req.SpotifyID)
	} else {
		listeners, err = w.fetchWithProvider(ctx, req.SpotifyID, provider)
	}
	if err != nil {
		log.Printf("[worker] Failed to fetch listeners for %s via %s: %v", req.SpotifyID, usedProvider, err)
		if err := w.updateArtistStatus(req.ArtistID, "failed"); err != nil {
			return fmt.Errorf("set failed: %w", err)
		}
		w.setScrapeJobFinished(req.RequestID, "failed", "fetch_failed")
		return fmt.Errorf("fetch failed for %s via %s: %w", req.SpotifyID, usedProvider, err)
	}

	// Update the artist record with new listener count
	if err := w.updateArtistListeners(req.ArtistID, listeners); err != nil {
		return fmt.Errorf("update listeners: %w", err)
	}

	log.Printf("[worker] Successfully updated %s with %d monthly listeners via %s", req.ArtistName, listeners, usedProvider)
	w.setScrapeJobFinished(req.RequestID, "succeeded", "")
	w.queueTotalSongsRecalc(req.ArtistID)
	w.clearFailedJobsForArtist(req.ArtistID, req.RequestID)
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

func (w *Worker) clearFailedJobsForArtist(artistID, succeededRequestID string) {
	if artistID == "" {
		return
	}

	records, err := w.app.FindRecordsByFilter(
		"scrape_jobs",
		"artist = {:artist} && status = {:status} && request_id != {:request_id}",
		"",
		500,
		0,
		dbx.Params{
			"artist":     artistID,
			"status":     "failed",
			"request_id": succeededRequestID,
		},
	)
	if err != nil {
		log.Printf("[worker] Warning: failed to load failed jobs for artist %s: %v", artistID, err)
		return
	}

	if len(records) == 0 {
		return
	}

	note := "recovered_by_retry"
	if strings.TrimSpace(succeededRequestID) != "" {
		note = "recovered_by_retry:" + succeededRequestID
	}

	for _, rec := range records {
		rec.Set("status", "succeeded")
		rec.Set("finished_at", time.Now())
		if rec.GetString("error") == "" {
			rec.Set("error", note)
		} else {
			rec.Set("error", note+" | "+rec.GetString("error"))
		}
		if saveErr := w.app.Save(rec); saveErr != nil {
			log.Printf("[worker] Warning: failed to reconcile failed job %s: %v", rec.Id, saveErr)
		}
	}
}

func (w *Worker) queueTotalSongsRecalc(artistID string) {
	if strings.TrimSpace(artistID) == "" {
		return
	}

	const debounce = 3 * time.Second

	w.recalcMu.Lock()
	defer w.recalcMu.Unlock()

	w.recalcPending[artistID] = struct{}{}
	if w.recalcTimer == nil {
		w.recalcTimer = time.AfterFunc(debounce, w.flushTotalSongsRecalc)
		return
	}
	w.recalcTimer.Reset(debounce)
}

func (w *Worker) flushTotalSongsRecalc() {
	w.recalcMu.Lock()
	pending := make(map[string]struct{}, len(w.recalcPending))
	for artistID := range w.recalcPending {
		pending[artistID] = struct{}{}
	}
	w.recalcPending = make(map[string]struct{})
	w.recalcTimer = nil
	w.recalcMu.Unlock()

	if len(pending) == 0 {
		return
	}

	if err := w.recalculateTotalSongsForArtists(pending); err != nil {
		log.Printf("[worker] Warning: failed to recalculate total_songs ranks: %v", err)
	}
}

func (w *Worker) recalculateTotalSongsForArtists(artistIDs map[string]struct{}) error {
	byGenre := map[string]map[string]struct{}{
		"rock_metal":      map[string]struct{}{},
		"everything_else": map[string]struct{}{},
	}

	for artistID := range artistIDs {
		record, err := w.app.FindRecordById("artists", artistID)
		if err != nil {
			continue
		}
		if record.GetString("list_status") == "waiting" {
			continue
		}
		genre := record.GetString("genre_group")
		if _, ok := byGenre[genre]; !ok {
			continue
		}
		byGenre[genre][artistID] = struct{}{}
	}

	for genre, targets := range byGenre {
		if len(targets) == 0 {
			continue
		}

		records, err := w.app.FindRecordsByFilter(
			"artists",
			"genre_group = {:genre} && list_status != {:waiting}",
			"-monthly_listeners,name",
			0,
			0,
			dbx.Params{"genre": genre, "waiting": "waiting"},
		)
		if err != nil {
			return fmt.Errorf("list artists for %s: %w", genre, err)
		}

		totalCount := len(records)
		for index, record := range records {
			if _, tracked := targets[record.Id]; !tracked {
				continue
			}

			targetTotalSongs := totalCount - index
			if record.GetInt("total_songs") == targetTotalSongs {
				continue
			}

			record.Set("total_songs", targetTotalSongs)
			if err := w.app.Save(record); err != nil {
				return fmt.Errorf("save total_songs for artist %s: %w", record.Id, err)
			}
		}
	}

	return nil
}

func (w *Worker) fetchTimeout(provider spotify.Provider) time.Duration {
	switch provider {
	case spotify.ProviderApify:
		if w.cfg.RequestTimeout > 2*time.Minute {
			return w.cfg.RequestTimeout
		}
		return 2 * time.Minute
	case spotify.ProviderScrapingAnt, spotify.ProviderScraperAPI:
		if w.cfg.RequestTimeout > 60*time.Second {
			return w.cfg.RequestTimeout
		}
		return 60 * time.Second
	case spotify.ProviderBrowserless, spotify.ProviderLocalHeadless:
		if w.cfg.RequestTimeout > 30*time.Second {
			return w.cfg.RequestTimeout
		}
		return 30 * time.Second
	}

	// ProviderAny fallback: account for all configured providers.
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
	if w.cfg.HasScraperAPI() {
		providerCount++
	}
	if w.cfg.HasApify() {
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

func (w *Worker) maxAckPending() int {
	limit := w.cfg.MaxConcurrency
	if w.cfg.LocalConcurrency > limit {
		limit = w.cfg.LocalConcurrency
	}
	if w.cfg.ScraperAPIConcurrency > limit {
		limit = w.cfg.ScraperAPIConcurrency
	}
	if limit <= 0 {
		return 1
	}
	return limit
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
