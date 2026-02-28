//go:build goexperiment.jsonv2

// Package worker provides a NATS-based background worker for processing Spotify scrape requests.
//
// Architecture: pull-based per-provider goroutine pools.
//
// A single durable JetStream consumer pulls messages from the scrape.request
// stream into a bounded Go channel. Each configured provider runs a pool of
// goroutines sized to its concurrency limit. Provider goroutines pull from the
// shared channel as they finish requests, so a provider with N slots always
// tries to keep N requests in flight. This replaces the previous push-based
// adaptive model where one handler selected a provider per message.
package worker

import (
	"ListenLedger/config"
	"ListenLedger/internal/fetcher"
	"ListenLedger/internal/messaging"
	"ListenLedger/internal/quota"
	"ListenLedger/internal/spotify"

	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
)

// providerSlot pairs a provider with its concurrency limit.
type providerSlot struct {
	provider    spotify.Provider
	concurrency int
}

// providerGroup holds the cancel context and live-goroutine count for one
// provider's goroutine pool.  When quota exhaustion is detected any goroutine
// in the group calls shutdown() which cancels the shared context; every sibling
// goroutine sees the cancellation and exits after NAK-ing any in-hand message.
type providerGroup struct {
	ctx      context.Context
	cancel   context.CancelFunc
	provider spotify.Provider
	label    string
	alive    sync.WaitGroup
	dead     chan struct{} // closed once alive.Wait() returns
}

// inflightMsg wraps a JetStream message with its parsed request and metadata
// so provider goroutines don't need to re-parse.
type inflightMsg struct {
	msg  jetstream.Msg
	meta *jetstream.MsgMetadata
	req  messaging.ScrapeRequested
}

// msgResult is the outcome of handleMsg so providerLoop can react.
type msgResult int

const (
	msgOK           msgResult = iota // processed (ack/nak already sent)
	msgQuotaExpired                  // quota exhaustion detected — caller should exit
)

// Worker handles background scraping jobs via NATS.
type Worker struct {
	app     *pocketbase.PocketBase
	nc      *nats.Conn
	js      jetstream.JetStream
	cfg     *config.Config
	fetcher *fetcher.Service
	quota   *quota.Checker

	// NATS consumer handle (for drain on shutdown).
	consume jetstream.ConsumeContext

	// JetStream tuning (resolved at Start time).
	maxDeliver int
	ackWait    time.Duration
	progress   time.Duration
	backoff    []time.Duration

	// work is the shared channel that the single NATS consumer feeds.
	// Provider goroutine pools pull from this channel.
	work chan inflightMsg

	// groups holds per-provider goroutine pool metadata.  Used during
	// shutdown to wait for each pool independently and to log which
	// providers are still alive.
	groups []*providerGroup

	// allGroupsDead is closed when every provider group has exited (e.g.
	// all providers hit quota).  Triggers draining the NATS consumer so
	// messages stop piling up in the work channel with nobody to process
	// them.
	allGroupsDead chan struct{}
	drainOnce     sync.Once

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	recalcMu      sync.Mutex
	recalcTimer   *time.Timer
	recalcPending map[string]struct{}
}

// New creates a new worker instance.
func New(app *pocketbase.PocketBase, nc *nats.Conn, js jetstream.JetStream, cfg *config.Config) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		app:           app,
		nc:            nc,
		js:            js,
		cfg:           cfg,
		quota:         quota.NewChecker(cfg),
		ctx:           ctx,
		cancel:        cancel,
		recalcPending: make(map[string]struct{}),
	}
}

// providerSlots returns one entry per enabled provider with its concurrency.
func (w *Worker) providerSlots() []providerSlot {
	var slots []providerSlot

	if w.cfg.HasLocalHeadless() {
		conc := w.cfg.LocalConcurrency
		if conc <= 0 {
			conc = 1
		}
		slots = append(slots, providerSlot{provider: spotify.ProviderLocalHeadless, concurrency: conc})
	}
	if w.cfg.HasBrowserless() {
		conc := w.cfg.MaxConcurrency
		if conc <= 0 {
			conc = 1
		}
		slots = append(slots, providerSlot{provider: spotify.ProviderBrowserless, concurrency: conc})
	}
	if w.cfg.HasScrapingAnt() {
		conc := w.cfg.MaxConcurrency
		if conc <= 0 {
			conc = 1
		}
		slots = append(slots, providerSlot{provider: spotify.ProviderScrapingAnt, concurrency: conc})
	}
	if w.cfg.HasScraperAPI() {
		conc := w.cfg.ScraperAPIConcurrency
		if conc <= 0 {
			conc = 1
		}
		slots = append(slots, providerSlot{provider: spotify.ProviderScraperAPI, concurrency: conc})
	}
	if w.cfg.HasApify() {
		conc := w.cfg.MaxConcurrency
		if conc <= 0 {
			conc = 1
		}
		slots = append(slots, providerSlot{provider: spotify.ProviderApify, concurrency: conc})
	}

	return slots
}

// totalConcurrency sums all provider concurrency slots.
func (w *Worker) totalConcurrency() int {
	total := 0
	for _, s := range w.providerSlots() {
		total += s.concurrency
	}
	if total <= 0 {
		total = 1
	}
	return total
}

// Start begins listening for scrape requests on NATS.
func (w *Worker) Start() {
	// Initialize the Spotify client and fetcher.
	client, err := spotify.NewClient(w.cfg)
	if err != nil {
		log.Printf("[worker] Warning: Could not initialize Spotify client: %v", err)
		log.Printf("[worker] Worker will start but scraping will be disabled until tokens are configured")
	} else {
		w.fetcher = fetcher.NewService(client, w.cfg)
	}

	// Resolve JetStream tuning knobs.
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
		ackWait = 2 * w.maxFetchTimeout()
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
	maxProgress := ackWait / 2
	if maxProgress < time.Second {
		maxProgress = time.Second
	}
	if progress > maxProgress {
		progress = maxProgress
	}
	w.progress = progress

	totalConc := w.totalConcurrency()

	// Create the shared work channel. Buffer it to totalConcurrency so the
	// NATS consume callback can hand off messages without blocking on a full
	// channel while all provider goroutines are busy.
	w.work = make(chan inflightMsg, totalConc)

	// Single durable JetStream consumer (restart-safe) for the shared queue.
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	consumer, err := messaging.EnsureScrapeWorkerConsumer(ctx, w.js, jetstream.ConsumerConfig{
		Durable:       messaging.ScrapeWorkerConsumerName,
		FilterSubject: messaging.SubjectScrapeRequest,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    maxDeliver,
		BackOff:       backoff,
		MaxAckPending: totalConc,
	})
	if err != nil {
		log.Printf("[worker] Failed to ensure scrape consumer: %v", err)
		return
	}

	// Align local tuning to actual server-side consumer config.
	if info, infoErr := consumer.Info(ctx); infoErr == nil && info != nil {
		if info.Config.MaxDeliver > 0 {
			w.maxDeliver = info.Config.MaxDeliver
		}
		if info.Config.AckWait > 0 {
			w.ackWait = info.Config.AckWait
			mp := w.ackWait / 2
			if mp < time.Second {
				mp = time.Second
			}
			if w.progress > mp {
				w.progress = mp
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

	// Start the NATS consumer callback that feeds the work channel.
	consume, err := consumer.Consume(w.dispatchToChannel)
	if err != nil {
		log.Printf("[worker] Failed to start consumer: %v", err)
		return
	}
	w.consume = consume

	// Spawn per-provider goroutine pools.
	slots := w.providerSlots()
	if len(slots) == 0 {
		// No providers configured — run a single goroutine that will report
		// fetcher-unavailable for every message.
		log.Println("[worker] No providers configured; starting single fallback worker")
		g := w.newProviderGroup(spotify.ProviderAny)
		g.alive.Add(1)
		w.wg.Add(1)
		go w.providerLoop(g, 0)
	} else {
		for _, slot := range slots {
			g := w.newProviderGroup(slot.provider)
			for i := 0; i < slot.concurrency; i++ {
				g.alive.Add(1)
				w.wg.Add(1)
				go w.providerLoop(g, i)
			}
			log.Printf("[worker] Started %d goroutine(s) for provider %s", slot.concurrency, providerLabel(slot.provider))
		}
	}

	// Monitor each provider group; when all groups are dead (e.g. every
	// provider hit its quota) drain the NATS consumer so messages aren't
	// buffered with no one to process them.
	w.allGroupsDead = make(chan struct{})
	go w.watchAllGroups()

	log.Printf("[worker] Started listening for scrape requests (pull-based, %d total slots across %d provider(s))", totalConc, len(slots))
}

// newProviderGroup creates a providerGroup with a context derived from the
// worker's root context and registers it in w.groups.
func (w *Worker) newProviderGroup(provider spotify.Provider) *providerGroup {
	ctx, cancel := context.WithCancel(w.ctx)
	g := &providerGroup{
		ctx:      ctx,
		cancel:   cancel,
		provider: provider,
		label:    providerLabel(provider),
		dead:     make(chan struct{}),
	}
	// Start a goroutine that closes g.dead once every goroutine in the
	// group has exited.
	go func() {
		g.alive.Wait()
		close(g.dead)
	}()
	w.groups = append(w.groups, g)
	return g
}

// watchAllGroups waits for every provider group to die, then drains the NATS
// consumer and closes the work channel.  This prevents messages from piling up
// in the buffered channel when no provider goroutines are alive to process
// them (e.g. every configured provider has hit its quota limit).
func (w *Worker) watchAllGroups() {
	for _, g := range w.groups {
		<-g.dead
		log.Printf("[worker] Provider group %s has fully exited", g.label)
	}

	// All groups are gone.
	log.Println("[worker] All provider groups have exited — draining NATS consumer")
	w.drainOnce.Do(func() {
		if w.consume != nil {
			w.consume.Drain()
		}
		close(w.allGroupsDead)
	})
}

// Stop gracefully drains the NATS consumer and waits for in-flight work.
func (w *Worker) Stop() {
	w.cancel()

	w.recalcMu.Lock()
	if w.recalcTimer != nil {
		w.recalcTimer.Stop()
		w.recalcTimer = nil
	}
	w.recalcMu.Unlock()

	// Drain NATS consumer (idempotent with watchAllGroups via drainOnce).
	w.drainOnce.Do(func() {
		if w.consume != nil {
			w.consume.Drain()
		}
		if w.allGroupsDead != nil {
			close(w.allGroupsDead)
		}
	})

	// Close the work channel so provider goroutines exit after draining.
	if w.work != nil {
		close(w.work)
	}

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

// dispatchToChannel is the NATS Consume callback. It parses the message and
// pushes it into the work channel for provider goroutines to pick up.
func (w *Worker) dispatchToChannel(msg jetstream.Msg) {
	meta, _ := msg.Metadata()

	req, err := messaging.UnmarshalScrapeRequested(msg.Data())
	if err != nil {
		log.Printf("[worker] Failed to unmarshal request (terminating): %v", err)
		w.publishScrapeDLQ(msg, meta, nil, "unmarshal_error")
		if termErr := msg.Term(); termErr != nil {
			log.Printf("[worker] Failed to terminate poison message: %v", termErr)
		}
		return
	}

	// Start an InProgress heartbeat immediately so NATS doesn't redeliver
	// while the message is queued in the Go channel waiting for a provider.
	done := make(chan struct{})
	go w.inProgressLoop(msg, done)

	select {
	case w.work <- inflightMsg{msg: msg, meta: meta, req: req}:
		// Queued — stop the dispatch-side heartbeat; the provider goroutine
		// will start its own.  The gap is effectively instant since the
		// goroutine is blocked on the channel receive.
		close(done)
	case <-w.allGroupsDead:
		close(done)
		// Every provider group is dead (quota exhaustion everywhere).
		// NAK so NATS can redeliver after a restart.
		if nakErr := msg.Nak(); nakErr != nil {
			log.Printf("[worker] Failed to NAK message (all providers exhausted): %v", nakErr)
		}
	case <-w.ctx.Done():
		close(done)
		// Worker shutting down — NAK so NATS redelivers.
		if nakErr := msg.Nak(); nakErr != nil {
			log.Printf("[worker] Failed to NAK message on shutdown: %v", nakErr)
		}
	}
}

// providerLoop is the main loop for a single provider goroutine. It pulls
// messages from the shared work channel and processes them using the given
// provider.  When a quota-exhaustion error is detected (or the group context
// is cancelled by a sibling), the goroutine exits.  Any message it had already
// pulled from the channel is NAK-ed back to JetStream so other providers (or a
// future restart) can pick it up.
func (w *Worker) providerLoop(g *providerGroup, slot int) {
	defer w.wg.Done()
	defer g.alive.Done()

	log.Printf("[worker] Provider %s slot %d ready", g.label, slot)

	for {
		select {
		case <-g.ctx.Done():
			// Another goroutine in this group detected quota exhaustion (or
			// the worker is shutting down).
			log.Printf("[worker] Provider %s slot %d: group context cancelled, exiting", g.label, slot)
			return
		case item, ok := <-w.work:
			if !ok {
				// Channel closed — normal shutdown.
				log.Printf("[worker] Provider %s slot %d exiting (channel closed)", g.label, slot)
				return
			}

			result := w.handleMsg(item, g.provider, g.label)
			if result == msgQuotaExpired {
				log.Printf("[worker] Provider %s slot %d: quota exhausted, shutting down provider pool", g.label, slot)
				// Cancel the group context so sibling goroutines exit too.
				g.cancel()
				return
			}
		}
	}
}

// handleMsg processes a single scrape request using the specified provider.
// It returns msgQuotaExpired when the fetch error wraps spotify.ErrQuotaExhausted
// so the caller can shut down the provider pool.  In that case the message is
// NAK-ed back to JetStream (without consuming a delivery attempt when possible)
// so that goroutines for other providers can pick it up.
func (w *Worker) handleMsg(item inflightMsg, provider spotify.Provider, label string) msgResult {
	msg := item.msg
	meta := item.meta
	req := item.req

	// --- Pre-flight quota guard for Apify ---
	// Check the Apify /limits endpoint before launching an expensive Actor run.
	// If budget or memory is insufficient the message is NAK-ed immediately so
	// another provider (or a future restart) can handle it, saving credits and
	// avoiding a guaranteed HTTP 402.
	if provider == spotify.ProviderApify {
		checkCtx, checkCancel := context.WithTimeout(w.ctx, 5*time.Second)
		info := w.quota.CheckApify(checkCtx)
		checkCancel()
		if !info.Available {
			log.Printf("[worker] Apify pre-flight check failed for %s: %s — NAK-ing message", req.ArtistID, info.Error)
			if nakErr := msg.Nak(); nakErr != nil {
				log.Printf("[worker] Failed to NAK message on Apify pre-flight: %v", nakErr)
			}
			return msgQuotaExpired
		}
	}

	// Start an InProgress heartbeat for this provider's processing time.
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

	if err := w.processRequest(req, meta, provider, label); err != nil {
		stopProgress()

		// --- Quota exhaustion: NAK and signal the caller to stop. ---
		if errors.Is(err, spotify.ErrQuotaExhausted) {
			log.Printf("[worker] Quota exhausted for provider %s while processing %s: %v", label, req.ArtistID, err)
			// NAK without delay so the message is immediately available for
			// another provider's goroutine.
			if nakErr := msg.Nak(); nakErr != nil {
				log.Printf("[worker] Failed to NAK message on quota exhaustion: %v", nakErr)
			}
			return msgQuotaExpired
		}

		log.Printf("[worker] Retryable error processing %s (provider=%s): %v", req.ArtistID, label, err)

		// If we've exhausted retries, dead-letter the message.
		if meta != nil && int(meta.NumDelivered) >= w.maxDeliver {
			w.setScrapeJobFinished(req.RequestID, "failed", "retry_exhausted")
			w.publishScrapeDLQ(msg, meta, &req, "retry_exhausted: "+err.Error())
			if termErr := msg.Term(); termErr != nil {
				log.Printf("[worker] Failed to terminate retry-exhausted message: %v", termErr)
			}
			return msgOK
		}

		if nakErr := msg.NakWithDelay(w.retryDelay(meta)); nakErr != nil {
			log.Printf("[worker] Failed to NAK message: %v", nakErr)
		}
		return msgOK
	}

	stopProgress()
	ackCtx, cancel := context.WithTimeout(w.ctx, 2*time.Second)
	defer cancel()
	if err := msg.DoubleAck(ackCtx); err != nil {
		log.Printf("[worker] Failed to DoubleAck message: %v", err)
	}
	return msgOK
}

// ---------------------------------------------------------------------------
// Provider helpers
// ---------------------------------------------------------------------------

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

// fetchTimeout returns the per-request context timeout for a given provider.
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

	// ProviderAny fallback.
	timeout := w.cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if timeout < 30*time.Second {
		timeout = 30 * time.Second
	}
	return timeout
}

// maxFetchTimeout returns the worst-case fetch timeout across all configured
// providers. Used to compute a safe AckWait.
func (w *Worker) maxFetchTimeout() time.Duration {
	worst := 30 * time.Second
	for _, s := range w.providerSlots() {
		t := w.fetchTimeout(s.provider)
		if t > worst {
			worst = t
		}
	}
	return worst
}

// ---------------------------------------------------------------------------
// Retry / progress helpers
// ---------------------------------------------------------------------------

func (w *Worker) retryDelay(meta *jetstream.MsgMetadata) time.Duration {
	if len(w.backoff) == 0 {
		return 10 * time.Second
	}
	if meta == nil || meta.NumDelivered == 0 {
		return w.backoff[0]
	}

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

// ---------------------------------------------------------------------------
// DLQ
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Core processing
// ---------------------------------------------------------------------------

func (w *Worker) processRequest(req messaging.ScrapeRequested, meta *jetstream.MsgMetadata, provider spotify.Provider, label string) error {
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
		label,
		req.ArtistName,
		req.SpotifyID,
	)

	w.setScrapeJobProcessing(req.RequestID)

	// Update artist status to pending.
	if err := w.updateArtistStatus(req.ArtistID, "pending"); err != nil {
		return fmt.Errorf("set pending: %w", err)
	}

	// Check if fetcher is available.
	if w.fetcher == nil {
		log.Printf("[worker] Fetcher not available, marking as failed")
		if err := w.updateArtistStatus(req.ArtistID, "failed"); err != nil {
			return fmt.Errorf("set failed: %w", err)
		}
		w.setScrapeJobFinished(req.RequestID, "failed", "fetcher_unavailable")
		return nil
	}

	// Fetch the listener count using the specific provider assigned to this goroutine.
	ctx, cancel := context.WithTimeout(w.ctx, w.fetchTimeout(provider))
	defer cancel()

	listeners, err := w.fetcher.FetchOne(ctx, req.SpotifyID, provider)
	if err != nil {
		log.Printf("[worker] Failed to fetch listeners for %s via %s: %v", req.SpotifyID, label, err)

		// On quota exhaustion the message will be NAK-ed and retried by
		// another provider, so keep the artist as "pending" and the scrape
		// job as "processing" — don't mark anything as permanently failed.
		if errors.Is(err, spotify.ErrQuotaExhausted) {
			return fmt.Errorf("fetch failed for %s via %s: %w", req.SpotifyID, label, err)
		}

		if statusErr := w.updateArtistStatus(req.ArtistID, "failed"); statusErr != nil {
			return fmt.Errorf("set failed: %w", statusErr)
		}
		w.setScrapeJobFinished(req.RequestID, "failed", "fetch_failed")
		return fmt.Errorf("fetch failed for %s via %s: %w", req.SpotifyID, label, err)
	}

	// Update the artist record with new listener count.
	if err := w.updateArtistListeners(req.ArtistID, listeners); err != nil {
		return fmt.Errorf("update listeners: %w", err)
	}

	log.Printf("[worker] Successfully updated %s with %d monthly listeners via %s", req.ArtistName, listeners, label)
	w.setScrapeJobFinished(req.RequestID, "succeeded", "")
	w.queueTotalSongsRecalc(req.ArtistID)
	w.clearFailedJobsForArtist(req.ArtistID, req.RequestID)
	return nil
}

// ---------------------------------------------------------------------------
// PocketBase helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Total-songs recalculation (debounced)
// ---------------------------------------------------------------------------

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
		"rock_metal":      {},
		"everything_else": {},
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

// ---------------------------------------------------------------------------
// Artist record helpers
// ---------------------------------------------------------------------------

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
