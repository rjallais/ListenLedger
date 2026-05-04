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
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/pocketbase"

	"ListenLedger/config"
	"ListenLedger/internal/fetcher"
	"ListenLedger/internal/messaging"
	"ListenLedger/internal/quota"
	"ListenLedger/internal/spotify"
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
	ctx   context.Context
	label string
	alive sync.WaitGroup

	cancel context.CancelFunc
	dead   chan struct{} // closed once alive.Wait() returns

	provider spotify.Provider
}

// inflightMsg wraps a JetStream message with its parsed request and metadata
// so provider goroutines don't need to re-parse.
type inflightMsg struct {
	req messaging.ScrapeRequested

	msg jetstream.Msg
	// dispatchProgress keeps ack heartbeats alive while the message waits in
	// the shared work channel for an available provider slot.
	dispatchProgress *progressHandle
	meta             *jetstream.MsgMetadata
}

type progressHandle struct {
	once sync.Once
	done chan struct{}
}

func newProgressHandle() *progressHandle {
	return &progressHandle{done: make(chan struct{})}
}

func (h *progressHandle) Stop() {
	if h == nil {
		return
	}
	h.once.Do(func() {
		close(h.done)
	})
}

// msgResult is the outcome of handleMsg so providerLoop can react.
type msgResult int

const (
	msgOK           msgResult = iota // processed (ack/nak already sent)
	msgQuotaExpired                  // quota exhaustion detected — caller should exit
)

const requestSuccessCacheTTL = 30 * time.Minute
const localPoolFailureThreshold = 12

var errRequestAlreadySucceeded = errors.New("request already succeeded")
var errTerminalFailure = errors.New("terminal failure")

// Worker handles background scraping jobs via NATS.
type Worker struct {
	backoff        []time.Duration
	metricsStarted time.Time

	js      jetstream.JetStream
	consume jetstream.ConsumeContext
	ctx     context.Context

	// dispatchMu serializes accepting checks/updates with dispatching Add/Wait.
	dispatchMu  sync.Mutex
	dispatching sync.WaitGroup
	wg          sync.WaitGroup

	// work is the shared channel that the single NATS consumer feeds.
	// Provider goroutine pools pull from this channel.
	work chan inflightMsg

	app     *pocketbase.PocketBase
	nc      *nats.Conn
	cfg     *config.Config
	fetcher *fetcher.Service
	quota   *quota.Checker

	// groups holds per-provider goroutine pool metadata. Used during shutdown to
	// wait for each pool independently and to log which providers are still alive.
	groups []*providerGroup

	// allGroupsDead is closed when every provider group has exited (e.g. all
	// providers hit quota). Triggers draining the NATS consumer so messages stop
	// piling up in the work channel with nobody to process them.
	allGroupsDead chan struct{}
	cancel        context.CancelFunc
	recalcTimer   *time.Timer

	recalcPending     map[string]struct{}
	succeededRequests map[string]time.Time
	metricsProvider   map[string]*providerMetrics

	maxDeliver    int
	ackWait       time.Duration
	progress      time.Duration
	providerCount int

	recalcMu    sync.Mutex
	succeededMu sync.Mutex
	metricsMu   sync.Mutex

	// accepting gates whether dispatch callbacks may enqueue into work.
	accepting atomic.Bool
	started   bool
	// dispatching tracks in-flight dispatch callbacks so shutdown can safely
	// drain and close the work queue without racing active sends.
	workCloseOnce sync.Once
	drainOnce     sync.Once
}

// New creates a new worker instance.
func New(app *pocketbase.PocketBase, nc *nats.Conn, js jetstream.JetStream, cfg *config.Config) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		app:               app,
		nc:                nc,
		js:                js,
		cfg:               cfg,
		quota:             quota.NewChecker(cfg),
		ctx:               ctx,
		cancel:            cancel,
		recalcPending:     make(map[string]struct{}),
		succeededRequests: make(map[string]time.Time),
		metricsProvider:   make(map[string]*providerMetrics),
	}
}

func (w *Worker) Start() error {
	w.dispatchMu.Lock()
	if w.started {
		w.dispatchMu.Unlock()
		return fmt.Errorf("worker already started")
	}
	if err := w.ctx.Err(); err != nil {
		w.dispatchMu.Unlock()
		return fmt.Errorf("worker cannot be restarted after stop: %w", err)
	}
	w.started = true
	w.accepting.Store(true)
	w.dispatchMu.Unlock()

	startedOK := false
	defer func() {
		if startedOK {
			return
		}
		w.dispatchMu.Lock()
		w.started = false
		w.accepting.Store(false)
		w.dispatchMu.Unlock()
	}()

	w.initMetrics()
	w.initFetcherClient()
	w.resolveJetStreamTuning()

	totalConc := w.totalConcurrency()
	w.work = make(chan inflightMsg, totalConc)

	consume, err := w.createAndAlignConsumer(w.ctx, totalConc)
	if err != nil {
		return fmt.Errorf("failed to create or align JetStream consumer: %w", err)
	}
	w.consume = consume

	slots := w.providerSlots()
	w.providerCount = max(1, len(slots))
	w.spawnProviderPools(slots)
	w.launchBackgroundWorkers()
	startedOK = true

	log.Printf("[worker] Started listening for scrape requests (pull-based, %d total slots across %d provider(s))", totalConc, w.providerCount)
	return nil
}

// initFetcherClient initializes the Spotify client and fetcher service.
// Errors are logged as warnings; scraping is disabled until tokens are configured.
func (w *Worker) initFetcherClient() {
	client, err := spotify.NewClient(w.cfg)
	if err != nil {
		log.Printf("[worker] Warning: Could not initialize Spotify client: %v", err)
		log.Printf("[worker] Worker will start but scraping will be disabled until tokens are configured")
		return
	}
	w.fetcher = fetcher.NewService(client, w.cfg)
}

// resolveJetStreamTuning derives maxDeliver, backoff, ackWait, and progress from
// config, applying safe defaults where values are absent or out of range.
func (w *Worker) resolveJetStreamTuning() {
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
}

// createAndAlignConsumer ensures the durable JetStream consumer exists, reads back
// the server-side config, and returns the active ConsumeContext.
// Returns (consume, nil) on success or (nil, error) on failure.
func (w *Worker) createAndAlignConsumer(ctx context.Context, totalConc int) (jetstream.ConsumeContext, error) {
	ensureCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	consumer, err := messaging.EnsureScrapeWorkerConsumer(ensureCtx, w.js, jetstream.ConsumerConfig{
		Durable:       messaging.ScrapeWorkerConsumerName,
		FilterSubject: messaging.SubjectScrapeRequest,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       w.ackWait,
		MaxDeliver:    w.maxDeliver,
		BackOff:       w.backoff,
		MaxAckPending: totalConc,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure scrape consumer: %w", err)
	}

	alignCtx, alignCancel := context.WithTimeout(ctx, 2*time.Second)
	defer alignCancel()
	w.alignFromConsumerInfo(alignCtx, consumer)

	consume, err := consumer.Consume(w.dispatchToChannel)
	if err != nil {
		return nil, fmt.Errorf("start consumer: %w", err)
	}
	return consume, nil
}

func (w *Worker) alignFromConsumerInfo(ctx context.Context, consumer jetstream.Consumer) {
	info, infoErr := consumer.Info(ctx)
	if infoErr != nil {
		log.Printf("[worker] Warning: failed to load consumer info: %v", infoErr)
		return
	}
	if info == nil {
		return
	}
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
}

// spawnProviderPools starts one goroutine pool per configured provider slot.
// If no slots are configured a single fallback pool using ProviderAny is started.
func (w *Worker) spawnProviderPools(slots []providerSlot) {
	if len(slots) == 0 {
		log.Printf("[worker] No providers configured; starting single fallback worker")
		g := w.newProviderGroup(spotify.ProviderAny, 1)
		w.wg.Add(1)
		go w.providerLoop(g, 0)
		return
	}
	for _, slot := range slots {
		g := w.newProviderGroup(slot.provider, slot.concurrency)
		for i := 0; i < slot.concurrency; i++ {
			w.wg.Add(1)
			go w.providerLoop(g, i)
		}
		log.Printf("[worker] Started %d goroutine(s) for provider %s", slot.concurrency, providerLabel(slot.provider))
	}
}

// launchBackgroundWorkers starts the all-groups watchdog, metrics reporter,
// and stale-job sweeper goroutines.
func (w *Worker) launchBackgroundWorkers() {
	w.allGroupsDead = make(chan struct{})
	go w.watchAllGroups()
	w.wg.Add(1)
	go w.metricsReporter()
	w.wg.Add(1)
	go w.sweepStaleJobs()
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
		w.dispatchMu.Lock()
		w.started = false
		w.accepting.Store(false)
		consume := w.consume
		allGroupsDead := w.allGroupsDead
		w.dispatchMu.Unlock()

		if consume != nil {
			consume.Drain()
		}
		if allGroupsDead != nil {
			close(allGroupsDead)
		}

		w.dispatching.Wait()
		w.rejectQueuedWork()
		w.closeWork()
	})

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("[worker] Stopped gracefully")
	case <-time.After(30 * time.Second):
		log.Printf("[worker] Stop timed out, forcing shutdown")
	}

	if w.fetcher != nil {
		if err := w.fetcher.Close(); err != nil {
			log.Printf("[worker] Warning: Failed to close fetcher: %v", err)
		}
	}

	w.logMetricsSummary("stop")
}

func (w *Worker) rejectQueuedWork() {
	if w.work == nil {
		return
	}

	for {
		select {
		case item, ok := <-w.work:
			if !ok {
				return
			}
			item.dispatchProgress.Stop()
			if nakErr := item.msg.Nak(); nakErr != nil {
				log.Printf("[worker] Failed to NAK queued message during shutdown: %v", nakErr)
			}
		default:
			return
		}
	}
}

func (w *Worker) closeWork() {
	w.workCloseOnce.Do(func() {
		if w.work != nil {
			close(w.work)
		}
	})
}
