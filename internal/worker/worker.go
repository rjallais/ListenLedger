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
	msg jetstream.Msg
	// dispatchProgress keeps ack heartbeats alive while the message waits in
	// the shared work channel for an available provider slot.
	dispatchProgress *progressHandle
	meta             *jetstream.MsgMetadata
	req              messaging.ScrapeRequested
}

type progressHandle struct {
	done chan struct{}
	once sync.Once
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
	// accepting gates whether dispatch callbacks may enqueue into work.
	accepting atomic.Bool
	// dispatching tracks in-flight dispatch callbacks so shutdown can safely
	// drain and close the work queue without racing active sends.
	dispatching   sync.WaitGroup
	workCloseOnce sync.Once

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

	succeededMu       sync.Mutex
	succeededRequests map[string]time.Time

	metricsMu       sync.Mutex
	metricsStarted  time.Time
	metricsProvider map[string]*providerMetrics
	providerCount   int
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

func (w *Worker) Start() {
	w.initMetrics()

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
	w.accepting.Store(true)

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
	w.providerCount = max(1, len(slots))
	if len(slots) == 0 {
		// No providers configured — run a single goroutine that will report
		// fetcher-unavailable for every message.
		log.Printf("[worker] No providers configured; starting single fallback worker")
		g := w.newProviderGroup(spotify.ProviderAny, 1)
		w.wg.Add(1)
		go w.providerLoop(g, 0)
	} else {
		for _, slot := range slots {
			g := w.newProviderGroup(slot.provider, slot.concurrency)
			for i := 0; i < slot.concurrency; i++ {
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
	w.wg.Add(1)
	go w.metricsReporter()

	log.Printf("[worker] Started listening for scrape requests (pull-based, %d total slots across %d provider(s))", totalConc, len(slots))
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
	w.accepting.Store(false)
	w.drainOnce.Do(func() {
		if w.consume != nil {
			w.consume.Drain()
		}
		if w.allGroupsDead != nil {
			close(w.allGroupsDead)
		}
	})

	w.dispatching.Wait()
	w.rejectQueuedWork()
	w.closeWork()

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
