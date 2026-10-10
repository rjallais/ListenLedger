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
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/pocketbase"

	toolbeltdb "github.com/delaneyj/toolbelt/db"

	"ListenLedger/config"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/fetcher"
	"ListenLedger/internal/messaging"
	"ListenLedger/internal/projections"
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

	db               *toolbeltdb.Database
	artistRepo       *artist.Repository
	artistProjection *projections.ArtistProjection
	jobStore         eventsourcing.Store
	jobRepo          *scrapejob.Repository

	// groups holds per-provider goroutine pool metadata. Used during shutdown to
	// wait for each pool independently and to log which providers are still alive.
	groups []*providerGroup

	// allGroupsDead is closed when every provider group has exited (e.g. all
	// providers hit quota). Triggers draining the NATS consumer so messages stop
	// piling up in the work channel with nobody to process them.
	allGroupsDead chan struct{}
	cancel        context.CancelFunc
	rankTimer     *time.Timer

	rankPending       map[string]struct{}
	succeededRequests map[string]time.Time
	metricsProvider   map[string]*providerMetrics

	maxDeliver    int
	ackWait       time.Duration
	progress      time.Duration
	providerCount int

	rankMu      sync.Mutex
	succeededMu sync.Mutex
	metricsMu   sync.Mutex

	// lastPurge tracks the last succeeded-job retention purge (guarded by
	// purgeMu) so the hourly maintenance in sweepStaleJobs also runs once at
	// startup.
	lastPurge time.Time
	purgeMu   sync.Mutex

	// accepting gates whether dispatch callbacks may enqueue into work.
	accepting atomic.Bool
	started   bool
	// dispatching tracks in-flight dispatch callbacks so shutdown can safely
	// drain and close the work queue without racing active sends.
	workCloseOnce sync.Once
	drainOnce     sync.Once
}

// Option configures optional dependencies for Worker.
type Option func(*Worker)

// WithDatabase configures the worker with SQLite database, event store, and projection.
// It must be applied after New assigns nc/js: the projection reads w.nc and
// w.js at option time, so constructing a Worker any other way leaves it with
// nil connections that silently publish nothing.
func WithDatabase(db *toolbeltdb.Database) Option {
	return func(w *Worker) {
		w.db = db
		if db != nil {
			// Authoritative event log: SQLite by default, JetStream when
			// LISTENLEDGER_EVENT_STORE=jetstream (see SelectedStore).
			events, err := eventsourcing.SelectedStore(db, w.js)
			if err != nil {
				slog.Error("event store selection failed, staying on SQLite", "error", err)
				events = eventsourcing.NewSQLiteStore(db)
			}
			w.artistRepo = artist.NewRepository(events)
			w.artistProjection = projections.NewArtistProjection(slog.Default(), db, w.nc)
			// Durable domain.events.> publishing (append-only log = source of truth).
			// Core-NATS fanout for live SSE stays as the ephemeral UI hint.
			if w.js != nil {
				w.artistProjection.SetJetStream(w.js)
			}
			w.jobStore = events
			w.jobRepo = scrapejob.NewRepository(events)
		}
	}
}

// New creates a new worker instance.
func New(app *pocketbase.PocketBase, nc *nats.Conn, js jetstream.JetStream, cfg *config.Config, opts ...Option) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	w := &Worker{
		app:               app,
		nc:                nc,
		js:                js,
		cfg:               cfg,
		quota:             quota.NewChecker(cfg),
		ctx:               ctx,
		cancel:            cancel,
		rankPending:       make(map[string]struct{}),
		succeededRequests: make(map[string]time.Time),
		metricsProvider:   make(map[string]*providerMetrics),
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
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
//
// AckWait is never allowed below minSafeAckWait: a stale or explicitly
// configured value under the worst-case fetch timeout guarantees redelivery
// while a fetch is still running (each redelivery burns a provider attempt
// until MaxDeliver is exhausted).
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
	if floor := w.minSafeAckWait(); ackWait < floor {
		if w.cfg.ScrapeAckWait > 0 {
			log.Printf("[worker] Configured SCRAPE_ACK_WAIT=%s is below the safe floor %s for the enabled providers; clamping", w.cfg.ScrapeAckWait, floor)
		}
		ackWait = floor
	}
	w.ackWait = ackWait

	progress := w.cfg.ScrapeInProgressInterval
	if progress <= 0 {
		progress = 20 * time.Second
	}
	maxProgress := max(ackWait/2, time.Second)
	if progress > maxProgress {
		progress = maxProgress
	}
	w.progress = progress
}

// minSafeAckWait returns the floor for server-side redelivery timing derived
// from the worst-case fetch timeout across enabled providers.
// Callers must have populated w.backoff (see resolveJetStreamTuning) first.
func (w *Worker) minSafeAckWait() time.Duration {
	floor := max(2*w.maxFetchTimeout(), 2*time.Minute)
	for _, d := range w.backoff {
		if d > floor {
			floor = d
		}
	}
	return floor
}

// serverBackoff returns the BackOff schedule installed on the JetStream
// consumer. When BackOff is set, the server normalizes the stored AckWait to
// BackOff[0] and redelivers unacked messages on that schedule — so BackOff,
// not AckWait, is the effective safety net.
//
// This must stay above minSafeAckWait: while a provider goroutine is alive it
// sends InProgress heartbeats every w.progress (20s), but any heartbeat gap
// (preflight checks, PB stalls, crashes) longer than BackOff[0] duplicates
// the fetch. A single entry suffices — explicit NAKs in processing.go carry
// their own NakWithDelay pacing from w.backoff, so the server schedule only
// fires for genuinely orphaned deliveries.
func (w *Worker) serverBackoff() []time.Duration {
	return []time.Duration{w.minSafeAckWait()}
}

// createAndAlignConsumer ensures the durable JetStream consumer exists, heals
// stale server-side tuning, reads back the live config, and returns the
// active ConsumeContext.
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
		BackOff:       w.serverBackoff(),
		MaxAckPending: totalConc,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure scrape consumer: %w", err)
	}

	healCtx, healCancel := context.WithTimeout(ctx, 10*time.Second)
	defer healCancel()
	consumer, err = w.healConsumerTuning(healCtx, consumer, totalConc)
	if err != nil {
		return nil, err
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

// healConsumerTuning deletes and recreates the durable consumer when the
// stored redelivery schedule is below the safe floor (e.g. BackOff starting
// at 10s persisted by older defaults while the slowest enabled fetch needs
// minutes). When BackOff is set the server redelivers on that schedule
// regardless of AckWait, so a short BackOff[0] duplicates every slow scrape
// on any heartbeat gap. CreateOrUpdate does not reliably converge
// pre-existing consumers, hence delete + recreate. Returns the live handle.
func (w *Worker) healConsumerTuning(ctx context.Context, consumer jetstream.Consumer, totalConc int) (jetstream.Consumer, error) {
	info, err := consumer.Info(ctx)
	if err != nil || info == nil {
		// Leave diagnosis to alignFromConsumerInfo; don't fail startup here.
		return consumer, nil
	}
	floor := w.minSafeAckWait()
	staleBackoff := len(info.Config.BackOff) == 0 || info.Config.BackOff[0] < floor
	if info.Config.AckWait >= floor && !staleBackoff {
		return consumer, nil
	}

	log.Printf("[worker] Healing stale consumer tuning: durable=%s ack_wait=%s backoff=%v below safe floor %s (max fetch timeout %s) — recreating consumer",
		info.Config.Durable, info.Config.AckWait, info.Config.BackOff, floor, w.maxFetchTimeout())

	stream, err := w.js.Stream(ctx, messaging.ScrapeRequestsStreamName)
	if err != nil {
		return nil, fmt.Errorf("heal consumer: load scrape stream: %w", err)
	}
	if err := stream.DeleteConsumer(ctx, messaging.ScrapeWorkerConsumerName); err != nil {
		return nil, fmt.Errorf("heal consumer: delete stale consumer: %w", err)
	}
	healed, err := messaging.EnsureScrapeWorkerConsumer(ctx, w.js, jetstream.ConsumerConfig{
		Durable:       messaging.ScrapeWorkerConsumerName,
		FilterSubject: messaging.SubjectScrapeRequest,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       w.ackWait,
		MaxDeliver:    w.maxDeliver,
		BackOff:       w.serverBackoff(),
		MaxAckPending: totalConc,
	})
	if err != nil {
		return nil, fmt.Errorf("heal consumer: recreate consumer: %w", err)
	}
	return healed, nil
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
	// Never adopt a server-side AckWait below the safe floor: stale durables
	// (e.g. AckWait=10s from older defaults) would otherwise survive every
	// restart via this alignment and redeliver slow fetches mid-flight.
	// healConsumerTuning recreates such consumers before we get here; this
	// guard covers the case where healing was skipped or failed.
	if info.Config.AckWait >= w.minSafeAckWait() {
		w.ackWait = info.Config.AckWait
	} else {
		log.Printf("[worker] Ignoring unsafe server AckWait=%s (floor %s); keeping %s",
			info.Config.AckWait, w.minSafeAckWait(), w.ackWait)
	}
	// Deliberately do NOT adopt the server BackOff into w.backoff: the server
	// schedule is the unacked-delivery safety net (see serverBackoff), while
	// w.backoff paces explicit NakWithDelay retries in processing.go and must
	// stay short for fast-fail errors.
	mp := max(w.ackWait/2, time.Second)
	if w.progress > mp {
		w.progress = mp
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
		w.wg.Go(func() { w.providerLoop(g, 0) })
		return
	}
	for _, slot := range slots {
		g := w.newProviderGroup(slot.provider, slot.concurrency)
		for i := range slot.concurrency {
			w.wg.Go(func() { w.providerLoop(g, i) })
		}
		log.Printf("[worker] Started %d goroutine(s) for provider %s", slot.concurrency, providerLabel(slot.provider))
	}
}

// launchBackgroundWorkers starts the all-groups watchdog, metrics reporter,
// and stale-job sweeper goroutines.
func (w *Worker) launchBackgroundWorkers() {
	w.allGroupsDead = make(chan struct{})
	go w.watchAllGroups()
	w.wg.Go(w.metricsReporter)
	w.wg.Go(w.sweepStaleJobs)
}

// Stop gracefully drains the NATS consumer and waits for in-flight work.
func (w *Worker) Stop() {
	w.cancel()

	w.rankMu.Lock()
	if w.rankTimer != nil {
		w.rankTimer.Stop()
		w.rankTimer = nil
	}
	w.rankMu.Unlock()

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
