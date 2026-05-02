//go:build goexperiment.jsonv2

package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/messaging"
	"ListenLedger/internal/spotify"
)

// msgEnvelope bundles the JetStream message and its associated scrape-request
// metadata so error-handling helpers can accept a single value instead of a
// long parameter list.
type msgEnvelope struct {
	msg   jetstream.Msg
	meta  *jetstream.MsgMetadata
	req   messaging.ScrapeRequested
	label string
}

func (w *Worker) handleMsg(ctx context.Context, item inflightMsg, provider spotify.Provider, label string) msgResult {
	env := msgEnvelope{msg: item.msg, meta: item.meta, req: item.req, label: label}
	delivered := uint64(0)
	if env.meta != nil {
		delivered = env.meta.NumDelivered
	}
	w.recordAttempt(label, delivered)

	item.dispatchProgress.Stop()

	if w.isRequestAlreadySucceeded(env.req.RequestID) {
		return w.handleDedupSkip(ctx, env)
	}

	if provider == spotify.ProviderLocalHeadless || provider == spotify.ProviderLocalBrowserless {
		if result, ok := w.checkLocalProviderHealth(env.msg, label); ok {
			return result
		}
	}

	if provider == spotify.ProviderApify {
		if result, ok := w.checkApifyPreflight(ctx, env); ok {
			return result
		}
	}

	// Start an InProgress heartbeat for this provider's processing time.
	progress := newProgressHandle()
	go w.inProgressLoop(env.msg, progress.done)
	defer progress.Stop()

	if err := w.processRequest(ctx, env, provider); err != nil {
		progress.Stop()
		return w.handleProcessError(ctx, env, err)
	}

	progress.Stop()
	if err := w.ackMsg(ctx, env.msg); err != nil {
		w.recordAckError(label)
		log.Printf("[worker] Failed to ack message: %v", err)
	}
	return msgOK
}

// handleDedupSkip acks a message that was already handled by another provider
// and returns msgOK to signal the caller that no further work is needed.
func (w *Worker) handleDedupSkip(ctx context.Context, env msgEnvelope) msgResult {
	w.recordDedupSkipped(env.label)
	log.Printf("[worker] Skipping already-succeeded request_id=%s (provider=%s)", env.req.RequestID, env.label)
	if err := w.ackMsg(ctx, env.msg); err != nil {
		w.recordAckError(env.label)
		log.Printf("[worker] Failed to ack deduplicated message: %v", err)
	}
	return msgOK
}

// checkLocalProviderHealth returns (msgQuotaExpired, true) if the local
// provider pool should be parked, or (_, false) if processing should continue.
func (w *Worker) checkLocalProviderHealth(msg jetstream.Msg, label string) (msgResult, bool) {
	park, attempts, retryable := w.shouldParkLocalPool(label)
	if !park {
		return msgOK, false
	}
	log.Printf(
		"[worker] Provider %s unhealthy (attempts=%d retryable=%d succeeded=0) — parking provider pool for this run",
		label,
		attempts,
		retryable,
	)
	if nakErr := msg.NakWithDelay(3 * time.Second); nakErr != nil {
		log.Printf("[worker] Failed to NAK message while parking local provider pool: %v", nakErr)
	}
	return msgQuotaExpired, true
}

// checkApifyPreflight checks Apify budget/memory before launching an expensive
// Actor run. Returns (msgQuotaExpired, true) if the pre-flight fails.
//
// Pre-flight guard: verifies the Apify /limits endpoint before launching an
// expensive Actor run. If budget or memory is insufficient the message is
// NAK-ed immediately so another provider (or a future restart) can handle it,
// saving credits and avoiding a guaranteed HTTP 402.
func (w *Worker) checkApifyPreflight(ctx context.Context, env msgEnvelope) (msgResult, bool) {
	checkCtx, checkCancel := context.WithTimeout(ctx, 5*time.Second)
	defer checkCancel()
	info := w.quota.CheckApify(checkCtx)
	if info.Available {
		return msgOK, false
	}
	log.Printf("[worker] Apify pre-flight check failed for %s: %s — NAK-ing message", env.req.ArtistID, info.Error)
	w.recordQuotaExhausted(env.label)
	if nakErr := env.msg.Nak(); nakErr != nil {
		log.Printf("[worker] Failed to NAK message on Apify pre-flight: %v", nakErr)
	}
	return msgQuotaExpired, true
}

// handleProcessError maps errors returned by processRequest to the appropriate
// JetStream disposition (ack, NAK, NAK-with-delay, Term) and msgResult.
func (w *Worker) handleProcessError(ctx context.Context, env msgEnvelope, err error) msgResult {
	if errors.Is(err, errRequestAlreadySucceeded) {
		return w.handleDedupSkip(ctx, env)
	}

	if errors.Is(err, errTerminalFailure) {
		return w.handleTerminalFailure(ctx, env)
	}

	if isContextError(ctx, err) {
		log.Printf("[worker] Context cancelled while processing %s via %s: %v", env.req.ArtistID, env.label, err)
		if nakErr := env.msg.Nak(); nakErr != nil {
			log.Printf("[worker] Failed to NAK message after context cancellation: %v", nakErr)
		}
		return msgOK
	}

	if errors.Is(err, spotify.ErrQuotaExhausted) {
		return w.handleQuotaExhausted(env, err)
	}

	if errors.Is(err, spotify.ErrRateLimited) {
		return w.handleRateLimited(env, err)
	}

	return w.handleRetryableError(ctx, env, err)
}

// handleTerminalFailure acks a message that has permanently failed.
func (w *Worker) handleTerminalFailure(ctx context.Context, env msgEnvelope) msgResult {
	w.recordTerminalFailure(env.label)
	if ackErr := w.ackMsg(ctx, env.msg); ackErr != nil {
		w.recordAckError(env.label)
		log.Printf("[worker] Failed to ack terminal-failed message: %v", ackErr)
	}
	return msgOK
}

// handleQuotaExhausted NAKs the message without delay so another provider can
// pick it up immediately, and signals the caller to shut down this pool.
func (w *Worker) handleQuotaExhausted(env msgEnvelope, err error) msgResult {
	w.recordQuotaExhausted(env.label)
	log.Printf("[worker] Quota exhausted for provider %s while processing %s: %v", env.label, env.req.ArtistID, err)
	if nakErr := env.msg.Nak(); nakErr != nil {
		log.Printf("[worker] Failed to NAK message on quota exhaustion: %v", nakErr)
	}
	return msgQuotaExpired
}

// handleRateLimited NAKs the message with an appropriate backoff delay.
func (w *Worker) handleRateLimited(env msgEnvelope, err error) msgResult {
	w.recordRateLimited(env.label)
	delay := w.retryDelay(env.meta)
	if retryAfter, ok := spotify.RetryAfter(err); ok && retryAfter > delay {
		delay = retryAfter
	}
	delay = withJitter(delay, min(2*time.Second, delay/4))
	log.Printf("[worker] Provider %s rate-limited for %s; delaying redelivery by %s", env.label, env.req.ArtistID, delay.Round(time.Millisecond))
	if nakErr := env.msg.NakWithDelay(delay); nakErr != nil {
		log.Printf("[worker] Failed to NAK message on rate limit: %v", nakErr)
	}
	return msgOK
}

// handleRetryableError NAKs the message for redelivery, or dead-letters it if
// the delivery limit has been reached.
func (w *Worker) handleRetryableError(ctx context.Context, env msgEnvelope, err error) msgResult {
	w.recordRetryableError(env.label, err)
	log.Printf("[worker] Retryable error processing %s (provider=%s): %v", env.req.ArtistID, env.label, err)

	if env.meta != nil && int(env.meta.NumDelivered) >= w.maxDeliver {
		return w.handleDLQ(ctx, env, err)
	}

	if nakErr := env.msg.NakWithDelay(w.retryDelay(env.meta)); nakErr != nil {
		log.Printf("[worker] Failed to NAK message: %v", nakErr)
	}
	return msgOK
}

// handleDLQ publishes the message to the dead-letter queue after retries are
// exhausted and terminates it from the JetStream consumer.
func (w *Worker) handleDLQ(ctx context.Context, env msgEnvelope, err error) msgResult {
	reason := "retry_exhausted: " + err.Error()
	if dlqErr := w.publishScrapeDLQ(ctx, env, reason); dlqErr != nil {
		log.Printf("[worker] Failed to publish retry-exhausted message to DLQ: %v", dlqErr)
		if nakErr := env.msg.Nak(); nakErr != nil {
			log.Printf("[worker] Failed to NAK retry-exhausted message after DLQ publish failure: %v", nakErr)
		}
		return msgOK
	}
	if termErr := env.msg.Term(); termErr != nil {
		log.Printf("[worker] Failed to terminate retry-exhausted message: %v", termErr)
		return msgOK
	}
	w.setScrapeJobFinished(env.req.RequestID, "failed", "retry_exhausted")
	w.recordDLQ(env.label)
	return msgOK
}

// isContextError reports whether err is a context cancellation or deadline
// that applies to the current processing context.
func isContextError(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		(ctx.Err() != nil && errors.Is(err, ctx.Err()))
}

func (w *Worker) fetchTimeout(provider spotify.Provider) time.Duration {
	minTimeout := providerMinTimeout(provider)
	return max(w.cfg.RequestTimeout, minTimeout)
}

// providerMinTimeout returns the minimum safe fetch timeout for a given
// provider. This is the floor used by fetchTimeout.
func providerMinTimeout(provider spotify.Provider) time.Duration {
	switch provider {
	case spotify.ProviderApify:
		return 350 * time.Second
	case spotify.ProviderScrapingAnt:
		return 180 * time.Second
	case spotify.ProviderScraperAPI:
		return 180 * time.Second
	case spotify.ProviderLocalHeadless:
		return 300 * time.Second
	case spotify.ProviderLocalBrowserless:
		return 60 * time.Second
	case spotify.ProviderBrowserless:
		return 30 * time.Second
	default:
		return 30 * time.Second
	}
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

func withJitter(base, maxJitter time.Duration) time.Duration {
	if base <= 0 || maxJitter <= 0 {
		return base
	}
	jitter := time.Duration(rand.Int63n(int64(maxJitter + time.Nanosecond)))
	return base + jitter
}

func (w *Worker) inProgressLoop(msg jetstream.Msg, done <-chan struct{}) {
	if w.progress <= 0 {
		return
	}

	if err := msg.InProgress(); err != nil {
		log.Printf("[worker] Warning: initial InProgress failed: %v", err)
	}

	t := time.NewTicker(w.progress)
	defer t.Stop()

	allGroupsDead := w.allGroupsDead

	for {
		select {
		case <-done:
			return
		case <-w.ctx.Done():
			return
		case <-allGroupsDead:
			return
		case <-t.C:
			if err := msg.InProgress(); err != nil {
				log.Printf("[worker] Warning: InProgress failed: %v", err)
			}
		}
	}
}

func (w *Worker) ackMsg(ctx context.Context, msg jetstream.Msg) error {
	var lastErr error
	for attempt := range 3 {
		ackCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := msg.DoubleAck(ackCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt == 2 {
			break
		}

		delay := time.Duration(attempt+1) * 100 * time.Millisecond
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := msg.Ack(); err == nil {
		return nil
	} else if lastErr != nil {
		return errors.Join(lastErr, err)
	}
	return lastErr
}

// ---------------------------------------------------------------------------
// DLQ
// ---------------------------------------------------------------------------

func (w *Worker) publishScrapeDLQ(ctx context.Context, env msgEnvelope, reason string) error {
	payload := map[string]any{
		"reason":      reason,
		"at":          time.Now().Format(time.RFC3339),
		"subject":     env.msg.Subject(),
		"payload_b64": base64.StdEncoding.EncodeToString(env.msg.Data()),
		"request_id":  env.req.RequestID,
		"artist_id":   env.req.ArtistID,
		"spotify_id":  env.req.SpotifyID,
	}
	if env.meta != nil {
		payload["num_delivered"] = env.meta.NumDelivered
		payload["stream_seq"] = env.meta.Sequence.Stream
		payload["consumer_seq"] = env.meta.Sequence.Consumer
		payload["stream"] = env.meta.Stream
		payload["consumer"] = env.meta.Consumer
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal DLQ envelope: %w", err)
	}

	pubCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := w.js.Publish(pubCtx, messaging.SubjectScrapeDLQ, data); err != nil {
		return fmt.Errorf("publish DLQ message: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Core processing
// ---------------------------------------------------------------------------

func (w *Worker) processRequest(ctx context.Context, env msgEnvelope, provider spotify.Provider) error {
	startedAt := time.Now()
	req := env.req

	if w.isRequestAlreadySucceeded(req.RequestID) {
		log.Printf("[worker] Ignoring stale redelivery for already-succeeded request_id=%s", req.RequestID)
		return errRequestAlreadySucceeded
	}

	w.logProcessingStart(req, env.meta, env.label)
	w.setScrapeJobProcessing(req.RequestID)

	if err := w.updateArtistStatus(ctx, req.ArtistID, "pending"); err != nil {
		return fmt.Errorf("set pending: %w", err)
	}

	if w.fetcher == nil {
		return w.handleFetcherUnavailable(ctx, req)
	}

	listeners, err := w.fetchListeners(ctx, req, provider, env.label)
	if err != nil {
		return err
	}

	if w.isRequestAlreadySucceeded(req.RequestID) {
		log.Printf("[worker] Ignoring stale duplicate completion for request_id=%s", req.RequestID)
		return errRequestAlreadySucceeded
	}

	return w.persistListeners(req, env.label, listeners, startedAt)
}

// logProcessingStart logs the standard processing start message.
func (w *Worker) logProcessingStart(req messaging.ScrapeRequested, meta *jetstream.MsgMetadata, label string) {
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
}

// handleFetcherUnavailable marks the artist as failed when no fetcher is configured.
func (w *Worker) handleFetcherUnavailable(ctx context.Context, req messaging.ScrapeRequested) error {
	log.Printf("[worker] Fetcher not available, marking as failed")
	if err := w.updateArtistStatus(ctx, req.ArtistID, "failed"); err != nil {
		return fmt.Errorf("set failed: %w", err)
	}
	w.setScrapeJobFinished(req.RequestID, "failed", "fetcher_unavailable")
	return errTerminalFailure
}

// fetchListeners executes the fetch with a provider-appropriate timeout and
// handles quota/rate-limit errors that should not mark the artist as failed.
func (w *Worker) fetchListeners(ctx context.Context, req messaging.ScrapeRequested, provider spotify.Provider, label string) (int, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, w.fetchTimeout(provider))
	defer cancel()

	listeners, err := w.fetcher.FetchOne(fetchCtx, req.SpotifyID, provider)
	if err != nil {
		log.Printf("[worker] Failed to fetch listeners for %s via %s: %v", req.SpotifyID, label, err)

		// On quota exhaustion or rate limiting the message will be NAK-ed and retried by
		// another provider, so keep the artist as "pending" and the scrape
		// job as "processing" — don't mark anything as permanently failed.
		if errors.Is(err, spotify.ErrQuotaExhausted) || errors.Is(err, spotify.ErrRateLimited) {
			return 0, fmt.Errorf("fetch failed for %s via %s: %w", req.SpotifyID, label, err)
		}

		if w.isRequestAlreadySucceeded(req.RequestID) {
			log.Printf("[worker] Ignoring stale fetch error for already-succeeded request_id=%s", req.RequestID)
			return 0, errRequestAlreadySucceeded
		}

		return 0, fmt.Errorf("fetch failed for %s via %s: %w", req.SpotifyID, label, err)
	}
	return listeners, nil
}

// persistListeners writes the fetched listener count to PocketBase and marks
// the scrape job as succeeded.
func (w *Worker) persistListeners(req messaging.ScrapeRequested, label string, listeners int, startedAt time.Time) error {
	persistCtx, persistCancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer persistCancel()

	if err := w.updateArtistListeners(persistCtx, req.ArtistID, listeners); err != nil {
		return fmt.Errorf("update listeners: %w", err)
	}

	log.Printf("[worker] Successfully updated %s with %d monthly listeners via %s", req.ArtistName, listeners, label)
	w.setScrapeJobFinished(req.RequestID, "succeeded", "")
	w.markRequestSucceeded(req.RequestID)
	w.recordSucceeded(label, time.Since(startedAt))
	w.queueTotalSongsRecalc(req.ArtistID)
	w.clearFailedJobsForArtist(persistCtx, req.ArtistID, req.RequestID)
	return nil
}
