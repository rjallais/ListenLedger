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

func (w *Worker) handleMsg(ctx context.Context, item inflightMsg, provider spotify.Provider, label string) msgResult {
	msg := item.msg
	meta := item.meta
	req := item.req
	delivered := uint64(0)
	if meta != nil {
		delivered = meta.NumDelivered
	}
	w.recordAttempt(label, delivered)

	item.dispatchProgress.Stop()

	if w.isRequestAlreadySucceeded(req.RequestID) {
		w.recordDedupSkipped(label)
		log.Printf("[worker] Skipping already-succeeded request_id=%s (provider=%s)", req.RequestID, label)
		if err := w.ackMsg(ctx, msg); err != nil {
			w.recordAckError(label)
			log.Printf("[worker] Failed to ack deduplicated message: %v", err)
		}
		return msgOK
	}

	if provider == spotify.ProviderLocalHeadless {
		if park, attempts, retryable := w.shouldParkLocalPool(label); park {
			log.Printf(
				"[worker] Local provider unhealthy (attempts=%d retryable=%d succeeded=0) — parking local provider pool for this run",
				attempts,
				retryable,
			)
			if nakErr := msg.NakWithDelay(3 * time.Second); nakErr != nil {
				log.Printf("[worker] Failed to NAK message while parking local provider pool: %v", nakErr)
			}
			return msgQuotaExpired
		}
	}

	// --- Pre-flight quota guard for Apify ---
	// Check the Apify /limits endpoint before launching an expensive Actor run.
	// If budget or memory is insufficient the message is NAK-ed immediately so
	// another provider (or a future restart) can handle it, saving credits and
	// avoiding a guaranteed HTTP 402.
	if provider == spotify.ProviderApify {
		checkCtx, checkCancel := context.WithTimeout(ctx, 5*time.Second)
		info := w.quota.CheckApify(checkCtx)
		checkCancel()
		if !info.Available {
			log.Printf("[worker] Apify pre-flight check failed for %s: %s — NAK-ing message", req.ArtistID, info.Error)
			w.recordQuotaExhausted(label)
			if nakErr := msg.Nak(); nakErr != nil {
				log.Printf("[worker] Failed to NAK message on Apify pre-flight: %v", nakErr)
			}
			return msgQuotaExpired
		}
	}

	// Start an InProgress heartbeat for this provider's processing time.
	progress := newProgressHandle()
	go w.inProgressLoop(msg, progress.done)
	defer progress.Stop()

	if err := w.processRequest(ctx, req, meta, provider, label); err != nil {
		progress.Stop()

		if errors.Is(err, errRequestAlreadySucceeded) {
			w.recordDedupSkipped(label)
			if ackErr := w.ackMsg(ctx, msg); ackErr != nil {
				w.recordAckError(label)
				log.Printf("[worker] Failed to ack deduplicated message: %v", ackErr)
			}
			return msgOK
		}
		if errors.Is(err, errTerminalFailure) {
			w.recordTerminalFailure(label)
			if ackErr := w.ackMsg(ctx, msg); ackErr != nil {
				w.recordAckError(label)
				log.Printf("[worker] Failed to ack terminal-failed message: %v", ackErr)
			}
			return msgOK
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx.Err() != nil && errors.Is(err, ctx.Err())) {
			log.Printf("[worker] Context cancelled while processing %s via %s: %v", req.ArtistID, label, err)
			if nakErr := msg.Nak(); nakErr != nil {
				log.Printf("[worker] Failed to NAK message after context cancellation: %v", nakErr)
			}
			return msgOK
		}

		// --- Quota exhaustion: NAK and signal the caller to stop. ---
		if errors.Is(err, spotify.ErrQuotaExhausted) {
			w.recordQuotaExhausted(label)
			log.Printf("[worker] Quota exhausted for provider %s while processing %s: %v", label, req.ArtistID, err)
			// NAK without delay so the message is immediately available for
			// another provider's goroutine.
			if nakErr := msg.Nak(); nakErr != nil {
				log.Printf("[worker] Failed to NAK message on quota exhaustion: %v", nakErr)
			}
			return msgQuotaExpired
		}

		if errors.Is(err, spotify.ErrRateLimited) {
			w.recordRateLimited(label)
			delay := w.retryDelay(meta)
			if retryAfter, ok := spotify.RetryAfter(err); ok && retryAfter > delay {
				delay = retryAfter
			}
			delay = withJitter(delay, min(2*time.Second, delay/4))
			log.Printf("[worker] Provider %s rate-limited for %s; delaying redelivery by %s", label, req.ArtistID, delay.Round(time.Millisecond))
			if nakErr := msg.NakWithDelay(delay); nakErr != nil {
				log.Printf("[worker] Failed to NAK message on rate limit: %v", nakErr)
			}
			return msgOK
		}

		w.recordRetryableError(label, err)
		log.Printf("[worker] Retryable error processing %s (provider=%s): %v", req.ArtistID, label, err)

		// If we've exhausted retries, dead-letter the message.
		if meta != nil && int(meta.NumDelivered) >= w.maxDeliver {
			if dlqErr := w.publishScrapeDLQ(ctx, msg, meta, &req, "retry_exhausted: "+err.Error()); dlqErr != nil {
				log.Printf("[worker] Failed to publish retry-exhausted message to DLQ: %v", dlqErr)
				if nakErr := msg.Nak(); nakErr != nil {
					log.Printf("[worker] Failed to NAK retry-exhausted message after DLQ publish failure: %v", nakErr)
				}
				return msgOK
			}
			w.recordDLQ(label)
			w.setScrapeJobFinished(req.RequestID, "failed", "retry_exhausted")
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

	progress.Stop()
	if err := w.ackMsg(ctx, msg); err != nil {
		w.recordAckError(label)
		log.Printf("[worker] Failed to ack message: %v", err)
	}
	return msgOK
}

func (w *Worker) fetchTimeout(provider spotify.Provider) time.Duration {
	switch provider {
	case spotify.ProviderApify:
		if w.cfg.RequestTimeout > 350*time.Second {
			return w.cfg.RequestTimeout
		}
		return 350 * time.Second
	case spotify.ProviderScrapingAnt:
		if w.cfg.RequestTimeout > 180*time.Second {
			return w.cfg.RequestTimeout
		}
		return 180 * time.Second
	case spotify.ProviderScraperAPI:
		if w.cfg.RequestTimeout > 180*time.Second {
			return w.cfg.RequestTimeout
		}
		return 180 * time.Second
	case spotify.ProviderBrowserless:
		if w.cfg.RequestTimeout > 30*time.Second {
			return w.cfg.RequestTimeout
		}
		return 30 * time.Second
	case spotify.ProviderLocalHeadless:
		if w.cfg.RequestTimeout > 300*time.Second {
			return w.cfg.RequestTimeout
		}
		return 300 * time.Second
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

func (w *Worker) publishScrapeDLQ(ctx context.Context, msg jetstream.Msg, meta *jetstream.MsgMetadata, req *messaging.ScrapeRequested, reason string) error {
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
		return fmt.Errorf("marshal DLQ envelope: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := w.js.Publish(ctx, messaging.SubjectScrapeDLQ, data); err != nil {
		return fmt.Errorf("publish DLQ message: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Core processing
// ---------------------------------------------------------------------------

func (w *Worker) processRequest(ctx context.Context, req messaging.ScrapeRequested, meta *jetstream.MsgMetadata, provider spotify.Provider, label string) error {
	startedAt := time.Now()

	if w.isRequestAlreadySucceeded(req.RequestID) {
		log.Printf("[worker] Ignoring stale redelivery for already-succeeded request_id=%s", req.RequestID)
		return errRequestAlreadySucceeded
	}

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
	if err := w.updateArtistStatus(ctx, req.ArtistID, "pending"); err != nil {
		return fmt.Errorf("set pending: %w", err)
	}

	// Check if fetcher is available.
	if w.fetcher == nil {
		log.Printf("[worker] Fetcher not available, marking as failed")
		if err := w.updateArtistStatus(ctx, req.ArtistID, "failed"); err != nil {
			return fmt.Errorf("set failed: %w", err)
		}
		w.setScrapeJobFinished(req.RequestID, "failed", "fetcher_unavailable")
		return errTerminalFailure
	}

	// Fetch the listener count using the specific provider assigned to this goroutine.
	ctx, cancel := context.WithTimeout(ctx, w.fetchTimeout(provider))
	defer cancel()

	listeners, err := w.fetcher.FetchOne(ctx, req.SpotifyID, provider)
	if err != nil {
		log.Printf("[worker] Failed to fetch listeners for %s via %s: %v", req.SpotifyID, label, err)

		// On quota exhaustion or rate limiting the message will be NAK-ed and retried by
		// another provider, so keep the artist as "pending" and the scrape
		// job as "processing" — don't mark anything as permanently failed.
		if errors.Is(err, spotify.ErrQuotaExhausted) || errors.Is(err, spotify.ErrRateLimited) {
			return fmt.Errorf("fetch failed for %s via %s: %w", req.SpotifyID, label, err)
		}

		if w.isRequestAlreadySucceeded(req.RequestID) {
			log.Printf("[worker] Ignoring stale fetch error for already-succeeded request_id=%s", req.RequestID)
			return errRequestAlreadySucceeded
		}

		return fmt.Errorf("fetch failed for %s via %s: %w", req.SpotifyID, label, err)
	}

	if w.isRequestAlreadySucceeded(req.RequestID) {
		log.Printf("[worker] Ignoring stale duplicate completion for request_id=%s", req.RequestID)
		return errRequestAlreadySucceeded
	}

	// Update the artist record with new listener count.
	if err := w.updateArtistListeners(ctx, req.ArtistID, listeners); err != nil {
		return fmt.Errorf("update listeners: %w", err)
	}

	log.Printf("[worker] Successfully updated %s with %d monthly listeners via %s", req.ArtistName, listeners, label)
	w.setScrapeJobFinished(req.RequestID, "succeeded", "")
	w.markRequestSucceeded(req.RequestID)
	w.recordSucceeded(label, time.Since(startedAt))
	w.queueTotalSongsRecalc(req.ArtistID)
	w.clearFailedJobsForArtist(req.ArtistID, req.RequestID)
	return nil
}
