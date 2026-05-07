//go:build goexperiment.jsonv2

package worker

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/messaging"
	"ListenLedger/internal/spotify"
)

func normalizeConcurrency(concurrency int) int {
	if concurrency <= 0 {
		return 1
	}
	return concurrency
}

func appendProviderSlot(slots []providerSlot, enabled bool, provider spotify.Provider, concurrency int) []providerSlot {
	if !enabled {
		return slots
	}
	return append(slots, providerSlot{
		provider:    provider,
		concurrency: normalizeConcurrency(concurrency),
	})
}

// providerSlots returns one entry per enabled provider with its concurrency.
func (w *Worker) providerSlots() []providerSlot {
	slots := make([]providerSlot, 0, 6)
	slots = appendProviderSlot(slots, w.cfg.HasLocalHeadless(), spotify.ProviderLocalHeadless, w.cfg.LocalConcurrency)
	slots = appendProviderSlot(slots, w.cfg.HasBrowserless(), spotify.ProviderBrowserless, w.cfg.BrowserlessConcurrency)
	slots = appendProviderSlot(slots, w.cfg.HasScrapingAnt(), spotify.ProviderScrapingAnt, w.cfg.MaxConcurrency)
	slots = appendProviderSlot(slots, w.cfg.HasScraperAPI(), spotify.ProviderScraperAPI, w.cfg.ScraperAPIConcurrency)
	// Keep Apify provider processing single-flight at the worker level.
	slots = appendProviderSlot(slots, w.cfg.HasApify(), spotify.ProviderApify, 1)
	slots = appendProviderSlot(slots, w.cfg.HasLocalBrowserless(), spotify.ProviderLocalBrowserless, w.cfg.LocalBrowserlessConcurrency)
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

// newProviderGroup creates a providerGroup with a context derived from the
// worker's root context and registers it in w.groups.
func (w *Worker) newProviderGroup(provider spotify.Provider, initialAlive int) *providerGroup {
	ctx, cancel := context.WithCancel(w.ctx)
	g := &providerGroup{
		ctx:      ctx,
		cancel:   cancel,
		provider: provider,
		label:    providerLabel(provider),
		dead:     make(chan struct{}),
	}
	if initialAlive > 0 {
		g.alive.Add(initialAlive)
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
	log.Printf("[worker] All provider groups have exited - draining NATS consumer")
	w.drainOnce.Do(func() {
		w.dispatchMu.Lock()
		defer w.dispatchMu.Unlock()

		w.accepting.Store(false)
		if w.consume != nil {
			w.consume.Drain()
		}
		close(w.allGroupsDead)
		w.dispatching.Wait()
		w.rejectQueuedWork()
		w.closeWork()
	})
}

// dispatchToChannel is the NATS Consume callback. It parses the message and
// pushes it into the work channel for provider goroutines to pick up.
func (w *Worker) dispatchToChannel(msg jetstream.Msg) {
	meta, _ := msg.Metadata()

	req, ok := w.parseDispatchRequest(w.ctx, msg, meta)
	if !ok {
		return
	}

	// Keep heartbeats running while the message is queued in the channel.
	progress := newProgressHandle()
	go w.inProgressLoop(msg, progress.done)

	if !w.startDispatch(msg, progress) {
		return
	}
	defer w.dispatching.Done()

	select {
	case w.work <- inflightMsg{msg: msg, dispatchProgress: progress, meta: meta, req: req}:
		// The provider goroutine will stop this heartbeat once it starts
		// processing the message.
	case <-w.allGroupsDead:
		progress.Stop()
		// Every provider group is dead (quota exhaustion everywhere).
		// NAK so NATS can redeliver after a restart.
		w.nakDispatchMessage(msg, "all providers exhausted")
	case <-w.ctx.Done():
		progress.Stop()
		// Worker shutting down — NAK so NATS redelivers.
		w.nakDispatchMessage(msg, "on shutdown")
	}
}

func (w *Worker) parseDispatchRequest(ctx context.Context, msg jetstream.Msg, meta *jetstream.MsgMetadata) (messaging.ScrapeRequested, bool) {
	req, err := messaging.UnmarshalScrapeRequested(msg.Data())
	if err == nil {
		return req, true
	}

	log.Printf("[worker] Failed to unmarshal request (terminating): %v", err)
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 3*time.Second)
	defer dlqCancel()
	if dlqErr := w.publishScrapeDLQ(dlqCtx, msgEnvelope{msg: msg, meta: meta}, "unmarshal_error"); dlqErr != nil {
		log.Printf("[worker] Failed to publish poison message to DLQ: %v", dlqErr)
		if nakErr := msg.Nak(); nakErr != nil {
			log.Printf("[worker] Failed to NAK poison message after DLQ publish failure: %v", nakErr)
		}
		return messaging.ScrapeRequested{}, false
	}
	if termErr := msg.Term(); termErr != nil {
		log.Printf("[worker] Failed to terminate poison message: %v", termErr)
	}
	return messaging.ScrapeRequested{}, false
}

func (w *Worker) startDispatch(msg jetstream.Msg, progress *progressHandle) bool {
	w.dispatchMu.Lock()
	if !w.accepting.Load() {
		w.dispatchMu.Unlock()
		progress.Stop()
		w.nakDispatchMessage(msg, "after dispatch shutdown")
		return false
	}

	w.dispatching.Add(1)
	if !w.accepting.Load() {
		w.dispatching.Done()
		w.dispatchMu.Unlock()
		progress.Stop()
		w.nakDispatchMessage(msg, "after dispatch shutdown")
		return false
	}
	w.dispatchMu.Unlock()
	return true
}

func (w *Worker) nakDispatchMessage(msg jetstream.Msg, reason string) {
	if nakErr := msg.Nak(); nakErr != nil {
		log.Printf("[worker] Failed to NAK message %s: %v", reason, nakErr)
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

			result := w.handleMsg(g.ctx, item, g.provider, g.label)
			if result == msgQuotaExpired {
				log.Printf("[worker] Provider %s slot %d: quota exhausted, shutting down provider pool", g.label, slot)
				// Cancel the group context so sibling goroutines exit too.
				g.cancel()
				return
			}
		}
	}
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
	case spotify.ProviderLocalBrowserless:
		return messaging.ScrapeProviderLocalBrowserless
	default:
		return messaging.ScrapeProviderAny
	}
}
