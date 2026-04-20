//go:build goexperiment.jsonv2

package worker

import (
	"context"
	"log"

	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/messaging"
	"ListenLedger/internal/spotify"
)

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
		conc := w.cfg.BrowserlessConcurrency
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
		// Keep Apify provider processing single-flight at the worker level.
		slots = append(slots, providerSlot{provider: spotify.ProviderApify, concurrency: 1})
	}
	if w.cfg.HasLocalBrowserless() {
		conc := w.cfg.LocalBrowserlessConcurrency
		if conc <= 0 {
			conc = 1
		}
		slots = append(slots, providerSlot{provider: spotify.ProviderLocalBrowserless, concurrency: conc})
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

	req, err := messaging.UnmarshalScrapeRequested(msg.Data())
	if err != nil {
		log.Printf("[worker] Failed to unmarshal request (terminating): %v", err)
		if dlqErr := w.publishScrapeDLQ(w.ctx, msgEnvelope{msg: msg, meta: meta}, "unmarshal_error"); dlqErr != nil {
			log.Printf("[worker] Failed to publish poison message to DLQ: %v", dlqErr)
			if nakErr := msg.Nak(); nakErr != nil {
				log.Printf("[worker] Failed to NAK poison message after DLQ publish failure: %v", nakErr)
			}
			return
		}
		if termErr := msg.Term(); termErr != nil {
			log.Printf("[worker] Failed to terminate poison message: %v", termErr)
		}
		return
	}

	// Keep heartbeats running while the message is queued in the channel.
	progress := newProgressHandle()
	go w.inProgressLoop(msg, progress.done)

	if !w.accepting.Load() {
		progress.Stop()
		if nakErr := msg.Nak(); nakErr != nil {
			log.Printf("[worker] Failed to NAK message after dispatch shutdown: %v", nakErr)
		}
		return
	}

	w.dispatching.Add(1)
	defer w.dispatching.Done()

	if !w.accepting.Load() {
		progress.Stop()
		if nakErr := msg.Nak(); nakErr != nil {
			log.Printf("[worker] Failed to NAK message after dispatch shutdown: %v", nakErr)
		}
		return
	}

	select {
	case w.work <- inflightMsg{msg: msg, dispatchProgress: progress, meta: meta, req: req}:
		// The provider goroutine will stop this heartbeat once it starts
		// processing the message.
	case <-w.allGroupsDead:
		progress.Stop()
		// Every provider group is dead (quota exhaustion everywhere).
		// NAK so NATS can redeliver after a restart.
		if nakErr := msg.Nak(); nakErr != nil {
			log.Printf("[worker] Failed to NAK message (all providers exhausted): %v", nakErr)
		}
	case <-w.ctx.Done():
		progress.Stop()
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
