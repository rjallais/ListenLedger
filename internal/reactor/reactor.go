// Package reactor runs durable JetStream consumers over DOMAIN_EVENTS with
// an explicit delivery contract. A handler returns nil to ack,
// ErrUnprocessable to terminate a poison message (it stays in the stream for
// replay and audit, but is dropped from delivery), or any other error to nak
// with a redelivery delay. Consumer configuration defaults are applied at
// this boundary so an omitted field never silently inherits a surprising
// JetStream server default.
package reactor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
)

// ErrUnprocessable marks an event no retry can succeed on (permanently
// undecodable payload, schema this binary cannot ever read). Terming keeps
// the fact durable in the stream while unblocking delivery.
var ErrUnprocessable = errors.New("reactor: event is unprocessable")

// ErrRetryLater marks an expected retryable deferral (e.g. young lag awaiting
// inline projection completion). NAKs with the configured NakDelay and logs at
// debug level instead of warning level.
var ErrRetryLater = errors.New("reactor: retry later")

// Handler processes one domain event. Returning nil acks; returning
// ErrUnprocessable terms; any other error naks with the configured delay.
type Handler func(ctx context.Context, evt eventsourcing.Event) error

// Consumer defaults: serial delivery keeps per-stream ordering (read-side
// repair relies on it), unlimited redelivery pairs with term-not-max-deliver
// for poison, and the backoff schedule keeps failed handlers from spinning.
const (
	defaultMaxAckPending = 1
	defaultAckWait       = 30 * time.Second
	defaultMaxDeliver    = -1
	defaultNakDelay      = 5 * time.Second
)

var defaultBackOff = []time.Duration{
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	time.Minute,
}

// Config is the desired state of one durable consumer over DOMAIN_EVENTS.
type Config struct {
	// Name is the durable consumer name: it IS the resume position, so it
	// must stay stable across releases.
	Name string
	// Description lands in consumer metadata for nats CLI inspection.
	Description string
	// Filters are JetStream filter subjects (e.g. domain.events.artist.>).
	// Empty matches the whole stream.
	Filters []string
	// MaxAckPending bounds in-flight messages; 1 (default) preserves strict
	// ordering.
	MaxAckPending int
	// MaxDeliver bounds redeliveries; 0 means unlimited (default).
	MaxDeliver int
	// AckWait bounds one delivery attempt before the server redelivers.
	AckWait time.Duration
	// NakDelay spaces redeliveries after ErrRetryLater deferrals (and acts as
	// the single-step fallback when BackOff is unset).
	NakDelay time.Duration
	// BackOff spaces redeliveries after unexpected handler errors, indexed by
	// delivery attempt (meta.NumDelivered-1, clamped to the last entry).
	// Applied explicitly via msg.NakWithDelay because JetStream's consumer
	// BackOff only applies to AckWait timeouts, not explicit Nak() calls.
	BackOff []time.Duration
}

func (c Config) applyDefaults() Config {
	if c.MaxAckPending <= 0 {
		c.MaxAckPending = defaultMaxAckPending
	}
	if c.MaxDeliver == 0 {
		c.MaxDeliver = defaultMaxDeliver
	}
	if c.AckWait <= 0 {
		c.AckWait = defaultAckWait
	}
	customNakDelay := c.NakDelay > 0
	if !customNakDelay {
		c.NakDelay = defaultNakDelay
	}
	if len(c.BackOff) == 0 {
		if customNakDelay {
			c.BackOff = []time.Duration{c.NakDelay}
		} else {
			c.BackOff = slices.Clone(defaultBackOff)
		}
	}
	return c
}

func (c Config) errorNakDelay(numDelivered uint64) time.Duration {
	if len(c.BackOff) == 0 {
		return c.NakDelay
	}
	idx := 0
	if numDelivered > 1 {
		idx = min(int(numDelivered-1), len(c.BackOff)-1)
	}
	if d := c.BackOff[idx]; d > 0 {
		return d
	}
	return c.NakDelay
}

// Reactor binds one durable consumer and processes events until stopped.
type Reactor struct {
	log     *slog.Logger
	js      jetstream.JetStream
	cfg     Config
	handler Handler
}

// New validates the wiring and returns a reactor ready to Run.
func New(log *slog.Logger, js jetstream.JetStream, cfg Config, handler Handler) (*Reactor, error) {
	cfg = cfg.applyDefaults()
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, fmt.Errorf("reactor: config name is required")
	}
	if handler == nil {
		return nil, fmt.Errorf("reactor %s: handler is required", cfg.Name)
	}
	if js == nil {
		return nil, fmt.Errorf("reactor %s: JetStream not configured", cfg.Name)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Reactor{log: log, js: js, cfg: cfg, handler: handler}, nil
}

// Run consumes DOMAIN_EVENTS until ctx is done, then drains in-flight
// messages. It returns an error only for consumer wiring failures at startup;
// per-event failures nak and redeliver, and undecodable messages term.
func (r *Reactor) Run(ctx context.Context) error {
	consumer, err := r.ensureConsumer(ctx)
	if err != nil {
		return err
	}

	consume, err := consumer.Consume(func(msg jetstream.Msg) {
		r.handle(ctx, msg)
	})
	if err != nil {
		return fmt.Errorf("reactor %s: start consume: %w", r.cfg.Name, err)
	}

	<-ctx.Done()
	r.drain(consume)
	return nil
}

// drain stops delivery after in-flight messages complete, bounded so a stuck
// handler cannot hang shutdown.
func (r *Reactor) drain(consume jetstream.ConsumeContext) {
	consume.Drain()
	select {
	case <-consume.Closed():
	case <-time.After(10 * time.Second):
		consume.Stop()
		<-consume.Closed()
	}
}

// ensureConsumer creates or updates the durable consumer on DOMAIN_EVENTS.
func (r *Reactor) ensureConsumer(ctx context.Context) (jetstream.Consumer, error) {
	ensureCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	stream, err := r.js.Stream(ensureCtx, messaging.DomainEventsStreamName)
	if err != nil {
		return nil, fmt.Errorf("reactor %s: get %s stream: %w", r.cfg.Name, messaging.DomainEventsStreamName, err)
	}
	consumer, err := stream.CreateOrUpdateConsumer(ensureCtx, jetstream.ConsumerConfig{
		Durable:        r.cfg.Name,
		Description:    r.cfg.Description,
		FilterSubjects: r.cfg.Filters,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        r.cfg.AckWait,
		MaxDeliver:     r.cfg.MaxDeliver,
		MaxAckPending:  r.cfg.MaxAckPending,
	})
	if err != nil {
		return nil, fmt.Errorf("reactor %s: ensure consumer: %w", r.cfg.Name, err)
	}
	return consumer, nil
}

// handle maps one delivery onto the ack contract. The message body is the
// domain event envelope (RON), published by the outbox relay in sqlite mode
// and by the JetStream store itself in jetstream mode.
func (r *Reactor) handle(ctx context.Context, msg jetstream.Msg) {
	evt, err := eventsourcing.UnmarshalEvent(msg.Data())
	if err != nil {
		// Not a domain event envelope at all: no retry can fix the bytes.
		// The SQLite log keeps the fact; audit still reports it.
		r.log.Error("reactor: undecodable message, terminating delivery",
			"reactor", r.cfg.Name, "subject", msg.Subject(), "error", err)
		_ = msg.Term()
		return
	}

	var numDelivered uint64 = 1
	if meta, metaErr := msg.Metadata(); metaErr == nil && meta != nil {
		evt.GlobalPosition = int64(meta.Sequence.Stream)
		if meta.NumDelivered > 0 {
			numDelivered = meta.NumDelivered
		}
	}

	err = r.handler(ctx, evt)
	switch {
	case err == nil:
		if ackErr := msg.Ack(); ackErr != nil {
			r.log.Warn("reactor: ack failed; message will be redelivered",
				"reactor", r.cfg.Name, "event_id", evt.ID, "error", ackErr)
		}
	case errors.Is(err, ErrUnprocessable):
		r.log.Error("reactor: unprocessable event, terminating delivery",
			"reactor", r.cfg.Name, "event_id", evt.ID, "event_type", evt.EventType, "error", err)
		_ = msg.Term()
	case errors.Is(err, ErrRetryLater):
		r.log.Debug("reactor: handler deferred, redelivering after delay",
			"reactor", r.cfg.Name, "event_id", evt.ID, "event_type", evt.EventType,
			"retry_in", r.cfg.NakDelay, "error", err)
		if nakErr := msg.NakWithDelay(r.cfg.NakDelay); nakErr != nil {
			r.log.Warn("reactor: nak failed", "reactor", r.cfg.Name, "event_id", evt.ID, "error", nakErr)
		}
	default:
		delay := r.cfg.errorNakDelay(numDelivered)
		r.log.Warn("reactor: handler failed, redelivering after delay",
			"reactor", r.cfg.Name, "event_id", evt.ID, "event_type", evt.EventType,
			"attempt", numDelivered, "retry_in", delay, "error", err)
		if nakErr := msg.NakWithDelay(delay); nakErr != nil {
			r.log.Warn("reactor: nak failed", "reactor", r.cfg.Name, "event_id", evt.ID, "error", nakErr)
		}
	}
}
