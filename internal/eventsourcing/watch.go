package eventsourcing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/messaging"
)

// WatchOption configures optional filtering and lifecycle signals for Store.Watch.
type WatchOption func(*watchConfig)

type eventFilter struct {
	streamType string
	streamID   string
	eventType  string
}

func (f eventFilter) matches(evt Event) bool {
	if f.streamType != "*" && f.streamType != evt.StreamType {
		return false
	}
	if f.streamID != "*" && f.streamID != evt.StreamID {
		return false
	}
	if f.eventType != "*" && f.eventType != evt.EventType {
		return false
	}
	return true
}

type watchConfig struct {
	rawPatterns []string
	filters     []eventFilter
	catchUp     chan<- struct{}
}

// WithWatchFilters restricts Watch delivery to events matching at least one
// pattern in Rita's 1-, 2-, or 3-token grammar:
//   - "<stream-type>"                        (e.g. "artist")
//   - "<stream-type>.<stream-id>"            (e.g. "artist.ar_123")
//   - "<stream-type>.<stream-id>.<event>"    (e.g. "artist.*.ArtistCreated")
//
// Omitted trailing tokens default to "*".
func WithWatchFilters(patterns ...string) WatchOption {
	return func(cfg *watchConfig) {
		cfg.rawPatterns = append(cfg.rawPatterns, patterns...)
	}
}

// WithCatchUpSignal closes the provided channel once the watcher has delivered
// all historical backlog present when Watch started (Rita-style catch-up barrier).
func WithCatchUpSignal(done chan<- struct{}) WatchOption {
	return func(cfg *watchConfig) {
		cfg.catchUp = done
	}
}

func buildWatchConfig(opts []WatchOption) (watchConfig, error) {
	var cfg watchConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	for _, raw := range cfg.rawPatterns {
		f, err := parseWatchFilter(raw)
		if err != nil {
			return watchConfig{}, err
		}
		cfg.filters = append(cfg.filters, f)
	}
	return cfg, nil
}

func parseWatchFilter(pattern string) (eventFilter, error) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return eventFilter{}, fmt.Errorf("watch filter: empty pattern")
	}
	if after, ok := strings.CutPrefix(pattern, messaging.SubjectDomainEventsPrefix+"."); ok {
		pattern = after
	}
	if pattern == ">" || pattern == "*" {
		return eventFilter{streamType: "*", streamID: "*", eventType: "*"}, nil
	}
	parts := strings.Split(pattern, ".")
	if len(parts) > 3 {
		return eventFilter{}, fmt.Errorf("watch filter %q: expected at most 3 tokens (<type>.<id>.<event>)", pattern)
	}
	tokens := [3]string{"*", "*", "*"}
	for i, p := range parts {
		if p == "" {
			return eventFilter{}, fmt.Errorf("watch filter %q: empty token", pattern)
		}
		if p == ">" {
			if i != len(parts)-1 {
				return eventFilter{}, fmt.Errorf("watch filter %q: '>' wildcard must be last token", pattern)
			}
			break
		}
		tokens[i] = p
	}
	return eventFilter{
		streamType: tokens[0],
		streamID:   tokens[1],
		eventType:  tokens[2],
	}, nil
}

func (cfg watchConfig) matches(evt Event) bool {
	if len(cfg.filters) == 0 {
		return true
	}
	for _, f := range cfg.filters {
		if f.matches(evt) {
			return true
		}
	}
	return false
}

func (cfg watchConfig) subjectFilters() []string {
	if len(cfg.filters) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(cfg.filters))
	out := make([]string, 0, len(cfg.filters))
	for _, f := range cfg.filters {
		subj := messaging.SubjectDomainEventPattern(f.streamType, f.streamID, f.eventType)
		if _, ok := seen[subj]; !ok {
			seen[subj] = struct{}{}
			out = append(out, subj)
		}
	}
	return out
}

// watcher bridges a log tail loop into the Watcher channel contract.
// Events are pushed until Stop cancels the context; the channel closes when
// the loop exits, so callers can range over it and observe termination.
type watcher struct {
	events  chan Event
	stop    func() error
	stopped bool
	mu      sync.Mutex
}

func newWatcher(stop func() error) *watcher {
	return &watcher{events: make(chan Event, 16), stop: stop}
}

func (w *watcher) Events() <-chan Event { return w.events }

func (w *watcher) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return nil
	}
	w.stopped = true
	if w.stop != nil {
		return w.stop()
	}
	return nil
}

// SQLiteWatch polls LoadFromGlobalPosition, so every row written after
// afterPosition is delivered exactly once, oldest first. Polling is cheap at
// current scale (~1800 events/day); the 300ms idle gap trades a few hundred
// milliseconds of tail latency for zero idle database load.
func (s *SQLiteStore) Watch(ctx context.Context, afterPosition int64, opts ...WatchOption) (Watcher, error) {
	if s.db == nil {
		return nil, fmt.Errorf("sqlite store: database not configured")
	}
	wcfg, err := buildWatchConfig(opts)
	if err != nil {
		return nil, err
	}
	if afterPosition < 0 {
		afterPosition = 0
	}

	ctx, cancel := context.WithCancel(ctx)
	w := newWatcher(func() error {
		cancel()
		return nil
	})

	markCaughtUp := sync.OnceFunc(func() {
		if wcfg.catchUp != nil {
			close(wcfg.catchUp)
		}
	})

	go func() {
		defer close(w.events)
		pos := afterPosition
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		firstPoll := true
		for {
			if !firstPoll {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
			firstPoll = false
			batch, err := s.LoadFromGlobalPosition(ctx, pos, 50)
			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					return
				}
				slog.Warn("[sqlite watch] poll failed, retrying", "error", err)
				continue
			}
			for _, evt := range batch {
				if evt.GlobalPosition <= pos {
					continue
				}
				pos = evt.GlobalPosition
				if !wcfg.matches(evt) {
					continue
				}
				select {
				case w.events <- evt:
				case <-ctx.Done():
					return
				}
			}
			if len(batch) < 50 {
				markCaughtUp()
			}
		}
	}()

	return w, nil
}

// JetStreamWatch tails DOMAIN_EVENTS with a dedicated pull consumer starting
// at the next sequence after afterPosition. Each watcher gets its own
// ephemeral durable consumer (name includes a random suffix) that is deleted
// on Stop, so concurrent watchers never share a resume position. The server
// also deletes the consumer after InactiveThreshold with no pull requests
// (e.g. a reader blocked on the events channel); the watcher then closes its
// channel instead of retrying forever, so callers observe the close and
// resume with Watch(ctx, lastPos).
func (s *JetStreamStore) Watch(ctx context.Context, afterPosition int64, opts ...WatchOption) (Watcher, error) {
	if s.js == nil {
		return nil, fmt.Errorf("jetstream store: JetStream not configured")
	}
	wcfg, err := buildWatchConfig(opts)
	if err != nil {
		return nil, err
	}
	if afterPosition < 0 {
		afterPosition = 0
	}
	ctx, cancel := context.WithCancel(ctx)
	w := newWatcher(func() error {
		cancel()
		return nil
	})

	markCaughtUp := sync.OnceFunc(func() {
		if wcfg.catchUp != nil {
			close(wcfg.catchUp)
		}
	})

	stream, err := s.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("jetstream watch: stream lookup: %w", err)
	}
	// NATS consumer names must not contain '.', '*', '>' or whitespace.
	// A random UUID keeps concurrent watchers on separate consumers.
	name := "watch_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	ccfg := jetstream.ConsumerConfig{
		Durable:           name,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: 30 * time.Second,
		MemoryStorage:     true,
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       uint64(afterPosition + 1),
	}
	// Multiple patterns may overlap, and the server rejects overlapping
	// FilterSubjects. wcfg.matches already filters on the client side.
	if subjects := wcfg.subjectFilters(); len(subjects) == 1 {
		ccfg.FilterSubject = subjects[0]
	} else {
		ccfg.FilterSubject = messaging.SubjectDomainEventsWildcard
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, ccfg)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("jetstream watch: create consumer: %w", err)
	}

	go func() {
		defer close(w.events)
		defer func() {
			dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()
			_ = stream.DeleteConsumer(dctx, cons.CachedInfo().Name)
		}()

		if cons.CachedInfo().NumPending == 0 {
			markCaughtUp()
		}

		for {
			if err := ctx.Err(); err != nil {
				return
			}
			batch, err := cons.Fetch(50, jetstream.FetchMaxWait(250*time.Millisecond))
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, jetstream.ErrNoMessages) {
					markCaughtUp()
					continue
				}
				if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDeleted) {
					slog.Warn("[jetstream watch] consumer gone, closing watcher", "error", err)
					return
				}
				slog.Warn("[jetstream watch] fetch failed, retrying", "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			gotAny := false
			for msg := range batch.Messages() {
				gotAny = true
				if msg.Data() == nil {
					continue
				}
				evt, err := UnmarshalEvent(msg.Data())
				if err != nil {
					slog.Warn("[jetstream watch] undecodable event, skipping", "error", err)
					continue
				}
				// The serialized envelope does not carry the stream sequence
				// (global_position is omitempty and never set on publish), so
				// fill it from JetStream message metadata: watchers resume
				// from Watch(ctx, lastPos) using this position.
				var numPending uint64
				if meta, err := msg.Metadata(); err == nil && meta != nil {
					evt.GlobalPosition = int64(meta.Sequence.Stream)
					numPending = meta.NumPending
				}
				if !wcfg.matches(evt) {
					if numPending == 0 {
						markCaughtUp()
					}
					continue
				}
				select {
				case w.events <- evt:
				case <-ctx.Done():
					return
				}
				if numPending == 0 {
					markCaughtUp()
				}
			}
			if !gotAny {
				markCaughtUp()
			}
			// Delivery-phase errors surface on the batch, not from Fetch.
			if err := batch.Error(); err != nil && !errors.Is(err, context.Canceled) {
				if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDeleted) {
					slog.Warn("[jetstream watch] consumer gone, closing watcher", "error", err)
					return
				}
				slog.Warn("[jetstream watch] batch delivery failed", "error", err)
			}
		}
	}()

	return w, nil
}
