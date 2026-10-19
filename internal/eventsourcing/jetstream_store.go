// Package eventsourcing JetStream-backed event store.
//
// JetStreamStore implements Store against the DOMAIN_EVENTS JetStream stream
// (Delaney flip: JetStream is the authority, SQLite is disposable). Append
// uses per-aggregate OCC on the stream's subject scope, MsgID=event ID for
// short-window dedup. Load replays one stream's subjects via an ephemeral
// filtered pull consumer; snapshots (jetstream_snapshot.go) let long streams
// replay only the tail. Wired into the runtime via SelectedStore in
// jetstream mode; SQLite remains the default.
package eventsourcing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/messaging"
)

// Load tuning for the filtered replay consumer.
const (
	// loadFetchBatchSize bounds one NoWait drain. Persisted history is
	// already available server-side, so this only sizes round trips, not
	// latency: the loop ends on ErrNoMessages either way.
	loadFetchBatchSize = 100
	// consumerDeleteTimeout bounds ephemeral-consumer cleanup. Kept short
	// deliberately: the delete runs synchronously in Load's defer, so every
	// millisecond here is tail latency on every read. A timeout failure is
	// benign — the 30s inactivity threshold reaps the consumer.
	consumerDeleteTimeout = 2 * time.Second
)

// JetStreamStore is an event store backed by JetStream DOMAIN_EVENTS.
type JetStreamStore struct {
	js jetstream.JetStream
}

// NewJetStreamStore creates a JetStreamStore over an ensured DOMAIN_EVENTS stream.
func NewJetStreamStore(js jetstream.JetStream) *JetStreamStore {
	return &JetStreamStore{js: js}
}

// Compile-time: JetStreamStore satisfies the write-path read contract.
var _ Store = (*JetStreamStore)(nil)

// Append publishes events to their per-type subjects with per-aggregate OCC
// and MsgID dedup on the event ID. A CAS miss surfaces as
// ErrConcurrencyConflict, same contract as SQLite.
//
// OCC is scoped to the aggregate ID across all stream types (wildcard
// domain.events.*.<id>.*), matching the Load replay scope: SQLite keys
// history by stream ID with a type-consistency guard, so the JetStream check
// must too. A lookup of the aggregate's latest persisted event both rejects
// a stale expectedVersion and anchors the publishes with
// WithExpectLastSequenceForSubject, so a concurrent append to this aggregate
// between the reload and the publish fails the batch. Concurrent appends to
// *other* aggregates share nothing in this scope (the old stream-wide anchor
// false-conflicted on those). The wildcard check is server-enforced —
// verified against the pinned server sources, not assumed.
//
// Each publish in a multi-event batch anchors to the preceding publish's
// acknowledgement Sequence, not to base+i: scope sequences are not
// consecutive within one aggregate when other aggregates interleave, so
// base+i would false-conflict after the first event persisted.
//
// Partial-write contract: publishes are per-event (JetStream has no
// multi-publish transaction in this client), so a mid-batch failure can
// leave a prefix persisted. On any error the caller must reload the stream
// and retry only the unwritten tail; already-persisted versions resurface as
// ErrConcurrencyConflict, never as silent duplicates.
func (s *JetStreamStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...Event) error {
	if s.js == nil {
		return fmt.Errorf("jetstream store: JetStream not configured")
	}
	if len(events) == 0 {
		return nil
	}
	// Validate the whole batch before publishing anything: a version skew
	// must fail without mutating the stream. Batch identity matches SQLite
	// (one stream ID, one aggregate type, consecutive versions).
	streamType := events[0].StreamType
	if streamType == "" {
		return fmt.Errorf("jetstream store: event %s has an empty stream type", events[0].ID)
	}
	for i, evt := range events {
		want := expectedVersion + int64(i) + 1
		if evt.Version != want {
			return fmt.Errorf("%w: stream %s event %d has version %d, want %d",
				ErrConcurrencyConflict, streamID, i, evt.Version, want)
		}
		if evt.StreamID != streamID {
			return fmt.Errorf("jetstream store: event %s belongs to stream %s, want %s",
				evt.ID, evt.StreamID, streamID)
		}
		if evt.StreamType != streamType {
			return fmt.Errorf("jetstream store: stream %s has mixed aggregate types %q and %q",
				streamID, streamType, evt.StreamType)
		}
	}
	stream, err := s.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		return fmt.Errorf("jetstream store: stream: %w", err)
	}
	scope := messaging.SubjectDomainEventScope(streamID)
	// Layer 1: reject a stale reader. Reload the aggregate's latest
	// persisted event and compare its version against expectedVersion. Fail
	// closed on read errors — an unreadable log must not accept writes.
	last, err := stream.GetLastMsgForSubject(ctx, scope)
	if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return fmt.Errorf("jetstream store: reload %s: %w", streamID, err)
	}
	var anchor uint64
	var maxVersion int64
	var existingType string
	if err == nil && last != nil {
		lastEvt, err := UnmarshalEvent(last.Data)
		if err != nil {
			return fmt.Errorf("jetstream store: decode last %s: %w", streamID, err)
		}
		anchor = last.Sequence
		maxVersion = lastEvt.Version
		existingType = lastEvt.StreamType
	}
	if maxVersion != expectedVersion {
		return fmt.Errorf("%w: stream %s expected version %d, but found %d",
			ErrConcurrencyConflict, streamID, expectedVersion, maxVersion)
	}
	// Same order as SQLite: version first, then the aggregate-type guard.
	if existingType != "" && existingType != streamType {
		return fmt.Errorf("jetstream store: stream %s has aggregate type %q, cannot append type %q",
			streamID, existingType, streamType)
	}
	// Layer 2: publish under the per-aggregate anchor, advancing it from
	// each acknowledgement. A concurrent append to this aggregate between
	// the reload and now moved its latest sequence, so the publish misses
	// and the batch fails without partial writes from a stale view.
	for _, evt := range events {
		subject := messaging.SubjectDomainEvent(evt.StreamType, evt.StreamID, evt.EventType)
		data, err := evt.Bytes()
		if err != nil {
			return fmt.Errorf("jetstream store: marshal %s: %w", evt.ID, err)
		}
		opts := []jetstream.PublishOpt{
			jetstream.WithMsgID(evt.ID),
			jetstream.WithExpectLastSequenceForSubject(anchor, scope),
		}
		ack, err := s.js.Publish(ctx, subject, data, opts...)
		if err != nil {
			return wrapPublishError(streamID, evt.ID, err)
		}
		if ack != nil {
			anchor = ack.Sequence
		}
	}
	return nil
}

// Load replays one stream oldest-to-newest via an ephemeral filtered pull
// consumer: the server transfers only this aggregate's subjects, so the cost
// is O(stream), not O(log). (The prior GetMsg walk over FirstSeq..LastSeq
// transferred every aggregate on every load.)
//
// The replay is bounded by the stream's LastSeq captured before consuming:
// events published after Load starts (sequence past the cutoff) are skipped
// and stop the replay, so a continuously-written aggregate cannot extend the
// loop until the context expires. Draining uses FetchNoWait — persisted
// history is already available, so there is no reason to burn a MaxWait
// expiry on the short final batch (or on an absent aggregate).
func (s *JetStreamStore) Load(ctx context.Context, streamID string) ([]Event, error) {
	return s.loadSince(ctx, streamID, 0, 0)
}

// loadSince replays the events of one stream with Version > afterVersion,
// oldest to newest. afterSeq, when non-zero, is the DOMAIN_EVENTS sequence of
// the afterVersion event (a validated snapshot anchor): the consumer then
// starts right behind it instead of at the stream's first message, so the
// cost is O(tail) instead of O(stream). afterVersion == 0 replays everything.
func (s *JetStreamStore) loadSince(ctx context.Context, streamID string, afterVersion int64, afterSeq uint64) ([]Event, error) {
	if s.js == nil {
		return nil, fmt.Errorf("jetstream store: JetStream not configured")
	}
	stream, err := s.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		return nil, fmt.Errorf("jetstream store: stream: %w", err)
	}
	// Fast path: inspect the aggregate's tip first via server-side subject
	// index. Empty streams, fully-caught-up tails, and single-event tails
	// (e.g. newly requested scrape jobs) resolve in a single lookup without
	// creating/deleting a consumer.
	last, err := stream.GetLastMsgForSubject(ctx, messaging.SubjectDomainEventScope(streamID))
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			if afterVersion > 0 {
				return nil, fmt.Errorf("jetstream store: stream %s has no events but anchor is v%d", streamID, afterVersion)
			}
			return nil, nil // empty stream: nothing to replay
		}
		return nil, fmt.Errorf("jetstream store: last msg for %s: %w", streamID, err)
	}
	lastEvt, err := UnmarshalEvent(last.Data)
	if err != nil {
		return nil, fmt.Errorf("jetstream store: decode last %s: %w", streamID, err)
	}
	switch {
	case lastEvt.Version < afterVersion:
		// The anchor is ahead of the log (stream rebuilt or truncated):
		// callers must fall back to a full replay, never trust the anchor.
		return nil, fmt.Errorf("jetstream store: stream %s tip is v%d, behind anchor v%d", streamID, lastEvt.Version, afterVersion)
	case lastEvt.Version == afterVersion:
		return nil, nil // anchor is the tip: empty tail
	case lastEvt.Version == afterVersion+1 && lastEvt.StreamID == streamID:
		return []Event{lastEvt}, nil
	}
	cutoff := last.Sequence
	cfg := jetstream.ConsumerConfig{
		FilterSubject:     messaging.SubjectDomainEventFilter(streamID),
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: 30 * time.Second,
		MemoryStorage:     true,
	}
	if afterSeq > 0 {
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = afterSeq + 1
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("jetstream store: consumer for %s: %w", streamID, err)
	}
	// Ephemeral consumer: delete explicitly on a fresh context — the caller's
	// ctx may be canceled by the time Load returns, which would leak the
	// consumer until the inactivity threshold reaps it. The inactivity
	// threshold remains as backstop if this delete fails.
	defer func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), consumerDeleteTimeout)
		defer cancel()
		_ = stream.DeleteConsumer(dctx, cons.CachedInfo().Name)
	}()
	out := make([]Event, 0, lastEvt.Version-afterVersion)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := cons.FetchNoWait(loadFetchBatchSize)
		if err != nil {
			if errors.Is(err, jetstream.ErrNoMessages) {
				break // caught up: no more matching messages
			}
			return nil, fmt.Errorf("jetstream store: fetch %s: %w", streamID, err)
		}
		reachedCutoff := false
		got := 0
		for msg := range batch.Messages() {
			meta, err := msg.Metadata()
			if err != nil {
				return nil, fmt.Errorf("jetstream store: metadata %s: %w", streamID, err)
			}
			if meta.Sequence.Stream > cutoff {
				reachedCutoff = true // live arrival: stop, don't error
				continue
			}
			evt, err := UnmarshalEvent(msg.Data())
			if err != nil {
				return nil, fmt.Errorf("jetstream store: decode %s: %w", streamID, err)
			}
			if evt.StreamID != streamID {
				continue // defensive: filter is exact, trust the data
			}
			if evt.Version > afterVersion {
				out = append(out, evt)
			}
			got++
			if meta.Sequence.Stream == cutoff {
				reachedCutoff = true
			}
		}
		if err := batch.Error(); err != nil {
			if errors.Is(err, jetstream.ErrNoMessages) {
				break
			}
			return nil, fmt.Errorf("jetstream store: batch %s: %w", streamID, err)
		}
		if reachedCutoff || got == 0 {
			break // drained up to the aggregate's tip sequence
		}
	}
	return out, nil
}

// wrapPublishError reports a failed publish for one event. Version races
// wrap ErrConcurrencyConflict; deterministic server rejections (APIError:
// stream full, message too large, bad subject) pass through plainly since
// nothing persisted; transport failures (timeout, lost ack, disconnect: no
// APIError) wrap ErrAppendIndeterminate so AppendWithRetry reconciles the
// unknown outcome instead of assuming success or failure.
func wrapPublishError(streamID, eventID string, err error) error {
	if isWrongLastSeq(err) {
		return fmt.Errorf("%w: stream %s: %w", ErrConcurrencyConflict, streamID, err)
	}
	var jsErr jetstream.JetStreamError
	if errors.As(err, &jsErr) && jsErr.APIError() != nil {
		return fmt.Errorf("jetstream store: publish %s: %w", eventID, err)
	}
	return fmt.Errorf("%w: jetstream store: publish %s: %w", ErrAppendIndeterminate, eventID, err)
}

// isWrongLastSeq reports a JetStream CAS miss (both single and clustered codes).
func isWrongLastSeq(err error) bool {
	var jsErr jetstream.JetStreamError
	if !errors.As(err, &jsErr) {
		return false
	}
	apiErr := jsErr.APIError()
	if apiErr == nil {
		return false
	}
	return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant
}
