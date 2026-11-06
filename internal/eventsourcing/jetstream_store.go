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
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"

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
	// maxAtomicBatchMsgs caps the number of messages in a single atomic batch
	// (matching the server-side jetstream.limits.batch.max_msgs limit, default 1000).
	maxAtomicBatchMsgs = 1000
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
var _ BatchAppender = (*JetStreamStore)(nil)
var _ SequencedAppender = (*JetStreamStore)(nil)

// Append publishes events to their per-type subjects with per-aggregate OCC
// and MsgID dedup on the event ID. A CAS miss surfaces as
// ErrConcurrencyConflict, same contract as SQLite.
func (s *JetStreamStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...Event) error {
	return s.AppendSequenced(ctx, streamID, expectedVersion, 0, events...)
}

// AppendSequenced publishes events with a known stream sequence anchor (when
// expectedLastSeq > 0 or expectedVersion == 0), skipping the pre-flight
// GetLastMsgForSubject round-trip while still enforcing server-side OCC.
func (s *JetStreamStore) AppendSequenced(ctx context.Context, streamID string, expectedVersion int64, expectedLastSeq uint64, events ...Event) error {
	if s.js == nil {
		return fmt.Errorf("jetstream store: JetStream not configured")
	}
	if len(events) == 0 {
		return nil
	}
	// Validate the whole batch before publishing anything: a version skew
	// must fail without mutating the stream. Batch identity matches SQLite
	// (one stream ID, one aggregate type, consecutive versions).
	scope, anchor, err := s.resolveAnchor(ctx, nil, streamID, expectedVersion, expectedLastSeq, events)
	if err != nil {
		return err
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
		msg := nats.NewMsg(subject)
		msg.Data = data
		SetEventHeaders(msg.Header, evt)
		opts := []jetstream.PublishOpt{
			jetstream.WithMsgID(evt.ID),
			jetstream.WithExpectLastSequenceForSubject(anchor, scope),
		}
		ack, err := s.js.PublishMsg(ctx, msg, opts...)
		if err != nil {
			return wrapPublishError(streamID, evt.ID, err)
		}
		if ack != nil {
			anchor = ack.Sequence
		}
	}
	return nil
}

// resolveAnchor validates one stream append and returns its subject scope and
// last-sequence anchor; nothing is published. When expectedVersion == 0 (new
// stream, anchor must be 0) or knownSeq > 0 (caller loaded the aggregate and
// recorded its stream sequence), the server-side Expected-Last-Subj-Seq check
// enforces OCC directly without an extra GetLastMsgForSubject round-trip.
func (s *JetStreamStore) resolveAnchor(ctx context.Context, cachedStream *jetstream.Stream, streamID string, expectedVersion int64, knownSeq uint64, events []Event) (scope string, anchor uint64, err error) {
	streamType := events[0].StreamType
	if streamType == "" {
		return "", 0, fmt.Errorf("jetstream store: event %s has an empty stream type", events[0].ID)
	}
	for i, evt := range events {
		want := expectedVersion + int64(i) + 1
		if evt.Version != want {
			return "", 0, fmt.Errorf("%w: stream %s event %d has version %d, want %d",
				ErrConcurrencyConflict, streamID, i, evt.Version, want)
		}
		if evt.StreamID != streamID {
			return "", 0, fmt.Errorf("jetstream store: event %s belongs to stream %s, want %s",
				evt.ID, evt.StreamID, streamID)
		}
		if evt.StreamType != streamType {
			return "", 0, fmt.Errorf("jetstream store: stream %s has mixed aggregate types %q and %q",
				streamID, streamType, evt.StreamType)
		}
	}
	scope = messaging.SubjectDomainEventScope(streamID)
	if expectedVersion == 0 {
		return scope, 0, nil
	}
	if knownSeq > 0 {
		return scope, knownSeq, nil
	}
	var stream jetstream.Stream
	if cachedStream != nil && *cachedStream != nil {
		stream = *cachedStream
	} else {
		stream, err = s.js.Stream(ctx, messaging.DomainEventsStreamName)
		if err != nil {
			return "", 0, fmt.Errorf("jetstream store: stream: %w", err)
		}
		if cachedStream != nil {
			*cachedStream = stream
		}
	}
	return preflight(ctx, stream, streamID, expectedVersion, events)
}

// rawMsgEnvelope extracts envelope identity and version from a RawStreamMsg,
// using O(1) NATS headers when present and falling back to decoding the RON
// message body for pre-header messages.
func rawMsgEnvelope(raw *jetstream.RawStreamMsg) (id, streamID, streamType, eventType string, version int64, err error) {
	if id, streamID, streamType, eventType, version, ok := EventHeaderMeta(raw.Header); ok {
		return id, streamID, streamType, eventType, version, nil
	}
	evt, err := UnmarshalEvent(raw.Data)
	if err != nil {
		return "", "", "", "", 0, err
	}
	return evt.ID, evt.StreamID, evt.StreamType, evt.EventType, evt.Version, nil
}

// preflight validates one stream append against the server's subject tip when
// the caller did not supply a known sequence anchor.
func preflight(ctx context.Context, stream jetstream.Stream, streamID string, expectedVersion int64, events []Event) (scope string, anchor uint64, err error) {
	streamType := events[0].StreamType
	scope = messaging.SubjectDomainEventScope(streamID)
	// Layer 1: reject a stale reader. Reload the aggregate's latest
	// persisted event and compare its version against expectedVersion.
	last, err := stream.GetLastMsgForSubject(ctx, scope)
	if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return "", 0, fmt.Errorf("jetstream store: reload %s: %w", streamID, err)
	}
	var maxVersion int64
	var existingType string
	if err == nil && last != nil {
		_, _, existingType, _, maxVersion, err = rawMsgEnvelope(last)
		if err != nil {
			return "", 0, fmt.Errorf("jetstream store: decode last %s: %w", streamID, err)
		}
		anchor = last.Sequence
	}
	if maxVersion != expectedVersion {
		return "", 0, fmt.Errorf("%w: stream %s expected version %d, but found %d",
			ErrConcurrencyConflict, streamID, expectedVersion, maxVersion)
	}
	// Same order as SQLite: version first, then the aggregate-type guard.
	if existingType != "" && existingType != streamType {
		return "", 0, fmt.Errorf("jetstream store: stream %s has aggregate type %q, cannot append type %q",
			streamID, existingType, streamType)
	}
	return scope, anchor, nil
}

// AppendBatch commits events across multiple streams in one server-atomic
// JetStream batch (stream AllowAtomicPublish): the server stages every
// message, re-validates all expectations at commit time under the stream's
// isolation lock, and either persists the whole batch or nothing. A
// concurrent append to any stream in the batch — including one landing
// between the pre-flight reload and the commit — fails the entire batch
// with nothing persisted; there is no partial-write window.
//
// Per-stream OCC: when ExpectedVersion == 0 or ExpectedLastSeq > 0, the batch
// anchors directly without a pre-flight RPC; otherwise pre-flight reloads the
// tip. Only each stream's first event carries the Expected-Last-Subj-Seq
// headers: the server rejects a second per-subject expected-check on a scope
// already written within the same batch, so the remaining events carry only
// their MsgID and rely on the batch's in-order, all-or-nothing commit.
func (s *JetStreamStore) AppendBatch(ctx context.Context, batches []StreamAppend) error {
	if s.js == nil {
		return fmt.Errorf("jetstream store: JetStream not configured")
	}
	if len(batches) == 0 {
		return nil
	}

	type planned struct {
		streamID string
		scope    string
		anchor   uint64
		events   []Event
	}

	var stream jetstream.Stream
	plannedBatches := make([]planned, 0, len(batches))
	// Pre-flight: validate every batch and capture its anchor before anything
	// is staged. A mismatch here fails fast with no mutation — matching SQLite.
	for _, b := range batches {
		if len(b.Events) == 0 {
			continue
		}
		scope, anchor, err := s.resolveAnchor(ctx, &stream, b.StreamID, b.ExpectedVersion, b.ExpectedLastSeq, b.Events)
		if err != nil {
			return err
		}
		plannedBatches = append(plannedBatches, planned{
			streamID: b.StreamID,
			scope:    scope,
			anchor:   anchor,
			events:   b.Events,
		})
	}
	if len(plannedBatches) == 0 {
		return nil
	}

	msgs := make([]*nats.Msg, 0, len(plannedBatches))
	for _, p := range plannedBatches {
		for i, evt := range p.events {
			subject := messaging.SubjectDomainEvent(evt.StreamType, evt.StreamID, evt.EventType)
			data, err := evt.Bytes()
			if err != nil {
				return fmt.Errorf("jetstream store: marshal %s: %w", evt.ID, err)
			}
			msg := nats.NewMsg(subject)
			msg.Data = data
			SetEventHeaders(msg.Header, evt)
			if i == 0 {
				msg.Header.Set(jetstream.ExpectedLastSubjSeqSubjHeader, p.scope)
				msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(p.anchor, 10))
			}
			msgs = append(msgs, msg)
		}
	}
	// Atomic batches are capped server-side (jetstream.limits.batch.max_msgs,
	// default 1000); larger batches are rejected after staging.
	if len(msgs) > maxAtomicBatchMsgs {
		return fmt.Errorf("jetstream store: atomic batch of %d messages exceeds limit %d", len(msgs), maxAtomicBatchMsgs)
	}
	if _, err := jetstreamext.PublishMsgBatch(ctx, s.js, msgs); err != nil {
		// The server does not say which stream missed its anchor, so label
		// with the first stream; SaveBatch's per-aggregate fallback names the
		// real offender when it re-saves.
		return wrapPublishError(plannedBatches[0].streamID, "batch", err)
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
	lastEvt.GlobalPosition = int64(last.Sequence)
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
			evt.GlobalPosition = int64(meta.Sequence.Stream)
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
