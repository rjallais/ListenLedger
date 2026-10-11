// Store selection for the SQLite-to-JetStream write flip.
//
// LISTENLEDGER_EVENT_STORE selects the authoritative event store behind the
// Store interface: "sqlite" (default) keeps the SQLite events table as the
// log; "jetstream" makes DOMAIN_EVENTS the authority (writes via per-event
// publish with per-aggregate OCC, reads via filtered consumers). Anything
// else fails fast at startup — a typo must never silently pick a store.
//
// Jetstream mode notes: repositories degrade snapshot paths to full replay
// (JetStreamStore is not a SnapshotStore); write sites must use
// AppendWithRetry (JetStream publishes are non-atomic); the SQLite events
// table freezes (admin/audit/replay tooling reads stale history — the audit
// tool is the parity checker before flipping production).
package eventsourcing

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/messaging"
)

// EventStoreMode selects the authoritative event store.
const (
	// EventStoreSQLite keeps the SQLite events table as the log.
	EventStoreSQLite = "sqlite"
	// EventStoreJetStream makes JetStream DOMAIN_EVENTS the authority.
	EventStoreJetStream = "jetstream"
	// EventStoreEnv names the selector variable.
	EventStoreEnv = "LISTENLEDGER_EVENT_STORE"
)

// SelectedStoreName reads the store selector; empty means SQLite.
func SelectedStoreName() (string, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(EventStoreEnv)))
	switch raw {
	case "", EventStoreSQLite:
		return EventStoreSQLite, nil
	case EventStoreJetStream:
		return EventStoreJetStream, nil
	default:
		return "", fmt.Errorf("eventsourcing: invalid %s %q (want %q or %q)",
			EventStoreEnv, raw, EventStoreSQLite, EventStoreJetStream)
	}
}

// SelectedStore builds the authoritative store for the current mode. In
// jetstream mode js must be non-nil (wiring sites already hold a JetStream
// context; CLIs without NATS stay on SQLite or fail here instead of
// diverging silently).
func SelectedStore(db *toolbeltdb.Database, js jetstream.JetStream) (Store, error) {
	mode, err := SelectedStoreName()
	if err != nil {
		return nil, err
	}
	switch mode {
	case EventStoreJetStream:
		if js == nil {
			return nil, fmt.Errorf("eventsourcing: %s=%q but JetStream is not configured",
				EventStoreEnv, EventStoreJetStream)
		}
		return NewJetStreamStore(js), nil
	default:
		return NewSQLiteStore(db), nil
	}
}

// CheckJetStreamParity verifies the JetStream log mirrors the SQLite log
// before jetstream mode serves reads and writes from it, then records the
// authority marker so later boots skip re-verification (steady-state
// JetStream runs ahead by design — re-checking counts would brick every
// restart after the first JetStream write). One O(1) count check plus one
// JetStream lookup per stream for tip verification (O(streams), sequential —
// fine at current scale; revisit with a timeout budget or sampling if the
// stream count grows):
//
//  1. Outbox lag is zero: every logged event was handed to JetStream at
//     least once (the outbox migration backfilled pre-outbox history).
//  2. Stream message count equals the SQLite event count: nothing expired
//     or went missing, and nothing extra was published.
//
// Soundness: the only DOMAIN_EVENTS publishers (outbox relay, projection
// sync path) both use MsgID=event ID, so redeliveries dedup and the stream
// holds exactly the published ID set. A subset of the SQLite IDs with equal
// size is the full set. Returns nil on parity, descriptive error otherwise.
func CheckJetStreamParity(ctx context.Context, db *toolbeltdb.Database, js jetstream.JetStream) error {
	if js == nil {
		return fmt.Errorf("eventsourcing: parity check needs JetStream, got nil")
	}
	store := NewSQLiteStore(db)
	// Steady state short-circuit: once the gate has passed and recorded the
	// authority marker, JetStream is ahead by design (SQLite frozen). Only
	// the first flip boot pays for full verification.
	if marked, err := store.GetCheckpoint(ctx, CheckpointJetStreamAuthority); err == nil && marked != 0 {
		slog.Info("eventsourcing: jetstream authority previously established, skipping parity re-verification")
		return nil
	}
	lag, err := store.UnpublishedCount(ctx)
	if err != nil {
		return fmt.Errorf("eventsourcing: parity check outbox lag: %w", err)
	}
	if lag != 0 {
		return fmt.Errorf("eventsourcing: parity check failed: %d unpublished outbox rows (let the relay drain, then flip)", lag)
	}
	want, err := store.EventCount(ctx)
	if err != nil {
		return fmt.Errorf("eventsourcing: parity check event count: %w", err)
	}
	stream, err := js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		return fmt.Errorf("eventsourcing: parity check stream: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("eventsourcing: parity check stream info: %w", err)
	}
	if got := info.State.Msgs; got != uint64(want) {
		return fmt.Errorf("eventsourcing: parity check failed: SQLite holds %d events, JetStream holds %d (backfill or retention gap — do not flip)", want, got)
	}
	// Per-stream tip verification: counts alone cannot catch a balanced
	// substitution (one event missing, another extra). Every SQLite stream's
	// tip must match JetStream's by version and event ID. This also
	// cross-checks the subject encoding end to end (SQLite ID -> JS subject
	// -> resolved tip). Fail closed: a stream with no JetStream tip, an
	// unreadable tip, or any version/ID mismatch refuses the flip.
	// Scale: one server-side index lookup per stream, jetstream-mode boots
	// only, cancellable via ctx — sub-second at current stream counts.
	tips, err := store.StreamTips(ctx)
	if err != nil {
		return fmt.Errorf("eventsourcing: parity check stream tips: %w", err)
	}
	for _, tip := range tips {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("eventsourcing: parity check tip %s: %w", tip.StreamID, err)
		}
		last, err := stream.GetLastMsgForSubject(ctx, messaging.SubjectDomainEventScope(tip.StreamID))
		if err != nil {
			return fmt.Errorf("eventsourcing: parity check failed: stream %s has no JetStream tip: %w", tip.StreamID, err)
		}
		lastEvt, err := UnmarshalEvent(last.Data)
		if err != nil {
			return fmt.Errorf("eventsourcing: parity check failed: stream %s tip undecodable: %w", tip.StreamID, err)
		}
		if lastEvt.Version != tip.Version || lastEvt.ID != tip.EventID {
			return fmt.Errorf("eventsourcing: parity check failed: stream %s tip is v%d %s in SQLite, v%d %s in JetStream",
				tip.StreamID, tip.Version, tip.EventID, lastEvt.Version, lastEvt.ID)
		}
	}
	// Record the flip: later boots skip re-verification (see above). A
	// marker-write failure fails closed — boot without a recorded flip must
	// re-verify rather than assume authority.
	if err := store.SaveCheckpoint(ctx, CheckpointJetStreamAuthority, 1); err != nil {
		return fmt.Errorf("eventsourcing: parity passed but authority marker unwritable: %w", err)
	}
	return nil
}
