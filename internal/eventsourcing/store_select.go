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
	"fmt"
	"os"
	"strings"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"github.com/nats-io/nats.go/jetstream"
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
