// Package storeutil provides helpers for standalone CLI tools to safely open
// the authoritative event store (SQLite or embedded JetStream) and verify environment guards.
package storeutil

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/appdir"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
)

// OpenEventStore builds the authoritative store for the current mode. In
// jetstream mode it boots an embedded server on the data dir's NATS store
// (never ensuring stream config — the app owns that); in sqlite mode it
// refuses to run after the one-way flip, which would fork the frozen log.
func OpenEventStore(ctx context.Context, dataDir string, database *toolbeltdb.Database) (eventsourcing.Store, func(), error) {
	mode, err := eventsourcing.SelectedStoreName()
	if err != nil {
		return nil, nil, err
	}
	sqliteStore := eventsourcing.NewSQLiteStore(database)
	marked, err := sqliteStore.GetCheckpoint(ctx, eventsourcing.CheckpointJetStreamAuthority)
	if err != nil {
		return nil, nil, fmt.Errorf("reading jetstream authority marker: %w", err)
	}
	if mode == eventsourcing.EventStoreJetStream {
		if marked == 0 {
			return nil, nil, fmt.Errorf("%s=%q but the flip has not happened (no authority marker); appending now would break the parity gate — run in %s=%q mode until the flip",
				eventsourcing.EventStoreEnv, eventsourcing.EventStoreJetStream, eventsourcing.EventStoreEnv, eventsourcing.EventStoreSQLite)
		}
		js, cleanup, err := OpenStoreJetStream(ctx, dataDir)
		if err != nil {
			return nil, nil, err
		}
		store, err := eventsourcing.SelectedStore(database, js)
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		return store, cleanup, nil
	}

	if marked != 0 {
		return nil, nil, fmt.Errorf("jetstream authority already established; writing the frozen SQLite log would fork it (run with %s=%q)",
			eventsourcing.EventStoreEnv, eventsourcing.EventStoreJetStream)
	}
	return sqliteStore, func() {}, nil
}

// OpenStoreJetStream boots an embedded NATS server on the data dir's
// existing NATS store and returns a JetStream context over DOMAIN_EVENTS.
func OpenStoreJetStream(ctx context.Context, dataDir string) (jetstream.JetStream, func(), error) {
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true,
		JetStream: true, StoreDir: filepath.Join(dataDir, "nats"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("starting embedded NATS: %w", err)
	}
	go ns.Start()
	cleanup := func() { ns.Shutdown() }
	if !ns.ReadyForConnections(10 * time.Second) {
		cleanup()
		return nil, nil, fmt.Errorf("embedded NATS not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("connecting embedded NATS: %w", err)
	}
	cleanupBoth := func() { nc.Close(); ns.Shutdown() }
	js, err := messaging.NewJetStream(nc)
	if err != nil {
		cleanupBoth()
		return nil, nil, fmt.Errorf("creating JetStream context: %w", err)
	}
	if _, err := js.Stream(ctx, messaging.DomainEventsStreamName); err != nil {
		cleanupBoth()
		return nil, nil, fmt.Errorf("DOMAIN_EVENTS stream: %w", err)
	}
	return js, cleanupBoth, nil
}

// IsLiveDataDir reports whether dataDir is the live default rather than an explicit staging copy.
func IsLiveDataDir(dataDir string) bool {
	abs, err := AbsPath(dataDir)
	if err != nil {
		return true
	}
	liveAbs, err := AbsPath(appdir.ResolveDataDir())
	if err != nil {
		return true
	}
	return abs == liveAbs
}

// AbsPath returns absolute clean path resolving symlinks.
func AbsPath(path string) (string, error) {
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	return abs, nil
}

// CheckPortAvailable returns whether the TCP port on localhost is available.
func CheckPortAvailable(port string) bool {
	ln, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}
