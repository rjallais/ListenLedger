package storeutil

import (
	"log/slog"
	"strings"
	"testing"

	"ListenLedger/internal/db"
	"ListenLedger/internal/eventsourcing"
)

// TestOpenEventStoreRefusesFrozenSQLiteLog pins the flip guard for CLI tools:
// after the jetstream_authority marker is recorded, sqlite mode must fail
// closed instead of forking the frozen log, and jetstream mode before the
// flip must fail closed too (no parity was ever verified).
func TestOpenEventStoreRefusesFrozenSQLiteLog(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = database.Close() }()

	t.Setenv(eventsourcing.EventStoreEnv, eventsourcing.EventStoreSQLite)
	store, cleanup, err := OpenEventStore(ctx, dataDir, database)
	if err != nil {
		t.Fatalf("sqlite mode before the flip should open: %v", err)
	}
	if store == nil {
		t.Fatal("sqlite mode before the flip returned a nil store")
	}
	cleanup()

	t.Setenv(eventsourcing.EventStoreEnv, eventsourcing.EventStoreJetStream)
	if _, _, err := OpenEventStore(ctx, dataDir, database); err == nil {
		t.Fatal("jetstream mode before the flip should refuse (no authority marker)")
	}

	sqliteStore := eventsourcing.NewSQLiteStore(database)
	if err := sqliteStore.SaveCheckpoint(ctx, eventsourcing.CheckpointJetStreamAuthority, 1); err != nil {
		t.Fatalf("saving authority marker: %v", err)
	}

	t.Setenv(eventsourcing.EventStoreEnv, eventsourcing.EventStoreSQLite)
	_, _, err = OpenEventStore(ctx, dataDir, database)
	if err == nil || !strings.Contains(err.Error(), "frozen SQLite log") {
		t.Fatalf("sqlite mode after the flip should refuse, got err=%v", err)
	}
}
