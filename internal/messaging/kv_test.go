package messaging

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func setupTestKV(t *testing.T) (jetstream.JetStream, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true,
		JetStream: true, StoreDir: filepath.Join(t.TempDir(), "nats-store"),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}
	if err := EnsureKVBuckets(ctx, js); err != nil {
		t.Fatalf("EnsureKVBuckets: %v", err)
	}
	return js, ctx
}

func TestKV_PutGetPop(t *testing.T) {
	js, ctx := setupTestKV(t)

	if _, ok, err := KVGet(ctx, js, KVCorrelation, "ar_1"); err != nil || ok {
		t.Fatalf("KVGet missing = %v, %v; want \"\", false, nil", ok, err)
	}
	if err := KVPut(ctx, js, KVCorrelation, "ar_1", "req_1"); err != nil {
		t.Fatalf("KVPut: %v", err)
	}
	if val, ok, err := KVGet(ctx, js, KVCorrelation, "ar_1"); err != nil || !ok || val != "req_1" {
		t.Fatalf("KVGet = %q, %v, %v; want req_1, true, nil", val, ok, err)
	}
	if val, ok, err := KVPop(ctx, js, KVCorrelation, "ar_1"); err != nil || !ok || val != "req_1" {
		t.Fatalf("KVPop = %q, %v, %v; want req_1, true, nil", val, ok, err)
	}
	if _, ok, _ := KVGet(ctx, js, KVCorrelation, "ar_1"); ok {
		t.Fatal("KVGet after pop: want missing")
	}
}

func TestKV_PopRaceKeepsReplacement(t *testing.T) {
	js, ctx := setupTestKV(t)

	if err := KVPut(ctx, js, KVCorrelation, "ar_race", "req_old"); err != nil {
		t.Fatalf("KVPut: %v", err)
	}
	// Capture the revision a concurrent popper would have read, then
	// replace the value out-of-band. A delete conditional on the stale
	// revision must fail and the replacement must survive.
	kv, err := js.KeyValue(ctx, KVCorrelation)
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	entry, err := kv.Get(ctx, "ar_race")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := kv.PutString(ctx, "ar_race", "req_new"); err != nil {
		t.Fatalf("PutString replacement: %v", err)
	}
	if err := kv.Delete(ctx, "ar_race", jetstream.LastRevision(entry.Revision())); !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		t.Fatalf("stale-revision Delete = %v; want ErrKeyRevisionMismatch", err)
	}
	if val, ok, err := KVGet(ctx, js, KVCorrelation, "ar_race"); err != nil || !ok || val != "req_new" {
		t.Fatalf("KVGet after stale delete = %q, %v, %v; want req_new, true, nil", val, ok, err)
	}
	// The live pop path still works on the current revision.
	if val, ok, err := KVPop(ctx, js, KVCorrelation, "ar_race"); err != nil || !ok || val != "req_new" {
		t.Fatalf("KVPop = %q, %v, %v; want req_new, true, nil", val, ok, err)
	}
}

func TestEnsureDomainEventsStreamAsTruth(t *testing.T) {
	js, ctx := setupTestKV(t)

	if err := EnsureDomainEventsStreamAsTruth(ctx, js); err != nil {
		t.Fatalf("EnsureDomainEventsStreamAsTruth: %v", err)
	}
	stream, err := js.Stream(ctx, DomainEventsStreamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.MaxAge != DomainEventsTruthRetention {
		t.Fatalf("MaxAge = %s, want %s", info.Config.MaxAge, DomainEventsTruthRetention)
	}
	if info.Config.MaxBytes != DomainEventsTruthMaxBytes {
		t.Fatalf("MaxBytes = %d, want %d", info.Config.MaxBytes, DomainEventsTruthMaxBytes)
	}
}
