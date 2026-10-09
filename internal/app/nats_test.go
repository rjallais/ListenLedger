package app

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"ListenLedger/config"
)

func TestEmbeddedNATSLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	natsDir := filepath.Join(tempDir, "nats")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	cfg := config.DefaultConfig()
	cfg.NATSStoreDir = natsDir
	cfg.NATSMaxMemoryStore = 32 * 1024 * 1024
	cfg.NATSMaxFileStore = 64 * 1024 * 1024

	en, err := bootstrapNATS(ctx, tempDir, cfg)
	if err != nil {
		t.Fatalf("bootstrapNATS failed: %v", err)
	}

	if en.Server == nil {
		t.Fatal("expected non-nil NATS server")
	}
	if en.Conn == nil || en.Conn.IsClosed() {
		t.Fatal("expected connected NATS client")
	}
	if en.JS == nil {
		t.Fatal("expected initialized JetStream")
	}

	// Verify we can publish and roundtrip a ping on the connection
	testSub := "test.nats.lifecycle"
	sub, err := en.Conn.SubscribeSync(testSub)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	if err := en.Conn.Publish(testSub, []byte("hello")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("failed to receive test msg: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(msg.Data))
	}

	// Verify graceful close
	if err := en.Close(ctx); err != nil {
		t.Fatalf("en.Close failed: %v", err)
	}

	if !en.Conn.IsClosed() {
		t.Fatal("expected connection to be closed after Close()")
	}
}
