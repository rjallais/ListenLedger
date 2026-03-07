//go:build goexperiment.jsonv2

package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/messaging"
)

func bootstrapNATS(ctx context.Context, dataDir string) (*natsserver.Server, *nats.Conn, jetstream.JetStream, error) {
	natsStoreDir := filepath.Join(dataDir, "nats")
	ns, err := startEmbeddedNATS(ctx, natsStoreDir)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to start embedded NATS: %w", err)
	}
	log.Println("[nats] Embedded NATS server started on", ns.ClientURL())

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		return nil, nil, nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := messaging.NewJetStream(nc)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, nil, nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	if err := ensureJetStreamStreams(ctx, js); err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, nil, nil, err
	}

	return ns, nc, js, nil
}

func ensureJetStreamStreams(ctx context.Context, js jetstream.JetStream) error {
	if err := ensureJetStreamStream(ctx, js, messaging.EnsureScrapeRequestStream); err != nil {
		return fmt.Errorf("failed to ensure scrape request stream: %w", err)
	}
	if err := ensureJetStreamStream(ctx, js, messaging.EnsureScrapeDLQStream); err != nil {
		return fmt.Errorf("failed to ensure scrape dlq stream: %w", err)
	}
	if err := ensureJetStreamStream(ctx, js, messaging.EnsureEventsStream); err != nil {
		return fmt.Errorf("failed to ensure events stream: %w", err)
	}

	return nil
}

func ensureJetStreamStream(ctx context.Context, js jetstream.JetStream, ensure func(context.Context, jetstream.JetStream) error) error {
	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return ensure(streamCtx, js)
}

// startEmbeddedNATS launches an in-process NATS server.
func startEmbeddedNATS(ctx context.Context, storeDir string) (*natsserver.Server, error) {
	if err := os.MkdirAll(storeDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create NATS store dir: %w", err)
	}

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoSigs:    true,
		NoLog:     true,
		JetStream: true,
		StoreDir:  storeDir,
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, err
	}

	go ns.Start()

	readyUntil := time.NewTimer(5 * time.Second)
	defer readyUntil.Stop()

	poll := time.NewTicker(25 * time.Millisecond)
	defer poll.Stop()

	for {
		if ns.ReadyForConnections(0) {
			return ns, nil
		}

		select {
		case <-ctx.Done():
			ns.Shutdown()
			return nil, fmt.Errorf("NATS server startup canceled: %w", ctx.Err())
		case <-readyUntil.C:
			ns.Shutdown()
			return nil, fmt.Errorf("NATS server failed to become ready")
		case <-poll.C:
		}
	}
}
