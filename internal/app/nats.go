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

func bootstrapNATS(dataDir string) (*natsserver.Server, *nats.Conn, jetstream.JetStream, error) {
	natsStoreDir := filepath.Join(dataDir, "nats")
	ns, err := startEmbeddedNATS(natsStoreDir)
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

	if err := ensureJetStreamStreams(js); err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, nil, nil, err
	}

	return ns, nc, js, nil
}

func ensureJetStreamStreams(js jetstream.JetStream) error {
	jsCtx, cancelJS := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelJS()

	if err := messaging.EnsureScrapeRequestStream(jsCtx, js); err != nil {
		return fmt.Errorf("failed to ensure scrape request stream: %w", err)
	}
	if err := messaging.EnsureScrapeDLQStream(jsCtx, js); err != nil {
		return fmt.Errorf("failed to ensure scrape dlq stream: %w", err)
	}
	if err := messaging.EnsureEventsStream(jsCtx, js); err != nil {
		return fmt.Errorf("failed to ensure events stream: %w", err)
	}

	return nil
}

// startEmbeddedNATS launches an in-process NATS server.
func startEmbeddedNATS(storeDir string) (*natsserver.Server, error) {
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

	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		return nil, fmt.Errorf("NATS server failed to become ready")
	}

	return ns, nil
}
