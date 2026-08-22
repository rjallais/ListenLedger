//go:build goexperiment.jsonv2

package app

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
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
	log.Printf("[nats] embedded NATS started at %s", ns.ClientURL())

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

	port, err := resolveNATSPort(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve embedded NATS port: %w", err)
	}

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      port,
		NoSigs:    true,
		NoLog:     true,
		JetStream: true,
		StoreDir:  storeDir,
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("create embedded NATS server (store_dir=%s): %w", opts.StoreDir, err)
	}

	go ns.Start()

	ready := make(chan bool, 1)
	go func() {
		ready <- ns.ReadyForConnections(5 * time.Second)
	}()

	select {
	case <-ctx.Done():
		ns.Shutdown()
		return nil, fmt.Errorf("NATS server startup canceled: %w", ctx.Err())
	case ok := <-ready:
		if !ok {
			ns.Shutdown()
			return nil, fmt.Errorf("NATS server failed to become ready")
		}
		return ns, nil
	}
}

func resolveNATSPort(ctx context.Context) (int, error) {
	if p, ok := os.LookupEnv("NATS_PORT"); ok {
		if p == "" {
			return -1, nil
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return 0, fmt.Errorf("invalid NATS_PORT %q: %w", p, err)
		}
		if n < 1 || n > 65535 {
			return 0, fmt.Errorf("invalid NATS_PORT %q: must be 1-65535", p)
		}
		if isPortFree(ctx, n) {
			return n, nil
		}
		log.Printf("[nats] NATS_PORT %d in use, falling back to random port", n)
	}
	return -1, nil
}

func isPortFree(ctx context.Context, port int) bool {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}
