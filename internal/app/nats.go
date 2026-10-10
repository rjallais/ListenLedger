package app

import (
	"context"
	"errors"
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

	"ListenLedger/config"
	"ListenLedger/internal/messaging"
)

// EmbeddedNATS wraps an in-process NATS server, its client connection, and JetStream context,
// providing resilient lifecycle management patterned after Delaney's Toolbelt.
type EmbeddedNATS struct {
	Server *natsserver.Server
	Conn   *nats.Conn
	JS     jetstream.JetStream
}

// Close gracefully drains client connections, flushes in-flight messages, shuts down the
// embedded server, and blocks until the server has fully stopped (bounded by ctx).
func (e *EmbeddedNATS) Close(ctx context.Context) error {
	if e == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	drainErr := drainClientConn(ctx, e.Conn)
	shutdownErr := shutdownServer(ctx, e.Server)
	return errors.Join(drainErr, shutdownErr)
}

func drainClientConn(ctx context.Context, conn *nats.Conn) error {
	if conn == nil || conn.IsClosed() {
		return nil
	}

	// Drain flushes by itself; no separate Flush call is needed. Close the
	// connection on every early return: with MaxReconnects(-1) an abandoned
	// conn would otherwise redial the stopped server forever.
	if err := conn.Drain(); err != nil {
		conn.Close()
		return fmt.Errorf("drain NATS client connection: %w", err)
	}
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for !conn.IsClosed() {
		select {
		case <-ctx.Done():
			conn.Close()
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}

func shutdownServer(ctx context.Context, server *natsserver.Server) error {
	if server == nil {
		return nil
	}
	server.Shutdown()
	shutdownDone := make(chan struct{})
	go func() {
		server.WaitForShutdown()
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("embedded NATS server shutdown interrupted: %w", ctx.Err())
	}
}

func bootstrapNATS(ctx context.Context, dataDir string, cfg *config.Config) (*EmbeddedNATS, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}
	natsStoreDir := cfg.NATSStoreDir
	if natsStoreDir == "" {
		natsStoreDir = filepath.Join(dataDir, "nats")
	}

	// DOMAIN_EVENTS as truth reserves DomainEventsTruthMaxBytes of file
	// store: a smaller NATS_MAX_FILE_STORE override makes the embedded
	// server reject the stream config, so fail fast with the knob named
	// instead of dying mid-bootstrap.
	if cfg.NATSMaxFileStore > 0 && cfg.NATSMaxFileStore < messaging.DomainEventsTruthMaxBytes {
		return nil, fmt.Errorf("NATS_MAX_FILE_STORE (%d bytes) is below the %d bytes DOMAIN_EVENTS requires as truth stream; raise the override or unset it",
			cfg.NATSMaxFileStore, int64(messaging.DomainEventsTruthMaxBytes))
	}

	ns, err := startEmbeddedNATS(ctx, natsStoreDir, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to start embedded NATS: %w", err)
	}
	log.Printf("[nats] embedded NATS started at %s", ns.ClientURL())

	nc, err := connectNATSWithRetry(ctx, ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		ns.WaitForShutdown()
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := messaging.NewJetStream(nc)
	if err != nil {
		_ = nc.Drain()
		ns.Shutdown()
		ns.WaitForShutdown()
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	if err := ensureJetStreamStreams(ctx, js); err != nil {
		_ = nc.Drain()
		ns.Shutdown()
		ns.WaitForShutdown()
		return nil, err
	}

	return &EmbeddedNATS{
		Server: ns,
		Conn:   nc,
		JS:     js,
	}, nil
}

func connectNATSWithRetry(ctx context.Context, url string) (*nats.Conn, error) {
	backoff := 50 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("nats connect canceled: %w", ctx.Err())
		default:
		}

		nc, err := nats.Connect(url,
			nats.Timeout(2*time.Second),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(1*time.Second),
		)
		if err == nil {
			return nc, nil
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("nats connect retry canceled: %w", ctx.Err())
		case <-time.After(backoff):
			backoff = min(backoff*2, 500*time.Millisecond)
		}
	}
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
	if err := ensureJetStreamStream(ctx, js, messaging.EnsureDomainEventsStreamAsTruth); err != nil {
		return fmt.Errorf("failed to ensure domain events stream: %w", err)
	}
	if err := ensureJetStreamStream(ctx, js, messaging.EnsureKVBuckets); err != nil {
		return fmt.Errorf("failed to ensure KV buckets: %w", err)
	}

	return nil
}

func ensureJetStreamStream(ctx context.Context, js jetstream.JetStream, ensure func(context.Context, jetstream.JetStream) error) error {
	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return ensure(streamCtx, js)
}

// startEmbeddedNATS launches an in-process NATS server with exponential backoff readiness checks.
func startEmbeddedNATS(ctx context.Context, storeDir string, cfg *config.Config) (*natsserver.Server, error) {
	if err := os.MkdirAll(storeDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create NATS store dir: %w", err)
	}

	port, err := resolveNATSPort(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve embedded NATS port: %w", err)
	}

	opts := &natsserver.Options{
		Host:               "127.0.0.1",
		Port:               port,
		NoSigs:             true,
		JetStream:          true,
		StoreDir:           storeDir,
		JetStreamMaxMemory: cfg.NATSMaxMemoryStore,
		JetStreamMaxStore:  cfg.NATSMaxFileStore,
	}

	if cfg.NATSLogging || cfg.NATSDebug {
		opts.NoLog = false
		opts.Debug = cfg.NATSDebug
		opts.Logtime = true
	} else {
		opts.NoLog = true
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("create embedded NATS server (store_dir=%s): %w", opts.StoreDir, err)
	}
	if !opts.NoLog {
		ns.ConfigureLogger()
	}

	go ns.Start()

	// Tie the server lifetime to ctx like toolbelt's embeddednats: if the
	// context is canceled before Close runs (e.g. signal during startup),
	// the server still shuts down instead of leaking.
	go func() {
		<-ctx.Done()
		ns.Shutdown()
	}()

	if err := waitForNATSServer(ctx, ns, 5*time.Second); err != nil {
		ns.Shutdown()
		ns.WaitForShutdown()
		return nil, fmt.Errorf("embedded NATS failed readiness check: %w", err)
	}

	return ns, nil
}

func waitForNATSServer(ctx context.Context, ns *natsserver.Server, maxWait time.Duration) error {
	deadline := time.Now().Add(maxWait)
	step := 25 * time.Millisecond
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for NATS server canceled: %w", ctx.Err())
		default:
		}

		if ns.ReadyForConnections(step) {
			return nil
		}
		step = min(step*2, 250*time.Millisecond)
	}
	return fmt.Errorf("NATS server failed to become ready within %v", maxWait)
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
