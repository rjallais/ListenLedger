package worker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/config"
	"ListenLedger/internal/messaging"
)

// TestResolveJetStreamTuningClampsUnsafeAckWait ensures an explicitly
// configured AckWait below the worst-case fetch timeout is clamped instead
// of guaranteeing redelivery storms (e.g. AckWait=10s with Apify's 350s
// fetch timeout persisted on disk).
func TestResolveJetStreamTuningClampsUnsafeAckWait(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ApifyToken = "test-token"
	cfg.ScrapeAckWait = 10 * time.Second

	w := &Worker{cfg: cfg}
	w.resolveJetStreamTuning()

	floor := 2 * (350 * time.Second) // 2 x Apify providerMinTimeout
	if w.ackWait < floor {
		t.Fatalf("ackWait = %s, want >= %s", w.ackWait, floor)
	}
}

// TestMinSafeAckWaitFloor ensures the floor always covers the worst-case
// fetch timeout and the redelivery backoff, even with no explicit config.
func TestMinSafeAckWaitFloor(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ApifyToken = "test-token"

	w := &Worker{cfg: cfg}
	w.resolveJetStreamTuning()

	if w.ackWait < 2*time.Minute {
		t.Fatalf("ackWait = %s, want >= 2m", w.ackWait)
	}
	for _, d := range w.backoff {
		if w.ackWait < d {
			t.Fatalf("ackWait = %s below backoff %s", w.ackWait, d)
		}
	}
	if w.progress > w.ackWait/2 {
		t.Fatalf("progress = %s exceeds ackWait/2 = %s", w.progress, w.ackWait/2)
	}
}

// TestHealConsumerTuningRecreatesStaleConsumer reproduces the production
// incident where a durable with BackOff starting at 10s survived restarts
// while fetches need minutes. When BackOff is set the server redelivers on
// that schedule regardless of AckWait, so every slow scrape risked duplicate
// provider attempts on any heartbeat gap. Healing must reinstall a safe
// server schedule while leaving the short NakWithDelay retry pacing alone.
func TestHealConsumerTuningRecreatesStaleConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ns := startTestWorkerNATS(t, t.TempDir())
	nc := connectTestWorkerNATS(t, ns.ClientURL())
	defer nc.Close()
	defer ns.Shutdown()

	js, err := messaging.NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream() error = %v", err)
	}
	if err := messaging.EnsureScrapeRequestStream(ctx, js); err != nil {
		t.Fatalf("EnsureScrapeRequestStream() error = %v", err)
	}

	// Simulate the stale durable persisted by older defaults.
	stale, err := messaging.EnsureScrapeWorkerConsumer(ctx, js, jetstream.ConsumerConfig{
		Durable:       messaging.ScrapeWorkerConsumerName,
		FilterSubject: messaging.SubjectScrapeRequest,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       10 * time.Second,
		BackOff:       []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute},
		MaxDeliver:    3,
		MaxAckPending: 1,
	})
	if err != nil {
		t.Fatalf("create stale consumer: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.ApifyToken = "test-token"
	w := &Worker{cfg: cfg, js: js}
	w.resolveJetStreamTuning()

	healed, err := w.healConsumerTuning(ctx, stale, 27)
	if err != nil {
		t.Fatalf("healConsumerTuning() error = %v", err)
	}

	info, err := healed.Info(ctx)
	if err != nil {
		t.Fatalf("healed.Info() error = %v", err)
	}
	if len(info.Config.BackOff) == 0 || info.Config.BackOff[0] < w.minSafeAckWait() {
		t.Fatalf("healed BackOff = %v, want first entry >= floor %s", info.Config.BackOff, w.minSafeAckWait())
	}
	if info.Config.AckWait < w.minSafeAckWait() {
		t.Fatalf("healed AckWait = %s, want >= floor %s", info.Config.AckWait, w.minSafeAckWait())
	}

	// Alignment must not adopt the long server schedule into the short
	// NakWithDelay retry pacing.
	alignCtx, alignCancel := context.WithTimeout(ctx, 2*time.Second)
	defer alignCancel()
	w.alignFromConsumerInfo(alignCtx, healed)
	if len(w.backoff) == 0 || w.backoff[0] >= w.minSafeAckWait() {
		t.Fatalf("retry backoff pacing was clobbered by server schedule: %v", w.backoff)
	}
	if w.ackWait < w.minSafeAckWait() {
		t.Fatalf("ackWait = %s after align, want >= floor %s", w.ackWait, w.minSafeAckWait())
	}

	// A consumer already at/above the floor must be left untouched.
	untouched, err := w.healConsumerTuning(ctx, healed, 27)
	if err != nil {
		t.Fatalf("healConsumerTuning(healthy) error = %v", err)
	}
	again, err := untouched.Info(ctx)
	if err != nil {
		t.Fatalf("untouched.Info() error = %v", err)
	}
	if again.Config.AckWait != info.Config.AckWait {
		t.Fatalf("healthy consumer AckWait changed: %s -> %s", info.Config.AckWait, again.Config.AckWait)
	}
}

func startTestWorkerNATS(t *testing.T, storeDir string) *natsserver.Server {
	t.Helper()

	ns, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoSigs:    true,
		NoLog:     true,
		JetStream: true,
		StoreDir:  filepath.Join(storeDir, "nats-store"),
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		t.Fatal("NATS server failed to become ready")
	}
	return ns
}

func connectTestWorkerNATS(t *testing.T, url string) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	return nc
}
