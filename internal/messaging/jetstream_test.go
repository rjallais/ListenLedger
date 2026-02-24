//go:build goexperiment.jsonv2

package messaging

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestScrapeStreamDedupAndDurability(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), "nats-store")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	ns := startTestNATSServer(t, storeDir)
	nc := connectTestNATS(t, ns.ClientURL())

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream() error = %v", err)
	}
	if err := EnsureScrapeRequestStream(ctx, js); err != nil {
		t.Fatalf("EnsureScrapeRequestStream() error = %v", err)
	}

	req := NewScrapeRequested("artist-1", "spotify-1", "Artist Name", "req-1")
	msgID := ScrapeRequestMsgID(req.ArtistID)

	ack1, err := PublishScrapeRequested(ctx, js, req, msgID)
	if err != nil {
		t.Fatalf("PublishScrapeRequested(first) error = %v", err)
	}
	if ack1 == nil {
		t.Fatal("first ack should not be nil")
	}
	if ack1.Duplicate {
		t.Fatal("first publish unexpectedly marked duplicate")
	}

	ack2, err := PublishScrapeRequested(ctx, js, req, msgID)
	if err != nil {
		t.Fatalf("PublishScrapeRequested(second) error = %v", err)
	}
	if ack2 == nil {
		t.Fatal("second ack should not be nil")
	}
	if !ack2.Duplicate {
		t.Fatal("second publish should be marked duplicate")
	}

	stream, err := js.Stream(ctx, ScrapeRequestsStreamName)
	if err != nil {
		t.Fatalf("js.Stream() error = %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream.Info() error = %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream message count = %d, want 1", info.State.Msgs)
	}

	nc.Close()
	ns.Shutdown()

	ns2 := startTestNATSServer(t, storeDir)
	nc2 := connectTestNATS(t, ns2.ClientURL())
	defer func() {
		nc2.Close()
		ns2.Shutdown()
	}()

	js2, err := NewJetStream(nc2)
	if err != nil {
		t.Fatalf("NewJetStream(second) error = %v", err)
	}
	stream2, err := js2.Stream(ctx, ScrapeRequestsStreamName)
	if err != nil {
		t.Fatalf("js2.Stream() error = %v", err)
	}
	info2, err := stream2.Info(ctx)
	if err != nil {
		t.Fatalf("stream2.Info() error = %v", err)
	}
	if info2.State.Msgs != 1 {
		t.Fatalf("persisted stream message count = %d, want 1", info2.State.Msgs)
	}
}

func TestScrapeWorkerConsumerFetchAfterRestart(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), "nats-store")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	ns := startTestNATSServer(t, storeDir)
	nc := connectTestNATS(t, ns.ClientURL())

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream() error = %v", err)
	}
	if err := EnsureScrapeRequestStream(ctx, js); err != nil {
		t.Fatalf("EnsureScrapeRequestStream() error = %v", err)
	}

	_, err = EnsureScrapeWorkerConsumer(ctx, js, jetstream.ConsumerConfig{
		Durable:       ScrapeWorkerConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Minute,
		MaxDeliver:    3,
		MaxAckPending: 1,
	})
	if err != nil {
		t.Fatalf("EnsureScrapeWorkerConsumer() error = %v", err)
	}

	req := NewScrapeRequested("artist-1", "spotify-1", "Artist Name", "req-1")
	if _, err := PublishScrapeRequested(ctx, js, req, ScrapeRequestMsgID(req.ArtistID)); err != nil {
		t.Fatalf("PublishScrapeRequested() error = %v", err)
	}

	nc.Close()
	ns.Shutdown()

	// Restart and ensure the durable consumer can fetch the queued message.
	ns2 := startTestNATSServer(t, storeDir)
	nc2 := connectTestNATS(t, ns2.ClientURL())
	defer func() {
		nc2.Close()
		ns2.Shutdown()
	}()

	js2, err := NewJetStream(nc2)
	if err != nil {
		t.Fatalf("NewJetStream(restart) error = %v", err)
	}

	stream, err := js2.Stream(ctx, ScrapeRequestsStreamName)
	if err != nil {
		t.Fatalf("js2.Stream() error = %v", err)
	}
	consumer, err := stream.Consumer(ctx, ScrapeWorkerConsumerName)
	if err != nil {
		t.Fatalf("stream.Consumer() error = %v", err)
	}

	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("consumer.Fetch() error = %v", err)
	}

	got := 0
	for msg := range batch.Messages() {
		got++
		ackCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if err := msg.DoubleAck(ackCtx); err != nil {
			cancel()
			t.Fatalf("msg.DoubleAck() error = %v", err)
		}
		cancel()
	}
	if err := batch.Error(); err != nil {
		t.Fatalf("batch.Error() = %v", err)
	}
	if got != 1 {
		t.Fatalf("fetched msgs = %d, want 1", got)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream.Info() error = %v", err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("stream message count after ack = %d, want 0", info.State.Msgs)
	}
}

func TestScrapeDLQStreamDurability(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), "nats-store")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	ns := startTestNATSServer(t, storeDir)
	nc := connectTestNATS(t, ns.ClientURL())

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream() error = %v", err)
	}
	if err := EnsureScrapeDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureScrapeDLQStream() error = %v", err)
	}

	if _, err := js.Publish(ctx, SubjectScrapeDLQ, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("js.Publish() error = %v", err)
	}

	nc.Close()
	ns.Shutdown()

	ns2 := startTestNATSServer(t, storeDir)
	nc2 := connectTestNATS(t, ns2.ClientURL())
	defer func() {
		nc2.Close()
		ns2.Shutdown()
	}()

	js2, err := NewJetStream(nc2)
	if err != nil {
		t.Fatalf("NewJetStream(restart) error = %v", err)
	}
	stream, err := js2.Stream(ctx, ScrapeDLQStreamName)
	if err != nil {
		t.Fatalf("js2.Stream() error = %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream.Info() error = %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("dlq stream message count = %d, want 1", info.State.Msgs)
	}
}

func TestEventsStreamDurability(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), "nats-store")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	ns := startTestNATSServer(t, storeDir)
	nc := connectTestNATS(t, ns.ClientURL())

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream() error = %v", err)
	}
	if err := EnsureEventsStream(ctx, js); err != nil {
		t.Fatalf("EnsureEventsStream() error = %v", err)
	}

	if _, err := js.Publish(ctx, SubjectArtistUpdated, []byte(`{"version":"v1","artist_id":"a1"}`), jetstream.WithMsgID("evt-1")); err != nil {
		t.Fatalf("js.Publish() error = %v", err)
	}

	nc.Close()
	ns.Shutdown()

	ns2 := startTestNATSServer(t, storeDir)
	nc2 := connectTestNATS(t, ns2.ClientURL())
	defer func() {
		nc2.Close()
		ns2.Shutdown()
	}()

	js2, err := NewJetStream(nc2)
	if err != nil {
		t.Fatalf("NewJetStream(restart) error = %v", err)
	}
	stream, err := js2.Stream(ctx, EventsStreamName)
	if err != nil {
		t.Fatalf("js2.Stream() error = %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream.Info() error = %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("events stream message count = %d, want 1", info.State.Msgs)
	}
}

func startTestNATSServer(t *testing.T, storeDir string) *natsserver.Server {
	t.Helper()

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
		t.Fatalf("NewServer() error = %v", err)
	}

	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		t.Fatal("NATS server failed to become ready")
	}

	return ns
}

func connectTestNATS(t *testing.T, url string) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	return nc
}
