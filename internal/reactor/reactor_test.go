package reactor

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
)

func startTestNATS(t *testing.T) (*natsserver.Server, string) {
	t.Helper()
	dir := t.TempDir()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoSigs:    true,
		NoLog:     true,
		JetStream: true,
		StoreDir:  filepath.Join(dir, "nats-store"),
	})
	if err != nil {
		t.Fatalf("nats server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		t.Fatal("NATS server not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
	})
	return ns, ns.ClientURL()
}

func ensureTestDomainEvents(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := messaging.EnsureDomainEventsStream(ctx, js); err != nil {
		t.Fatalf("ensure domain events stream: %v", err)
	}
	return js
}

func publishEvent(t *testing.T, js jetstream.JetStream, evt eventsourcing.Event) {
	t.Helper()
	data, err := evt.Bytes()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	subject := messaging.SubjectDomainEvent(evt.StreamType, evt.StreamID, evt.EventType)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := messaging.PublishDomainEvent(ctx, js, subject, evt.ID, data); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting: %s", msg)
}

// TestReactorReceivesAndAcksEvent delivers one domain event through the
// durable consumer and asserts the handler sees it exactly once and acks it
// (no redelivery after the ack window).
func TestReactorReceivesAndAcksEvent(t *testing.T) {
	_, url := startTestNATS(t)
	js := ensureTestDomainEvents(t, url)

	received := make(chan eventsourcing.Event, 1)
	var mu sync.Mutex
	count := 0

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	r, err := New(slog.Default(), js, Config{
		Name:    "test-reactor-ack",
		Filters: []string{messaging.SubjectDomainEventsWildcard},
	}, func(_ context.Context, evt eventsourcing.Event) error {
		mu.Lock()
		count++
		mu.Unlock()
		received <- evt
		return nil
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	go func() { _ = r.Run(ctx) }()

	evt, err := eventsourcing.NewEvent("ar_t1", "artist", 1, "ArtistCreated",
		map[string]string{"name": "Tool"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	publishEvent(t, js, evt)

	select {
	case got := <-received:
		if got.ID != evt.ID {
			t.Fatalf("received event id %s, want %s", got.ID, evt.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never received the event")
	}

	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count == 1
	}, "event should be handled exactly once")
}

// TestReactorErrUnprocessableTermsDelivery asserts a handler returning
// ErrUnprocessable terminates delivery: the message is not redelivered.
func TestReactorErrUnprocessableTermsDelivery(t *testing.T) {
	_, url := startTestNATS(t)
	js := ensureTestDomainEvents(t, url)

	var mu sync.Mutex
	count := 0

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	r, err := New(slog.Default(), js, Config{
		Name:    "test-reactor-term",
		Filters: []string{messaging.SubjectDomainEventsWildcard},
	}, func(_ context.Context, evt eventsourcing.Event) error {
		mu.Lock()
		count++
		mu.Unlock()
		return ErrUnprocessable
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	go func() { _ = r.Run(ctx) }()

	evt, err := eventsourcing.NewEvent("ar_t2", "artist", 1, "ArtistCreated",
		map[string]string{"name": "Tool"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	publishEvent(t, js, evt)

	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count >= 1
	}, "handler should be invoked at least once")

	// After term, no further redelivery should arrive for a while.
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("handler invoked %d times after ErrUnprocessable, want 1", count)
	}
}

// TestReactorNakRedelivers asserts a handler returning a plain error naks
// with delay and the message is redelivered.
func TestReactorNakRedelivers(t *testing.T) {
	_, url := startTestNATS(t)
	js := ensureTestDomainEvents(t, url)

	var mu sync.Mutex
	count := 0
	boom := errors.New("transient")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	r, err := New(slog.Default(), js, Config{
		Name:    "test-reactor-nak",
		Filters: []string{messaging.SubjectDomainEventsWildcard},
	}, func(_ context.Context, evt eventsourcing.Event) error {
		mu.Lock()
		count++
		mu.Unlock()
		return boom
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	go func() { _ = r.Run(ctx) }()

	evt, err := eventsourcing.NewEvent("ar_t3", "artist", 1, "ArtistCreated",
		map[string]string{"name": "Tool"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	publishEvent(t, js, evt)

	waitFor(t, 8*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count >= 2
	}, "naked message should be redelivered at least twice")
}

func TestReactorStampsGlobalPositionAndMultiStepBackOff(t *testing.T) {
	_, url := startTestNATS(t)
	js := ensureTestDomainEvents(t, url)

	var (
		mu        sync.Mutex
		positions []int64
	)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	r, err := New(slog.Default(), js, Config{
		Name:    "test-reactor-backoff",
		Filters: []string{messaging.SubjectDomainEventsWildcard},
		BackOff: []time.Duration{50 * time.Millisecond, 100 * time.Millisecond},
	}, func(_ context.Context, evt eventsourcing.Event) error {
		mu.Lock()
		positions = append(positions, evt.GlobalPosition)
		n := len(positions)
		mu.Unlock()
		if n < 3 {
			return errors.New("retry")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	go func() { _ = r.Run(ctx) }()

	evt, err := eventsourcing.NewEvent("ar_t4", "artist", 1, "ArtistCreated",
		map[string]string{"name": "Tool"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	publishEvent(t, js, evt)

	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(positions) == 3
	}, "reactor should retry through BackOff steps and succeed on 3rd attempt")

	mu.Lock()
	defer mu.Unlock()
	for i, pos := range positions {
		if pos != 1 {
			t.Fatalf("positions[%d] = %d, want stream sequence 1", i, pos)
		}
	}
}

