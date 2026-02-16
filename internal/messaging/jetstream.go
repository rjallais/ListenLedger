//go:build goexperiment.jsonv2

package messaging

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// ScrapeRequestsStreamName is the JetStream stream name for scrape jobs.
	ScrapeRequestsStreamName = "SCRAPE_REQUESTS"
	// ScrapeDLQStreamName is the JetStream stream name for failed/poison scrape jobs.
	ScrapeDLQStreamName = "SCRAPE_DLQ"
	// ScrapeRequestDedupWindow controls queue de-duplication for repeated refresh clicks.
	ScrapeRequestDedupWindow = 30 * time.Second
	// ScrapeWorkerConsumerName is the durable consumer name for scrape jobs.
	ScrapeWorkerConsumerName = "SCRAPE_WORKER"

	// EventsStreamName is the JetStream stream used for replayable domain events.
	EventsStreamName = "EVENTS"
)

// NewJetStream creates a JetStream context from an existing NATS connection.
func NewJetStream(nc *nats.Conn) (jetstream.JetStream, error) {
	return jetstream.New(nc)
}

// EnsureScrapeRequestStream creates or updates the scrape request stream.
func EnsureScrapeRequestStream(ctx context.Context, js jetstream.JetStream) error {
	cfg := jetstream.StreamConfig{
		Name:       ScrapeRequestsStreamName,
		Subjects:   []string{SubjectScrapeRequest},
		Retention:  jetstream.WorkQueuePolicy,
		Storage:    jetstream.FileStorage,
		Discard:    jetstream.DiscardOld,
		MaxAge:     24 * time.Hour,
		MaxMsgs:    100_000,
		Duplicates: ScrapeRequestDedupWindow,
	}

	if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("ensure scrape stream: %w", err)
	}
	return nil
}

// EnsureScrapeDLQStream creates or updates the dead-letter stream for scrape jobs.
func EnsureScrapeDLQStream(ctx context.Context, js jetstream.JetStream) error {
	cfg := jetstream.StreamConfig{
		Name:      ScrapeDLQStreamName,
		Subjects:  []string{SubjectScrapeDLQ},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		Discard:   jetstream.DiscardOld,
		MaxAge:    7 * 24 * time.Hour,
		MaxMsgs:   100_000,
	}

	if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("ensure scrape dlq stream: %w", err)
	}
	return nil
}

// EnsureEventsStream creates or updates the replayable EVENTS stream.
func EnsureEventsStream(ctx context.Context, js jetstream.JetStream) error {
	cfg := jetstream.StreamConfig{
		Name:       EventsStreamName,
		Subjects:   []string{SubjectArtistUpdated},
		Retention:  jetstream.LimitsPolicy,
		Storage:    jetstream.FileStorage,
		Discard:    jetstream.DiscardOld,
		MaxAge:     7 * 24 * time.Hour,
		MaxMsgs:    1_000_000,
		Duplicates: 10 * time.Minute,
	}

	if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("ensure events stream: %w", err)
	}
	return nil
}

// EnsureScrapeWorkerConsumer creates or updates the durable scrape worker consumer.
func EnsureScrapeWorkerConsumer(ctx context.Context, js jetstream.JetStream, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	stream, err := js.Stream(ctx, ScrapeRequestsStreamName)
	if err != nil {
		return nil, fmt.Errorf("get scrape stream: %w", err)
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("ensure scrape worker consumer: %w", err)
	}
	return consumer, nil
}

// ScrapeRequestMsgID returns a stable de-duplication ID per artist.
func ScrapeRequestMsgID(artistID string) string {
	return "scrape.request:" + artistID
}

// PublishScrapeRequested publishes a scrape request through JetStream with optional de-duplication.
func PublishScrapeRequested(ctx context.Context, js jetstream.JetStream, req ScrapeRequested, msgID string) (*jetstream.PubAck, error) {
	data, err := MarshalScrapeRequested(req)
	if err != nil {
		return nil, err
	}

	opts := make([]jetstream.PublishOpt, 0, 1)
	if msgID != "" {
		opts = append(opts, jetstream.WithMsgID(msgID))
	}

	ack, err := js.Publish(ctx, SubjectScrapeRequest, data, opts...)
	if err != nil {
		return nil, err
	}
	return ack, nil
}
