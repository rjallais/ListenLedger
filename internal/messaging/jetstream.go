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
	// ScrapeWorkerBrowserlessConsumerName is the durable consumer for Browserless jobs.
	ScrapeWorkerBrowserlessConsumerName = "SCRAPE_WORKER_BROWSERLESS"
	// ScrapeWorkerScrapingAntConsumerName is the durable consumer for ScrapingAnt jobs.
	ScrapeWorkerScrapingAntConsumerName = "SCRAPE_WORKER_SCRAPINGANT"
	// ScrapeWorkerScraperAPIConsumerName is the durable consumer for ScraperAPI jobs.
	ScrapeWorkerScraperAPIConsumerName = "SCRAPE_WORKER_SCRAPERAPI"
	// ScrapeWorkerApifyConsumerName is the durable consumer for Apify jobs.
	ScrapeWorkerApifyConsumerName = "SCRAPE_WORKER_APIFY"
	// ScrapeWorkerLocalConsumerName is the durable consumer for local headless jobs.
	ScrapeWorkerLocalConsumerName = "SCRAPE_WORKER_LOCAL"

	// EventsStreamName is the JetStream stream used for replayable domain events.
	EventsStreamName = "EVENTS"
)

// ScrapeWorkerConsumerNames returns all known scrape consumer durables.
func ScrapeWorkerConsumerNames() []string {
	return []string{
		ScrapeWorkerConsumerName,
		ScrapeWorkerBrowserlessConsumerName,
		ScrapeWorkerScrapingAntConsumerName,
		ScrapeWorkerScraperAPIConsumerName,
		ScrapeWorkerApifyConsumerName,
		ScrapeWorkerLocalConsumerName,
	}
}

// NewJetStream creates a JetStream context from an existing NATS connection.
func NewJetStream(nc *nats.Conn) (jetstream.JetStream, error) {
	return jetstream.New(nc)
}

func ensureStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig, streamName string) error {
	if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("ensure %s stream: %w", streamName, err)
	}
	return nil
}

// EnsureScrapeRequestStream creates or updates the scrape request stream.
func EnsureScrapeRequestStream(ctx context.Context, js jetstream.JetStream) error {
	cfg := jetstream.StreamConfig{
		Name:       ScrapeRequestsStreamName,
		Subjects:   []string{SubjectScrapeRequest, SubjectScrapeRequestWildcard},
		Retention:  jetstream.WorkQueuePolicy,
		Storage:    jetstream.FileStorage,
		Discard:    jetstream.DiscardOld,
		MaxAge:     24 * time.Hour,
		MaxMsgs:    100_000,
		Duplicates: ScrapeRequestDedupWindow,
	}
	return ensureStream(ctx, js, cfg, "scrape")
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
	return ensureStream(ctx, js, cfg, "scrape dlq")
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
	return ensureStream(ctx, js, cfg, "events")
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
	return PublishScrapeRequestedToSubject(ctx, js, req, msgID, SubjectScrapeRequest)
}

func normalizeScrapeSubject(subject string) string {
	if subject == "" {
		return SubjectScrapeRequest
	}
	return subject
}

func publishOpts(msgID string) []jetstream.PublishOpt {
	if msgID == "" {
		return nil
	}
	return []jetstream.PublishOpt{jetstream.WithMsgID(msgID)}
}

// PublishScrapeRequestedToSubject publishes a scrape request to a specific queue subject.
func PublishScrapeRequestedToSubject(ctx context.Context, js jetstream.JetStream, req ScrapeRequested, msgID, subject string) (*jetstream.PubAck, error) {
	data, err := MarshalScrapeRequested(req)
	if err != nil {
		return nil, err
	}

	subject = normalizeScrapeSubject(subject)
	opts := publishOpts(msgID)

	ack, err := js.Publish(ctx, subject, data, opts...)
	if err != nil {
		return nil, err
	}
	return ack, nil
}
