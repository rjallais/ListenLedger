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

type streamConfig struct {
	Name       string
	Subjects   []string
	Retention  jetstream.RetentionPolicy
	MaxAge     time.Duration
	MaxMsgs    int64
	Duplicates time.Duration
}

func ensureStreamFromConfig(ctx context.Context, js jetstream.JetStream, sc streamConfig, displayName string) error {
	cfg := jetstream.StreamConfig{
		Name:       sc.Name,
		Subjects:   sc.Subjects,
		Retention:  sc.Retention,
		Storage:    jetstream.FileStorage,
		Discard:    jetstream.DiscardOld,
		MaxAge:     sc.MaxAge,
		MaxMsgs:    sc.MaxMsgs,
		Duplicates: sc.Duplicates,
	}
	if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("ensure %s stream: %w", displayName, err)
	}
	return nil
}

// EnsureScrapeRequestStream creates or updates the scrape request stream.
func EnsureScrapeRequestStream(ctx context.Context, js jetstream.JetStream) error {
	return ensureStreamFromConfig(ctx, js, streamConfig{
		Name:       ScrapeRequestsStreamName,
		Subjects:   []string{SubjectScrapeRequest, SubjectScrapeRequestWildcard},
		Retention:  jetstream.WorkQueuePolicy,
		MaxAge:     24 * time.Hour,
		MaxMsgs:    100_000,
		Duplicates: ScrapeRequestDedupWindow,
	}, "scrape")
}

// EnsureScrapeDLQStream creates or updates the dead-letter stream for scrape jobs.
func EnsureScrapeDLQStream(ctx context.Context, js jetstream.JetStream) error {
	return ensureStreamFromConfig(ctx, js, streamConfig{
		Name:      ScrapeDLQStreamName,
		Subjects:  []string{SubjectScrapeDLQ},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    7 * 24 * time.Hour,
		MaxMsgs:   100_000,
	}, "scrape dlq")
}

// EnsureEventsStream creates or updates the replayable EVENTS stream.
func EnsureEventsStream(ctx context.Context, js jetstream.JetStream) error {
	return ensureStreamFromConfig(ctx, js, streamConfig{
		Name:       EventsStreamName,
		Subjects:   []string{SubjectArtistUpdated},
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     7 * 24 * time.Hour,
		MaxMsgs:    1_000_000,
		Duplicates: 10 * time.Minute,
	}, "events")
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

type scrapePublishParams struct {
	JetStream jetstream.JetStream
	Request   ScrapeRequested
	MsgID     string
	Subject   string
}

 // PublishScrapeRequested publishes a scrape request through JetStream with optional de-duplication.
func PublishScrapeRequested(ctx context.Context, js jetstream.JetStream, req ScrapeRequested, msgID string) (*jetstream.PubAck, error) {
	return publishScrapeRequestedToSubject(ctx, scrapePublishParams{
		JetStream: js,
		Request:   req,
		MsgID:     msgID,
		Subject:   SubjectScrapeRequest,
	})
}

 // publishScrapeRequestedToSubject publishes a scrape request to a specific queue subject.
func publishScrapeRequestedToSubject(ctx context.Context, params scrapePublishParams) (*jetstream.PubAck, error) {
	data, err := MarshalScrapeRequested(params.Request)
	if err != nil {
		return nil, fmt.Errorf("marshal scrape request failed: %w", err)
	}

	subject := params.Subject
	if subject == "" {
		subject = SubjectScrapeRequest
	}

	var opts []jetstream.PublishOpt
	if params.MsgID != "" {
		opts = []jetstream.PublishOpt{jetstream.WithMsgID(params.MsgID)}
	}

	ack, err := params.JetStream.Publish(ctx, subject, data, opts...)
	if err != nil {
		return nil, fmt.Errorf("publish to subject %s failed: %w", subject, err)
	}
	return ack, nil
}
