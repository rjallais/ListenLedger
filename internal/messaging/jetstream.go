package messaging

import (
	"context"
	"fmt"
	"strings"
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

	// DomainEventsStreamName is the durable JetStream stream for domain events
	// (domain.events.>). It is a catch-up buffer for new subscribers and
	// replays — not the system of record. The SQLite events table is the
	// permanent, unbounded log; this stream must always carry an explicit
	// MaxAge/MaxBytes so a runaway producer degrades to oldest-dropped
	// instead of filling the disk. See DomainEventsRetention.
	DomainEventsStreamName = "DOMAIN_EVENTS"

	// DomainEventsRetention bounds the DOMAIN_EVENTS log. At the observed rate
	// (~1800 events/day, ~150 bytes each) 90 days is ~162k events / ~32 MB,
	// well inside the 512 MB default file store. SQLite remains the permanent
	// log; JetStream is the catch-up buffer for new subscribers and replays.
	// SCRAPE_REQUESTS deliberately stays at 24h (WorkQueue semantics: rows
	// older than that are phantoms per queuedJobExpiry).
	DomainEventsRetention = 90 * 24 * time.Hour

	// DomainEventsMaxBytes caps the DOMAIN_EVENTS file store well above the
	// ~32 MB the 90-day retention implies at the observed rate. With
	// DiscardOld, breaching the cap drops the oldest messages first — safe
	// because nothing rebuilds state from this stream.
	DomainEventsMaxBytes = 256 * 1024 * 1024

	// DomainEventsTruthRetention/MaxBytes size DOMAIN_EVENTS as the authority
	// (Delaney flip). 3y/2GB at the observed rate (~1800 events/day) holds the
	// full history with headroom; DiscardOld still degrades to oldest-dropped
	// instead of filling the disk. Monitor stream bytes; grow before it fills.
	DomainEventsTruthRetention = 3 * 365 * 24 * time.Hour
	DomainEventsTruthMaxBytes  = 2 * 1024 * 1024 * 1024
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
	MaxBytes   int64
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
		MaxBytes:   sc.MaxBytes,
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

// EnsureDomainEventsStream creates or updates the durable DOMAIN_EVENTS stream
// for domain.events.> subjects (LimitsPolicy, 90d retention, 256MB cap with
// oldest-dropped overflow, file storage).
func EnsureDomainEventsStream(ctx context.Context, js jetstream.JetStream) error {
	return ensureStreamFromConfig(ctx, js, streamConfig{
		Name:       DomainEventsStreamName,
		Subjects:   []string{SubjectDomainEventsWildcard},
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     DomainEventsRetention,
		MaxMsgs:    1_000_000,
		MaxBytes:   DomainEventsMaxBytes,
		Duplicates: 10 * time.Minute,
	}, "domain events")
}

// EnsureDomainEventsStreamAsTruth creates or updates DOMAIN_EVENTS as the
// authority: same subjects, 3y retention + 2GB cap (see truth constants).
// CreateOrUpdate keeps existing messages. Called at every startup by
// ensureJetStreamStreams; SQLite remains the read source in Phase 1.
func EnsureDomainEventsStreamAsTruth(ctx context.Context, js jetstream.JetStream) error {
	return ensureStreamFromConfig(ctx, js, streamConfig{
		Name:       DomainEventsStreamName,
		Subjects:   []string{SubjectDomainEventsWildcard},
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     DomainEventsTruthRetention,
		MaxMsgs:    10_000_000,
		MaxBytes:   DomainEventsTruthMaxBytes,
		Duplicates: 10 * time.Minute,
	}, "domain events (truth)")
}

// PublishDomainEvent publishes one domain event to JetStream with the event ID
// as MsgID so redelivered projections stay idempotent (duplicate-acked).
func PublishDomainEvent(ctx context.Context, js jetstream.JetStream, subject string, eventID string, data []byte) (*jetstream.PubAck, error) {
	if strings.TrimSpace(subject) == "" {
		return nil, fmt.Errorf("publish domain event: empty subject")
	}
	var opts []jetstream.PublishOpt
	if strings.TrimSpace(eventID) != "" {
		opts = []jetstream.PublishOpt{jetstream.WithMsgID(eventID)}
	}
	ack, err := js.Publish(ctx, subject, data, opts...)
	if err != nil {
		return nil, fmt.Errorf("publish to subject %s failed: %w", subject, err)
	}
	return ack, nil
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

// ScrapeRequestMsgID returns a stable de-duplication ID per scrape request.
// The request_id is the saga idempotency key (see commands.Log): two publishes
// with the same request_id dedup within ScrapeRequestDedupWindow, while two
// different requests for the same artist do NOT dedup — double-click
// protection lives in the artist aggregate (fetch_status==pending), not here.
// Empty IDs return "" (publish without MsgID): a shared fallback ID would
// wrongly dedup distinct requests against each other within the window.
func ScrapeRequestMsgID(requestID string) string {
	if strings.TrimSpace(requestID) == "" {
		return ""
	}
	return "scrape.request:" + requestID
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
