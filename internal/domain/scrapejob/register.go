package scrapejob

import "ListenLedger/internal/eventsourcing"

// init registers the scrape job event payloads. The registry is the census of
// the event contract: cmd/audit uses it to flag stored events whose payload
// this binary cannot decode.
func init() {
	eventsourcing.RegisterPayload[RequestedPayload](EventTypeScrapeRequested)
	eventsourcing.RegisterPayload[StartedPayload](EventTypeScrapeStarted)
	eventsourcing.RegisterPayload[SucceededPayload](EventTypeScrapeSucceeded)
	eventsourcing.RegisterPayload[FailedPayload](EventTypeScrapeFailed)
	eventsourcing.RegisterPayload[DeadLetteredPayload](EventTypeScrapeDeadLettered)
}
