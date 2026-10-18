package batch

import "ListenLedger/internal/eventsourcing"

// init registers the batch event payloads. The registry is the census of the
// event contract: cmd/audit uses it to flag stored events whose payload this
// binary cannot decode.
func init() {
	eventsourcing.RegisterPayload[StartedPayload](EventTypeBatchStarted)
	eventsourcing.RegisterPayload[ArtistCompletedPayload](EventTypeBatchArtistCompleted)
	eventsourcing.RegisterPayload[CompletedPayload](EventTypeBatchCompleted)
}
