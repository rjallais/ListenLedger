package tidalsync

import "ListenLedger/internal/eventsourcing"

// init registers the TIDAL sync event payloads. The registry is the census of
// the event contract: cmd/audit uses it to flag stored events whose payload
// this binary cannot decode.
func init() {
	eventsourcing.RegisterPayload[StartedPayload](EventTypeTidalSyncStarted)
	eventsourcing.RegisterPayload[FavoritesObservedPayload](EventTypeTidalFavoritesObserved)
	eventsourcing.RegisterPayload[CompletedPayload](EventTypeTidalSyncCompleted)
	eventsourcing.RegisterPayload[FailedPayload](EventTypeTidalSyncFailed)
}
