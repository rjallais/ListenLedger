package artist

import "ListenLedger/internal/eventsourcing"

// init registers the artist event payloads. The registry is the census of the
// event contract: cmd/audit uses it to flag stored events whose payload this
// binary cannot decode.
func init() {
	eventsourcing.RegisterPayload[CreatedPayload](EventTypeArtistCreated)
	eventsourcing.RegisterPayload[ListenersScrapedPayload](EventTypeArtistMonthlyListenersScraped)
	eventsourcing.RegisterPayload[StatusChangedPayload](EventTypeArtistStatusChanged)
	eventsourcing.RegisterPayload[FetchStatusChangedPayload](EventTypeArtistFetchStatusChanged)
	eventsourcing.RegisterPayload[CollectionSongsAdjustedPayload](EventTypeArtistCollectionSongsAdjusted)
}
