package song

import "ListenLedger/internal/eventsourcing"

// init registers the song event payloads. The registry is the census of the
// event contract: cmd/audit uses it to flag stored events whose payload this
// binary cannot decode.
func init() {
	eventsourcing.RegisterPayload[CreatedPayload](EventTypeSongCreated)
	eventsourcing.RegisterPayload[RecentFlagChangedPayload](EventTypeRecentFlagChanged)
	eventsourcing.RegisterPayload[MetadataEnrichedPayload](EventTypeSongMetadataEnriched)
	eventsourcing.RegisterPayload[TidalIDLinkedPayload](EventTypeTidalIDLinked)
	eventsourcing.RegisterPayload[MetadataCorrectedPayload](EventTypeSongMetadataCorrected)
	eventsourcing.RegisterPayload[ArtistCreditsCorrectedPayload](EventTypeSongArtistCreditsCorrected)
	eventsourcing.RegisterPayload[MarkedDuplicatePayload](EventTypeSongMarkedDuplicate)
}
