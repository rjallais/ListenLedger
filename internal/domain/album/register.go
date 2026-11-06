package album

import "ListenLedger/internal/eventsourcing"

// init registers the album event payloads. The registry is the census of the
// event contract: cmd/audit uses it to flag stored events whose payload this
// binary cannot decode.
func init() {
	eventsourcing.RegisterPayload[CreatedPayload](EventTypeAlbumCreated)
	eventsourcing.RegisterPayload[StatusChangedPayload](EventTypeAlbumStatusChanged)
	eventsourcing.RegisterPayload[SongCountsAdjustedPayload](EventTypeAlbumSongCountsAdjusted)
}
