package artist

import (
	"ListenLedger/internal/eventsourcing"
)

// SnapshotInterval bounds artist stream replay: every Nth version stores a
// freeze-frame so loaders replay at most N-1 events after it. Artist streams
// are the only long-lived aggregates (one listener/status fact per scrape);
// scrapejob/batch/album/song streams stay tiny and replay directly.
// Exported for the audit tool's long-stream warning: snapshot degradation
// matters to the event-store flip (JetStream reads replay full streams).
const SnapshotInterval = 200

// Repository handles loading and persisting Artist aggregates.
type Repository struct {
	*eventsourcing.Repository[*Artist]
}

// NewRepository creates a new Artist repository with snapshot support.
func NewRepository(store eventsourcing.Store) *Repository {
	return &Repository{
		Repository: eventsourcing.NewRepository(
			store,
			StreamTypeArtist,
			Replay,
			eventsourcing.WithSnapshotCodec(eventsourcing.SnapshotCodec[*Artist]{
				Interval: SnapshotInterval,
				NewEmpty: func(id string) *Artist {
					return &Artist{
						BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeArtist),
					}
				},
				Marshal: func(agg *Artist) ([]byte, error) {
					return agg.marshalSnapshot()
				},
				Restore: func(agg *Artist, version int64, payload []byte) error {
					return agg.restoreFromSnapshot(version, payload)
				},
			}),
		),
	}
}
