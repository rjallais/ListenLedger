package album

import (
	"ListenLedger/internal/eventsourcing"
)

// Repository loads and persists album aggregates.
type Repository struct {
	*eventsourcing.Repository[*Album]
}

// NewRepository creates an album repository.
func NewRepository(store eventsourcing.Store) *Repository {
	return &Repository{
		Repository: eventsourcing.NewRepository(store, StreamTypeAlbum, Replay),
	}
}
