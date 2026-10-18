package song

import (
	"ListenLedger/internal/eventsourcing"
)

// Repository loads and persists song aggregates.
type Repository struct {
	*eventsourcing.Repository[*Song]
}

// NewRepository creates a song repository.
func NewRepository(store eventsourcing.Store) *Repository {
	return &Repository{
		Repository: eventsourcing.NewRepository(store, StreamTypeSong, Replay),
	}
}
