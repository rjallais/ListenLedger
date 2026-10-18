package tidalsync

import (
	"ListenLedger/internal/eventsourcing"
)

// Repository loads and saves tidalsync aggregates.
type Repository struct {
	*eventsourcing.Repository[*Sync]
}

// NewRepository creates a new tidalsync repository.
func NewRepository(store eventsourcing.Store) *Repository {
	return &Repository{
		Repository: eventsourcing.NewRepository(store, StreamTypeTidalSync, Replay),
	}
}
