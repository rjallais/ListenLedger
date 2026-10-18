package scrapejob

import (
	"ListenLedger/internal/eventsourcing"
)

// Repository loads and persists Job aggregates.
type Repository struct {
	*eventsourcing.Repository[*Job]
}

// NewRepository creates a Repository.
func NewRepository(store eventsourcing.Store) *Repository {
	return &Repository{
		Repository: eventsourcing.NewRepository(store, StreamTypeScrapeJob, Replay),
	}
}
