package correctionproposal

import (
	"ListenLedger/internal/eventsourcing"
)

// Repository loads and saves correctionproposal aggregates.
type Repository struct {
	*eventsourcing.Repository[*Proposal]
}

// NewRepository creates a new correctionproposal repository.
func NewRepository(store eventsourcing.Store) *Repository {
	return &Repository{
		Repository: eventsourcing.NewRepository(store, StreamTypeCorrectionProposal, Replay),
	}
}
