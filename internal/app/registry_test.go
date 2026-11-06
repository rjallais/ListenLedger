package app

import (
	"testing"

	_ "ListenLedger/internal/domain/album"
	_ "ListenLedger/internal/domain/artist"
	_ "ListenLedger/internal/domain/batch"
	_ "ListenLedger/internal/domain/correctionproposal"
	_ "ListenLedger/internal/domain/scrapejob"
	_ "ListenLedger/internal/domain/song"
	_ "ListenLedger/internal/domain/tidalsync"
	"ListenLedger/internal/eventsourcing"
)

func TestValidatePayloadRegistryProduction(t *testing.T) {
	if err := eventsourcing.ValidatePayloadRegistry(); err != nil {
		t.Fatalf("ValidatePayloadRegistry() = %v, want nil (production boot would fail)", err)
	}
}
