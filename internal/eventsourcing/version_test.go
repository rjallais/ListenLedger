package eventsourcing_test

import (
	"bytes"
	"testing"

	"ListenLedger/internal/eventsourcing"
)

func TestNewEventStampsSchemaVersion(t *testing.T) {
	evt, err := eventsourcing.NewEvent("s1", "artist", 1, "ArtistCreated", map[string]string{"name": "Tool"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if got := eventsourcing.SchemaVersionOf(evt); got != eventsourcing.SchemaVersionV1 {
		t.Fatalf("SchemaVersionOf = %d, want %d", got, eventsourcing.SchemaVersionV1)
	}
}

func TestSchemaVersionOfLegacyDefaultsToV1(t *testing.T) {
	evt := eventsourcing.Event{ID: "e1", StreamID: "s1", EventType: "ArtistCreated"}
	if got := eventsourcing.SchemaVersionOf(evt); got != eventsourcing.SchemaVersionV1 {
		t.Fatalf("legacy SchemaVersionOf = %d, want %d", got, eventsourcing.SchemaVersionV1)
	}
}

func TestUpcastChainAndDecode(t *testing.T) {
	const eventType = "VersionTestMigrated"
	// v1 payload uses "title"; v2 readers expect "name".
	eventsourcing.RegisterUpcaster(eventType, 1, func(payload []byte) ([]byte, error) {
		return bytes.Replace(payload, []byte("title"), []byte("name"), 1), nil
	})
	eventsourcing.SetCurrentSchemaVersionFor(eventType, 2)

	legacy := eventsourcing.Event{
		ID:        "e1",
		StreamID:  "s1",
		EventType: eventType,
		Version:   1,
		Payload:   []byte(`{"title":"Powerslave"}`),
		Metadata:  []byte(`{}`),
	}
	var out struct {
		Name string `json:"name"`
	}
	if err := eventsourcing.DecodeEventPayload(legacy, &out); err != nil {
		t.Fatalf("DecodeEventPayload: %v", err)
	}
	if out.Name != "Powerslave" {
		t.Fatalf("upcast name = %q, want Powerslave", out.Name)
	}
}

func TestUpcastMissingMigrationFailsFast(t *testing.T) {
	// Version 0 predates versioning with no registered chain: fail fast
	// rather than silently misreading.
	if _, err := eventsourcing.UpcastPayload("VersionTestNoMigration", 0, []byte(`{}`)); err == nil {
		t.Fatal("UpcastPayload with version gap and no migration should error")
	}
}
