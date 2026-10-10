package eventsourcing

import (
	"fmt"
	"sync"
)

// Event schema versions. Every event carries its writer schema version in
// metadata ("schema_version"); pre-versioning rows default to version 1.
// Bump CurrentSchemaVersion only when a payload shape changes, and register
// an upcaster chain so old rows still decode.
const (
	// SchemaVersionV1 is the initial schema for all event types.
	SchemaVersionV1 = 1
	// CurrentSchemaVersion is the writer version stamped by NewEvent.
	CurrentSchemaVersion = SchemaVersionV1
	// schemaVersionKey is the metadata key carrying the writer version.
	schemaVersionKey = "schema_version"
)

var (
	upcasterMu sync.RWMutex
	// upcasters maps eventType -> fromVersion -> migration to fromVersion+1.
	// Migrations apply in order until CurrentSchemaVersionFor is reached.
	upcasters = make(map[string]map[int]func([]byte) ([]byte, error))
	// currentVersions overrides per-event-type writer versions (default: CurrentSchemaVersion).
	currentVersions = make(map[string]int)
)

// RegisterUpcaster registers a migration from one schema version to the next
// for an event type. Migrations must be pure (no I/O) and idempotent-safe:
// they run on every decode of an older row.
func RegisterUpcaster(eventType string, fromVersion int, migrate func([]byte) ([]byte, error)) {
	if eventType == "" || fromVersion <= 0 || migrate == nil {
		return
	}
	upcasterMu.Lock()
	defer upcasterMu.Unlock()
	chain, ok := upcasters[eventType]
	if !ok {
		chain = make(map[int]func([]byte) ([]byte, error))
		upcasters[eventType] = chain
	}
	chain[fromVersion] = migrate
}

// CurrentSchemaVersionFor returns the writer version for an event type.
func CurrentSchemaVersionFor(eventType string) int {
	upcasterMu.RLock()
	defer upcasterMu.RUnlock()
	if v, ok := currentVersions[eventType]; ok {
		return v
	}
	return CurrentSchemaVersion
}

// SetCurrentSchemaVersionFor advances the writer version for an event type.
// Call it when shipping a payload shape change alongside its upcaster chain
// (and the reader support); kept exported so migrations are declarative.
func SetCurrentSchemaVersionFor(eventType string, version int) {
	if eventType == "" || version <= 0 {
		return
	}
	upcasterMu.Lock()
	defer upcasterMu.Unlock()
	currentVersions[eventType] = version
}

// SchemaVersionOf extracts the writer schema version from an event's metadata.
// Missing or unparsable versions default to V1 (pre-versioning rows).
func SchemaVersionOf(evt Event) int {
	meta, err := DecodeMetadata(evt.Metadata)
	if err != nil || meta == nil {
		return SchemaVersionV1
	}
	raw, ok := meta[schemaVersionKey]
	if !ok {
		return SchemaVersionV1
	}
	switch v := raw.(type) {
	case int:
		if v > 0 {
			return v
		}
	case int64:
		if v > 0 {
			return int(v)
		}
	case float64:
		if v > 0 {
			return int(v)
		}
	}
	return SchemaVersionV1
}

// UpcastPayload migrates a payload from its writer version to the current
// version for the event type, applying registered migrations in order.
// Unregistered gaps return an error (fail-fast, never silently misread).
func UpcastPayload(eventType string, fromVersion int, payload []byte) ([]byte, error) {
	target := CurrentSchemaVersionFor(eventType)
	if fromVersion >= target {
		return payload, nil
	}
	upcasterMu.RLock()
	chain := upcasters[eventType]
	upcasterMu.RUnlock()
	out := payload
	for v := fromVersion; v < target; v++ {
		upcasterMu.RLock()
		migrate := chain[v]
		upcasterMu.RUnlock()
		if migrate == nil {
			return nil, fmt.Errorf("no upcaster for %s v%d -> v%d", eventType, v, v+1)
		}
		migrated, err := migrate(out)
		if err != nil {
			return nil, fmt.Errorf("upcasting %s v%d -> v%d: %w", eventType, v, v+1, err)
		}
		out = migrated
	}
	return out, nil
}

// DecodeEventPayload is the version-aware decode path for aggregate and
// projection code: it resolves the writer version from metadata, upcasts to
// current, then decodes (RON or legacy JSON). New code must use this instead
// of DecodePayload on event payloads.
func DecodeEventPayload(evt Event, target any) error {
	version := SchemaVersionOf(evt)
	payload, err := UpcastPayload(evt.EventType, version, evt.Payload)
	if err != nil {
		return err
	}
	return DecodePayload(payload, target)
}

// stampSchemaVersion ensures map metadata carries the writer schema version
// for the event type. It copies the map so the caller's map is never
// mutated. Non-map metadata passes through untouched.
func stampSchemaVersion(eventType string, metadata any) any {
	version := CurrentSchemaVersionFor(eventType)
	if metadata == nil {
		return map[string]any{schemaVersionKey: version}
	}
	m, ok := metadata.(map[string]any)
	if !ok {
		return metadata
	}
	if m == nil {
		return map[string]any{schemaVersionKey: version}
	}
	out := make(map[string]any, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	if _, exists := out[schemaVersionKey]; !exists {
		out[schemaVersionKey] = version
	}
	return out
}
