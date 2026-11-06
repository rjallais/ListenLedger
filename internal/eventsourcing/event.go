package eventsourcing

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/starfederation/ron-go"
)

// NATS message headers carrying the domain event envelope (Rita-style header
// envelope). Readers inspect these headers in O(1) for tip/version/parity
// verification without decoding the message body, falling back to
// UnmarshalEvent for pre-header messages.
const (
	HeaderStreamID      = "LL-Stream-ID"
	HeaderStreamType    = "LL-Stream-Type"
	HeaderVersion       = "LL-Version"
	HeaderEventType     = "LL-Event-Type"
	HeaderCreatedAt     = "LL-Created-At"
	HeaderSchemaVersion = "LL-Schema-Version"
)

// Event represents an immutable domain event envelope.
type Event struct {
	ID             string    `json:"id"`
	GlobalPosition int64     `json:"global_position,omitempty"`
	StreamID       string    `json:"stream_id"`
	StreamType     string    `json:"stream_type"`
	Version        int64     `json:"version"`
	EventType      string    `json:"event_type"`
	Payload        []byte    `json:"payload"`
	Metadata       []byte    `json:"metadata"`
	CreatedAt      time.Time `json:"created_at"`
	// Data holds the in-memory typed payload when constructed via NewEvent /
	// RecordThat or pre-decoded by the registry, avoiding redundant RON/JSON
	// unmarshaling during in-process command folding and projection.
	Data any `json:"-" ron:"-"`
}

// SetEventHeaders stamps the domain event's envelope metadata into NATS
// message headers so JetStream consumers and tip checks can inspect identity
// and version without decoding the RON body.
func SetEventHeaders(h nats.Header, evt Event) {
	if h == nil {
		return
	}
	if evt.ID != "" {
		h.Set(jetstream.MsgIDHeader, evt.ID)
	}
	if evt.StreamID != "" {
		h.Set(HeaderStreamID, evt.StreamID)
	}
	if evt.StreamType != "" {
		h.Set(HeaderStreamType, evt.StreamType)
	}
	if evt.Version > 0 {
		h.Set(HeaderVersion, strconv.FormatInt(evt.Version, 10))
	}
	if evt.EventType != "" {
		h.Set(HeaderEventType, evt.EventType)
	}
	if !evt.CreatedAt.IsZero() {
		h.Set(HeaderCreatedAt, evt.CreatedAt.UTC().Format(time.RFC3339Nano))
	}
	h.Set(HeaderSchemaVersion, strconv.Itoa(CurrentSchemaVersionFor(evt.EventType)))
}

// EventHeaderMeta extracts the envelope identity and version from NATS
// message headers in O(1). It returns ok=false when headers are absent
// (pre-header messages) or malformed so callers fall back to UnmarshalEvent.
func EventHeaderMeta(h nats.Header) (id, streamID, streamType, eventType string, version int64, ok bool) {
	if len(h) == 0 {
		return "", "", "", "", 0, false
	}
	vStr := h.Get(HeaderVersion)
	if vStr == "" {
		return "", "", "", "", 0, false
	}
	v, err := strconv.ParseInt(vStr, 10, 64)
	if err != nil || v <= 0 {
		return "", "", "", "", 0, false
	}
	id = h.Get(jetstream.MsgIDHeader)
	streamID = h.Get(HeaderStreamID)
	streamType = h.Get(HeaderStreamType)
	eventType = h.Get(HeaderEventType)
	if id == "" || streamID == "" || streamType == "" || eventType == "" {
		return "", "", "", "", 0, false
	}
	return id, streamID, streamType, eventType, v, true
}

// NewEvent creates an event envelope with generated ID and RON payload.
// When eventType is empty, it is inferred from the payload's registered type.
// The writer schema version is stamped into map metadata ("schema_version",
// defaulting pre-versioning readers to v1); see version.go.
func NewEvent(streamID, streamType string, version int64, eventType string, payload any, metadata any) (Event, error) {
	return newEvent(UUIDIDGen{}, WallClock{}, streamID, streamType, version, eventType, payload, metadata)
}

// newEvent stamps identity and time from the injected edges. Aggregates pass
// their configured IDGen and Clock so tests can pin IDs and timestamps.
func newEvent(idgen IDGen, clock Clock, streamID, streamType string, version int64, eventType string, payload any, metadata any) (Event, error) {
	if eventType == "" {
		inferred, ok := EventTypeOf(payload)
		if !ok {
			return Event{}, fmt.Errorf("%w: cannot infer event type from %T", ErrPayloadNotRegistered, payload)
		}
		eventType = inferred
	}
	metadata = stampSchemaVersion(eventType, metadata)
	payloadBytes, err := ron.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("marshaling event payload as RON: %w", err)
	}

	metadataBytes, err := ron.Marshal(metadata)
	if err != nil {
		return Event{}, fmt.Errorf("marshaling event metadata as RON: %w", err)
	}

	return Event{
		ID:         idgen.NewEventID(),
		StreamID:   streamID,
		StreamType: streamType,
		Version:    version,
		EventType:  eventType,
		Payload:    payloadBytes,
		Metadata:   metadataBytes,
		CreatedAt:  clock.Now(),
		Data:       payload,
	}, nil
}

// DecodePayload converts an event's RON or legacy JSON payload into a Go value.
func DecodePayload(payload []byte, target any) error {
	jsonPayload, err := eventPayloadJSON(payload)
	if err != nil {
		return err
	}
	if bytes.Contains(jsonPayload, []byte(`"#utc"`)) {
		jsonPayload, err = normalizeRONTimeValues(jsonPayload)
		if err != nil {
			return fmt.Errorf("normalizing RON event time values: %w", err)
		}
	}
	if err := json.Unmarshal(jsonPayload, target); err != nil {
		return fmt.Errorf("decoding event payload: %w", err)
	}
	return nil
}

func eventPayloadJSON(payload []byte) ([]byte, error) {
	if jsontext.Value(payload).IsValid() {
		return payload, nil
	}
	jsonPayload, err := ron.ToJSON(payload, ron.Mode(ron.Compact))
	if err != nil {
		return nil, fmt.Errorf("converting RON event payload to JSON: %w", err)
	}
	return jsonPayload, nil
}

func normalizeRONTimeValues(payload []byte) ([]byte, error) {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return payload, nil
	}

	switch payload[0] {
	case '{':
		return normalizeRONTimeObject(payload)
	case '[':
		return normalizeRONTimeArray(payload)
	default:
		return payload, nil
	}
}

func normalizeRONTimeObject(payload []byte) ([]byte, error) {
	var members map[string]jsontext.Value
	if err := json.Unmarshal(payload, &members); err != nil {
		return nil, err
	}
	if instant, ok := ronInstant(members); ok {
		return instant, nil
	}
	for key, value := range members {
		normalized, err := normalizeRONTimeValues(value)
		if err != nil {
			return nil, fmt.Errorf("normalizing member %q: %w", key, err)
		}
		members[key] = normalized
	}
	return json.Marshal(members)
}

func ronInstant(members map[string]jsontext.Value) (jsontext.Value, bool) {
	if len(members) != 1 {
		return nil, false
	}
	instant, ok := members["#utc"]
	return instant, ok
}

func normalizeRONTimeArray(payload []byte) ([]byte, error) {
	var values []jsontext.Value
	if err := json.Unmarshal(payload, &values); err != nil {
		return nil, err
	}
	for index, value := range values {
		normalized, err := normalizeRONTimeValues(value)
		if err != nil {
			return nil, fmt.Errorf("normalizing element %d: %w", index, err)
		}
		values[index] = normalized
	}
	return json.Marshal(values)
}

// Bytes serializes the event envelope as RON, embedding payload and metadata as values.
func (e Event) Bytes() ([]byte, error) {
	e.Data = nil // in-memory only; never part of the wire envelope
	data, err := ron.Marshal(eventEnvelope{
		ID:         e.ID,
		StreamID:   e.StreamID,
		StreamType: e.StreamType,
		Version:    e.Version,
		EventType:  e.EventType,
		Payload:    e.Payload,
		Metadata:   e.Metadata,
		CreatedAt:  e.CreatedAt,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling event envelope as RON: %w", err)
	}
	return data, nil
}

// UnmarshalEvent decodes an event envelope serialized by Event.Bytes.
// Envelopes are RON; payload/metadata stay opaque encoded values.
func UnmarshalEvent(data []byte) (Event, error) {
	jsonData, err := ron.ToJSON(data, ron.Mode(ron.Compact))
	if err != nil {
		return Event{}, fmt.Errorf("converting event envelope to JSON: %w", err)
	}
	var env struct {
		ID         string         `json:"id"`
		StreamID   string         `json:"stream_id"`
		StreamType string         `json:"stream_type"`
		Version    int64          `json:"version"`
		EventType  string         `json:"event_type"`
		Payload    jsontext.Value `json:"payload"`
		Metadata   jsontext.Value `json:"metadata"`
		CreatedAt  jsontext.Value `json:"created_at"`
	}
	if err := json.Unmarshal(jsonData, &env); err != nil {
		return Event{}, fmt.Errorf("unmarshaling event envelope: %w", err)
	}
	// RON instants arrive as {"#utc": ...} objects; reuse the payload
	// normalizer so CreatedAt decodes to a plain RFC3339 string first.
	createdAtJSON, err := normalizeRONTimeValues(env.CreatedAt)
	if err != nil {
		return Event{}, fmt.Errorf("normalizing event created_at: %w", err)
	}
	var createdAt time.Time
	if err := json.Unmarshal(createdAtJSON, &createdAt); err != nil {
		return Event{}, fmt.Errorf("decoding event created_at: %w", err)
	}
	return Event{
		ID:         env.ID,
		StreamID:   env.StreamID,
		StreamType: env.StreamType,
		Version:    env.Version,
		EventType:  env.EventType,
		Payload:    []byte(env.Payload),
		Metadata:   []byte(env.Metadata),
		CreatedAt:  createdAt,
	}, nil
}

type eventEnvelope struct {
	ID         string    `json:"id"`
	StreamID   string    `json:"stream_id"`
	StreamType string    `json:"stream_type"`
	Version    int64     `json:"version"`
	EventType  string    `json:"event_type"`
	Payload    ronValue  `json:"payload"`
	Metadata   ronValue  `json:"metadata"`
	CreatedAt  time.Time `json:"created_at"`
}

type ronValue []byte

// MarshalJSON lets ron.Marshal embed already encoded values instead of
// base64-encoding them. ron.Marshal parses Marshaler output as JSON, so this
// must return JSON: RON payloads are converted via ron.ToJSON, while
// migration-backfilled rows already carry legacy JSON and pass through as-is
// (previously ron.ToJSON choked on them, failing the whole relay batch).
func (v ronValue) MarshalJSON() ([]byte, error) {
	if jsontext.Value(v).IsValid() {
		return v, nil
	}
	data, err := ron.ToJSON(v, ron.Mode(ron.Compact))
	if err != nil {
		return nil, fmt.Errorf("converting embedded RON value to JSON: %w", err)
	}
	return data, nil
}
