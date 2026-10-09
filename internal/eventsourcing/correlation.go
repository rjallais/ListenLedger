package eventsourcing

import (
	"strings"
)

// Correlation carries saga tracing IDs for event metadata. The requestID is
// the saga instance key (scrapejob stream ID); batchID groups batch refresh
// siblings. Stored in event metadata (never in the payload — payloads stay
// domain facts, metadata stays causation).
//
// All domain transition methods accept `corr ...Correlation` variadically so
// existing callers without saga context keep compiling; saga paths pass one.
type Correlation struct {
	RequestID string
	BatchID   string
}

// Metadata merges base metadata with correlation IDs. Empty IDs are omitted.
// Returns nil when both base and correlation are empty, preserving the
// previous nil-metadata behavior for non-saga events.
func (c Correlation) Metadata(base map[string]any) map[string]any {
	requestID := strings.TrimSpace(c.RequestID)
	batchID := strings.TrimSpace(c.BatchID)
	if requestID == "" && batchID == "" {
		if len(base) == 0 {
			return nil
		}
		return base
	}
	out := make(map[string]any, len(base)+2)
	for k, v := range base {
		out[k] = v
	}
	if requestID != "" {
		out["request_id"] = requestID
	}
	if batchID != "" {
		out["batch_id"] = batchID
	}
	return out
}

// FirstCorrelation returns the first non-empty correlation, or zero value.
// Saga call sites pass at most one; helpers accept variadics for compatibility.
func FirstCorrelation(corrs []Correlation) Correlation {
	for _, c := range corrs {
		if strings.TrimSpace(c.RequestID) != "" || strings.TrimSpace(c.BatchID) != "" {
			return c
		}
	}
	return Correlation{}
}

// RequestIDFromMetadata extracts the saga request_id from decoded metadata.
// Returns "" when absent — pre-correlation events simply have none.
func RequestIDFromMetadata(meta map[string]any) string {
	if meta == nil {
		return ""
	}
	if v, ok := meta["request_id"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

// DecodeMetadata converts an event's RON or JSON metadata into a string-keyed map.
// Mirrors DecodePayload but targets the envelope metadata field.
func DecodeMetadata(metadata []byte) (map[string]any, error) {
	if len(metadata) == 0 {
		return nil, nil
	}
	var out map[string]any
	if err := DecodePayload(metadata, &out); err != nil {
		return nil, err
	}
	return out, nil
}
