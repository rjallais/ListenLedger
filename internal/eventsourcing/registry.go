package eventsourcing

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"
)

// The payload registry is the census of the event contract: event type ->
// payload struct. Domain packages register their payloads from init, so any
// binary that imports them can decode stored events without knowing the
// concrete type at the call site, and cmd/audit can flag stored events whose
// payload the binary cannot read.
//
// The registry is always present. An empty registry is a registry with no
// entries, never a nil check at each call site.

// payloadEntry is one registered event payload type.
type payloadEntry struct {
	// ptrType is the pointer type of the payload struct, used for reporting.
	ptrType reflect.Type
	// newPtr allocates a zero payload for decoding.
	newPtr func() any
}

var (
	payloadRegistryMu sync.RWMutex
	payloadRegistry   = make(map[string]payloadEntry)
	payloadTypeToName = make(map[reflect.Type]string)
)

// RegisterPayload registers the payload struct for an event type. Domain
// packages call it from init.
//
// A conflicting re-registration (same event type, different struct) and a
// non-struct payload both panic: each is a programming error, and failing
// during init keeps a broken event contract out of the log. Re-registering
// the identical type is a no-op, so a package imported twice stays safe.
func RegisterPayload[T any](eventType string) {
	typ := reflect.TypeFor[T]()
	if typ.Kind() != reflect.Struct {
		panic(fmt.Sprintf("eventsourcing: RegisterPayload[%s] for %q: payload must be a struct", typ, eventType))
	}
	if eventType == "" {
		panic("eventsourcing: RegisterPayload: event type cannot be empty")
	}

	entry := payloadEntry{
		ptrType: reflect.PointerTo(typ),
		newPtr:  func() any { return new(T) },
	}

	payloadRegistryMu.Lock()
	defer payloadRegistryMu.Unlock()
	if existing, ok := payloadRegistry[eventType]; ok {
		if existing.ptrType != entry.ptrType {
			panic(fmt.Sprintf("eventsourcing: event type %q is registered to %s, cannot re-register to %s",
				eventType, existing.ptrType, entry.ptrType))
		}
		return
	}
	if other, ok := payloadTypeToName[typ]; ok && other != eventType {
		panic(fmt.Sprintf("eventsourcing: payload %s is registered to %q, cannot also register to %q (RecordPayload inference would be ambiguous)",
			typ, other, eventType))
	}
	payloadRegistry[eventType] = entry
	payloadTypeToName[typ] = eventType
	payloadTypeToName[entry.ptrType] = eventType
}

// PayloadTypeOf returns the registered payload pointer type for an event type.
func PayloadTypeOf(eventType string) (reflect.Type, bool) {
	payloadRegistryMu.RLock()
	defer payloadRegistryMu.RUnlock()
	entry, ok := payloadRegistry[eventType]
	if !ok {
		return nil, false
	}
	return entry.ptrType, true
}

// EventTypeOf returns the registered event type name for a payload struct or
// struct pointer (Rita-style reverse type inference).
func EventTypeOf(payload any) (string, bool) {
	if payload == nil {
		return "", false
	}
	typ := reflect.TypeOf(payload)
	payloadRegistryMu.RLock()
	defer payloadRegistryMu.RUnlock()
	name, ok := payloadTypeToName[typ]
	return name, ok
}

// RegisteredEventTypes returns the event types that have a registered
// payload, sorted for stable reporting.
func RegisteredEventTypes() []string {
	payloadRegistryMu.RLock()
	types := make([]string, 0, len(payloadRegistry))
	for eventType := range payloadRegistry {
		types = append(types, eventType)
	}
	payloadRegistryMu.RUnlock()

	slices.Sort(types)
	return types
}

// DecodeRegisteredPayload decodes an event into a new instance of its
// registered payload type, applying schema upcasters. It returns
// ErrPayloadNotRegistered for an event type the registry does not know.
func DecodeRegisteredPayload(evt Event) (any, error) {
	payloadRegistryMu.RLock()
	entry, ok := payloadRegistry[evt.EventType]
	payloadRegistryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPayloadNotRegistered, evt.EventType)
	}

	target := entry.newPtr()
	if err := DecodeEventPayload(evt, target); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", evt.EventType, err)
	}
	return target, nil
}

// EventTypesOf returns the distinct event types carried by events, sorted.
// Reporting and tests use it to assert contract coverage.
func EventTypesOf(events []Event) []string {
	seen := make(map[string]struct{}, len(events))
	for _, evt := range events {
		seen[evt.EventType] = struct{}{}
	}
	return slices.Sorted(maps.Keys(seen))
}

// ValidatePayloadRegistry round-trips every registered payload through the
// codec. A payload the codec cannot marshal or read fails at boot instead of
// on the first read of a stream that carries it.
func ValidatePayloadRegistry() error {
	var errs []error
	for _, eventType := range RegisteredEventTypes() {
		typ, ok := PayloadTypeOf(eventType)
		if !ok {
			continue
		}
		zero := reflect.New(typ.Elem()).Interface()
		evt, err := NewEvent("registry_validation", "registry_validation", 1, eventType, zero, nil)
		if err != nil {
			errs = append(errs, fmt.Errorf("marshaling %s: %w", eventType, err))
			continue
		}
		evt.Data = nil
		if _, err := DecodeRegisteredPayload(evt); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
