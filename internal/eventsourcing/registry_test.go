package eventsourcing

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// registryPayload is a stand-in payload for registry tests.
type registryPayload struct {
	ArtistID string    `json:"artist_id"`
	Count    int64     `json:"count"`
	At       time.Time `json:"at"`
}

// badPayload cannot be round-tripped through the codec.
type badPayload struct {
	Handler func() `json:"handler"`
}

// swapPayloadRegistry isolates a test from registrations made by other tests
// and by init, restoring the previous registry when the test ends.
func swapPayloadRegistry(t *testing.T) {
	t.Helper()
	payloadRegistryMu.Lock()
	saved := payloadRegistry
	savedNames := payloadTypeToName
	payloadRegistry = make(map[string]payloadEntry)
	payloadTypeToName = make(map[reflect.Type]string)
	payloadRegistryMu.Unlock()
	t.Cleanup(func() {
		payloadRegistryMu.Lock()
		payloadRegistry = saved
		payloadTypeToName = savedNames
		payloadRegistryMu.Unlock()
	})
}

func TestRegisterPayloadDecodesRegisteredType(t *testing.T) {
	swapPayloadRegistry(t)
	RegisterPayload[registryPayload]("TestRegistryDecode")

	evt, err := NewEvent("artist_1", "artist", 1, "TestRegistryDecode",
		registryPayload{ArtistID: "artist_1", Count: 7}, nil)
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}

	decoded, err := DecodeRegisteredPayload(evt)
	if err != nil {
		t.Fatalf("DecodeRegisteredPayload() error = %v", err)
	}
	got, ok := decoded.(*registryPayload)
	if !ok {
		t.Fatalf("DecodeRegisteredPayload() returned %T, want *registryPayload", decoded)
	}
	if got.ArtistID != "artist_1" || got.Count != 7 {
		t.Errorf("decoded payload = %+v, want artist_1/7", got)
	}

	if typ, ok := PayloadTypeOf("TestRegistryDecode"); !ok || typ != reflect.PointerTo(reflect.TypeFor[registryPayload]()) {
		t.Errorf("PayloadTypeOf() = %v/%v, want the registryPayload pointer", typ, ok)
	}
}

func TestDecodeRegisteredPayloadUnknownType(t *testing.T) {
	swapPayloadRegistry(t)

	_, err := DecodeRegisteredPayload(Event{EventType: "TestRegistryUnknown", Payload: []byte("{}")})
	if !errors.Is(err, ErrPayloadNotRegistered) {
		t.Fatalf("DecodeRegisteredPayload() error = %v, want ErrPayloadNotRegistered", err)
	}
}

func TestRegisterPayloadRejectsConflictingType(t *testing.T) {
	swapPayloadRegistry(t)
	RegisterPayload[registryPayload]("TestRegistryConflict")

	// Re-registering the identical type is a no-op.
	RegisterPayload[registryPayload]("TestRegistryConflict")

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("RegisterPayload() with a conflicting type did not panic")
		}
		if msg, _ := recovered.(string); !strings.Contains(msg, "TestRegistryConflict") {
			t.Errorf("panic message = %q, want it to name the event type", msg)
		}
	}()
	RegisterPayload[badPayload]("TestRegistryConflict")
}

func TestRegisterPayloadRejectsNonStruct(t *testing.T) {
	swapPayloadRegistry(t)

	defer func() {
		if recover() == nil {
			t.Fatal("RegisterPayload() with a non-struct payload did not panic")
		}
	}()
	RegisterPayload[int]("TestRegistryNonStruct")
}

func TestValidatePayloadRegistry(t *testing.T) {
	swapPayloadRegistry(t)
	RegisterPayload[registryPayload]("TestRegistryValid")

	if err := ValidatePayloadRegistry(); err != nil {
		t.Fatalf("ValidatePayloadRegistry() error = %v, want nil", err)
	}

	RegisterPayload[badPayload]("TestRegistryInvalid")
	err := ValidatePayloadRegistry()
	if err == nil {
		t.Fatal("ValidatePayloadRegistry() = nil, want an error naming the unreadable payload")
	}
	if !strings.Contains(err.Error(), "TestRegistryInvalid") {
		t.Errorf("ValidatePayloadRegistry() error = %v, want it to name TestRegistryInvalid", err)
	}
}

type registryPayloadOther struct {
	Name string `json:"name"`
}

func TestRegisteredEventTypesIsSorted(t *testing.T) {
	swapPayloadRegistry(t)
	RegisterPayload[registryPayload]("TestRegistryB")
	RegisterPayload[registryPayloadOther]("TestRegistryA")

	got := RegisteredEventTypes()
	want := []string{"TestRegistryA", "TestRegistryB"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("RegisteredEventTypes() = %v, want %v", got, want)
	}
}

func TestRegisterPayloadRejectsDuplicateStruct(t *testing.T) {
	swapPayloadRegistry(t)
	RegisterPayload[registryPayload]("TestEventOne")

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("RegisterPayload() registering same struct under a different name did not panic")
		}
		if msg, _ := recovered.(string); !strings.Contains(msg, "TestEventOne") || !strings.Contains(msg, "TestEventTwo") {
			t.Errorf("panic message = %q, want it to name both event types", msg)
		}
	}()
	RegisterPayload[registryPayload]("TestEventTwo")
}

func TestEventTypeOfAndRecordPayload(t *testing.T) {
	swapPayloadRegistry(t)
	RegisterPayload[registryPayload]("TestInferredEvent")

	if name, ok := EventTypeOf(registryPayload{}); !ok || name != "TestInferredEvent" {
		t.Fatalf("EventTypeOf(value) = %q, %v; want TestInferredEvent, true", name, ok)
	}
	if name, ok := EventTypeOf(&registryPayload{}); !ok || name != "TestInferredEvent" {
		t.Fatalf("EventTypeOf(ptr) = %q, %v; want TestInferredEvent, true", name, ok)
	}
	if _, ok := EventTypeOf(badPayload{}); ok {
		t.Fatal("EventTypeOf(unregistered) = true, want false")
	}

	agg := NewBaseAggregate("ar_1", "artist")
	evt, err := agg.RecordPayload(registryPayload{ArtistID: "ar_1", Count: 42}, nil)
	if err != nil {
		t.Fatalf("RecordPayload() error = %v", err)
	}
	if evt.EventType != "TestInferredEvent" {
		t.Fatalf("evt.EventType = %q, want TestInferredEvent", evt.EventType)
	}
	if evt.Data == nil {
		t.Fatal("evt.Data should hold the in-memory payload")
	}

	var decoded registryPayload
	if err := DecodeEventPayload(evt, &decoded); err != nil {
		t.Fatalf("DecodeEventPayload() error = %v", err)
	}
	if decoded.ArtistID != "ar_1" || decoded.Count != 42 {
		t.Fatalf("decoded = %+v, want ar_1/42", decoded)
	}
}

