package eventsourcing

import (
	"testing"
	"time"
)

// TestRecordThatUsesInjectedClockAndIDGen pins event identity and timestamp
// from the injected edges, so replay and projection tests are deterministic.
func TestRecordThatUsesInjectedClockAndIDGen(t *testing.T) {
	fixed := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	clock := NewFakeClock(fixed)
	agg := NewBaseAggregate("artist_1", "artist",
		WithClock(clock), WithIDGen(&SeqIDGen{}))

	first, err := agg.RecordThat("ArtistCreated", map[string]string{"name": "Tool"}, nil)
	if err != nil {
		t.Fatalf("RecordThat() error = %v", err)
	}
	if first.ID != "evt_1" {
		t.Errorf("first event ID = %q, want %q", first.ID, "evt_1")
	}
	if !first.CreatedAt.Equal(fixed) {
		t.Errorf("first event CreatedAt = %s, want %s", first.CreatedAt, fixed)
	}

	clock.Advance(time.Hour)
	second, err := agg.RecordThat("ArtistMonthlyListenersScraped", map[string]int64{"monthly_listeners": 1}, nil)
	if err != nil {
		t.Fatalf("RecordThat() error = %v", err)
	}
	if second.ID != "evt_2" {
		t.Errorf("second event ID = %q, want %q", second.ID, "evt_2")
	}
	if want := fixed.Add(time.Hour); !second.CreatedAt.Equal(want) {
		t.Errorf("second event CreatedAt = %s, want %s", second.CreatedAt, want)
	}
	if second.Version != 2 {
		t.Errorf("second event version = %d, want 2", second.Version)
	}
}

// TestBaseAggregateNowTracksInjectedClock keeps domain payload timestamps on
// the same clock as the envelope.
func TestBaseAggregateNowTracksInjectedClock(t *testing.T) {
	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	agg := NewBaseAggregate("artist_1", "artist",
		WithClock(NewFakeClock(fixed)))

	if got := agg.Now(); !got.Equal(fixed) {
		t.Errorf("Now() = %s, want %s", got, fixed)
	}
}

// TestRecordThatDefaultsToWallClockAndUUIDs pins the production default: a
// generated evt_-prefixed ID and a timestamp near now.
func TestRecordThatDefaultsToWallClockAndUUIDs(t *testing.T) {
	agg := NewBaseAggregate("artist_1", "artist")

	evt, err := agg.RecordThat("ArtistCreated", map[string]string{"name": "Tool"}, nil)
	if err != nil {
		t.Fatalf("RecordThat() error = %v", err)
	}
	if len(evt.ID) != len("evt_")+36 || evt.ID[:4] != "evt_" {
		t.Errorf("default event ID = %q, want an evt_-prefixed UUID", evt.ID)
	}
	if since := time.Since(evt.CreatedAt); since < 0 || since > time.Minute {
		t.Errorf("default CreatedAt = %s, want within a minute of now", evt.CreatedAt)
	}
}

// TestZeroValueBaseAggregateFallsBackToDefaults keeps the pre-injection
// contract: an aggregate built as a struct literal (never passed through
// NewBaseAggregate) records events with UUID IDs and wall-clock stamps
// instead of panicking on nil clock/idgen.
func TestZeroValueBaseAggregateFallsBackToDefaults(t *testing.T) {
	var agg BaseAggregate

	if agg.Now().IsZero() {
		t.Error("Now() on zero-value BaseAggregate returned zero time")
	}
	evt, err := agg.RecordThat("ArtistCreated", map[string]string{"name": "Tool"}, nil)
	if err != nil {
		t.Fatalf("RecordThat() error = %v", err)
	}
	if evt.ID == "" {
		t.Error("RecordThat() left the event ID empty on a zero-value aggregate")
	}
	if evt.CreatedAt.IsZero() {
		t.Error("RecordThat() left CreatedAt zero on a zero-value aggregate")
	}
}

// TestNewEventKeepsGeneratingIdentity guards the exported constructor used by
// tooling and tests: it still stamps an ID and a timestamp on its own.
func TestNewEventKeepsGeneratingIdentity(t *testing.T) {
	evt, err := NewEvent("artist_1", "artist", 1, "ArtistCreated", map[string]string{"name": "Tool"}, nil)
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	if evt.ID == "" {
		t.Error("NewEvent() left the event ID empty")
	}
	if evt.CreatedAt.IsZero() {
		t.Error("NewEvent() left CreatedAt zero")
	}
}
