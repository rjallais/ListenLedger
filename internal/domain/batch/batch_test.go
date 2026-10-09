package batch

import (
	"testing"

	"ListenLedger/internal/eventsourcing"
)

func TestNewBatchLifecycle(t *testing.T) {
	agg, err := NewBatch("b1", []string{"ar_1", "ar_2", "ar_1", ""}, map[string]int{"P5RockIncluded": 2})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	// Duplicates and blanks collapse.
	if agg.Total() != 2 {
		t.Fatalf("Total = %d, want 2", agg.Total())
	}
	if agg.Done() {
		t.Fatal("new batch should not be Done")
	}
	uncommitted := agg.UncommittedEvents()
	if len(uncommitted) != 1 || uncommitted[0].EventType != EventTypeBatchStarted {
		t.Fatalf("uncommitted = %v", uncommitted)
	}

	// Unknown artist and duplicates record nothing.
	if _, recorded, _ := agg.RecordCompletion("ghost"); recorded {
		t.Fatal("unknown artist should record nothing")
	}

	if _, recorded, err := agg.RecordCompletion("ar_1"); err != nil || !recorded {
		t.Fatalf("RecordCompletion = %t, %v", recorded, err)
	}
	if agg.Completed() != 1 || agg.Done() {
		t.Fatalf("after 1/2: completed=%d done=%t", agg.Completed(), agg.Done())
	}
	if _, recorded, _ := agg.RecordCompletion("ar_1"); recorded {
		t.Fatal("duplicate should record nothing")
	}

	if _, recorded, err := agg.RecordCompletion("ar_2"); err != nil || !recorded {
		t.Fatalf("RecordCompletion = %t, %v", recorded, err)
	}
	if !agg.Done() {
		t.Fatal("2/2 should be Done")
	}
	if _, recorded, err := agg.RecordClosed(); err != nil || !recorded {
		t.Fatalf("RecordClosed = %t, %v", recorded, err)
	}
	if _, recorded, _ := agg.RecordClosed(); recorded {
		t.Fatal("second close should record nothing")
	}
}

func TestNewBatchEmptyEndsAtBirth(t *testing.T) {
	agg, err := NewBatch("b_empty", nil, nil)
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if !agg.Done() {
		t.Fatal("empty batch should be Done")
	}
	types := make([]string, 0)
	for _, e := range agg.UncommittedEvents() {
		types = append(types, e.EventType)
	}
	if len(types) != 2 || types[0] != EventTypeBatchStarted || types[1] != EventTypeBatchCompleted {
		t.Fatalf("empty batch events = %v", types)
	}
}

func TestReplay(t *testing.T) {
	agg, err := NewBatch("b1", []string{"ar_1", "ar_2"}, map[string]int{"P1RockRecent": 2})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if _, _, err := agg.RecordCompletion("ar_1"); err != nil {
		t.Fatalf("RecordCompletion: %v", err)
	}
	stored := append([]eventsourcing.Event(nil), agg.UncommittedEvents()...)

	replayed, err := Replay("b1", stored)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if replayed.Total() != 2 || replayed.Completed() != 1 || replayed.Done() {
		t.Fatalf("replayed: total=%d completed=%d done=%t",
			replayed.Total(), replayed.Completed(), replayed.Done())
	}
	if replayed.Stats()["P1RockRecent"] != 2 {
		t.Fatalf("replayed stats = %v", replayed.Stats())
	}
	if !replayed.Members()["ar_1"] || replayed.Members()["ar_2"] {
		t.Fatalf("replayed members = %v", replayed.Members())
	}

	if _, err := Replay("b1", nil); err == nil {
		t.Fatal("Replay(empty) should error")
	}
}
