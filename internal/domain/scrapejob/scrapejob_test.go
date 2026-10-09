package scrapejob

import (
	"testing"

	"ListenLedger/internal/eventsourcing"
)

func TestLifecycle(t *testing.T) {
	agg, err := NewScrapeJob("req_1", "ar_1", "refresh")
	if err != nil {
		t.Fatalf("NewScrapeJob: %v", err)
	}
	if agg.Status() != StatusQueued || agg.Attempts() != 0 || agg.IsTerminal() {
		t.Fatalf("initial: status=%s attempts=%d terminal=%t", agg.Status(), agg.Attempts(), agg.IsTerminal())
	}

	if _, recorded, err := agg.RecordStarted(); err != nil || !recorded {
		t.Fatalf("RecordStarted = %t, %v", recorded, err)
	}
	if agg.Status() != StatusProcessing || agg.Attempts() != 1 {
		t.Fatalf("started: status=%s attempts=%d", agg.Status(), agg.Attempts())
	}
	// Redelivery records another attempt.
	if _, recorded, err := agg.RecordStarted(); err != nil || !recorded {
		t.Fatalf("redelivery RecordStarted = %t, %v", recorded, err)
	}
	if agg.Attempts() != 2 {
		t.Fatalf("attempts = %d, want 2", agg.Attempts())
	}

	if _, recorded, err := agg.RecordSucceeded("mobile-ssr", 250); err != nil || !recorded {
		t.Fatalf("RecordSucceeded = %t, %v", recorded, err)
	}
	if !agg.IsTerminal() || agg.Status() != StatusSucceeded {
		t.Fatalf("terminal: status=%s", agg.Status())
	}
	// Closed streams record nothing further.
	if _, recorded, _ := agg.RecordStarted(); recorded {
		t.Fatal("post-success Started should record nothing")
	}
	if _, recorded, _ := agg.RecordFailed("x"); recorded {
		t.Fatal("post-success Failed should record nothing")
	}
}

func TestFailedReopensOnRetry(t *testing.T) {
	agg, err := NewScrapeJob("req_2", "ar_2", "")
	if err != nil {
		t.Fatalf("NewScrapeJob: %v", err)
	}
	if _, _, err := agg.RecordStarted(); err != nil {
		t.Fatalf("RecordStarted: %v", err)
	}
	if _, _, err := agg.RecordFailed("stale_timeout"); err != nil {
		t.Fatalf("RecordFailed: %v", err)
	}
	if agg.IsTerminal() || agg.Status() != StatusFailed {
		t.Fatalf("failed: status=%s terminal=%t", agg.Status(), agg.IsTerminal())
	}
	// Retry re-opens.
	if _, recorded, err := agg.RecordStarted(); err != nil || !recorded {
		t.Fatalf("retry RecordStarted = %t, %v", recorded, err)
	}
	if agg.Status() != StatusProcessing {
		t.Fatalf("status = %s, want processing", agg.Status())
	}
	// Dead letter closes finally.
	if _, _, err := agg.RecordDeadLettered("retry_exhausted"); err != nil {
		t.Fatalf("RecordDeadLettered: %v", err)
	}
	if !agg.IsTerminal() || agg.Status() != StatusDeadLettered {
		t.Fatalf("status=%s terminal=%t", agg.Status(), agg.IsTerminal())
	}
	if _, recorded, _ := agg.RecordFailed("late"); recorded {
		t.Fatal("post-DLQ Failed should record nothing")
	}
}

func TestReplay(t *testing.T) {
	agg, err := NewScrapeJob("req_3", "ar_3", "batch")
	if err != nil {
		t.Fatalf("NewScrapeJob: %v", err)
	}
	if _, _, err := agg.RecordStarted(); err != nil {
		t.Fatalf("RecordStarted: %v", err)
	}
	if _, _, err := agg.RecordFailed("boom"); err != nil {
		t.Fatalf("RecordFailed: %v", err)
	}
	stored := append([]eventsourcing.Event(nil), agg.UncommittedEvents()...)

	replayed, err := Replay("req_3", stored)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if replayed.Status() != StatusFailed || replayed.Attempts() != 1 || replayed.IsTerminal() {
		t.Fatalf("replayed: status=%s attempts=%d terminal=%t",
			replayed.Status(), replayed.Attempts(), replayed.IsTerminal())
	}
	if _, err := Replay("req_3", nil); err == nil {
		t.Fatal("Replay(empty) should error")
	}
	if _, err := NewScrapeJob("", "ar", ""); err == nil {
		t.Fatal("NewScrapeJob(empty request) should error")
	}
}
