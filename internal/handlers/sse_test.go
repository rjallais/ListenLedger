package handlers

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a-h/templ"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/config"
	"ListenLedger/templates"
)

func TestRanksTickSignalForGenre(t *testing.T) {
	tick, ok := ranksTickSignalForGenre("rock_metal")
	if !ok || tick != "ranksTickRockMetal" {
		t.Fatalf("ranksTickSignalForGenre(rock_metal) = %q, %t", tick, ok)
	}
	tick, ok = ranksTickSignalForGenre("everything_else")
	if !ok || tick != "ranksTickEverythingElse" {
		t.Fatalf("ranksTickSignalForGenre(everything_else) = %q, %t", tick, ok)
	}
	for _, genre := range []string{"", "rock_metal;alert(1)", "artist.updated", "ROCK_METAL"} {
		if tick, ok := ranksTickSignalForGenre(genre); ok || tick != "" {
			t.Fatalf("ranksTickSignalForGenre(%q) = %q, %t; want rejection", genre, tick, ok)
		}
	}
}

// renderDatalines renders one element patch through the real Datastar SSE
// encoder and returns the emitted event lines.
func renderDatalines(t *testing.T, cfg *config.Config, comp templ.Component, opts ...datastar.PatchElementOption) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/queue/retry", nil)
	if err := RenderDatastarWithConfig(rec, req, cfg, comp, opts...); err != nil {
		t.Fatalf("RenderDatastarWithConfig() error = %v", err)
	}
	return rec.Body.String()
}

// View transitions are the config default for fragment patches, but a caller
// can opt a single patch out. The emitted event must reflect the last
// WithUseViewTransitions, whichever side it comes from.
func TestRenderDatastarViewTransitionOverride(t *testing.T) {
	comp := templates.QueueJobsWidgetContent(templates.QueueStats{})
	cfgOn := &config.Config{UseViewTransitions: true}

	if body := renderDatalines(t, cfgOn, comp); !strings.Contains(body, "useViewTransition") {
		t.Fatal("default patch should carry the useViewTransition dataline")
	}
	if body := renderDatalines(t, cfgOn, comp, noViewTransitionsOpts...); strings.Contains(body, "useViewTransition") {
		t.Fatal("noViewTransitionsOpts must override the config default")
	}
	if body := renderDatalines(t, noViewTransitionsConfig, comp); strings.Contains(body, "useViewTransition") {
		t.Fatal("UseViewTransitions=false config must disable view transitions")
	}
}

// The SSE chrome lanes (queue counters, batch progress) are pushed on every
// scrape completion, so they must not request view transitions. Guards the
// regression where transition churn was applied to high-frequency patches.
func TestSSEChromePatchesSkipViewTransitions(t *testing.T) {
	h := &Handler{}

	var got []datastar.PatchElementOption
	record := func(_ templ.Component, opts ...datastar.PatchElementOption) error {
		got = opts
		return nil
	}
	h.patchBatchProgressSnapshot(record, nil, batchProgressSnapshot{ID: "b1", Total: 2, Completed: 1}, slog.Default())

	rec := httptest.NewRecorder()
	sse := datastar.NewSSE(rec, httptest.NewRequest(http.MethodPost, "/api/events", nil))
	if err := sse.PatchElements(`<div id="batch-refresh-result"></div>`,
		patchOpts(&config.Config{UseViewTransitions: true}, "", got...)...); err != nil {
		t.Fatalf("PatchElements() error = %v", err)
	}
	if body := rec.Body.String(); strings.Contains(body, "useViewTransition") {
		t.Fatalf("batch progress patch must not request view transitions, got:\n%s", body)
	}
}

func TestBatchProgressTrackerRejectsStaleAndDone(t *testing.T) {
	tracker := newBatchProgressTracker()

	if !tracker.accept(batchProgressSnapshot{ID: "b1", Total: 2, Completed: 1}) {
		t.Fatal("first snapshot should be accepted")
	}
	if tracker.accept(batchProgressSnapshot{ID: "b1", Total: 2, Completed: 0}) {
		t.Fatal("lower completion count should be rejected")
	}
	if !tracker.accept(batchProgressSnapshot{ID: "b1", Total: 2, Completed: 2, Done: true}) {
		t.Fatal("completion snapshot should be accepted")
	}
	if tracker.accept(batchProgressSnapshot{ID: "b1", Total: 2, Completed: 3}) {
		t.Fatal("update after Done should be rejected")
	}
	if tracker.accept(batchProgressSnapshot{ID: "b1", Total: 2, Completed: 2, Done: false}) {
		t.Fatal("reopening a finished batch should be rejected")
	}
	if !tracker.accept(batchProgressSnapshot{ID: "b2", Total: 1, Completed: 0}) {
		t.Fatal("a different batch should be accepted")
	}
}
