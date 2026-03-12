//go:build goexperiment.jsonv2

package spotify

import (
	"context"
	"testing"
	"time"
)

func TestParseLocalHTMLMonthlyListenersNotReady(t *testing.T) {
	body := []byte(`<html><body><div>Loading artist page...</div></body></html>`)

	listeners, ready, err := parseLocalHTMLMonthlyListeners(body)
	if err != nil {
		t.Fatalf("parseLocalHTMLMonthlyListeners() error = %v", err)
	}
	if ready {
		t.Fatalf("parseLocalHTMLMonthlyListeners() ready = %v, want false", ready)
	}
	if listeners != 0 {
		t.Fatalf("parseLocalHTMLMonthlyListeners() listeners = %d, want 0 when not ready", listeners)
	}
}

func TestParseLocalHTMLMonthlyListenersJSONBlob(t *testing.T) {
	body := []byte(`<html><script>window.__data={"monthlyListeners":123456};</script></html>`)

	listeners, ready, err := parseLocalHTMLMonthlyListeners(body)
	if err != nil {
		t.Fatalf("parseLocalHTMLMonthlyListeners() error = %v", err)
	}
	if !ready {
		t.Fatal("parseLocalHTMLMonthlyListeners() ready = false, want true")
	}
	if listeners != 123456 {
		t.Fatalf("parseLocalHTMLMonthlyListeners() listeners = %d, want 123456", listeners)
	}
}

func TestParseLocalHTMLMonthlyListenersVisibleText(t *testing.T) {
	body := []byte(`<html><body><span>2.4M monthly listeners</span></body></html>`)

	listeners, ready, err := parseLocalHTMLMonthlyListeners(body)
	if err != nil {
		t.Fatalf("parseLocalHTMLMonthlyListeners() error = %v", err)
	}
	if !ready {
		t.Fatal("parseLocalHTMLMonthlyListeners() ready = false, want true")
	}
	if listeners != 2400000 {
		t.Fatalf("parseLocalHTMLMonthlyListeners() listeners = %d, want 2400000", listeners)
	}
}

func TestParseLocalHTMLMonthlyListenersZeroListenerPayload(t *testing.T) {
	body := []byte(`<html><script>{"data":{"artistUnion":{"stats":{"monthlyListeners":null}}}}</script></html>`)

	listeners, ready, err := parseLocalHTMLMonthlyListeners(body)
	if err != nil {
		t.Fatalf("parseLocalHTMLMonthlyListeners() error = %v", err)
	}
	if !ready {
		t.Fatal("parseLocalHTMLMonthlyListeners() ready = false, want true")
	}
	if listeners != 0 {
		t.Fatalf("parseLocalHTMLMonthlyListeners() listeners = %d, want 0", listeners)
	}
}

func TestParseLocalHTMLMonthlyListenersArtistUnionWithoutStats(t *testing.T) {
	body := []byte(`<html><script>{"data":{"artistUnion":{"profile":{}}}}</script></html>`)

	listeners, ready, err := parseLocalHTMLMonthlyListeners(body)
	if err != nil {
		t.Fatalf("parseLocalHTMLMonthlyListeners() error = %v", err)
	}
	if !ready {
		t.Fatal("parseLocalHTMLMonthlyListeners() ready = false, want true")
	}
	if listeners != 0 {
		t.Fatalf("parseLocalHTMLMonthlyListeners() listeners = %d, want 0", listeners)
	}
}

func TestLocalBrowserRetireGraceUsesRemainingDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	grace := localBrowserRetireGrace(ctx, 5*time.Second)
	if grace <= 0 || grace > 2*time.Second {
		t.Fatalf("localBrowserRetireGrace() = %s, want within (0, 2s]", grace)
	}
}
