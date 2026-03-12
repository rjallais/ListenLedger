//go:build goexperiment.jsonv2

package spotify

import "testing"

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
