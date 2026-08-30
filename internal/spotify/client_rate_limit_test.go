package spotify

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestParseRetryAfterHeaderSeconds(t *testing.T) {
	got := parseRetryAfterHeader("12", time.Now())
	if got != 12*time.Second {
		t.Fatalf("parseRetryAfterHeader() = %s, want 12s", got)
	}
}

func TestParseRetryAfterHeaderDate(t *testing.T) {
	now := time.Date(2026, time.March, 1, 14, 0, 0, 0, time.UTC)
	header := now.Add(20 * time.Second).Format(http.TimeFormat)
	got := parseRetryAfterHeader(header, now)
	if got != 20*time.Second {
		t.Fatalf("parseRetryAfterHeader() = %s, want 20s", got)
	}
}

func TestRetryAfterExtractor(t *testing.T) {
	err := &RateLimitError{
		Provider:   "scraperapi",
		StatusCode: http.StatusTooManyRequests,
		RetryAfter: 9 * time.Second,
	}
	got, ok := RetryAfter(err)
	if !ok {
		t.Fatalf("RetryAfter() ok = false, want true")
	}
	if got != 9*time.Second {
		t.Fatalf("RetryAfter() = %s, want 9s", got)
	}
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("errors.Is(err, ErrRateLimited) = false, want true")
	}
}

func TestMarkScraperAPIRateLimitedSetsCooldown(t *testing.T) {
	client := &Client{}

	got := client.markScraperAPIRateLimited("1")
	if got <= 0 {
		t.Fatalf("markScraperAPIRateLimited() = %s, want > 0", got)
	}

	remaining := client.scraperAPICooldownRemaining(time.Now())
	if remaining <= 0 {
		t.Fatalf("scraperAPICooldownRemaining() = %s, want > 0", remaining)
	}
}
