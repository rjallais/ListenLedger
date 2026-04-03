//go:build goexperiment.jsonv2

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"ListenLedger/internal/songbackfill"
)

func TestLatestBackfillReportPathChoosesNewestJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	paths := []string{
		filepath.Join(dir, "song_artist_backfill_20260311_030153.json"),
		filepath.Join(dir, "song_artist_backfill_20260311_030453.json"),
		filepath.Join(dir, "song_artist_review_queue_20260311_030453.json"),
	}
	for _, path := range paths {
		if err := os.WriteFile(path, []byte("{}\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%q): %v", path, err)
		}
	}

	got, err := latestBackfillReportPath(dir)
	if err != nil {
		t.Fatalf("latestBackfillReportPath() error = %v", err)
	}
	want := filepath.Join(dir, "song_artist_backfill_20260311_030453.json")
	if got != want {
		t.Fatalf("latestBackfillReportPath() = %q, want %q", got, want)
	}
}

func TestPrioritizeSongsForRetryOrdersLikelyResolvableFirst(t *testing.T) {
	t.Parallel()

	songs := []songbackfill.SongInput{
		{ID: "manual", Title: "Dreadbringer", ArtistName: "Aborted, ..."},
		{ID: "mismatch", Title: "One Last Goodbye", ArtistName: "Anathema"},
		{ID: "missing", Title: "Spaceship", ArtistName: "Benny Benassi, …"},
		{ID: "new", Title: "Fresh Track", ArtistName: "Zed, .."},
		{ID: "ambiguous", Title: "Complicated", ArtistName: "Dimitri Vegas, ..."},
	}

	hints := map[string]resolutionHints{
		"manual": {
			Category:      "needs_manual_credit_lookup",
			Priority:      5,
			RetryEligible: false,
		},
		"mismatch": {
			Category:      "artist_name_mismatch",
			Priority:      2,
			RetryEligible: true,
		},
		"missing": {
			Category:      "missing_artist_record",
			Priority:      1,
			RetryEligible: true,
		},
		"ambiguous": {
			Category:      "ambiguous_external_credit",
			Priority:      4,
			RetryEligible: false,
		},
	}

	got := prioritizeSongsForRetry(songs, hints)
	if len(got) != len(songs) {
		t.Fatalf("len(prioritizeSongsForRetry()) = %d, want %d", len(got), len(songs))
	}

	gotIDs := []string{got[0].ID, got[1].ID, got[2].ID, got[3].ID, got[4].ID}
	wantIDs := []string{"missing", "mismatch", "new", "ambiguous", "manual"}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("order[%d] = %q, want %q (full=%v)", i, gotIDs[i], wantIDs[i], gotIDs)
		}
	}
}

func TestSanitizeStoredArtistNameNormalizesTrailingDots(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"B.o.B, ..":             "B.o.B, ...",
		"Kehlani, …":            "Kehlani, ...",
		"  Benny Benassi,  .. ": "Benny Benassi, ...",
	}

	for input, want := range tests {
		if got := sanitizeStoredArtistName(input); got != want {
			t.Fatalf("sanitizeStoredArtistName(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestBuildSummaryCountsNameOnlyUpdatesAsCandidates(t *testing.T) {
	t.Parallel()

	summary := buildSummary([]songbackfill.Resolution{{
		Action:             songbackfill.ActionUpdateNameOnly,
		OriginalArtistName: "Kehlani, ...",
		UpdatedArtistName:  "Kehlani, Ty Dolla $ign",
		Confidence:         0.95,
	}}, 0.90)

	if summary.UpdateCandidates != 1 {
		t.Fatalf("UpdateCandidates = %d, want 1", summary.UpdateCandidates)
	}
}

func TestFetchTidalAccessTokenUsesClientCredentials(t *testing.T) {
	t.Parallel()

	handlerErrors := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			handlerErrors <- fmt.Sprintf("Method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Basic "+base64.StdEncoding.EncodeToString([]byte("client-id:client-secret")) {
			handlerErrors <- fmt.Sprintf("Authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); !strings.Contains(got, "application/x-www-form-urlencoded") {
			handlerErrors <- fmt.Sprintf("Content-Type = %q", got)
		}
		if err := r.ParseForm(); err != nil {
			handlerErrors <- fmt.Sprintf("ParseForm() error = %v", err)
		} else if got := r.Form.Get("grant_type"); got != "client_credentials" {
			handlerErrors <- fmt.Sprintf("grant_type = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "token-123",
			"token_type":   "Bearer",
			"expires_in":   86400,
		})
	}))
	defer server.Close()

	token, err := fetchTidalAccessToken(context.Background(), server.Client(), server.URL, "client-id", "client-secret")
	if err != nil {
		t.Fatalf("fetchTidalAccessToken() error = %v", err)
	}
	if token != "token-123" {
		t.Fatalf("fetchTidalAccessToken() = %q, want token-123", token)
	}

	close(handlerErrors)
	for handlerErr := range handlerErrors {
		t.Errorf("handler validation failed: %s", handlerErr)
	}
}
