//go:build goexperiment.jsonv2

package messaging

import "testing"

func TestScrapeRequestedRoundTrip(t *testing.T) {
	in := NewScrapeRequested("artist-1", "spotify-1", "Artist Name", "req-1")

	data, err := MarshalScrapeRequested(in)
	if err != nil {
		t.Fatalf("MarshalScrapeRequested() error = %v", err)
	}

	out, err := UnmarshalScrapeRequested(data)
	if err != nil {
		t.Fatalf("UnmarshalScrapeRequested() error = %v", err)
	}

	if out.Version != SchemaVersionV1 {
		t.Fatalf("Version = %q, want %q", out.Version, SchemaVersionV1)
	}
	if out.RequestID != "req-1" {
		t.Fatalf("RequestID = %q, want req-1", out.RequestID)
	}
	if out.ArtistID != "artist-1" {
		t.Fatalf("ArtistID = %q, want artist-1", out.ArtistID)
	}
	if out.SpotifyID != "spotify-1" {
		t.Fatalf("SpotifyID = %q, want spotify-1", out.SpotifyID)
	}
	if out.ArtistName != "Artist Name" {
		t.Fatalf("ArtistName = %q, want Artist Name", out.ArtistName)
	}
	if out.QueuedAt == "" {
		t.Fatal("QueuedAt should be populated")
	}
}

func TestArtistUpdatedRoundTrip(t *testing.T) {
	in := NewArtistUpdated("artist-1", "Artist Name", 1234, "idle", "req-1")

	data, err := MarshalArtistUpdated(in)
	if err != nil {
		t.Fatalf("MarshalArtistUpdated() error = %v", err)
	}

	out, err := UnmarshalArtistUpdated(data)
	if err != nil {
		t.Fatalf("UnmarshalArtistUpdated() error = %v", err)
	}

	if out.Version != SchemaVersionV1 {
		t.Fatalf("Version = %q, want %q", out.Version, SchemaVersionV1)
	}
	if out.ArtistID != "artist-1" {
		t.Fatalf("ArtistID = %q, want artist-1", out.ArtistID)
	}
	if out.Name != "Artist Name" {
		t.Fatalf("Name = %q, want Artist Name", out.Name)
	}
	if out.MonthlyListeners != 1234 {
		t.Fatalf("MonthlyListeners = %d, want 1234", out.MonthlyListeners)
	}
	if out.FetchStatus != "idle" {
		t.Fatalf("FetchStatus = %q, want idle", out.FetchStatus)
	}
	if out.UpdatedAt == "" {
		t.Fatal("UpdatedAt should be populated")
	}
}

func TestSubjectScrapeRequestForProvider(t *testing.T) {
	if got := SubjectScrapeRequestForProvider(ScrapeProviderBrowserless); got != "scrape.request.browserless" {
		t.Fatalf("SubjectScrapeRequestForProvider(browserless) = %q, want %q", got, "scrape.request.browserless")
	}
	if got := SubjectScrapeRequestForProvider("unknown"); got != SubjectScrapeRequest {
		t.Fatalf("SubjectScrapeRequestForProvider(unknown) = %q, want %q", got, SubjectScrapeRequest)
	}
}

func TestScrapeProviderFromSubject(t *testing.T) {
	if got := ScrapeProviderFromSubject("scrape.request.scraperapi"); got != ScrapeProviderScraperAPI {
		t.Fatalf("ScrapeProviderFromSubject(scrape.request.scraperapi) = %q, want %q", got, ScrapeProviderScraperAPI)
	}
	if got := ScrapeProviderFromSubject(SubjectScrapeRequest); got != ScrapeProviderAny {
		t.Fatalf("ScrapeProviderFromSubject(scrape.request) = %q, want %q", got, ScrapeProviderAny)
	}
}
