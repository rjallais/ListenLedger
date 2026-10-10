package messaging

import (
	"bytes"
	"testing"
)

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

func TestRanksSubjectRoundTrip(t *testing.T) {
	for genre, wantSubject := range map[string]string{
		"rock_metal":      "ranks.updated.rock_metal",
		"everything_else": "ranks.updated.everything_else",
	} {
		if got := SubjectRanksUpdatedForGenre(genre); got != wantSubject {
			t.Fatalf("SubjectRanksUpdatedForGenre(%q) = %q, want %q", genre, got, wantSubject)
		}
		if got := RanksGenreFromSubject(wantSubject); got != genre {
			t.Fatalf("RanksGenreFromSubject(%q) = %q, want %q", wantSubject, got, genre)
		}
	}
	if got := SubjectRanksUpdatedForGenre(""); got != SubjectRanksUpdated {
		t.Fatalf("SubjectRanksUpdatedForGenre(\"\") = %q, want %q", got, SubjectRanksUpdated)
	}
	for _, subject := range []string{SubjectRanksUpdated, SubjectArtistUpdated, "ranks.updated.", "other.subject"} {
		if got := RanksGenreFromSubject(subject); got != "" {
			t.Fatalf("RanksGenreFromSubject(%q) = %q, want empty", subject, got)
		}
	}
}

func TestSubjectDomainEvent(t *testing.T) {
	got := SubjectDomainEvent("artist", "ar_123", "ArtistCreated")
	want := "domain.events.artist.ar_123.ArtistCreated"
	if got != want {
		t.Fatalf("SubjectDomainEvent() = %q, want %q", got, want)
	}
	// Dots in IDs are escaped since NATS uses dots as token separators.
	got = SubjectDomainEvent("artist", "ar.123", "ArtistCreated")
	if got != "domain.events.artist.ar-d-123.ArtistCreated" {
		t.Fatalf("SubjectDomainEvent(dotted) = %q", got)
	}
}

func TestSubjectTokenEncodingInjective(t *testing.T) {
	// "a.b" and "a_b" must not share a scope or filter: the old "_" mapping
	// conflated them and Append for one ID would read the other's history.
	if SubjectDomainEventScope("a.b") == SubjectDomainEventScope("a_b") {
		t.Fatalf("Scope conflates a.b and a_b: %q", SubjectDomainEventScope("a.b"))
	}
	if SubjectDomainEventFilter("a.b") == SubjectDomainEventFilter("a_b") {
		t.Fatalf("Filter conflates a.b and a_b: %q", SubjectDomainEventFilter("a.b"))
	}
	if SubjectDomainEvent("artist", "a.b", "E") == SubjectDomainEvent("artist", "a_b", "E") {
		t.Fatal("Subject conflates a.b and a_b")
	}
	// Dashes escape too: "-" (empty marker) vs "--" (encoded literal).
	if SubjectDomainEventScope("-") == SubjectDomainEventScope("--") {
		t.Fatal("Scope conflates - and --")
	}
	// Edge whitespace is escaped, not trimmed: "a " and "a" stay distinct.
	if SubjectDomainEventScope("a ") == SubjectDomainEventScope("a") {
		t.Fatal("Scope conflates 'a ' and 'a'")
	}
	if SubjectDomainEventScope("") == SubjectDomainEventScope("   ") {
		t.Fatal("Scope conflates empty and whitespace-only IDs")
	}
}

func TestUnknownMessageVersionsRejected(t *testing.T) {
	scrape, err := MarshalScrapeRequested(NewScrapeRequested("a", "s", "N", "r"))
	if err != nil {
		t.Fatalf("MarshalScrapeRequested() error = %v", err)
	}
	// Rewrite version to an unknown v2: must fail fast, never misread.
	v2scrape := bytes.Replace(scrape, []byte(`"version":"v1"`), []byte(`"version":"v2"`), 1)
	if _, err := UnmarshalScrapeRequested(v2scrape); err == nil {
		t.Fatal("UnmarshalScrapeRequested(v2) should error")
	}

	updated, err := MarshalArtistUpdated(NewArtistUpdated("a", "N", 1, "idle", "r"))
	if err != nil {
		t.Fatalf("MarshalArtistUpdated() error = %v", err)
	}
	v2updated := bytes.Replace(updated, []byte(`"version":"v1"`), []byte(`"version":"v2"`), 1)
	if _, err := UnmarshalArtistUpdated(v2updated); err == nil {
		t.Fatal("UnmarshalArtistUpdated(v2) should error")
	}
}

func TestScrapeRequestMsgID(t *testing.T) {
	if got := ScrapeRequestMsgID("req-1"); got != "scrape.request:req-1" {
		t.Fatalf("ScrapeRequestMsgID(req-1) = %q, want scrape.request:req-1", got)
	}
	// Empty IDs must not share a fallback MsgID: distinct requests would
	// wrongly dedup against each other within the duplicates window.
	for _, empty := range []string{"", "   "} {
		if got := ScrapeRequestMsgID(empty); got != "" {
			t.Fatalf("ScrapeRequestMsgID(%q) = %q, want empty (publish without MsgID)", empty, got)
		}
	}
}
