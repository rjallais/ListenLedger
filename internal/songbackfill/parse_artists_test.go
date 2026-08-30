package songbackfill

import "testing"

func TestParseStoredArtistsEllipsis(t *testing.T) {
	t.Parallel()

	parsed := parseStoredArtists("Kehlani, …")
	if !parsed.HasEllipsis {
		t.Fatalf("expected ellipsis to be detected")
	}
	if parsed.PrimaryPrefix != "Kehlani" {
		t.Fatalf("PrimaryPrefix = %q, want %q", parsed.PrimaryPrefix, "Kehlani")
	}
	if len(parsed.Names) != 0 {
		t.Fatalf("Names = %#v, want none for ellipsis inputs", parsed.Names)
	}
}

func TestParseStoredArtistsPreservesNothingNowhereAsSingleArtist(t *testing.T) {
	t.Parallel()

	parsed := parseStoredArtists("nothing,nowhere.")
	if parsed.HasEllipsis {
		t.Fatalf("HasEllipsis = %v, want false", parsed.HasEllipsis)
	}
	if !parsed.PreserveWhole {
		t.Fatalf("PreserveWhole = %v, want true", parsed.PreserveWhole)
	}
	if len(parsed.Names) != 1 || parsed.Names[0] != "nothing,nowhere." {
		t.Fatalf("Names = %#v, want single preserved artist", parsed.Names)
	}
}

func TestIsStylizedSingleArtistVariant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"Earth, Wind and Fire", "Earth, Wind and Fire", true},
		{"Earth, Wind & Fire", "Earth, Wind & Fire", true},
		{"Crosby, Stills & Nash", "Crosby, Stills & Nash", true},
		{"A, B & C", "A, B & C", false},
		{"Artist feat. Other", "Artist feat. Other", false},
		{"Artist (feat. Other)", "Artist (feat. Other)", false},
		{"Multi Word, Segment", "Multi Word, Segment", false},
		{"lowercase, name", "lowercase, name", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isStylizedSingleArtistVariant(tt.input)
			if got != tt.expected {
				t.Fatalf("isStylizedSingleArtistVariant(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestIsSingleArtistNameWithConjunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"Earth, Wind and Fire", "Earth, Wind and Fire", true},
		{"Earth, Wind & Fire", "Earth, Wind & Fire", true},
		{"A, B & C", "A, B & C", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isSingleArtistName(tt.input)
			if got != tt.expected {
				t.Fatalf("isSingleArtistName(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}
