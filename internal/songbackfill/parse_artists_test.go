//go:build goexperiment.jsonv2

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
		t.Fatal("expected HasEllipsis = false")
	}
	if !parsed.PreserveWhole {
		t.Fatal("expected PreserveWhole = true")
	}
	if len(parsed.Names) != 1 || parsed.Names[0] != "nothing,nowhere." {
		t.Fatalf("Names = %#v, want single preserved artist", parsed.Names)
	}
}
