package handlers

import "testing"

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
