package templates

import "testing"

func TestRanksTickSignal(t *testing.T) {
	if got := RanksTickSignal("rock_metal"); got != "ranksTickRockMetal" {
		t.Fatalf("RanksTickSignal(rock_metal) = %q", got)
	}
	if got := RanksTickSignal("everything_else"); got != "ranksTickEverythingElse" {
		t.Fatalf("RanksTickSignal(everything_else) = %q", got)
	}
	if got := RanksTickSignal("weird genre!"); got != "ranksTickWeirdGenre" {
		t.Fatalf("RanksTickSignal(weird genre!) = %q", got)
	}
}
