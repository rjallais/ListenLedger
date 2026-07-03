//go:build goexperiment.jsonv2

package handlers

import (
	"testing"
	"time"

	"ListenLedger/templates"
)

func TestNextRecentBatchAssignmentFromEntriesStartsAtBatchSize(t *testing.T) {
	seq, pos := nextRecentBatchAssignmentFromEntries(nil, time.Now(), songsRecentBatchWindow)
	if seq != 1 || pos != songsRecentBatchSize {
		t.Fatalf("nextRecentBatchAssignmentFromEntries(nil) = (%d, %d), want (1, %d)", seq, pos, songsRecentBatchSize)
	}
}

func TestNextRecentBatchAssignmentFromEntriesCountsDownWithinBatch(t *testing.T) {
	now := time.Now()
	entries := []songListEntry{
		{
			song:      templates.Song{IsRecent: true, BatchSeq: 3, BatchPos: 13},
			createdAt: now.Add(-2 * time.Hour),
		},
		{
			song:      templates.Song{IsRecent: true, BatchSeq: 3, BatchPos: 12},
			createdAt: now.Add(-1 * time.Hour),
		},
	}

	seq, pos := nextRecentBatchAssignmentFromEntries(entries, now, songsRecentBatchWindow)
	if seq != 3 || pos != 11 {
		t.Fatalf("nextRecentBatchAssignmentFromEntries(countdown batch) = (%d, %d), want (3, 11)", seq, pos)
	}
}

func TestNextRecentBatchAssignmentFromEntriesStartsNewBatchAtOne(t *testing.T) {
	now := time.Now()
	entries := []songListEntry{
		{
			song:      templates.Song{IsRecent: true, BatchSeq: 4, BatchPos: 13},
			createdAt: now.Add(-3 * time.Hour),
		},
		{
			song:      templates.Song{IsRecent: true, BatchSeq: 4, BatchPos: 1},
			createdAt: now.Add(-1 * time.Hour),
		},
	}

	seq, pos := nextRecentBatchAssignmentFromEntries(entries, now, songsRecentBatchWindow)
	if seq != 5 || pos != 13 {
		t.Fatalf("nextRecentBatchAssignmentFromEntries(full batch) = (%d, %d), want (5, 13)", seq, pos)
	}
}

func TestSongBatchSortingFollowsCountdownPositions(t *testing.T) {
	now := time.Now()
	entries := []songListEntry{
		{
			song:      templates.Song{ID: "oldest", BatchSeq: 2, BatchPos: 13},
			createdAt: now.Add(-3 * time.Hour),
		},
		{
			song:      templates.Song{ID: "middle", BatchSeq: 2, BatchPos: 12},
			createdAt: now.Add(-2 * time.Hour),
		},
		{
			song:      templates.Song{ID: "newest", BatchSeq: 2, BatchPos: 11},
			createdAt: now.Add(-1 * time.Hour),
		},
	}

	recentOrdered := append([]songListEntry(nil), entries...)
	sortRecentSongEntries(recentOrdered)
	if recentOrdered[0].song.ID != "oldest" || recentOrdered[1].song.ID != "middle" || recentOrdered[2].song.ID != "newest" {
		t.Fatalf("sortRecentSongEntries order = [%s %s %s], want [oldest middle newest]",
			recentOrdered[0].song.ID,
			recentOrdered[1].song.ID,
			recentOrdered[2].song.ID,
		)
	}

	playlistOrdered := append([]songListEntry(nil), entries...)
	sortEntriesByMode(playlistOrdered, playlistSortAddedDesc, playlistSortMode)
	if playlistOrdered[0].song.ID != "newest" || playlistOrdered[1].song.ID != "middle" || playlistOrdered[2].song.ID != "oldest" {
		t.Fatalf("sortEntriesByMode(playlist, added_desc) order = [%s %s %s], want [newest middle oldest]",
			playlistOrdered[0].song.ID,
			playlistOrdered[1].song.ID,
			playlistOrdered[2].song.ID,
		)
	}

	waitingRemovalOrdered := append([]songListEntry(nil), entries...)
	sortEntriesByMode(waitingRemovalOrdered, playlistSortAddedDesc, waitingRemovalSortMode)
	if waitingRemovalOrdered[0].song.ID != "newest" || waitingRemovalOrdered[1].song.ID != "middle" || waitingRemovalOrdered[2].song.ID != "oldest" {
		t.Fatalf("sortEntriesByMode(waiting, single batch) order = [%s %s %s], want [newest middle oldest]",
			waitingRemovalOrdered[0].song.ID,
			waitingRemovalOrdered[1].song.ID,
			waitingRemovalOrdered[2].song.ID,
		)
	}
}

func TestWaitingRemovalSortingUsesLowestBatchAndPositionFirst(t *testing.T) {
	now := time.Now()
	entries := []songListEntry{
		{
			song:      templates.Song{ID: "batch2-pos2", BatchSeq: 2, BatchPos: 2},
			createdAt: now.Add(-1 * time.Hour),
		},
		{
			song:      templates.Song{ID: "batch1-pos3", BatchSeq: 1, BatchPos: 3},
			createdAt: now.Add(-4 * time.Hour),
		},
		{
			song:      templates.Song{ID: "batch1-pos1", BatchSeq: 1, BatchPos: 1},
			createdAt: now.Add(-5 * time.Hour),
		},
		{
			song:      templates.Song{ID: "batch2-pos1", BatchSeq: 2, BatchPos: 1},
			createdAt: now.Add(-2 * time.Hour),
		},
	}

	waitingRemovalOrdered := append([]songListEntry(nil), entries...)
	sortEntriesByMode(waitingRemovalOrdered, playlistSortAddedDesc, waitingRemovalSortMode)
	if waitingRemovalOrdered[0].song.ID != "batch1-pos1" ||
		waitingRemovalOrdered[1].song.ID != "batch1-pos3" ||
		waitingRemovalOrdered[2].song.ID != "batch2-pos1" ||
		waitingRemovalOrdered[3].song.ID != "batch2-pos2" {
		t.Fatalf("sortEntriesByMode(waiting, removal order) = [%s %s %s %s], want [batch1-pos1 batch1-pos3 batch2-pos1 batch2-pos2]",
			waitingRemovalOrdered[0].song.ID,
			waitingRemovalOrdered[1].song.ID,
			waitingRemovalOrdered[2].song.ID,
			waitingRemovalOrdered[3].song.ID,
		)
	}
}

func TestParseSongReleaseDate(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		valid    bool
	}{
		{"2026-06-14", "2026-06-14", true},
		{"10 October 1995", "1995-10-10", true},
		{"18 September 2015", "2015-09-18", true},
		{"2015", "2015-01-01", true},
		{"invalid-date", "", false},
	}

	for _, tc := range tests {
		parsed, valid := parseSongReleaseDate(tc.input)
		if valid != tc.valid {
			t.Errorf("parseSongReleaseDate(%q) valid = %v, want %v", tc.input, valid, tc.valid)
		}
		if tc.valid {
			formatted := parsed.Format("2006-01-02")
			if formatted != tc.expected {
				t.Errorf("parseSongReleaseDate(%q) = %s, want %s", tc.input, formatted, tc.expected)
			}
		}
	}
}

func TestCompareByReleaseDateAsc(t *testing.T) {
	now := time.Now()
	entries := []songListEntry{
		{
			song:             templates.Song{ID: "newer"},
			releaseDate:      time.Date(2015, 9, 18, 0, 0, 0, 0, time.UTC),
			releaseDateValid: true,
			createdAt:        now,
		},
		{
			song:             templates.Song{ID: "older"},
			releaseDate:      time.Date(1995, 10, 10, 0, 0, 0, 0, time.UTC),
			releaseDateValid: true,
			createdAt:        now,
		},
		{
			song:             templates.Song{ID: "invalid"},
			releaseDate:      time.Time{},
			releaseDateValid: false,
			createdAt:        now,
		},
	}

	// Ascending: older < newer
	if !compareByReleaseDateAsc(entries[1], entries[0]) {
		t.Errorf("Expected older to be before newer in ascending sort")
	}
	if compareByReleaseDateAsc(entries[0], entries[1]) {
		t.Errorf("Expected newer to be after older in ascending sort")
	}

	// Valid should come before invalid
	if !compareByReleaseDateAsc(entries[0], entries[2]) {
		t.Errorf("Expected valid to be before invalid in ascending sort")
	}
	if compareByReleaseDateAsc(entries[2], entries[0]) {
		t.Errorf("Expected invalid to be after valid in ascending sort")
	}
}
