//go:build goexperiment.jsonv2

package handlers

import (
	"testing"
	"time"

	"ListenLedger/templates"
)

func TestNextRecentBatchAssignmentFromEntriesStartsAtBatchSize(t *testing.T) {
	seq, pos := nextRecentBatchAssignmentFromEntries(nil, time.Now())
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

	seq, pos := nextRecentBatchAssignmentFromEntries(entries, now)
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

	seq, pos := nextRecentBatchAssignmentFromEntries(entries, now)
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

	h := &Handler{}

	recentOrdered := append([]songListEntry(nil), entries...)
	h.sortRecentSongEntries(recentOrdered)
	if recentOrdered[0].song.ID != "oldest" || recentOrdered[1].song.ID != "middle" || recentOrdered[2].song.ID != "newest" {
		t.Fatalf("sortRecentSongEntries order = [%s %s %s], want [oldest middle newest]",
			recentOrdered[0].song.ID,
			recentOrdered[1].song.ID,
			recentOrdered[2].song.ID,
		)
	}

	playlistOrdered := append([]songListEntry(nil), entries...)
	h.sortPlaylistEntries(playlistOrdered, playlistSortAddedDesc)
	if playlistOrdered[0].song.ID != "newest" || playlistOrdered[1].song.ID != "middle" || playlistOrdered[2].song.ID != "oldest" {
		t.Fatalf("sortPlaylistEntries(added_desc) order = [%s %s %s], want [newest middle oldest]",
			playlistOrdered[0].song.ID,
			playlistOrdered[1].song.ID,
			playlistOrdered[2].song.ID,
		)
	}
}
