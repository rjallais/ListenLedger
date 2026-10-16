package handlers

import (
	"sort"
	"strings"
	"time"

	"ListenLedger/templates"
)

// songReleaseDateLayouts defines the accepted release date layouts, supporting
// standard ISO YYYY-MM-DD as well as human-friendly and legacy formats.
var songReleaseDateLayouts = []string{
	"2006-01-02",
	"2 January 2006",
	"02 January 2006",
	"January 2, 2006",
	"Jan 2, 2006",
	"2006-01",
	"2006",
}

// formatParsedReleaseDate converts an already parsed release date to UI format.
func formatParsedReleaseDate(stored string, t time.Time, valid bool) string {
	if !valid {
		return stored
	}
	trimmed := strings.TrimSpace(stored)
	if len(trimmed) <= 4 {
		return stored // empty string or plain year — pass through
	}
	if len(trimmed) == 7 && trimmed[4] == '-' {
		return t.Format("January 2006")
	}
	return t.Format("2 January 2006")
}

// formatReleaseDateForUI converts a stored date to a human-friendly format
// like "2 January 2006". Plain years (e.g. "2006") or unparseable values are
// returned as-is.
func formatReleaseDateForUI(stored string) string {
	trimmed := strings.TrimSpace(stored)
	if len(trimmed) <= 4 {
		return stored // empty string or plain year — pass through
	}
	t, ok := parseSongReleaseDate(trimmed)
	return formatParsedReleaseDate(stored, t, ok)
}

func parseSongReleaseDate(stored string) (time.Time, bool) {
	trimmed := strings.TrimSpace(stored)
	if trimmed == "" {
		return time.Time{}, false
	}
	for _, layout := range songReleaseDateLayouts {
		if t, err := time.Parse(layout, trimmed); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

type songListEntry struct {
	song             templates.Song
	createdAt        time.Time
	releaseDate      time.Time
	releaseDateValid bool
	titleLower       string
}

func normalizePlaylistSort(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case playlistSortReleaseAsc:
		return playlistSortReleaseAsc
	default:
		return playlistSortAddedDesc
	}
}

func sortRecentSongEntries(entries []songListEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		left := entries[i]
		right := entries[j]

		if left.song.BatchSeq != right.song.BatchSeq {
			return left.song.BatchSeq > right.song.BatchSeq
		}
		// Within each batch, older insertions keep higher positions and stay
		// longer in the playlist.
		if left.song.BatchPos != right.song.BatchPos {
			return left.song.BatchPos > right.song.BatchPos
		}
		if !left.createdAt.Equal(right.createdAt) {
			return left.createdAt.Before(right.createdAt)
		}
		if left.releaseDateValid != right.releaseDateValid {
			return left.releaseDateValid
		}
		if !left.releaseDate.Equal(right.releaseDate) {
			return left.releaseDate.After(right.releaseDate)
		}
		if left.titleLower != right.titleLower {
			return left.titleLower < right.titleLower
		}

		return left.song.ID < right.song.ID
	})
}

func sortNotRecentSongEntries(entries []songListEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		left := entries[i]
		right := entries[j]

		if left.releaseDateValid != right.releaseDateValid {
			return left.releaseDateValid
		}
		if !left.releaseDate.Equal(right.releaseDate) {
			return left.releaseDate.After(right.releaseDate)
		}
		if !left.createdAt.Equal(right.createdAt) {
			return left.createdAt.After(right.createdAt)
		}
		if left.titleLower != right.titleLower {
			return left.titleLower < right.titleLower
		}
		return left.song.ID < right.song.ID
	})
}

// compareByReleaseDateAsc returns true when left should sort before right by
// ascending release date, falling through to createdAt then title then ID.
// Songs with missing or invalid dates sort after valid dates.
func compareByReleaseDateAsc(left, right songListEntry) bool {
	if left.releaseDateValid != right.releaseDateValid {
		return left.releaseDateValid
	}
	if !left.releaseDate.Equal(right.releaseDate) {
		return left.releaseDate.Before(right.releaseDate)
	}
	if !left.createdAt.Equal(right.createdAt) {
		return left.createdAt.Before(right.createdAt)
	}
	return compareTitleThenID(left, right)
}

func compareInt(a, b int, descending bool) bool {
	if descending {
		return a > b
	}
	return a < b
}

func compareTime(a, b time.Time, descending bool) bool {
	if descending {
		return a.After(b)
	}
	return a.Before(b)
}

func compareByBatchSeq(left, right songListEntry, descending bool) bool {
	if left.song.BatchSeq != right.song.BatchSeq {
		return compareInt(left.song.BatchSeq, right.song.BatchSeq, descending)
	}
	if left.song.BatchPos != right.song.BatchPos {
		return left.song.BatchPos < right.song.BatchPos
	}
	if !left.createdAt.Equal(right.createdAt) {
		return compareTime(left.createdAt, right.createdAt, descending)
	}
	return left.song.ID < right.song.ID
}

// compareByBatchSeqDesc sorts by batch seq descending, pos ascending, then
// createdAt descending — used for the current-playlist "added-desc" view.
func compareByBatchSeqDesc(left, right songListEntry) bool {
	return compareByBatchSeq(left, right, true)
}

// compareByBatchSeqAsc sorts by batch seq ascending, pos ascending, then
// createdAt ascending — used for the waiting-removal "added-desc" view.
func compareByBatchSeqAsc(left, right songListEntry) bool {
	return compareByBatchSeq(left, right, false)
}

// compareByWaitingReleaseAsc sorts waiting-removal entries by release date asc,
// then batch seq/pos asc, createdAt asc, title, ID.
func compareByWaitingReleaseAsc(left, right songListEntry) bool {
	if left.releaseDateValid != right.releaseDateValid {
		return left.releaseDateValid
	}
	if !left.releaseDate.Equal(right.releaseDate) {
		return left.releaseDate.Before(right.releaseDate)
	}
	return compareByBatchSeqAsc(left, right)
}

func compareTitleThenID(left, right songListEntry) bool {
	if left.titleLower != right.titleLower {
		return left.titleLower < right.titleLower
	}
	return left.song.ID < right.song.ID
}

// assignSortOrders precomputes both the added-desc order index and the release-asc
// order index for each song in the section. This enables instant client-side DOM sorting
// without network roundtrips or idiomorph DOM tree walks.
func assignSortOrders(entries []songListEntry, mode songSortMode) {
	if len(entries) == 0 {
		return
	}
	addedOrder := append([]songListEntry(nil), entries...)
	sort.SliceStable(addedOrder, func(i, j int) bool {
		return mode.defaultCmp(addedOrder[i], addedOrder[j])
	})
	addedIndices := make(map[string]int, len(addedOrder))
	for i, entry := range addedOrder {
		addedIndices[entry.song.ID] = i
	}

	releaseOrder := append([]songListEntry(nil), entries...)
	sort.SliceStable(releaseOrder, func(i, j int) bool {
		return mode.releaseAsc(releaseOrder[i], releaseOrder[j])
	})
	releaseIndices := make(map[string]int, len(releaseOrder))
	for i, entry := range releaseOrder {
		releaseIndices[entry.song.ID] = i
	}

	for i := range entries {
		id := entries[i].song.ID
		entries[i].song.OrderAdded = addedIndices[id]
		entries[i].song.OrderRelease = releaseIndices[id]
	}
}

type songSortMode struct {
	releaseAsc func(left, right songListEntry) bool
	defaultCmp func(left, right songListEntry) bool
}

var playlistSortMode = songSortMode{
	releaseAsc: compareByReleaseDateAsc,
	defaultCmp: compareByBatchSeqDesc,
}

var waitingRemovalSortMode = songSortMode{
	releaseAsc: compareByWaitingReleaseAsc,
	defaultCmp: compareByBatchSeqAsc,
}

func sortEntriesByMode(entries []songListEntry, playlistSort string, mode songSortMode) {
	cmp := mode.defaultCmp
	if normalizePlaylistSort(playlistSort) == playlistSortReleaseAsc {
		cmp = mode.releaseAsc
	}
	sort.SliceStable(entries, func(i, j int) bool { return cmp(entries[i], entries[j]) })
}

// partitionRecentEntries splits entries into recent and not-recent slices.
func partitionRecentEntries(entries []songListEntry) (recent, notRecent []songListEntry) {
	recent = make([]songListEntry, 0, len(entries))
	notRecent = make([]songListEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.song.IsRecent {
			recent = append(recent, entry)
		} else {
			notRecent = append(notRecent, entry)
		}
	}
	return
}

// splitPlaylistBuckets partitions sorted recent entries into current-playlist
// and waiting-removal buckets based on songsCurrentPlaylistSize.
func splitPlaylistBuckets(recent []songListEntry) (current, waiting []songListEntry) {
	current = make([]songListEntry, 0, min(len(recent), songsCurrentPlaylistSize))
	waiting = make([]songListEntry, 0, max(0, len(recent)-songsCurrentPlaylistSize))
	for i, entry := range recent {
		if i < songsCurrentPlaylistSize {
			current = append(current, entry)
		} else {
			waiting = append(waiting, entry)
		}
	}
	return
}

func filterNotRecentEntries(entries []songListEntry) []songListEntry {
	notRecent := make([]songListEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.song.IsRecent {
			notRecent = append(notRecent, entry)
		}
	}
	return notRecent
}

func paginateEntries(entries []songListEntry, offset, limit int) []templates.Song {
	total := len(entries)
	if offset >= total {
		return []templates.Song{}
	}
	end := min(offset+limit, total)
	page := make([]templates.Song, 0, end-offset)
	for _, entry := range entries[offset:end] {
		page = append(page, entry.song)
	}
	return page
}
