//go:build goexperiment.jsonv2

package templates

import (
	"fmt"
	"net/url"
	"strconv"
)

type artistStatusOption struct {
	Label       string
	DotClass    string
	ButtonClass string
	ItemClass   string
	Action      string
}

type artistBadgeProps struct {
	Classes  string
	Label    string
	IconPath string
	ShowIcon bool
}

type paginationNavButton struct {
	Label    string
	Href     string
	Disabled bool
}

type paginationPageLink struct {
	Number int
	Href   string
	Active bool
}

type selectOption struct {
	Value    string
	Label    string
	Selected bool
}

type batchPriorityStat struct {
	Label string
	Count string
}

func intString(value int) string {
	return strconv.Itoa(value)
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func artistsTBodyID(genre string) string {
	return "artists-tbody-" + genre
}

func artistRowID(artistID string) string {
	return "artist-" + artistID
}

func artistCardID(artistID string) string {
	return "artist-card-" + artistID
}

func artistListenerElementID(artistID string) string {
	return "artist-listeners-" + artistID
}

func artistCardListenerElementID(artistID string) string {
	return "artist-card-listeners-" + artistID
}

func artistUpdatedElementID(artistID string) string {
	return "artist-updated-" + artistID
}

func artistCardUpdatedElementID(artistID string) string {
	return "artist-card-updated-" + artistID
}

func artistCollectionDecAction(artistID string) string {
	return fmt.Sprintf("@post('/api/artists/%s/collection/dec')", url.PathEscape(artistID))
}

func artistCollectionIncAction(artistID string) string {
	return fmt.Sprintf("@post('/api/artists/%s/collection/inc')", url.PathEscape(artistID))
}

func artistRefreshPostAction(artistID string) string {
	return fmt.Sprintf("@post('/api/refresh/%s')", url.PathEscape(artistID))
}

func artistCollectionText(artist Artist) string {
	if artist.TotalSongs > 0 {
		return fmt.Sprintf("%d/%d", artist.CollectionSongs, artist.TotalSongs)
	}
	return intString(artist.CollectionSongs)
}

func listStatusOptions(artistID, currentStatus string) []artistStatusOption {
	return []artistStatusOption{
		{
			Label:       "Included",
			DotClass:    "badge badge-success badge-xs",
			ButtonClass: statusOptionButtonClass(false, currentStatus == "included"),
			Action:      artistStatusPostAction(artistID, "included"),
		},
		{
			Label:       "Recently Added",
			DotClass:    "badge badge-info badge-xs",
			ButtonClass: statusOptionButtonClass(false, currentStatus == "recently_added"),
			Action:      artistStatusPostAction(artistID, "recently_added"),
		},
		{
			Label:       "Not Added",
			DotClass:    "badge badge-ghost badge-xs",
			ButtonClass: statusOptionButtonClass(false, currentStatus == "not_added"),
			Action:      artistStatusPostAction(artistID, "not_added"),
		},
		{
			Label:       "Move to Queue",
			DotClass:    "badge badge-warning badge-xs",
			ButtonClass: statusOptionButtonClass(true, currentStatus == "waiting"),
			ItemClass:   "border-t border-base-300 mt-1 pt-1",
			Action:      artistStatusPostAction(artistID, "waiting"),
		},
	}
}

func statusOptionButtonClass(warning, active bool) string {
	className := "text-left"
	if warning {
		className += " text-warning"
	}
	if active {
		className += " active"
	}
	return className
}

func artistStatusPostAction(artistID, status string) string {
	return fmt.Sprintf("@post('/api/artists/%s/status/%s')", url.PathEscape(artistID), url.PathEscape(status))
}

func listStatusBadgeProps(status string) artistBadgeProps {
	switch status {
	case "included":
		return artistBadgeProps{
			Classes:  "badge badge-success badge-sm gap-1",
			Label:    "included",
			IconPath: "M5 13l4 4L19 7",
			ShowIcon: true,
		}
	case "recently_added":
		return artistBadgeProps{
			Classes:  "badge badge-info badge-sm gap-1",
			Label:    "recently added",
			IconPath: "M12 6v6m0 0v6m0-6h6m-6 0H6",
			ShowIcon: true,
		}
	case "not_added":
		return artistBadgeProps{
			Classes:  "badge badge-ghost badge-sm gap-1",
			Label:    "not added",
			IconPath: "M6 18L18 6M6 6l12 12",
			ShowIcon: true,
		}
	case "waiting":
		return artistBadgeProps{
			Classes:  "badge badge-warning badge-sm gap-1",
			Label:    "queued",
			IconPath: "M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z",
			ShowIcon: true,
		}
	default:
		return artistBadgeProps{
			Classes: "badge badge-ghost badge-sm",
			Label:   "unknown",
		}
	}
}

func fetchStatusBadgeProps(status string) artistBadgeProps {
	switch status {
	case "pending":
		return artistBadgeProps{Classes: "badge badge-warning", Label: "pending"}
	case "failed":
		return artistBadgeProps{Classes: "badge badge-error", Label: "failed"}
	default:
		return artistBadgeProps{Classes: "badge badge-ghost", Label: "idle"}
	}
}

func paginationURL(genre string, page int) string {
	return fmt.Sprintf("/artists?genre=%s&page=%d", url.QueryEscape(genre), page)
}

func paginationRange(current, total int) []int {
	start := current - 2
	if start < 1 {
		start = 1
	}
	end := start + 4
	if end > total {
		end = total
		start = end - 4
		if start < 1 {
			start = 1
		}
	}

	var pages []int
	for page := start; page <= end; page++ {
		pages = append(pages, page)
	}
	return pages
}

func paginationPreviousButton(p Pagination) paginationNavButton {
	if p.CurrentPage <= 1 {
		return paginationNavButton{Label: "«", Disabled: true}
	}
	return paginationNavButton{
		Label: "«",
		Href:  paginationURL(p.Genre, p.CurrentPage-1),
	}
}

func paginationNextButton(p Pagination) paginationNavButton {
	if p.CurrentPage >= p.TotalPages {
		return paginationNavButton{Label: "»", Disabled: true}
	}
	return paginationNavButton{
		Label: "»",
		Href:  paginationURL(p.Genre, p.CurrentPage+1),
	}
}

func paginationPageLinks(p Pagination) []paginationPageLink {
	pages := paginationRange(p.CurrentPage, p.TotalPages)
	links := make([]paginationPageLink, 0, len(pages))
	for _, page := range pages {
		links = append(links, paginationPageLink{
			Number: page,
			Href:   paginationURL(p.Genre, page),
			Active: page == p.CurrentPage,
		})
	}
	return links
}

func paginationSummaryText(p Pagination) string {
	return fmt.Sprintf(
		"Showing page %d of %d (%d total artists)",
		p.CurrentPage,
		p.TotalPages,
		p.TotalCount,
	)
}

func artistQueueTitle(count int) string {
	return fmt.Sprintf("Artists Waiting to be Added (%d)", count)
}

func artistQueueLoadMoreAction(nextOffset int) string {
	return fmt.Sprintf("@get('/api/artists/waiting?offset=%d&limit=1')", nextOffset)
}

func artistQueueLoadMoreLabel(nextOffset int) string {
	return fmt.Sprintf("Show Next Artist (%d shown)", nextOffset)
}

func artistGenreOptions(currentGenre string) []selectOption {
	return []selectOption{
		{Value: "rock_metal", Label: "🎸 Rock & Metal", Selected: currentGenre == "rock_metal"},
		{Value: "everything_else", Label: "🎵 Everything Else", Selected: currentGenre == "everything_else"},
	}
}

func artistListStatusOptions() []selectOption {
	return []selectOption{
		{Value: "recently_added", Label: "Recently Added", Selected: true},
		{Value: "included", Label: "Included"},
		{Value: "not_added", Label: "Not Added"},
		{Value: "waiting", Label: "Waiting (Queue)"},
	}
}

func batchRefreshCountOptions() []selectOption {
	return []selectOption{
		{Value: "5", Label: "5 artists"},
		{Value: "10", Label: "10 artists", Selected: true},
		{Value: "25", Label: "25 artists"},
		{Value: "50", Label: "50 artists"},
		{Value: "100", Label: "100 artists"},
	}
}

func batchQueuedSummary(queued int) string {
	return fmt.Sprintf("%d artists queued for refresh", queued)
}

func batchPriorityStats(stats map[string]int) []batchPriorityStat {
	return []batchPriorityStat{
		{Label: "P0 (queued)", Count: intString(stats["P0_Queued"])},
		{Label: "P1-P2 (recent)", Count: intString(stats["P1_RockRecent"] + stats["P2_OtherRecent"])},
		{Label: "P3-P4 (not added)", Count: intString(stats["P3_RockNotAdded"] + stats["P4_OtherNotAdded"])},
		{Label: "P5-P6 (included)", Count: intString(stats["P5_RockIncluded"] + stats["P6_OtherIncluded"])},
	}
}

func batchIDText(batchID string) string {
	return "Batch ID: " + batchID
}

func artistSignalsJSON(artist Artist) string {
	return fmt.Sprintf(
		`{"artistListeners":{%q:%d},"artistUpdated":{%q:%q},"artistFetchStatus":{%q:%q}}`,
		artist.ID,
		artist.MonthlyListeners,
		artist.ID,
		artist.LastUpdated,
		artist.ID,
		artist.FetchStatus,
	)
}

func artistListenerSignal(artistID string) string {
	return fmt.Sprintf("($artistListeners[%q] ?? 0).toLocaleString('en-US')", artistID)
}

func artistUpdatedSignal(artistID string) string {
	return fmt.Sprintf("$artistUpdated[%q]", artistID)
}

func artistFetchStatusSignal(artistID string) string {
	return fmt.Sprintf("$artistFetchStatus[%q]", artistID)
}
