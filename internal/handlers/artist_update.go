//go:build goexperiment.jsonv2

package handlers

import (
	"database/sql"
	"errors"
	"log"
	"net/http"

	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/templates"
)

// handleUpdateListStatus updates the list_status of an artist.
func (h *Handler) handleUpdateListStatus(e *core.RequestEvent) error {
	ctx := e.Request.Context()
	artistID := e.Request.PathValue("artistId")
	if artistID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "artist ID required"})
	}

	newStatus := e.Request.PathValue("status")
	if !allowedListStatuses[newStatus] {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid status value"})
	}

	record, err := h.findArtistRecord(ctx, artistID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return e.JSON(http.StatusNotFound, map[string]string{"error": "artist not found"})
		}
		log.Printf("[artist_update] findArtistRecord error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to lookup artist"})
	}

	oldStatus := record.GetString("list_status")
	record.Set("list_status", newStatus)
	if err := h.app.SaveWithContext(ctx, record); err != nil {
		log.Printf("[artist_update] Save error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update artist"})
	}

	currentGenre := currentGenreFromRequest(e.Request)

	// Build rank cache for O(1) lookup instead of O(N) query per artist
	genre := record.GetString("genre_group")
	rankCache, err := h.buildArtistRankMap(ctx, genre)
	if err != nil {
		log.Printf("[artist_update] warning: failed to build rank cache: %v", err)
	}
	totalSongs := h.dynamicTotalSongs(ctx, record, rankCache)

	return renderUpdatedArtistStatus(e, artistStatusUpdateParams{
		Event:        e,
		OldStatus:    oldStatus,
		NewStatus:    newStatus,
		CurrentGenre: currentGenre,
		Artist:       artistFromRecord(record, totalSongs),
	})
}

// handleUpdateCollectionSongs increments or decrements the collection_songs count.
func (h *Handler) handleUpdateCollectionSongs(e *core.RequestEvent) error {
	ctx := e.Request.Context()
	artistID := e.Request.PathValue("artistId")
	if artistID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "artist ID required"})
	}

	delta, err := parseSongCountAction(e.Request.PathValue("action"))
	if err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	record, err := h.findArtistRecord(ctx, artistID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return e.JSON(http.StatusNotFound, map[string]string{"error": "artist not found"})
		}
		log.Printf("[artist_update] findArtistRecord error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to lookup artist"})
	}

	updateArtistCollectionSongs(record, delta)
	if err := h.app.SaveWithContext(ctx, record); err != nil {
		log.Printf("[artist_update] Save error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update artist"})
	}

	// Build rank cache for O(1) lookup instead of O(N) query per artist
	genre := record.GetString("genre_group")
	rankCache, err := h.buildArtistRankMap(ctx, genre)
	if err != nil {
		log.Printf("[artist_update] warning: failed to build rank cache: %v", err)
	}
	totalSongs := h.dynamicTotalSongs(ctx, record, rankCache)

	return renderDatastar(e, templates.ArtistRow(artistFromRecord(record, totalSongs)))
}
