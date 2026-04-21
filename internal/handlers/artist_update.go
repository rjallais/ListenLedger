//go:build goexperiment.jsonv2

package handlers

import (
	"log"
	"net/http"

	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/templates"
)

// handleUpdateListStatus updates the list_status of an artist.
func (h *Handler) handleUpdateListStatus(e *core.RequestEvent) error {
	artistID := e.Request.PathValue("artistId")
	if artistID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "artist ID required"})
	}

	newStatus := e.Request.PathValue("status")
	if !allowedListStatuses[newStatus] {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid status value"})
	}

	record, err := h.findArtistRecord(artistID)
	if err != nil {
		log.Printf("[artist_update] findArtistRecord error: %v", err)
		return e.JSON(http.StatusNotFound, map[string]string{"error": "artist not found"})
	}

	oldStatus := record.GetString("list_status")
	record.Set("list_status", newStatus)
	if err := h.app.Save(record); err != nil {
		log.Printf("[artist_update] Save error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update artist"})
	}

	currentGenre := currentGenreFromRequest(e.Request)
	totalSongs := h.dynamicArtistTotalSongs(record)
	return renderUpdatedArtistStatus(e, oldStatus, newStatus, currentGenre, artistFromRecord(record, totalSongs))
}

// handleUpdateCollectionSongs increments or decrements the collection_songs count.
func (h *Handler) handleUpdateCollectionSongs(e *core.RequestEvent) error {
	artistID := e.Request.PathValue("artistId")
	if artistID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "artist ID required"})
	}

	delta, err := parseCollectionSongsAction(e.Request.PathValue("action"))
	if err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	record, err := h.findArtistRecord(artistID)
	if err != nil {
		log.Printf("[artist_update] findArtistRecord error: %v", err)
		return e.JSON(http.StatusNotFound, map[string]string{"error": "artist not found"})
	}

	updateArtistCollectionSongs(record, delta)
	if err := h.app.Save(record); err != nil {
		log.Printf("[artist_update] Save error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update artist"})
	}

	return renderDatastar(e, templates.ArtistRow(artistFromRecord(record, h.dynamicArtistTotalSongs(record))))
}
