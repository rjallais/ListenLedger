package handlers

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

// HandleUpdateTheme sets the theme preference in an HTTP cookie and returns 204 No Content.
func (h *Handler) HandleUpdateTheme(w http.ResponseWriter, r *http.Request) {
	theme := strings.TrimSpace(chi.URLParam(r, "theme"))
	if theme != "light" && theme != "dark" && theme != "system" {
		theme = "system"
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "theme",
		Value:    theme,
		Path:     "/",
		MaxAge:   365 * 24 * 3600,
		SameSite: http.SameSiteLaxMode,
	})

	w.WriteHeader(http.StatusNoContent)
}
