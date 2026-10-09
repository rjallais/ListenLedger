package handlers

import (
	"net/http"
	"time"

	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/internal/buildinfo"
	"ListenLedger/internal/quota"
)

// HandleQuota returns the quota status for all configured providers as a standard http.HandlerFunc.
func (h *Handler) HandleQuota(w http.ResponseWriter, r *http.Request) {
	checker := quota.NewChecker(h.cfg)
	quotas := checker.CheckAll(r.Context())

	_ = writeJSON(w, http.StatusOK, map[string]any{
		"providers":     quotas,
		"has_available": quota.HasAvailableFrom(quotas),
		"best_provider": quota.GetBestFrom(quotas),
	})
}

// handleQuota bridges PocketBase RequestEvent to HandleQuota.
func (h *Handler) handleQuota(e *core.RequestEvent) error {
	h.HandleQuota(e.Response, e.Request)
	return nil
}

// HandleAppHealth returns a lightweight JSON health check with app name and uptime.
func (h *Handler) HandleAppHealth(w http.ResponseWriter, _ *http.Request) {
	uptime := time.Since(h.startedAt)
	_ = writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"app":        "ListenLedger",
		"version":    buildinfo.Version,
		"uptime_s":   int(uptime.Seconds()),
		"started_at": h.startedAt.UTC().Format(time.RFC3339),
	})
}

// handleAppHealth bridges PocketBase RequestEvent to HandleAppHealth.
func (h *Handler) handleAppHealth(e *core.RequestEvent) error {
	h.HandleAppHealth(e.Response, e.Request)
	return nil
}
