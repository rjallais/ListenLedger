//go:build goexperiment.jsonv2

package handlers

import (
	"net/http"
	"time"

	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/internal/buildinfo"
	"ListenLedger/internal/quota"
)

// handleQuota returns the quota status for all configured providers.
func (h *Handler) handleQuota(e *core.RequestEvent) error {
	checker := quota.NewChecker(h.cfg)
	quotas := checker.CheckAll(e.Request.Context())

	return e.JSON(http.StatusOK, map[string]any{
		"providers":     quotas,
		"has_available": quota.HasAvailableFrom(quotas),
		"best_provider": quota.GetBestFrom(quotas),
	})
}

// handleAppHealth returns a lightweight JSON health check with app name and uptime.
func (h *Handler) handleAppHealth(e *core.RequestEvent) error {
	uptime := time.Since(h.startedAt)
	return e.JSON(http.StatusOK, map[string]any{
		"status":     "ok",
		"app":        "ListenLedger",
		"version":    buildinfo.Version,
		"uptime_s":   int(uptime.Seconds()),
		"started_at": h.startedAt.UTC().Format(time.RFC3339),
	})
}
