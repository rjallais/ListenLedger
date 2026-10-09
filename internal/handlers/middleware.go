package handlers

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/delaneyj/toolbelt"
	"github.com/go-chi/chi/v5/middleware"

	"ListenLedger/templates"
)

// LoggerFromContext extracts the contextual slog.Logger attached to the request,
// or returns slog.Default() if none is present.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := toolbelt.CtxSlog(ctx); ok && logger != nil {
		return logger
	}
	return slog.Default()
}

// SlogMiddleware returns an HTTP middleware that extracts or generates a Request ID,
// attaches a contextual *slog.Logger to r.Context() via toolbelt.CtxWithSlog,
// and records request metrics (method, path, status, duration, response size).
func SlogMiddleware(baseLogger *slog.Logger) func(next http.Handler) http.Handler {
	if baseLogger == nil {
		baseLogger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			reqID := middleware.GetReqID(r.Context())
			if reqID == "" {
				reqID = r.Header.Get("X-Request-Id")
			}

			reqLogger := baseLogger
			if reqID != "" {
				reqLogger = reqLogger.With(slog.String("req_id", reqID))
			}
			ctx := toolbelt.CtxWithSlog(r.Context(), reqLogger)

			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r.WithContext(ctx))

			path := r.URL.Path
			isHealthOrStatic := path == "/api/listenledger/health" ||
				strings.HasPrefix(path, "/static/") ||
				path == "/robots.txt"
			status := ww.Status()
			duration := time.Since(start)

			// Omit noisy static asset & health polling at INFO level; log all requests at DEBUG.
			if isHealthOrStatic && status < 400 {
				reqLogger.Debug("http request",
					"method", r.Method,
					"path", path,
					"status", status,
					"duration_ms", duration.Milliseconds(),
					"bytes", ww.BytesWritten(),
				)
			} else if status >= 500 {
				reqLogger.Error("http request failed",
					"method", r.Method,
					"path", path,
					"status", status,
					"duration_ms", duration.Milliseconds(),
					"bytes", ww.BytesWritten(),
				)
			} else {
				reqLogger.Info("http request",
					"method", r.Method,
					"path", path,
					"status", status,
					"duration_ms", duration.Milliseconds(),
					"bytes", ww.BytesWritten(),
				)
			}
		})
	}
}

// ThemeMiddleware extracts the theme preference from the HTTP request cookie ("theme")
// and attaches it to the request context via templates.WithTheme. With no
// cookie (or an invalid value) the brand default is dark, matching the
// dark-first design direction; explicit light/system choices are respected.
func ThemeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		theme := "dark"
		if c, err := r.Cookie("theme"); err == nil {
			v := strings.TrimSpace(c.Value)
			if v == "light" || v == "dark" || v == "system" {
				theme = v
			}
		}
		ctx := templates.WithTheme(r.Context(), theme)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
