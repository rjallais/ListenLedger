package handlers

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func TestSlogMiddleware_ContextAndLogging(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	baseLogger := slog.New(handler)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(SlogMiddleware(baseLogger))

	var capturedReqID string
	r.Get("/test-slog", func(w http.ResponseWriter, req *http.Request) {
		logger := LoggerFromContext(req.Context())
		if logger == nil {
			t.Fatal("expected non-nil logger from context")
		}
		logger.Info("handling test request")
		capturedReqID = middleware.GetReqID(req.Context())
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	req := httptest.NewRequest(http.MethodGet, "/test-slog", nil)
	req.Header.Set("X-Request-Id", "custom-req-123")
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	logOutput := buf.String()
	if capturedReqID != "custom-req-123" {
		t.Fatalf("expected reqID 'custom-req-123', got %q", capturedReqID)
	}
	if !bytes.Contains(buf.Bytes(), []byte("custom-req-123")) {
		t.Fatalf("expected log output to include request id, got: %s", logOutput)
	}
	if !bytes.Contains(buf.Bytes(), []byte("handling test request")) {
		t.Fatalf("expected handler log in output, got: %s", logOutput)
	}
	if !bytes.Contains(buf.Bytes(), []byte("http request")) {
		t.Fatalf("expected middleware completion log in output, got: %s", logOutput)
	}
}

func TestLoggerFromContext_Fallback(t *testing.T) {
	ctx := context.Background()
	logger := LoggerFromContext(ctx)
	if logger == nil {
		t.Fatal("expected fallback logger when none in context, got nil")
	}
}
