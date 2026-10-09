package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"ListenLedger/templates"
)

func TestHandleUpdateTheme(t *testing.T) {
	h := &Handler{}

	r := chi.NewRouter()
	r.Post("/api/theme/{theme}", h.HandleUpdateTheme)

	tests := []struct {
		themeInput    string
		expectedValue string
	}{
		{"dark", "dark"},
		{"light", "light"},
		{"system", "system"},
		{"invalid", "system"},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(http.MethodPost, "/api/theme/"+tt.themeInput, nil)
		rec := httptest.NewRecorder()

		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Errorf("theme %q: expected status 204, got %d", tt.themeInput, rec.Code)
		}

		cookies := rec.Result().Cookies()
		var themeCookie *http.Cookie
		for _, c := range cookies {
			if c.Name == "theme" {
				themeCookie = c
				break
			}
		}

		if themeCookie == nil {
			t.Fatalf("theme %q: expected 'theme' cookie to be set", tt.themeInput)
		}
		if themeCookie.Value != tt.expectedValue {
			t.Errorf("theme %q: expected cookie value %q, got %q", tt.themeInput, tt.expectedValue, themeCookie.Value)
		}
	}
}

func TestThemeMiddleware(t *testing.T) {
	var observedTheme string
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedTheme = templates.ThemeFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	handler := ThemeMiddleware(testHandler)

	// Case 1: No cookie -> defaults to "system"
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if observedTheme != "system" {
		t.Errorf("expected default theme 'system', got %q", observedTheme)
	}

	// Case 2: Dark theme cookie
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: "theme", Value: "dark"})
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if observedTheme != "dark" {
		t.Errorf("expected theme 'dark', got %q", observedTheme)
	}

	// Case 3: Light theme cookie
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: "theme", Value: "light"})
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if observedTheme != "light" {
		t.Errorf("expected theme 'light', got %q", observedTheme)
	}
}

func TestLayoutRendersServerThemeClass(t *testing.T) {
	ctx := templates.WithTheme(context.Background(), "dark")
	var sb strings.Builder
	err := templates.Layout("Test", "albums").Render(ctx, &sb)
	if err != nil {
		t.Fatalf("failed to render layout: %v", err)
	}

	html := sb.String()
	if !strings.Contains(html, `class="dark"`) || !strings.Contains(html, `data-signals="{ theme: &#39;dark&#39; }"`) {
		t.Errorf("expected server-rendered dark html tag, got:\n%s", html[:min(300, len(html))])
	}
	if strings.Contains(html, "localStorage") {
		t.Errorf("layout must not contain any localStorage references")
	}
}
