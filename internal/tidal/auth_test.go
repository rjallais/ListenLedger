package tidal

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCodeChallengeRFC7636Vector(t *testing.T) {
	// RFC 7636 Appendix B test vector
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	expectedChallenge := "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"

	challenge := ComputeCodeChallenge(verifier)
	if challenge != expectedChallenge {
		t.Fatalf("expected challenge %q, got %q", expectedChallenge, challenge)
	}
}

func TestGenerateCodeVerifierAndState(t *testing.T) {
	v1, err := GenerateCodeVerifier()
	if err != nil {
		t.Fatalf("GenerateCodeVerifier() error: %v", err)
	}
	if len(v1) < 43 {
		t.Fatalf("expected verifier length >= 43, got %d", len(v1))
	}

	v2, err := GenerateCodeVerifier()
	if err != nil {
		t.Fatalf("GenerateCodeVerifier() error: %v", err)
	}
	if v1 == v2 {
		t.Fatalf("expected unique verifiers, got duplicates")
	}

	s, err := GenerateState()
	if err != nil {
		t.Fatalf("GenerateState() error: %v", err)
	}
	if len(s) == 0 {
		t.Fatalf("empty state generated")
	}
}

func TestBuildAuthorizeURL(t *testing.T) {
	cfg := AuthConfig{
		ClientID: "test-client",
		AuthURL:  "https://login.tidal.com/authorize",
		Scopes:   []string{"collection.read"},
	}
	urlStr, err := BuildAuthorizeURL(cfg, "test-verifier", "test-state")
	if err != nil {
		t.Fatalf("BuildAuthorizeURL() error: %v", err)
	}

	if !strings.Contains(urlStr, "client_id=test-client") {
		t.Errorf("url missing client_id: %s", urlStr)
	}
	if !strings.Contains(urlStr, "code_challenge_method=S256") {
		t.Errorf("url missing code_challenge_method: %s", urlStr)
	}
	if !strings.Contains(urlStr, "state=test-state") {
		t.Errorf("url missing state: %s", urlStr)
	}
}

func TestExchangeCodeAndRefresh(t *testing.T) {
	ctx := t.Context()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		grantType := r.Form.Get("grant_type")
		if grantType == "authorization_code" {
			if r.Form.Get("code") != "valid-code" {
				http.Error(w, "invalid code", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.MarshalWrite(w, map[string]any{
				"access_token":  "access-123",
				"refresh_token": "refresh-123",
				"token_type":    "Bearer",
				"expires_in":    3600,
				"user_id":       987654,
			})
			return
		}

		if grantType == "refresh_token" {
			if r.Form.Get("refresh_token") != "refresh-123" {
				http.Error(w, "invalid refresh token", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.MarshalWrite(w, map[string]any{
				"access_token":  "access-456",
				"refresh_token": "refresh-456",
				"token_type":    "Bearer",
				"expires_in":    3600,
				"user_id":       "987654",
			})
			return
		}

		http.Error(w, "unsupported grant", http.StatusBadRequest)
	}))
	defer server.Close()

	cfg := AuthConfig{
		ClientID:   "test-client",
		TokenURL:   server.URL,
		HTTPClient: server.Client(),
	}

	token, err := ExchangeCode(ctx, server.Client(), cfg, "valid-code", "my-verifier")
	if err != nil {
		t.Fatalf("ExchangeCode() error = %v", err)
	}
	if token.AccessToken != "access-123" {
		t.Errorf("expected access token 'access-123', got %q", token.AccessToken)
	}
	if token.UserID != "987654" {
		t.Errorf("expected user id '987654', got %q", token.UserID)
	}

	var savedToken string
	cfg.PersistRefreshToken = func(ctx context.Context, token string) error {
		savedToken = token
		return nil
	}

	refreshed, err := RefreshAccessToken(ctx, server.Client(), cfg, token.RefreshToken)
	if err != nil {
		t.Fatalf("RefreshAccessToken() error = %v", err)
	}
	if refreshed.AccessToken != "access-456" {
		t.Errorf("expected refreshed access token 'access-456', got %q", refreshed.AccessToken)
	}
	if refreshed.RefreshToken != "refresh-456" {
		t.Errorf("expected rotated refresh token 'refresh-456', got %q", refreshed.RefreshToken)
	}
	if savedToken != "refresh-456" {
		t.Errorf("expected persisted rotated token 'refresh-456', got %q", savedToken)
	}
}
