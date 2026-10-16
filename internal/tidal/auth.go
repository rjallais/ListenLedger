package tidal

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultAuthURL     = "https://login.tidal.com/authorize"
	DefaultTokenURL    = "https://auth.tidal.com/v1/oauth2/token"
	DefaultRedirectURI = "http://127.0.0.1:8888/callback"
	DefaultScope       = "collection.read"
)

// AuthConfig defines the parameters required for TIDAL OAuth PKCE.
type AuthConfig struct {
	ClientID     string
	ClientSecret string
	AuthURL      string
	TokenURL     string
	RedirectURI  string
	Scopes       []string
	HTTPClient   *http.Client
}

// GenerateCodeVerifier produces a random high-entropy cryptographically secure verifier.
func GenerateCodeVerifier() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating code verifier: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// ComputeCodeChallenge computes the S256 code challenge for a verifier.
func ComputeCodeChallenge(verifier string) string {
	hash := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(hash[:])
}

// GenerateState produces a random hex state token for CSRF protection.
func GenerateState() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating state: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// BuildAuthorizeURL builds the interactive browser authorization URL.
func BuildAuthorizeURL(cfg AuthConfig, verifier, state string) (string, error) {
	clientID := strings.TrimSpace(cfg.ClientID)
	if clientID == "" {
		return "", errors.New("missing TIDAL client ID")
	}

	authURL := cmp.Or(strings.TrimSpace(cfg.AuthURL), DefaultAuthURL)
	redirectURI := cmp.Or(strings.TrimSpace(cfg.RedirectURI), DefaultRedirectURI)
	scope := DefaultScope
	if len(cfg.Scopes) > 0 {
		scope = strings.Join(cfg.Scopes, " ")
	}

	challenge := ComputeCodeChallenge(verifier)

	u, err := url.Parse(authURL)
	if err != nil {
		return "", fmt.Errorf("invalid auth URL %q: %w", authURL, err)
	}

	q := u.Query()
	q.Set("client_id", clientID)
	q.Set("redirect_uri", redirectURI)
	q.Set("response_type", "code")
	q.Set("scope", scope)
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")
	q.Set("state", state)
	u.RawQuery = q.Encode()

	return u.String(), nil
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	UserID       any    `json:"user_id"`
}

func parseUserID(val any) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		return fmt.Sprintf("%.0f", v)
	case int, int64:
		return fmt.Sprintf("%d", v)
	default:
		return fmt.Sprint(v)
	}
}

// ExchangeCode exchanges an authorization code and verifier for access and refresh tokens.
func ExchangeCode(ctx context.Context, client *http.Client, cfg AuthConfig, code, verifier string) (*Token, error) {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	tokenURL := cmp.Or(strings.TrimSpace(cfg.TokenURL), DefaultTokenURL)
	redirectURI := cmp.Or(strings.TrimSpace(cfg.RedirectURI), DefaultRedirectURI)

	data := url.Values{}
	data.Set("grant_type", "authorization_code")
	data.Set("client_id", cfg.ClientID)
	data.Set("code", code)
	data.Set("redirect_uri", redirectURI)
	data.Set("code_verifier", verifier)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("building token request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if cfg.ClientSecret != "" {
		basic := base64.StdEncoding.EncodeToString([]byte(cfg.ClientID + ":" + cfg.ClientSecret))
		req.Header.Set("Authorization", "Basic "+basic)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token endpoint returned status %s", resp.Status)
	}

	var tr tokenResponse
	if err := json.UnmarshalRead(resp.Body, &tr); err != nil {
		return nil, fmt.Errorf("decoding token response: %w", err)
	}

	if tr.AccessToken == "" {
		return nil, errors.New("token response missing access_token")
	}

	expiresIn := tr.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600
	}

	return &Token{
		AccessToken:  tr.AccessToken,
		RefreshToken: tr.RefreshToken,
		TokenType:    cmp.Or(tr.TokenType, "Bearer"),
		ExpiresIn:    expiresIn,
		UserID:       parseUserID(tr.UserID),
		Expiry:       time.Now().Add(time.Duration(expiresIn) * time.Second),
	}, nil
}

// RefreshAccessToken exchanges an existing refresh token for a fresh access token.
func RefreshAccessToken(ctx context.Context, client *http.Client, cfg AuthConfig, refreshToken string) (*Token, error) {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	refreshToken = strings.TrimSpace(refreshToken)
	if refreshToken == "" {
		return nil, errors.New("empty refresh token")
	}

	tokenURL := cmp.Or(strings.TrimSpace(cfg.TokenURL), DefaultTokenURL)

	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("refresh_token", refreshToken)
	data.Set("client_id", cfg.ClientID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("building refresh token request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if cfg.ClientSecret != "" {
		basic := base64.StdEncoding.EncodeToString([]byte(cfg.ClientID + ":" + cfg.ClientSecret))
		req.Header.Set("Authorization", "Basic "+basic)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting refresh token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("refresh token endpoint returned status %s", resp.Status)
	}

	var tr tokenResponse
	if err := json.UnmarshalRead(resp.Body, &tr); err != nil {
		return nil, fmt.Errorf("decoding refresh response: %w", err)
	}

	newRefreshToken := tr.RefreshToken
	if newRefreshToken == "" {
		newRefreshToken = refreshToken
	}

	expiresIn := tr.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600
	}

	token := &Token{
		AccessToken:  tr.AccessToken,
		RefreshToken: newRefreshToken,
		TokenType:    cmp.Or(tr.TokenType, "Bearer"),
		ExpiresIn:    expiresIn,
		UserID:       parseUserID(tr.UserID),
		Expiry:       time.Now().Add(time.Duration(expiresIn) * time.Second),
	}

	// If rotated, save back to FNOX
	if newRefreshToken != refreshToken {
		_ = SaveSecret(ctx, "TIDAL_REFRESH_TOKEN", newRefreshToken)
	}

	return token, nil
}

// AuthorizeLoopback performs the interactive PKCE authorization flow using a local loopback server.
func AuthorizeLoopback(ctx context.Context, cfg AuthConfig) (*Token, error) {
	redirectURI := cmp.Or(strings.TrimSpace(cfg.RedirectURI), DefaultRedirectURI)
	u, err := url.Parse(redirectURI)
	if err != nil {
		return nil, fmt.Errorf("invalid redirect URI: %w", err)
	}

	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "8888"
	}
	addr := net.JoinHostPort(host, port)
	if host == "localhost" || host == "127.0.0.1" {
		addr = ":" + port
	}

	verifier, err := GenerateCodeVerifier()
	if err != nil {
		return nil, err
	}
	state, err := GenerateState()
	if err != nil {
		return nil, err
	}

	authURL, err := BuildAuthorizeURL(cfg, verifier, state)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to bind loopback listener at %s: %w", addr, err)
	}
	defer func() { _ = listener.Close() }()

	type authResult struct {
		code string
		err  error
	}
	resChan := make(chan authResult, 1)

	mux := http.NewServeMux()
	mux.HandleFunc(u.Path, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if errParam := q.Get("error"); errParam != "" {
			desc := q.Get("error_description")
			http.Error(w, fmt.Sprintf("Authorization error: %s (%s)", errParam, desc), http.StatusBadRequest)
			resChan <- authResult{err: fmt.Errorf("tidal auth error: %s (%s)", errParam, desc)}
			return
		}

		if q.Get("state") != state {
			http.Error(w, "State parameter mismatch", http.StatusBadRequest)
			resChan <- authResult{err: errors.New("state parameter mismatch")}
			return
		}

		code := q.Get("code")
		if code == "" {
			http.Error(w, "Missing authorization code", http.StatusBadRequest)
			resChan <- authResult{err: errors.New("missing authorization code")}
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><body><h2>Authentication successful!</h2><p>You can return to the terminal and close this tab.</p></body></html>`))
		resChan <- authResult{code: code}
	})

	server := &http.Server{Handler: mux}
	go func() {
		_ = server.Serve(listener)
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	fmt.Printf("\nOpen this URL in your browser to authorize ListenLedger with TIDAL:\n\n%s\n\nWaiting for callback on %s...\n", authURL, redirectURI)

	var res authResult
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res = <-resChan:
	}

	if res.err != nil {
		return nil, res.err
	}

	token, err := ExchangeCode(ctx, cfg.HTTPClient, cfg, res.code, verifier)
	if err != nil {
		return nil, fmt.Errorf("exchanging code: %w", err)
	}

	if token.RefreshToken != "" {
		if err := SaveSecret(ctx, "TIDAL_REFRESH_TOKEN", token.RefreshToken); err != nil {
			fmt.Printf("Warning: failed to persist TIDAL_REFRESH_TOKEN to FNOX: %v\n", err)
		} else {
			fmt.Println("Successfully saved TIDAL_REFRESH_TOKEN into FNOX.")
		}
	}

	return token, nil
}
