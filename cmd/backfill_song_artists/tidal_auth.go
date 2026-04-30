//go:build goexperiment.jsonv2

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type tidalCredentials struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	HTTPTimeout  time.Duration
}

func resolveTidalToken(ctx context.Context, httpClient *http.Client, creds tidalCredentials, flagToken string) string {
	token := strings.TrimSpace(flagToken)
	if token == "" {
		token = strings.TrimSpace(os.Getenv("TIDAL_TOKEN"))
	}
	clientID := strings.TrimSpace(creds.ClientID)
	if clientID == "" {
		clientID = strings.TrimSpace(os.Getenv("TIDAL_CLIENT_ID"))
	}
	clientSecret := strings.TrimSpace(creds.ClientSecret)
	if clientSecret == "" {
		clientSecret = strings.TrimSpace(os.Getenv("TIDAL_CLIENT_SECRET"))
	}
	if token != "" {
		return token
	}
	if clientID == "" || clientSecret == "" {
		return ""
	}
	timeout := creds.HTTPTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t, err := fetchTidalAccessToken(ctxWithTimeout, httpClient, creds.TokenURL, clientID, clientSecret)
	if err != nil {
		log.Printf("[backfill_song_artists] warning: failed to obtain TIDAL access token: %v", err)
		return ""
	}
	log.Printf("[backfill_song_artists] obtained TIDAL access token via client credentials")
	return t
}

func fetchTidalAccessToken(ctx context.Context, client *http.Client, tokenURL, clientID, clientSecret string) (string, error) {
	tokenURL = strings.TrimSpace(tokenURL)
	if tokenURL == "" {
		tokenURL = "https://auth.tidal.com/v1/oauth2/token"
	}
	if strings.TrimSpace(clientID) == "" || strings.TrimSpace(clientSecret) == "" {
		return "", fmt.Errorf("missing TIDAL client credentials")
	}

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	basic := base64.StdEncoding.EncodeToString([]byte(clientID + ":" + clientSecret))
	req.Header.Set("Authorization", "Basic "+basic)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("tidal token endpoint returned %s", resp.Status)
	}

	var payload struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("failed to decode tidal token response: %w", err)
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", fmt.Errorf("tidal token response did not include access_token")
	}
	return strings.TrimSpace(payload.AccessToken), nil
}
