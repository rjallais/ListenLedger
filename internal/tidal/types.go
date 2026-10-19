// Package tidal provides OAuth 2.0 PKCE authentication, FNOX secrets integration,
// and OpenAPI v2 client interactions for TIDAL user collections.
package tidal

import (
	"time"
)

// Token holds OAuth 2.0 token credentials for a user session.
type Token struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	TokenType    string    `json:"token_type"`
	ExpiresIn    int       `json:"expires_in"`
	UserID       string    `json:"user_id"`
	Expiry       time.Time `json:"expiry"`
}

// ObservedTrack represents a track retrieved from a user's collection.
type ObservedTrack struct {
	ID               string    `json:"id"`
	Title            string    `json:"title"`
	ISRC             string    `json:"isrc,omitempty"`
	Duration         int       `json:"duration,omitempty"`
	ReleaseDate      string    `json:"release_date,omitempty"`
	ArtistNames      []string  `json:"artist_names,omitempty"`
	AlbumTitle       string    `json:"album_title,omitempty"`
	AlbumReleaseDate string    `json:"album_release_date,omitempty"`
	AddedAt          time.Time `json:"added_at,omitempty"`
}

// ObservedAlbum represents an album retrieved from a user's collection.
type ObservedAlbum struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	ArtistNames []string  `json:"artist_names,omitempty"`
	ReleaseDate string    `json:"release_date,omitempty"`
	ReleaseType string    `json:"release_type,omitempty"`
	AddedAt     time.Time `json:"added_at,omitempty"`
}

// ObservedArtist represents an artist followed or in a user's collection.
type ObservedArtist struct {
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	AddedAt time.Time `json:"added_at,omitempty"`
}
