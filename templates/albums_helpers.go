package templates

// AlbumRowID returns the DOM element ID for an album row.
func AlbumRowID(albumID string) string {
	return "album-" + albumID
}

// AlbumCardID returns the DOM element ID for a waiting album card.
func AlbumCardID(albumID string) string {
	return "album-card-" + albumID
}

// AlbumsTBodyID returns the DOM element ID for the album status container.
func AlbumsTBodyID(status string) string {
	return "albums-" + status
}
