package handlers

import (
	"fmt"
	"net/http"

	"github.com/CAFxX/httpcompression"
	"github.com/CAFxX/httpcompression/contrib/andybalholm/brotli"
)

// compressibleContentTypes lists the response content types the compressor is
// allowed to touch. Kept explicit (no catch-all) so already-compressed payloads
// like JPEG/PNG/WOFF2/ZIP are never re-compressed and CPU is never wasted on
// them. "text/html" also matches "text/html; charset=utf-8" (parameter-less
// MIME match), and likewise for the others.
var compressibleContentTypes = []string{
	"text/html",
	"text/css",
	"application/javascript",
	"application/json",
}

// brotliLevel is the andybalholm/brotli quality (0-11). 5 mirrors the level
// used for precompressed static assets (static.go) and the SSE lane: ~90%+ of
// Brotli's best ratio at a fraction of level 11's CPU, so it stays cheap enough
// to compress dynamic HTML/JSON on the fly.
const brotliLevel = 5

// brotliPriority places Brotli above Zstandard in the codec preference order.
// CAFxX/httpcompression's DefaultAdapter ranks Zstandard (-50) above its stock
// Brotli (-100); a more negative number is lower priority. We re-register
// Brotli at -10 so it is the top pick whenever the client accepts it, keeping
// Brotli (not zstd) as the primary codec for this app.
const brotliPriority = -10

// compressMiddleware is the response-compression middleware shared by all
// routes. Built once at startup; a configuration error is a programmer error
// and must fail fast rather than silently serve uncompressed responses.
var compressMiddleware = mustCompressMiddleware()

// mustCompressMiddleware builds the response-compression middleware.
//
// It uses github.com/CAFxX/httpcompression — the same compression engine that
// backs Datastar's own SSE compression (datastar.WithBrotli) — so general HTTP
// responses (HTML pages, /api/* JSON) and the SSE lane share one consistent
// Brotli-first stack instead of Datastar using CAFxX while the HTML path used a
// bespoke chi encoder.
//
// DefaultAdapter enables Zstandard, Brotli, Deflate and Gzip. In its stock
// ordering Zstandard outranks Brotli, so we re-register Brotli at a higher
// priority (brotliPriority) to make it the top choice whenever a client accepts
// it — Brotli is the most broadly supported of the modern codecs, whereas zstd
// support is still uneven across browsers. Clients that don't accept Brotli
// transparently fall back to zstd/gzip.
func mustCompressMiddleware() func(http.Handler) http.Handler {
	mw, err := httpcompression.DefaultAdapter(
		httpcompression.ContentTypes(compressibleContentTypes, false),
		// Re-register Brotli above Zstandard so Brotli wins when accepted.
		// (DefaultAdapter's Brotli priority is -100; Zstandard is -50.)
		httpcompression.Compressor(brotli.Encoding, brotliPriority, mustBrotliCompressor()),
	)
	if err != nil {
		panic(fmt.Errorf("handlers: build compression middleware: %w", err))
	}
	return mw
}

// mustBrotliCompressor constructs the Brotli compressor provider at brotliLevel.
func mustBrotliCompressor() httpcompression.CompressorProvider {
	c, err := brotli.New(brotli.Options{Quality: brotliLevel})
	if err != nil {
		panic(fmt.Errorf("handlers: build brotli compressor: %w", err))
	}
	return c
}
