//go:build goexperiment.jsonv2

// Package buildinfo exposes build-time metadata.
package buildinfo

// Version is the application version returned by health endpoints.
// Override at build time with:
// go build -ldflags "-X ListenLedger/internal/buildinfo.Version=1.2.3"
var Version = "dev"
