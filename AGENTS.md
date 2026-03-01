# AGENTS.md

Guide for AI agents working in the ListenLedger codebase.

## Big Picture
- Primary app is a Go web dashboard using PocketBase + embedded NATS + Templ + Datastar (SSE). Entry point: `main.go`.
- Background scraping flow: `/api/refresh/{artistId}` publishes `scrape.request` -> `internal/worker` consumes -> `internal/fetcher` retries -> `internal/spotify` (local headless, Browserless, ScrapingAnt, ScraperAPI, Apify) -> updates PocketBase and publishes `artist.updated` for SSE.
- Worker architecture: a single durable JetStream consumer feeds a shared Go channel. Each configured provider runs a goroutine pool sized to its concurrency limit (pull-based). Providers pull work as they have capacity — e.g. a provider with 5 slots keeps 5 requests in flight. On quota exhaustion (`spotify.ErrQuotaExhausted`), the provider's entire goroutine pool shuts down; when all providers are exhausted the NATS consumer is drained. NAK-ed messages remain in JetStream for redelivery by surviving providers or after a restart.
- Standalone utilities: `cmd/update_listeners` (PocketBase + chromedp bulk refresh with priority ordering) and `cmd/safebackup` (VACUUM INTO SQLite backups).

## Build/Lint/Test Commands

All Go commands require `GOEXPERIMENT=jsonv2` due to `encoding/json/v2` imports.

```bash
# Build the main application
GOEXPERIMENT=jsonv2 go build -o ListenLedger .

# Run all tests
GOEXPERIMENT=jsonv2 go test ./...

# Run a single test
GOEXPERIMENT=jsonv2 go test -v ./internal/messaging -run TestScrapeRequestedRoundTrip

# Run tests for a specific package
GOEXPERIMENT=jsonv2 go test -v ./internal/messaging

# Vet code
GOEXPERIMENT=jsonv2 go vet ./...

# Generate templ files (after editing templates/*.templ)
go tool templ generate

# Generate Tailwind CSS (after editing input.css)
go tool gotailwind -i input.css -o static/styles.css

# Hot reload development server
go tool air

# Run standalone utilities
GOEXPERIMENT=jsonv2 go run ./cmd/update_listeners
GOEXPERIMENT=jsonv2 go run ./cmd/safebackup
```

## Code Style Guidelines

### Build Constraint
Every `.go` file must start with the jsonv2 build constraint:
```go
//go:build goexperiment.jsonv2
```

### Import Ordering
Group imports in this order with blank lines between groups (matches `goimports` convention):
1. Standard library imports (`context`, `fmt`, etc.)
2. Third-party imports (`github.com/...`)
3. Local package imports (`ListenLedger/...`)

Example:
```go
import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pocketbase/pocketbase"

	"ListenLedger/config"
	"ListenLedger/internal/messaging"
)
```

### Package Documentation
Each package should have a brief doc comment describing its purpose:
```go
// Package messaging defines event contracts and serialization for NATS subjects.
package messaging
```

### Naming Conventions
- **Local variables**: `camelCase` (e.g., `artistID`, `totalCount`)
- **Exported identifiers**: `PascalCase` (e.g., `NewClient`, `FetchListenerCount`)
- **Constants**: `PascalCase` for exported, `camelCase` for unexported
- **Acronyms**: Preserve case (e.g., `ID` not `Id`, `HTTP` not `Http`)

### Structs and Constructors
- Prefer returning structs from constructor functions
- Use descriptive names like `New`, `NewService`, `NewClient`
- Group related fields with blank lines for readability

### Error Handling
- Wrap errors with context using `fmt.Errorf`:
  ```go
  return fmt.Errorf("failed to connect to NATS: %w", err)
  ```
- Log warnings for non-critical failures; return errors for critical ones
- Use `log.Printf("[component] message")` format for logs

### Context Usage
- Always pass `context.Context` as the first parameter to functions that do I/O
- Use `context.WithTimeout` for operations with deadlines
- Defer cancel functions immediately after creating contexts

### Testing Patterns
- Name tests: `Test<FunctionName>` or `Test<FunctionName><Scenario>`
- Use `t.Helper()` for test helper functions
- Use `t.Fatalf()` with descriptive messages for failures
- Place tests in the same package with `_test.go` suffix

Example:
```go
func TestScrapeRequestedRoundTrip(t *testing.T) {
	in := NewScrapeRequested("artist-1", "spotify-1", "Artist Name", "req-1")
	data, err := MarshalScrapeRequested(in)
	if err != nil {
		t.Fatalf("MarshalScrapeRequested() error = %v", err)
	}
	// ...
}
```

## Key Components (with examples)
- **PocketBase collections** are created on startup in `main.go` (albums/artists/songs). Schema export lives in `pb_schema.json`.
- **Artist status fields**: `list_status = included|recently_added|not_added|waiting`, `fetch_status = idle|pending|failed`.
- **NATS subjects**: `scrape.request` for jobs, `artist.updated` for UI updates (see `internal/worker/worker.go`).
- **Spotify providers**: local headless (chromedp), Browserless, ScrapingAnt, ScraperAPI, Apify. Each provider runs a pull-based goroutine pool in the worker; there is no fixed fallback order. Quota exhaustion is signalled by `spotify.ErrQuotaExhausted` and causes the provider's pool to shut down gracefully.
- **SSE UI updates**: `/api/events` uses Datastar fragments from `internal/handlers/handlers.go`.
- **Quota checks**: `/api/quota` in `internal/handlers/handlers.go` calls `internal/quota` (ScrapingAnt usage API, Apify `/v2/users/me/limits` for both USD budget and actor memory; Browserless/ScraperAPI assumed available). The `quota.Checker` struct exposes `ScrapingAntAPIBase`, `ScraperAPIBase`, and `ApifyAPIBase` fields that default to production URLs but can be overridden in unit tests with `httptest.NewServer` URLs. At runtime, `spotify.ErrQuotaExhausted` propagates from provider HTTP responses (401/402/403/429) through `internal/fetcher` (which skips retries on quota errors) to `internal/worker` (which NAKs the message and shuts down the provider pool).
- **Apify pre-flight guard**: Before the Apify provider pool processes a message, `internal/worker` calls `quota.CheckApify()` to verify USD budget and actor memory availability. If the check fails the message is NAK-ed immediately (returned to JetStream for other providers) and the Apify pool shuts down — avoiding a wasted Actor run that would 402.

## Developer Workflows
- Build/run requires the jsonv2 experiment (`//go:build goexperiment.jsonv2` across Go files).
- Templ: edit `templates/*.templ`, then run `go tool templ generate` to update `templates/*_templ.go`.
- Tailwind: edit `input.css`, then run `go tool gotailwind -i input.css -o static/styles.css`.
- Web app uses `pb_data/` for SQLite and is created on first run; PocketBase admin UI is at `/_/`.

## Project Conventions & Patterns
- Provider config is env-based in `config/config.go` (e.g., `BROWSERLESS_TOKEN`, `SCRAPINGANT_TOKEN`, `LOCAL_HEADLESS_ENABLED`, `LOCAL_CHROME_PATH`, `LOCAL_CONCURRENCY`, `MAX_CONCURRENCY`, `MAX_RETRIES`, `LOG_SUCCESSFUL_FETCHES`).
- Fetch retries use per-request timeouts and exponential backoff (`internal/fetcher/fetcher.go`).
- UI paging/lazy loading uses HTML fragment endpoints (e.g., `/api/albums/{status}`, `/api/artists/waiting`).
- CSV seeding is implemented in `seed.go` for `Music - Sheet1.csv` and `Music - Sheet2.csv` (currently commented out in `main.go`).

## Integration Points
- External services: Browserless BQL endpoint and ScrapingAnt HTTP API (see `internal/spotify/client.go`).
- Local scraping uses chromedp with a local Chrome binary (`internal/spotify/local.go`).
- CLI tooling uses chromedp and assumes a local Chrome path (see `cmd/update_listeners/main.go`).

## Gotchas
- Always build/run with `GOEXPERIMENT=jsonv2` or imports of `encoding/json/v2` fail.
- Do not edit `templates/*_templ.go` directly; regenerate from `.templ` sources.
- `static/styles.css` is generated; regenerate after CSS changes.
- Local headless scraping needs a Chrome binary; set `LOCAL_HEADLESS_ENABLED=false` or `LOCAL_CHROME_PATH` if not found.

## Skills
A skill is a set of local instructions in a `SKILL.md` file.

### Available project skills
- `pocketbase`: Comprehensive PocketBase development and deployment reference for schema design, API usage, security rules, migrations, realtime, and Go extension hooks.
  - file: `.agents/skills/whamp-pocketbase/SKILL.md`

### Trigger rules
- If the user asks about PocketBase setup, collections/schema, API rules, auth, files, relations, migrations, deployment, realtime, or PocketBase Go extension hooks, use the `pocketbase` skill.
- If the user explicitly names `$pocketbase` (or "pocketbase skill"), use this skill for that turn.

### How to use this skill
1. Open `SKILL.md` first and only load the minimal referenced files needed for the request.
2. Resolve relative paths from the skill root directory first:
   - `.agents/skills/whamp-pocketbase/`
3. Prefer existing `scripts/` and `assets/` in the skill before hand-writing large replacements.