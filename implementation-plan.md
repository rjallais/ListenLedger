# PocketBase Migration Guide (Go + Turso + WorkOS + NATS)

> **⚠️ HISTORICAL / STALE** — This plan was never executed. The project continues to use PocketBase, embedded NATS, and go-rod (not chromedp or Turso). See `docs/architecture/codebase-restructure-plan.md` for the active code-organization reference.

This document serves as the implementation blueprint for migrating the existing `PocketBase` repository to a decoupled, cloud-native architecture.

**This plan is designed to be executed by a coding agent on the actual project repository.**

## The Final Architecture Stack

1.  **Frontend:** `templ` (HTML Server-Side Rendering) + `Datastar` (Interactivity & SSE). **Zero heavy JavaScript frameworks.**
2.  **API/Router:** Go standard library `net/http` or `go-chi/chi`.
3.  **Database:** **Turso** (Serverless Edge libSQL).
4.  **Authentication:** **WorkOS AuthKit** (Hosted UI, pure OIDC/OAuth2 flow).
5.  **Event Bus & Workers:** **NATS JetStream** (Persistent WorkQueues).
6.  **CI/CD (GitOps):** Forgejo (Hub) -> GitHub & GitLab (Mirrors), using Chainguard base images.

---

## Migration Steps

Executing Agent: Follow these steps sequentially to replace PocketBase.

### Phase 1: Database Migration (Turso)
1.  **Remove PocketBase:** Delete `main.go` PocketBase setup and the `pb_data` directory. 
2.  **Initialize Turso Client:** Add `github.com/tursodatabase/go-libsql` to `go.mod`.
3.  **Schema Translation:** Create `internal/db/schema.sql`. Translate the old PocketBase collection schemas into standard SQLite `CREATE TABLE` statements (e.g., `artists`, `albums`, `songs`, `users`).
    *   *Note:* Use text-based CUIDs or UUIDs for primary keys instead of the default PocketBase 15-character strings.
4.  **Database Connection:** Create `internal/db/db.go`. Implement a standard `database/sql` connection pool that connects to the `TURSO_DATABASE_URL` using the `TURSO_AUTH_TOKEN`.
5.  **Refactor Queries:** Rewrite all data access functions (CRUD operations for songs, artists) from PocketBase API calls to raw SQL queries (`db.QueryRow()`, `db.Exec()`).

### Phase 2: Authentication (WorkOS AuthKit)
*The objective is a pure Go-driven UI flow without client-side JS SDKs.*

1.  **The Login Flow:** Implement a `/login` handler in Go. When a user visits this, redirect them (HTTP 302) to the WorkOS Hosted UI OAuth endpoint.
2.  **The Callback:** Implement a `/callback` handler. WorkOS will redirect the user here with a `code`. The Go server exchanges this `code` for an Access Token and a User Profile JSON from WorkOS.
3.  **Session Management:** The Go server creates a secure HTTP-Only cookie containing the Access Token (or a local session ID) and sends it to the browser.
4.  **Middleware:** Create `internal/middleware/auth.go`. This middleware checks the cookie on every protected route and verifies the JWT signature using the WorkOS Go SDK.
5.  **User Sync:** (Optional but recommended) Upon successful login, upsert the WorkOS user details into the local `users` table in Turso to link relationships (e.g., "Song added by User X").

### Phase 3: The Workqueue (NATS JetStream)
*The objective is to move heavy browser automation (`chromedp`) off the HTTP request path.*

1.  **Initialize NATS:** Add `github.com/nats-io/nats.go` to `go.mod`.
2.  **Create JetStream Context:** In `main.go`, connect to the NATS server and create a JetStream context. Ensure a stream exists (e.g., `STREAM: processing`, `SUBJECT: processing.scrape.*`).
3.  **Refactor HTTP Handlers:** Update the "Add Song" or "Update Monthly Listeners" Datastar endpoints. Instead of running the scraping logic inline, the handler should publish a structured JSON payload to NATS (`nc.Publish("processing.scrape.artist", payload)`), and return an immediate success response to the Datastar frontend.
4.  **Create Worker Process:** Create a new Go routine/package (e.g., `internal/workers/scraper.go`). This worker subscribes to the NATS subject using a Pull Consumer or WorkQueue stream. 
5.  **Worker Logic:** The worker performs the `chromedp` scraping. Once complete, it executes a SQL `UPDATE` against Turso with the new data, and `ACK`s the NATS message.

### Phase 4: Frontend Fixes (Datastar & SSE)
1.  **Data Struct Mapping:** Ensure the new SQL queries map correctly into the structs expected by the `templ` components.
2.  **SSE Updates:** Ensure the NATS worker or the main Go process still utilizes the existing Datastar SSE event format (`MergeFragmentTempl`) if live UI updates are required after a background job completes.

### Phase 5: Observability & Scraping Infrastructure (The $0/mo Stack)
1.  **Apify (Primary Scraping Engine):** Instead of fighting IP bans by hosting `chromedp` on a datacenter IP, offload the scraping entirely to Apify's Generous Free Tier ($5/mo credits = ~2,500 headless runs or 16,000+ HTTP runs). Apify provides a massive **25 concurrent runs** limit on the free tier, allowing for rapid batch processing.
    *   *Implementation Detail:* The Spotify monthly listener count is hydrated via a GraphQL/JSON response in a `<script>` tag, not raw HTML. The Go NATS worker should trigger an Apify Actor designed to target that specific JSON payload, bypassing the need to render the heavy DOM.
2.  **ScraperAPI (Fallback Engine):** For edge cases where the Apify credit limit is rapidly approaching, configure the Go NATS worker to fallback to `ScraperAPI`. They provide a hard guarantee of 1,000 free API requests per month with built-in proxy rotation, serving as a reliable secondary layer.
3.  **Sentry (Error Tracking):** Add the `sentry-go` SDK to both the `chi` router and NATS workers. Ensure `sentry.Recover()` is called in HTTP middleware and NATS worker panics to catch unhandled errors (5,000 errors/month free).
4.  **Axiom (Logging):** Configure the Go `slog` package to pipe structured JSON logs directly to Axiom (500GB/month free) for cheap, long-term observability.
5.  **Arcjet (Security):** Add the Arcjet Go SDK middleware to your public `chi` routes (like the Login page or API endpoints) to stop malicious bot traffic natively (100% Free for personal use).

## Verification Checklist
- [ ] Has all `github.com/pocketbase/pocketbase` code been removed?
- [ ] Does the app compile into a single static binary?
- [ ] Does logging into the app redirect through the `authkit.com` domain?
- [ ] Does clicking "Add Artist" generate a NATS message instead of hanging the HTTP request?
- [ ] Are Turso database connections functioning correctly via the environment variables?
- [ ] Does the NATS worker successfully trigger the Apify Actor (or fallback to ScraperAPI) and extract the GraphQL JSON payload without triggering bot protection?
