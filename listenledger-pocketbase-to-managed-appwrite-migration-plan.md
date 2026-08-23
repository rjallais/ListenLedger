# ListenLedger PocketBase to Managed Appwrite Migration Plan

## Summary
Migrate to Appwrite Cloud for Auth and TablesDB while keeping the Go server, templ/Datastar frontend, embedded NATS JetStream, and existing multi-provider scraping architecture. Use Chi for HTTP routing and middleware. Keep the data model as one shared music catalog protected by Appwrite login, not per-user catalogs.

Primary defaults:
- Appwrite Cloud TablesDB via the Go server SDK **v6** (`github.com/appwrite/sdk-for-go/v6`, compatible with Appwrite server 1.9.x; targets the `tablesdb` package).
- Shared catalog, server-mediated DB access with an Appwrite API key.
- Appwrite SSR OAuth2 token flow with server-set session cookies.
- Existing NATS worker/provider behavior stays unchanged.
- No browser-side Appwrite SDK access.

## The Northstar Spine (kept intact)

The `feat/northstar-sync` stack is the project's spine and this migration deliberately preserves it. Appwrite replaces only PocketBase's *data + auth* role:

| Spine component | Role | Migration impact |
|---|---|---|
| Go 1.26 + `GOEXPERIMENT=jsonv2` | Runtime & encoding (`encoding/json/v2`) | None — Appwrite Go SDK is plain HTTP/JSON |
| `log.Printf("[component] …")` convention | Structured logging standard (replaced slog) | New `internal/store`, `internal/auth`, Chi middleware follow it |
| Embedded NATS Server v2.12.4 + JetStream | Scrape queue (`scrape.request`), SSE events (`artist.updated`), DLQ | Untouched; state dir moves via `APP_DATA_DIR` with `PB_DATA_DIR` fallback |
| Pull-based per-provider goroutine pools | Worker concurrency & quota exhaustion shutdown | Untouched; only its persistence calls route through `store.Repository` |
| Multi-provider scraping (MobileSSR, local/self-hosted/cloud Browserless, ScrapingAnt, ScraperAPI, Apify, Browserbase) | Listener data acquisition | Untouched |
| templ + Datastar SSE (`/api/events`) | UI rendering & live updates | Untouched; Datastar fragments keep working against Chi handlers |
| Embedded esbuild pipeline (`cmd/build`: Tailwind/daisyUI CSS + JS bundling, watch mode, hot-reload SSE) | Asset toolchain (mise tasks `build:css`, `live:*`) | Untouched |
| mise task runner + `.mise.local.toml` secrets | Dev/build orchestration | Add Appwrite env vars; keep provider/NATS config unchanged |

Consequence: the migration surface is `internal/handlers` persistence calls, the auth bootstrap, and the CLI tools' storage layer — not the application architecture.

## Why Appwrite Cloud over PocketBase (updated August 2026)

This migration is justified by what Appwrite Cloud offers today that PocketBase does not:

| Capability | PocketBase 0.36.2 (current) | Appwrite Cloud (verified Aug 2026) |
|---|---|---|
| Managed hosting | None — we self-host and self-back-up SQLite | Fully managed: HA regional clusters, scaling, monitoring |
| Backups | Manual (`cmd/safebackup` VACUUM INTO) | Daily automated backups on Pro, 7-day retention; exportable via CLI/API |
| Auth | Admin/user collections, manual guards | Mature SSR OAuth2 token flow, sessions, per-user rate limiting |
| Transactions | SQLite transactions via Go hooks | GA staged transactions (`CreateTransaction` → stage → `UpdateTransaction(commit)`), cross-table, with `createOperations` batching and atomic `increment/decrement` operators; optimistic conflict detection at commit |
| Bulk writes | Loop over saves | Native `createRows`/`upsertRows`/`deleteRows`; server-SDK-only by design |
| Scale ceiling | Single SQLite file, single writer (`auxiliary.db` request logs alone ~1.2 GB) | Managed REST database; quotas below |
| Ops burden | We own uptime, backups, retention | Console + budget caps + alerts; but **Free-plan projects pause after 1 week of inactivity** — production requires Pro |

### Verified plan limits (appwrite.io/pricing, Aug 2026)

| Resource | Free | Pro ($25/mo) |
|---|---|---|
| Databases | 1 per project | Unlimited |
| Reads / month | 500K | 1.75M (+$0.06/100k) |
| Writes / month | 250K | 750K (+$0.10/100k) |
| Bulk rows / request | 100 | 1,000 |
| Max ops / transaction | 100 | 1,000 |
| Auth users (MAU) | 75K | 200K |
| Bandwidth | 5 GB/mo | 2 TB/mo |
| Logs retention | 1 hour | 7 days |
| Inactivity behavior | Project paused after 1 week idle | Always on |

Write-quota fit check: our steady-state write volume is scrape-driven (~1.6k artists refreshed periodically plus `scrape_jobs` row updates). A full-catalog refresh is roughly 3.2k writes (artist update + job updates) — well inside Free's 250K/month, but batch refreshes plus retries can spike; NATS smoothing keeps us comfortably within either plan. The deciding factors are therefore **inactivity pause** and **transaction/bulk op cap**, both pointing at Pro.

What we deliberately **do not** adopt (keep the current architecture): Appwrite Realtime (our SSE stream is driven by NATS `EVENTS` and stays), Appwrite Functions/Messaging/Sites, and any browser-side SDK.

## Current State & Backup (2026-08-08)

Source-of-truth facts gathered before migration starts:

- **Row counts (live `pb_data/data.db`, verified from a fresh snapshot):** `artists` 1,598 (1,597 distinct names, 0 duplicate `spotify_id`), `albums` 1,495, `songs` 1,436, `scrape_jobs` 15,795. `users` is empty (0 auth accounts) — consistent with the shared-catalog + central-auth model.
- **The four domain tables are registered PocketBase `base` collections but their physical SQLite tables carry no `created`/`updated` columns** (they were created by early custom migrations). `created_at`/`updated_at` population at migration time must be derived: `artists.last_updated`, `scrape_jobs.queued_at|started_at|finished_at`; `albums` and `songs` have no timestamps → set to migration time (or, best effort, derive song dates from `release_date`).
- `songs.artist_spotify_ids` is comma-separated TEXT (`0cGUm…,7c0X…`); split on `,` to Appwrite string arrays.
- `songs.release_date` is free text (e.g. `10 October 1995`); there is no `release_year` column in the source — compute at migration or keep the text field.
- `scrape_jobs.artist` already holds the PocketBase artist id (not a name) → direct rename to `artist_id`.
- `albums.remaining_songs` and `albums.completion_bp` do not exist in source — computed during migration (as planned).
- `auxiliary.db` (~1.2 GB, +WAL) is PocketBase's `_logs` request-log table — not collection data. Do not migrate; plan to clean up log retention instead.
- **Backups:** `backups/data_backup_20260808_041237.db` (fresh, counts verified above) is the migration baseline. `backups/data_backup_before_dedupe_20260613.db` (4,306 artists, pre-dedupe) is the historical reference for the duplicate-report acceptance check. NATS JetStream state lives in `pb_data/nats/` with streams `SCRAPE_REQUESTS`, `SCRAPE_DLQ`, and `EVENTS` (plus tooling output in `pb_data/backfill_reports/`).
- **Current stack pinned:** Go 1.26 with `GOEXPERIMENT=jsonv2` (build tags on handlers), PocketBase v0.36.2, `datastar-go` v1.1.0, embedded `nats-server` v2.12.4. `main.go` and the CLI commands (`seed`, `backfill_song_artists`, `update_listeners`, `tools/experiments/mark_recent_songs`) all bootstrap `pocketbase.NewWithConfig` directly — every one of these needs porting, not just the HTTP handlers.
- **Routes registered today** (`internal/handlers/routes.go`): `/static/{path...}` (own brotli/gzip in `handleStatic`), `/robots.txt`, `/`, `/albums`, `/artists`, `/songs` (gzip via `apis.Gzip()`), `/api/albums/{status}`, `/api/albums`, `/api/albums/{albumId}/status/{status}`, `/api/albums/{albumId}/collection/{action}`, `/api/albums/{albumId}/total/{action}`, `/api/artists/waiting`, `/api/refresh/batch`, `/api/refresh/{artistId}`, `/api/artists`, `/api/songs`, `/api/songs/{songId}/recent/{value}`, `/api/songs/sections`, `/api/songs/current-playlist`, `/api/songs/not-recent`, `/api/artists/{artistId}/status/{status}`, `/api/artists/{artistId}/collection/{action}`, `/api/events` (SSE), `/api/quota`, `/api/queue`, `/api/queue/retry`, `/api/listenledger/health`. The Chi routing table must reproduce this exactly.

## Important Interface Changes
Add `internal/store` as the application data boundary:
- Domain models: `Album`, `Artist`, `Song`, `ScrapeJob`.
- Repository methods for current workflows: album lists/counts/updates, artist lists/counts/status/listener updates, song create/recent-list updates, scrape job queue/retry/stale-job operations.
- `WithTransaction(ctx, fn)` mapped to Appwrite's GA transaction API (see "Transactions" below).
- Implement `PocketBaseStore` only as a temporary migration bridge, then remove it.
- Implement final `AppwriteStore` using `github.com/appwrite/sdk-for-go/v6` (`appwrite` client, `tablesdb`, `query`, `id` packages) — v6 targets Appwrite server 1.9.x and includes synchronous column/index creation on `createTable`; do **not** target v3/v4/v5.

### Transactions (GA; verified against docs August 2026)
Appwrite transactions are **staged**, not callback-scoped like SQLite:
1. `tablesdb.CreateTransaction()` → returns a transaction `$id`. Each transaction has a TTL between 1 minute and 1 hour (default 5 minutes); uncommitted transactions expire and are cleaned up automatically, so keep `WithTransaction` closures short and never hold one across user input.
2. Stage operations by passing `transactionId` to `createRow`/`updateRow`/`upsertRow`/`deleteRow`, their bulk variants (`createRows`/`upsertRows`/…), or stage many at once with `createOperations` (mixed `create|update|upsert|increment|decrement|delete|bulkCreate|bulkUpdate|bulkUpsert|bulkDelete` actions across tables/databases).
3. `tablesdb.UpdateTransaction(id, commit: true|false)` to commit or roll back.

The per-transaction operations cap is enforced server-side for every staging method including bulk and atomic numerics: 100 (Free) / 1,000 (Pro) / 2,500 (Scale).

Design consequences for `WithTransaction`:
- Implement it as *collect staging calls + commit* rather than an arbitrary SQL callback: the closure receives a `TxRepository` whose methods forward to the SDK with the shared `transactionId`.
- **Optimistic concurrency**: commit validates permissions, schema, and revisions atomically and fails with a conflict error if any touched row changed externally since staging. Wrap `WithTransaction` in a bounded retry loop (refetch, re-stage, commit) — plan for this from day one, it is not an edge case.
- **Keep transactions short** (TTL + plan op caps). Any workflow that could exceed the cap (e.g. batch song updates) must chunk commits.
- Prefer Appwrite's atomic operators (`incrementRowColumn`/`decrementRowColumn`, staged via `createOperations`) for the counter-heavy handlers (`/collection/{action}`, `/total/{action}`, `attempts`) outside transactions — they are applied safely at the storage layer to prevent lost updates under concurrency, removing most read-modify-write races without paying for a transaction.
- Publish NATS `artist.updated` only after commit succeeds — publishing mid-transaction would violate worker expectations on conflict rollback.

### Package/auth changes
Replace PocketBase HTTP event handlers:
- From `func(*core.RequestEvent) error`
- To `func(http.ResponseWriter, *http.Request) error`, wrapped by a small error adapter.

Add auth package:
- `internal/auth.Middleware` verifies Appwrite session cookie by calling `account.Get`.
- `internal/auth.CurrentUser(ctx)` exposes user ID/email/name.
- Mutating routes require same-origin `Origin`/`Referer` validation.

## Appwrite Schema
Create one database, default ID: `listenledger`.

Prefer a **declarative schema via the Appwrite CLI** (`appwrite init tables` + `appwrite push tables`) committed as `appwrite.json`, over hand-rolled Go in `cmd/appwrite_schema`. The CLI supports columns and indexes inline on table creation and is the documented idempotent path; keep `cmd/appwrite_schema` only as a fallback or remove it.

Practical column-type notes (current TablesDB limits): `varchar` max 16,383 chars and fully indexable only under 768 — all our sizes are fine; `text`/`mediumtext` are prefix-index only (fine for `scrape_jobs.error`); enums exist natively. Columns and indexes can be declared inline in `createTable` / the CLI manifest.

Tables:
- `albums`
  - `title` varchar(512), required
  - `artist_name` varchar(512), required
  - `collection_songs` integer min 0 default 0
  - `total_songs` integer min 0 default 0
  - `remaining_songs` integer min 0 default 0
  - `completion_bp` integer min 0 default 0
  - `status` enum: `full`, `processed_once`, `waiting`
  - `release_type` enum: `album`, `ep`, `single`
  - `created_at`, `updated_at` datetime

- `artists`
  - `name` varchar(512), required
  - `spotify_id` varchar(22), optional
  - `monthly_listeners` integer min 0 default 0
  - `genre_group` enum: `rock_metal`, `everything_else`
  - `list_status` enum: `included`, `recently_added`, `not_added`, `waiting`
  - `last_updated` datetime optional
  - `fetch_status` enum: `idle`, `pending`, `failed`
  - `collection_songs`, `total_songs` integer min 0 default 0
  - `created_at`, `updated_at` datetime

- `songs`
  - `title` varchar(512), required
  - `artist_name` varchar(1024), required
  - `album` varchar(512)
  - `release_date` varchar(32), preserving current text/ISO behavior
  - `release_year` integer min 0 default 0
  - `spotify_id` varchar(22)
  - `artist_spotify_ids` varchar(22) array
  - `release_type` enum: `album`, `ep`, `single`
  - `is_recent` boolean default false
  - `recent_batch_seq`, `recent_batch_pos` integer min 0 default 0
  - `created_at`, `updated_at` datetime

- `scrape_jobs`
  - `request_id` varchar(64), required, unique
  - `artist_id` varchar(36), required
  - `status` enum: `queued`, `processing`, `succeeded`, `failed`
  - `attempts` integer min 0 default 0
  - `error` text
  - `queued_at`, `started_at`, `finished_at` datetime

Indexes: declare the non-unique key indexes up front in the same manifest, matching actual query patterns — at minimum `albums.status`, `artists.list_status`, `artists.fetch_status`, `artists.spotify_id` (key), `songs.is_recent` + `recent_batch_seq`/`recent_batch_pos` (composite if the recent-list query orders by `(is_recent, recent_batch_seq, recent_batch_pos)`), `songs.spotify_id` (key), `scrape_jobs.request_id` (unique), `scrape_jobs.status` (+ composite with `queued_at` for queue sweeps), `scrape_jobs.artist_id`. Appwrite recommends an index for every queried column; count queries can use `WithListTotal(false)` where pagination does not need totals.

Do not create unique indexes for `artists.spotify_id` or `songs.spotify_id` initially because PocketBase used partial uniqueness and legacy duplicate handling. Enforce uniqueness in repository code and emit a migration duplicate report.

## Routing Plan
Add `github.com/go-chi/chi/v5`.

Public routes:
- `GET /login`
- `GET /auth/login/{provider}`
- `GET /auth/callback`
- `GET /auth/failure`
- `POST /auth/logout`
- `GET /static/{path...}` (port `handleStatic`: built-in brotli/gzip at binary level — exclude from `middleware.Compress` to avoid double compression)
- `GET /robots.txt`
- `GET /healthz`

Protected routes keep current URLs exactly as enumerated in "Current State" (main views, album/artist/song APIs, refresh, queue, quota, SSE). Use `GET /api/listenledger/health` for app-specific health (do not replace with `/api/health` — that path was PocketBase's).

Use Chi middleware:
- `middleware.RequestID`
- `middleware.Recoverer`
- `middleware.Compress(5)` — **except** `/api/events` (SSE) and `/static/*` (self-compressing). Replaces today's per-route `apis.Gzip()` bindings on `/albums`, `/artists`, `/songs`.
- custom auth middleware on protected groups
- custom same-origin guard on mutating methods

## Auth Flow
Use Appwrite SSR OAuth2 token flow:
1. `/auth/login/{provider}` validates provider against `APPWRITE_OAUTH_PROVIDERS`.
2. Generate random `state`, store in `ll_oauth_state` cookie with `HttpOnly`, `Secure`, `SameSite=Lax`, 10-minute max age.
3. Call `account.CreateOAuth2Token(provider, successURL, failureURL, scopes)` and redirect.
4. `/auth/callback` verifies `state`, reads `userId` and `secret`, calls `account.CreateSession(userId, secret)`.
5. Store session secret in `a_session_<PROJECT_ID>` unless overridden by `APPWRITE_SESSION_COOKIE`.
6. Middleware creates a per-request Appwrite session client and calls `account.Get`.
7. Require user email to match `AUTH_ALLOWED_EMAILS` or `AUTH_ALLOWED_DOMAINS`; in production, fail closed if neither is configured.

## Config
Add required env vars:
- `APPWRITE_ENDPOINT` (region-specific; region is permanent at project creation. Available regions Aug 2026: Frankfurt `fra`, New York `nyc`, Sydney `syd`, San Francisco `sfo`, Singapore `sgp`, Toronto `tor` — Bangalore/Amsterdam/London are "coming soon")
- `APPWRITE_PROJECT_ID`
- `APPWRITE_API_KEY`
- `APPWRITE_DATABASE_ID=listenledger`
- `APPWRITE_OAUTH_PROVIDERS`
- `AUTH_ALLOWED_EMAILS` or `AUTH_ALLOWED_DOMAINS`
- `PUBLIC_BASE_URL`
- `SESSION_COOKIE_SECURE=true`

Keep provider/NATS config unchanged — the northstar spine's env surface (`LOCAL_*`, `BROWSERLESS_*`, `SCRAPINGANT_TOKEN`, `SCRAPERAPI_TOKEN`, `APIFY_TOKEN`, `BROWSERBASE_*`, JetStream tuning) is untouched; add the Appwrite block alongside it in `.mise.local.toml`. Rename data-dir config gradually:
- Add `APP_DATA_DIR`
- Keep `PB_DATA_DIR` as a legacy fallback for one release so existing `pb_data/nats` can be reused.

## Migration Steps
1. Add `internal/store` models and interfaces.
2. Refactor handlers and worker to use `store.Repository` while still backed by PocketBase.
3. Add Appwrite config and client factory.
4. Add declarative schema manifest (`appwrite.json` via Appwrite CLI) or `cmd/appwrite_schema` fallback.
5. Add migration command: `cmd/migrate_appwrite`.
   - Reads PocketBase SQLite directly from `pb_data/data.db` (use the migration-day backup, not the live file — see Rollout).
   - Uses **bulk `upsertRows` staged via transactions (`createOperations`)** with original PocketBase IDs as row IDs, chunked to stay under both the bulk rows/request limit and the per-transaction op cap (100 on Free / 1,000 on Pro — with ~19.3k total rows, Pro chunks of 1,000 finish in ~20 commits; Free needs ~200). Bulk + transactions makes the migration idempotent (upsert semantics) and resumable: commit per chunk, checkpoint the last committed ID per table to a progress file, and create a fresh transaction per chunk (staged transactions expire after their TTL — default 5 min).
   - Converts `scrape_jobs.artist` to `artist_id` (it is already an artist id in the source).
   - Splits `songs.artist_spotify_ids` on `,` into arrays.
   - `created_at`/`updated_at` are derived, not preserved: the source tables lack
     `created`/`updated` columns (see Current State section). Use
     `artists.last_updated` and `scrape_jobs.*_at` where present; fall back to
     migration time for `albums`/`songs`.
   - Computes `albums.remaining_songs` and `albums.completion_bp`.
   - Reports duplicate Spotify IDs, orphan scrape jobs (artist id without a matching
     `artists` row), invalid dates, and row counts by table/status.
   - `--dry-run` writes the full row maps to JSONL instead of calling Appwrite, so transforms can be diffed before any network traffic.
6. Add AppwriteStore and run the app against Appwrite behind a feature flag: `DATA_BACKEND=appwrite`.
7. Replace PocketBase router bootstrap with Chi `http.Server`.
8. Remove PocketBase hooks and publish `artist.updated` from store methods/decorators after successful artist writes (publish only after the Appwrite transaction commits — publishing mid-transaction would violate worker expectations on conflict rollback).
9. Port CLI tools (all currently bootstrap `pocketbase.NewWithConfig` directly):
   - `cmd/seed` uses AppwriteStore.
   - `cmd/backfill_song_artists` uses AppwriteStore.
   - `cmd/update_listeners` uses AppwriteStore or queues NATS refresh jobs.
   - `tools/experiments/mark_recent_songs` uses AppwriteStore or is deleted.
   - Replace `cmd/safebackup` with `cmd/export_appwrite` JSON/CSV export. Note: Appwrite Cloud runs managed backups by plan tier, but keep an export command because cloud backups are restore-to-Appwrite, not portable SQLite files.
10. Remove PocketBase dependencies (`pocketbase`, `dbx`, `fexpr` tree) and migrations once the Appwrite path passes tests.

## Testing
Required test coverage:
- Store fake tests for all handler workflows.
- Appwrite row encode/decode tests for every table.
- Migration transform tests from SQLite fixture rows to Appwrite row maps.
- Transaction wrapper tests: staged-op collection, commit, conflict → retry, rollback on callback error.
- Auth middleware tests: no cookie, bad cookie, unauthorized email, authorized email.
- OAuth callback tests: missing state, mismatched state, missing `userId`/`secret`, session cookie set.
- Chi route tests for all existing endpoints.
- Worker tests proving `scrape.request`, quota exhaustion, retry, DLQ, stale job sweep, and `artist.updated` behavior remain unchanged.
- Datastar tests verifying SSE endpoints do not use compression (and static files are not double-compressed).
- Migration validation test comparing old/new counts by table and status.

Acceptance commands:
- `GOEXPERIMENT=jsonv2 go test ./...`
- `GOEXPERIMENT=jsonv2 go vet ./...`
- `go tool templ generate` if templates change
- `mise run build:css` if CSS changes (embedded esbuild via `cmd/build`; also bundles JS)

## Rollout
1. Backup PocketBase SQLite and NATS data (baseline already taken:
   `backups/data_backup_20260808_041237.db`; re-run a fresh
   `sqlite3 pb_data/data.db ".backup 'backups/data_backup_<timestamp>.db'"` on
   migration day and snapshot `pb_data/nats/`).
2. Create Appwrite project in the chosen region, API key (scopes: `databases.read/write`, `tables.*`, plus `users.read` if listing is ever needed; keep it minimal), database, and OAuth provider configuration (success/failure URLs per environment).
3. Push schema: `appwrite push tables` (or run `cmd/appwrite_schema`).
4. Run `cmd/migrate_appwrite --dry-run` and review the JSONL output.
5. Fix reported duplicate/invalid data in the SQLite snapshot.
6. Run actual migration (chunked bulk upserts; verify per-table counts match the validation report).
7. Start app with `DATA_BACKEND=appwrite` on a staging port against the same NATS data dir.
8. Verify UI, auth, queue, refresh, SSE, and CLI utilities.
9. Switch production env to Appwrite.
10. Keep PocketBase DB read-only for one release (and the legacy `PB_DATA_DIR` fallback), then remove legacy fallback.

## Risks & Mitigations
- **Free-plan inactivity pause:** Free projects pause after 1 week without activity — fatal for an always-on dashboard. Run production on Pro ($25/mo) with a budget cap; use Free only for throwaway integration tests.
- **Write/read quotas:** Free allows 250K writes / 500K reads per month (Pro: 750K/1.75M, overages $0.10/$0.06 per 100k). A full-catalog scrape refresh is ~3.2k writes; NATS queueing smooths bursts, but monitor usage alerts (sent at 75%/100%) during the first weeks. Exceeding Free limits freezes the project read-only.
- **Transaction op cap + bulk rows per request (100 on Free / 1,000 on Pro):** migration chunking and any bulk UI operations must respect both limits; buy Pro before migration if chunking proves painful, or keep chunks at ≤100 permanently.
- **Transaction TTL expiry:** staged transactions expire after 1 min–1 hr (default 5 min). Never stage across user interaction; the `WithTransaction` retry loop must create a *fresh* transaction on retry, not reuse an expired ID.
- **Optimistic conflict storms:** concurrent worker + UI writes to the same artist row; mitigated by retry-with-refetch in `WithTransaction` and by using atomic increment/decrement operators for counters.
- **No partial-unique indexes:** legacy duplicate Spotify IDs would fail a naive unique index; deferred uniqueness is enforced in code, with the migration duplicate report as the tracking artifact.
- **Region choice is permanent:** pick the Appwrite Cloud region closest to the server host before creating the project (available: FRA/NYC/SYD/SFO/SGP/TOR).
- **SDK churn:** target SDK v6 (server 1.9.x); pin the version in `go.mod` and wrap all SDK calls inside `AppwriteStore` so future majors are a single-package change.

## Sources (verified August 2026)
- Appwrite SSR auth flow: https://appwrite.io/docs/products/auth/server-side-rendering
- Appwrite Account Go API: https://appwrite.io/docs/references/cloud/server-go/account
- Go SDK v6 (server 1.9.x): https://github.com/appwrite/sdk-for-go — `go get github.com/appwrite/sdk-for-go/v6`; TablesDB package: https://pkg.go.dev/github.com/appwrite/sdk-for-go/tablesdb
- TablesDB REST reference (response format 1.9.x): https://appwrite.io/docs/references/cloud/server-rest/tablesDB
- Transactions (staging, TTL, op caps, conflicts): https://appwrite.io/docs/products/databases/transactions and https://appwrite.io/blog/post/announcing-transactions-api (updated Jun 2026)
- Operators (atomic increment/decrement, transaction-ready): https://appwrite.io/docs/products/databases/operators
- Bulk operations (server SDK only, plan limits): https://appwrite.io/docs/products/databases/bulk-operations
- Tables, column types, indexing rules: https://appwrite.io/docs/products/databases/tables
- Appwrite CLI tables commands: https://appwrite.io/docs/tooling/command-line/tables
- Regions (FRA/NYC/SYD/SFO/SGP/TOR): https://appwrite.io/docs/products/network/regions
- Pricing & plan limits (Free pause policy, quotas): https://appwrite.io/pricing and https://appwrite.io/docs/advanced/billing/pro
- Chi router and middleware: https://github.com/go-chi/chi
