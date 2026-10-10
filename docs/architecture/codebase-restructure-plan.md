# ListenLedger: Unified Architecture & Codebase Restructure Plan

## 1. Executive Summary & Purpose

ListenLedger is an autonomous personal music catalog tracking monthly Spotify artist listener counts, album collections, and recent song rotations. It is built as a single deployable Go binary powered by embedded SQLite (via PocketBase), embedded NATS JetStream for durable scrape job distribution, and a reactive frontend using `a-h/templ` and Datastar.

This master document synthesizes all previous planning and architectural evaluations into a single source of truth, incorporating modern Go idioms (Modern Go Guidelines CLI), frontend practices from **The Tao of Datastar**, and routing/lifecycle patterns from **Northstar** and **Toolbelt**.

---

## 2. Core Tenets & Guidelines Alignment

### 2.1 The Tao of Datastar
1. **Single Source of Truth in the DOM & Backend:** Persistent state resides in SQLite; transient UI state (signals) is strictly isolated to modals, inputs, and loading indicators.
2. **Fine-Grained Element Morphing via SSE:** SSE updates (`/api/events`) patch precise DOM fragments (cards, rows, badges) without full-page reloads.
3. **Indicator Signals & Visual Feedback:** Action triggers bind to explicit indicator signals (`data-indicator:_...`, `data-attr:disabled="..."`, and `<span class="loading loading-spinner"></span>`) to guarantee responsive user feedback.
4. **Native View Transitions & Accessibility:** Support native cross-document view transitions (`@view-transition { navigation: auto; }`) and proper WAI-ARIA roles (`aria-haspopup="true"`, `aria-expanded="false"`, `role="menu"`).

### 2.2 Northstar & Toolbelt Architectural Patterns
1. **Standard `net/http` Decoupled Handlers:** All application handlers adhere strictly to `func(w http.ResponseWriter, r *http.Request)`. Handlers are isolated from framework-specific wrappers, enabling native HTTP testing with `httptest.ResponseRecorder`.
2. **Chi Router Sub-Routing:** Application endpoints are organized hierarchically via `go-chi/chi/v5` sub-routers (`/albums`, `/artists`, `/songs`, `/api/refresh`, `/api/queue`, `/api/events`).
3. **Robust Embedded NATS Lifecycle:** The embedded NATS server lifecycle is decoupled from HTTP hooks, featuring exponential backoff polling during startup (`WaitForServer`), atomic state flags, and graceful draining on shutdown.

---

## 3. Directory Layout

```text
├── cmd/
│   ├── backfill_song_artists/     # One-off song artist association migrator
│   ├── build/                     # Asset pre-compiler & production build utility
│   ├── lookup_artists/            # Metadata resolution CLI
│   ├── safebackup/                # SQLite VACUUM INTO automated backups
│   ├── seed/                      # Test catalog seeder
│   └── update_listeners/          # Direct listener updater CLI
├── config/                        # Shared runtime configuration
├── docs/
│   └── architecture/
│       └── codebase-restructure-plan.md # Master architectural document
├── internal/
│   ├── app/                       # Application bootstrap, embedded NATS, & hooks
│   │   ├── app.go                 # App container & PocketBase lifecycle
│   │   ├── nats.go                # Embedded NATS server & JetStream setup
│   │   └── hooks.go               # Server lifecycle hooks
│   ├── handlers/                  # Decoupled net/http & Chi handlers
│   │   ├── doc.go                 # Package documentation
│   │   ├── handlers.go            # Central Handler struct & view constructors
│   │   ├── middleware.go          # Custom slog HTTP logging middleware
│   │   ├── albums.go              # Album views & status updates
│   │   ├── artists.go             # Artist catalog & waiting cards
│   │   ├── artist_helpers.go      # Artist rank, status, & batch helpers
│   │   ├── artist_refresh.go      # Single & batch scrape dispatch
│   │   ├── artist_update.go       # Artist list status & counter updates
│   │   ├── batch_progress.go      # In-memory batch progress tracking
│   │   ├── health.go              # Health check & provider quota endpoints
│   │   ├── queue.go               # JetStream queue status & job retry
│   │   ├── routes.go              # Chi router tree & PocketBase bridge
│   │   ├── shared.go              # Datastar/Templ rendering & JSON helpers
│   │   ├── songs.go               # Song list & multi-format date sorting
│   │   ├── songs_create.go        # Song modal creation & validation
│   │   ├── sse.go                 # Persistent SSE event hub (/api/events)
│   │   └── static.go              # Embedded & filesystem asset serving
│   ├── worker/                    # JetStream background scraping workers
│   │   ├── worker.go              # Worker manager & provider pools
│   │   ├── dispatch.go            # Consumer pull loop & heartbeats
│   │   ├── processing.go          # Job execution & retry loops
│   │   ├── dlq.go                 # Dead-letter queue publication & policies
│   │   ├── jobs.go                # DB scrape_job lifecycle tracking
│   │   ├── artist_updates.go      # Artist listener DB updates & event publish
│   │   └── metrics.go             # Worker throughput metrics
│   ├── fetcher/                   # Retry layer with per-request timeouts & backoff
│   ├── spotify/                   # Scraping provider implementations
│   ├── messaging/                 # JetStream stream definitions & schemas
│   ├── quota/                     # Multi-provider quota checkers & preflight
│   ├── priority/                  # Artist prioritization algorithms
│   ├── buildinfo/                 # Git commit & build timestamp metadata
│   ├── correlation/               # Distributed tracing & request IDs
│   └── appdir/                    # OS-specific data directory resolution
├── migrations/                    # PocketBase database migrations
├── static/                        # Pre-compressed CSS, JS, and favicons
└── templates/                     # a-h/templ UI templates & component helpers
```

---

## 4. Background Workers & Scraper Infrastructure

### 4.1 Embedded NATS JetStream Pipeline
The application embeds NATS Server v2.14.4 with persistent JetStream storage:
- **Streams:**
  - `SCRAPE_REQUESTS` (Subject: `scrape.request`): Durable queue of artist listener refresh jobs.
  - `SCRAPE_DLQ` (Subject: `scrape.dlq`): Dead-letter queue for permanently failed or poison scrape requests.
  - `EVENTS` (Subject: `artist.updated`): Broadcast stream triggering Datastar SSE updates.

### 4.2 Provider Concurrency & Quota Management
- **Pull-Based Worker Pools:** Each configured provider operates an independent goroutine pool matching its provisioned concurrency limit.
- **Graceful Quota Shutdown:** When a provider returns `spotify.ErrQuotaExhausted` (HTTP 401/402/403/429), its pool immediately terminates, leaving unprocessed messages in JetStream for alternative providers.
- **Dead-Letter Queue Isolation (`internal/worker/dlq.go`):** Unmarshaling failures and retry-exhausted messages are cleanly routed to `scrape.dlq` with complete diagnostic metadata, preventing poison pill queue stalls.

---

## 5. Implementation Roadmap & Status

### Phase 1: Repository Hygiene & Unification (Completed)
- [x] Fix multi-format release date sorting in songs page.
- [x] Research and synthesize The Tao of Datastar, Northstar, and Toolbelt.
- [x] Unify all previous `.md` plan files into this single master document.
- [x] Remove obsolete plan files (`implementation-plan.md`, `listenledger-pocketbase-to-managed-appwrite-migration-plan.md`).
- [x] Delete obsolete legacy templates and server wrappers (`internal/server/`, legacy HTML stubs).

### Phase 2: Tao of Datastar & Frontend Alignment (Completed)
- [x] Audit all Templ templates to ensure signals are used exclusively for transient UI state.
- [x] Standardize loading indicators across all action buttons (`data-indicator:_...`, `data-attr:disabled="..."`, and `<span data-show="..." class="loading loading-spinner"></span>`).
- [x] Enforce Brotli/Gzip compression on static assets with content negotiation and route-level Gzip middleware.
- [x] Standardize accessibility attributes (`aria-haspopup="true"`, `aria-expanded="false"`, `role="menu"`, and `aria-hidden="true"`).
- [x] Enable native cross-document View Transitions via `@view-transition { navigation: auto; }` in compiled Tailwind CSS.

### Phase 3: Bootstrap & Codebase Restructuring (Completed)
- [x] **Task 3.1: Toolbelt-style Embedded NATS Lifecycle**:
  - Implement robust embedded NATS startup with exponential backoff (`WaitForServer`) and context cancellation shutdown.
  - Separate NATS lifecycle and JetStream stream initializations from HTTP hooks.
- [x] **Task 3.2: Chi Router Architecture & Feature Sub-Routing (Northstar / Toolbelt)**:
  - Introduce `go-chi/chi/v5` router for application routes.
  - Group routes hierarchically into feature sub-routers (`/albums`, `/artists`, `/songs`, `/api/refresh`, `/api/queue`, `/api/events`).
  - Convert HTTP handlers to standard `(w http.ResponseWriter, r *http.Request)` signatures.
  - Mount Chi router seamlessly onto PocketBase's `se.Router`, keeping PocketBase admin (`/_/`) and collections API intact.
- [x] **Task 3.3: Worker Responsibility Splitting**:
  - Clarify worker package boundaries: dispatcher loop, concurrency pools, and DLQ management (`internal/worker/dlq.go`).

### Phase 4: Data Layer Abstraction (`internal/store`) (Next)
- [ ] Formalize `internal/store` domain models and repository interfaces.
- [ ] Implement `PocketBaseStore` wrapping existing PocketBase collections.
- [ ] Refactor HTTP handlers to call `store.Repository` rather than `core.App` directly.

### Phase 5: Cloud Run Remote Scraper Deployment
- [ ] Prepare `Dockerfile` and deployment script for Browserless on GCP Cloud Run.
- [ ] Configure authentication token and endpoint in `config/config.go`.
- [ ] Validate automated scale-to-zero and cold-start tolerance.

### Phase 6: Operational Verification & Maintenance
- [ ] Run full test suite: `go test ./...` and `go vet ./...`.
- [ ] Verify `cmd/safebackup` automated VACUUM INTO snapshots.
- [ ] Document runtime deployment and environment variables in updated README.
