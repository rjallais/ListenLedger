# ListenLedger: Event-Sourced CQRS Plan (SQLite + NATS, PB temporary)

Settled decisions: **SQLite `ledger.db` + NATS JetStream as durable pair, PocketBase kept temporarily as legacy read model then retired, all aggregates event-sourced at once.** Tooling: keep Air and embedded `cmd/build` (esbuild idle until JS is needed); serialize domain event payloads and envelopes as RON with `starfederation/ron-go`; retain JSON at Spotify and PocketBase protocol boundaries; use `zombiezen`-style SQLite discipline (CGO-free, via `modernc.org/sqlite` engine).

## 1. `bee` audit — what we reuse off the shelf vs reimplement

Source: `github.com/blinkinglight/bee` (`go 1.24`, `nats.go v1.43.0`, `nats-server v2.10.25`, `toolbelt v0.4.3`, `protobuf v1.36.6`). We run `nats.go v1.53.1 / server v2.14.4 / toolbelt v0.9.1-indirect` + new `jetstream.JetStream` API. Full audit of `command.go`, `projector.go`, `replay.go`, `query.go`, `event.go`, `publish.go`, `context.go`, `eventsregistry.go`, `bee_test.go`.

### 1.1 What `bee` actually is (1500 LOC)

* `Command(ctx, handler, co.WithAggreate)`: creates `COMMANDS` stream (`WorkQueue`, `cmds.>`, `Duplicates 5m`), pull-subscribes `Fetch(1)`, `proto.Unmarshal(CommandEnvelope)`, calls `Handle()->[]EventEnvelope`, publishes to `events.<agg>.<id>.<type>` (or `events.<parents>.<agg>.<id>.<type>`), notifies `notifications.<correlation>.success/error`, then `Ack`.
* `Project(ctx, fn, po.WithAggreate/ID/Durable)`: ensures `EVENTS` stream (`Limits`, `events.>`), subscribes `events.<agg>.<id>.>` with durable `events_<durable>`, `proto.Unmarshal` then `ApplyEvent`, `Ack` even on error.
* `Query(ctx, fn, qo.WithAggreate)`: core-NATS `QueueSubscribe(query.<agg>.get)`, `proto.Unmarshal(QueryEnvelope)`, `json.Marshal(result)` on `Respond`.
* `Replay` / `ReplayAndSubscribe[T]`: ephemeral subscribe + `InitialConsumerPending()` count-down, `ApplyEvent` per msg. SSE example: `updates := ReplayAndSubscribe(ctx, agg); for u := range updates { sse.MergeFragmentTempl(...) }`.
* `Event` (saga/process-manager): subscribes `events.<agg>.>` with durable `procmgrs_<name>`, maps `Event->[]Command`, publishes to `cmds.<agg>`.
* `eventsregistry.go`: **the gem** — `RegisterEvent[T](agg, type)` / `RegisterCommand[T]` generic maps + `UnmarshalEvent/UnmarshalCommand` that `json.Unmarshal(e.Payload)` into registered Go structs. Envelope is proto, **payload is JSON**. `helpers.go`: generic `Unmarshal[T]`.
* `context.go`: `WithNats/WithJetStream` ctx helpers.

### 1.2 Verdict: borrow patterns (~35%), do NOT `go get bee`

| `bee` piece | Reuse? | Why |
|---|---|---|
| `RegisterEvent/RegisterCommand` + JSON-payload registry | **Copy the registry pattern as `internal/es/registry.go`, decode event payloads as RON, drop proto dep** | Gives type-safe `ApplyEvent` switch without `buf/protoc-gen-go` burden. External provider and PocketBase protocol payloads remain JSON. |
| Subject naming `events.<agg>.<id>.<type>`, `cmds.<agg>`, `query.<agg>.get` | **Adopt** as `domain.events.artist.<id>.listeners_updated`, etc. | Cleaner than flat `artist.updated` / `scrape.request`. Keep old subjects as 1-release alias. |
| `ReplayAndSubscribe` SSE idea | **Reimplement on SQLite + JetStream** | We need `SELECT * FROM events WHERE aggregate_id=? ORDER BY seq` + live NATS tail, not ephemeral-consumer pending-count. |
| `Event` saga for derived flows | **Copy pattern** for `BatchCompleted`, `total_songs` recalc, `clearFailedJobsForArtist` | Replaces `worker/artist_updates.go` debounced recalc + `batch_progress.go` in-mem maps. |
| `Command` processor wholesale | **No** | Old `nats.JetStreamContext` API (`AddStream/PullSubscribe/Fetch`) vs our `jetstream.JetStream` (`CreateOrUpdateStream/Consumer/Consume`). Downgrades `nats.go`. No SQLite outbox, no `expected_version` OCC, no idempotency table. `Replay`-from-JetStream per command can't survive 350s Apify timeouts + stale sweeps. |
| `Project` wholesale | **No** | `Ack`-on-error swallows poison (we need DLQ with metadata in `worker/dlq.go`). No checkpoint table, no dual-write to PB compat + SQLite views, no snapshots (bee roadmap v0.3, unimplemented). |
| `Query` via NATS req-reply | **No** | Our queries are HTTP + Templ fragments + Datastar SSE (`/artists`, `/songs`, `/api/queue`). NATS req-reply adds hop with no benefit. |
| Proto envelopes | **No** | Bee envelopes are proto but payloads are JSON anyway. We use RON for domain event payloads and envelopes, inspectable via `nats` CLI without code generation. Keep JSON for external Spotify/PocketBase contracts. |

Net: **vendor the registry idea, reimplement transport on our stack.** No new `buf`, `protoc-gen-bee`, or version-pin conflict.

## 2. Target architecture (settled)

```
POST /api/... (command) -> Validate -> SQLite ledger.db Tx {append events + outbox + idempotency} -> 202 {request_id}  (<10ms)
Outbox relay -> JetStream `DOMAIN_EVENTS` (Limits, 7d, FileStorage) + legacy `scrape.request` / `artist.updated` alias
Worker saga: consume -> fetcher.FetchOne -> append result events -> ack/NAK-with-delay/Term (keep quota-pool shutdown, Apify preflight, DLQ)
Projectors: NATS -> SQLite views (permanent) + PocketBase collections (temporary compat, projector-only writes)
Reads/SSE: query SQLite views; SSE = SQLite replay + live NATS tail -> PatchElementTempl
```

* **Source of truth:** `ledger.db` (new file beside `pb_data/`, `LEDGER_DB_PATH` override, `WAL+NORMAL+foreign_keys=ON+busy_timeout=5000`, writer pool = 1 à la `toolbelt/db`, engine stays `modernc.org/sqlite` CGO-free).
* **Bus:** embedded NATS kept. `SCRAPE_REQUESTS` (WorkQueue) + `SCRAPE_DLQ` + `EVENTS` stay; add `DOMAIN_EVENTS` (`domain.events.>`). KV only for ephemeral (cooldowns/presence), never event log.
* **Dev:** keep Air (`.air.toml` + `send_interrupt` for PB/NATS drain, add `sql` to `include_ext`), `mise run live:templ/live:css/live:server` orchestration, `cmd/build` stays (esbuild idle — `no JS entrypoints` — until first `assets/js` bundle needed).

## 3. Aggregates — everything at once

| Stream key | Commands | Events |
|---|---|---|
| `Artist:{pbID}` | `RegisterArtist, ChangeListStatus, AdjustCollectionSongs, RequestRefresh, MarkFetchStarted/Failed, ApplyListenersFetched` | `ArtistRegistered, ListStatusChanged, CollectionSongsAdjusted, RefreshRequested, FetchStarted, ListenersUpdated, FetchFailed` |
| `ScrapeJob:{request_id}` | `QueueScrape, MarkProcessing, MarkSucceeded/Failed, Retry, DeadLetter` | `ScrapeRequested, ScrapeStarted, ScrapeSucceeded, ScrapeFailed, ScrapeRetried, ScrapeDeadLettered` |
| `Album:{id}`, `Song:{id}` | `Create/UpdateStatus, MarkRecent` | `AlbumCreated/StatusChanged, SongCreated/RecentFlagChanged` |
| `Batch:{batch_id}` | `StartBatch, CompleteArtist` | `BatchStarted, BatchArtistCompleted, BatchCompleted` |

Envelope (RON for domain events): `event_id, aggregate_type, aggregate_id, seq, event_type, payload, request_id/causation_id, at`. `expected_version` OCC on append. `request_id` UNIQUE for idempotency (replaces `correlation/registry.go` + 30s JetStream dedup + `succeededRequests` map).

## 4. Phases

### Phase 0 — `internal/es/` + contracts (no behavior change)
* `internal/es/envelope.go` (RON domain-event envelope), `registry.go` (ported from bee, no proto), `store.go` (`Append` with OCC, `Load`, `snapshots`), `outbox.go`.
* `internal/messaging/`: add `domain.events.*` subjects + `EnsureDomainEventsStream`. Keep `scrape.request`, `artist.updated`, `queue.updated`.
* Tests: registry round-trip, OCC conflict, replay ordering. `go vet ./... && go test ./internal/es ./internal/messaging -count=1`.

### Phase 1 — `ledger.db` instant writes + outbox relay
* `internal/ledger/ledger.go`: open `LEDGER_DB_PATH` (default `<dataDir>/ledger.db`), apply `toolbelt/db` pragmas, `MigrationsFromFS` for `events(outbox: id, event_rowid, subject, published_at)`, `idempotency(request_id PK, response)`, `snapshots`, `projection_checkpoints`.
* Rewrite `queueArtistRefresh` (`handlers/artist_helpers.go:471`), `HandleQueueRetry` (`handlers/queue.go`), CRUD `POST`s: validate -> one Tx -> `202`. Delete manual rollback helpers.
* `internal/app/outbox.go`: claim-unpublished -> `js.Publish(MsgID=event_id)` -> mark published (event_id per event avoids dedup collisions across events sharing a request; request_id stays the command-idempotency key). Crash-safe. Flag-gated `ES_ENABLED=false` default.
* `cmd/safebackup`: include `ledger.db` (`VACUUM INTO`).

### Phase 2 — Worker as saga (keep tuning)
* `worker/processing.go:processRequest`: replace `app.Save(artist/job)` with event appends. Keep `fetchTimeout` table, `ErrQuotaExhausted/ErrRateLimited` NAK paths, Apify preflight, `retryDelay` BackOff, `inProgressLoop` heartbeats, `dispatch.go` channel + `watchAllGroups` drain.
* `worker/jobs.go` mutations -> `Scrape*` events; `sweepStaleJobs` -> emits `ScrapeFailed{stale_timeout}`; `artist_updates.go` recalc -> saga emitting `ArtistTotalSongsRecalculated`; delete `correlation/`.
* DLQ: `ScrapeDeadLettered` (durable fact on the job stream) supplements the `SCRAPE_DLQ` message (operational envelope, keep `dlq.go` format). Order: publish DLQ -> `Term` the source message -> mark failed + record events. DLQ-publish failure NAKs for redelivery (recovery behavior preserved).

### Phase 3 — Projectors (dual-write window)
* `internal/projection/sqlite/`: idempotent `INSERT ... ON CONFLICT(event_id) DO NOTHING` into `artist_view (materialized total_songs rank)`, `queue_view`, `batch_view`; checkpoint per projector.
* `internal/projection/pocketbase/`: same events -> existing `Save` paths. PB becomes projector-only; guard against direct handler/worker writes.
* Backfill: PB -> `ArtistRegistered/ListenersUpdated/...` with original timestamps + `migration:pb` causation. Replay-into-fresh-DB diff check.

### Phase 4 — Reads + SSE cutover
* `internal/query/`: replace `app.RecordQuery("artists"/"scrape_jobs")` in `handlers/*`, `sse.go:73`, `batch_progress.go:180`, `queue.go:505` with view queries.
* SSE: `Replay(aggregate_id from SQLite) + Subscribe(domain.events.>)` -> `PatchElementTempl` (bee `ReplayAndSubscribe` shape, our storage). Handoff is gap-free: activate the live subscription before replay, buffer live events during replay, then apply the buffer from the replay cursor forward so events committed mid-replay are not missed. `batch_progress.go` maps -> `Batch` aggregate (restart-safe).
* `cmd/update_listeners|seed|backfill_song_artists`: go through commands or direct appends.

### Phase 5 — PB retirement
* `USE_PB_READS=false` default, `/_/` read-only one release, then drop `migrations/*_app_collections.go`, `pb_schema.json` sync, PB router bridge. Templ/CSS unchanged. `go tool templ generate`, `mise run build:css` verification.

## 5. Verification per phase
`go vet ./...`, `go test ./...`, new `go test ./internal/es ./internal/projection -run TestReplay -count=1`, manual: `POST /api/refresh/{id}` -> `202` <50ms with NATS down (outbox pending), `kill -9` mid-batch -> resume, `cmd/replay --from-seq=0` rebuilds views.
