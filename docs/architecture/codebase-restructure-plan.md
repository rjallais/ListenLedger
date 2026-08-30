# Codebase Restructure Plan

## Purpose

This document is the current reference plan for organizing the active PocketBase-based ListenLedger codebase.

It exists to keep repository structure decisions in one durable location instead of scattering temporary planning files across the repo root.

## Scope

This plan covers:

- repository hygiene
- source tree organization
- command/tooling boundaries
- incremental refactors for `main.go`, `internal/handlers`, and `internal/worker`

This plan does not change:

- PocketBase as the application runtime
- NATS and JetStream as the job transport
- HTTP routes, SSE behavior, or NATS subjects
- PocketBase schema and migrations

## Historical Note

The root-level `implementation-plan.md` is historical documentation for a different architecture direction. It is intentionally left in place, but it is not the active code-organization reference for this repository.

Future structural changes should update this document instead of creating new root-level planning files.

## Current Direction

The repository is being restructured incrementally with small, reviewable branches:

1. `docs/restructure-plan-and-repo-hygiene`
2. `refactor/bootstrap-internal-app`
3. `refactor/handlers-feature-split`
4. `refactor/worker-responsibility-split`

Each phase must preserve existing behavior and pass build/test verification before the next phase begins.

## Target Layout

```text
cmd/
  safebackup/
  seed/
  update_listeners/

docs/
  architecture/
    codebase-restructure-plan.md

internal/
  app/
  handlers/
  worker/
  spotify/
  fetcher/
  messaging/
  quota/
  priority/
  appdir/
  buildinfo/
  chrome/
  correlation/

migrations/
templates/
static/
testdata/
  spotify/
tools/
  experiments/
```

## Implementation Rules

- `main.go` should become a thin entrypoint that delegates to `internal/app`.
- `internal/handlers` should stay one package, but be split by feature.
- `internal/worker` should stay one package, but be split by responsibility.
- `cmd/` is reserved for supported binaries.
- `tools/experiments/` holds one-off or manual utilities that should not participate in normal builds.
- Spotify fixture HTML/JSON files belong under `testdata/spotify/`, not the repo root.

## Verification

After each refactor phase:

- `go build -o ListenLedger .`
- `go test ./...`
- `go vet ./...`

Additional targeted checks:

- `go test ./internal/handlers`
- `go test ./internal/worker`
- `go test ./internal/messaging`
- `go test ./internal/fetcher`
- `go test ./internal/quota`

## Ownership

When a future refactor changes package boundaries, supported commands, or repository layout, update this document in the same change.
