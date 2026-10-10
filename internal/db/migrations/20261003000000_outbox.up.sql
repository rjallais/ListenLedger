-- Outbox for crash-safe domain event publishing.
-- The write path appends events + outbox rows in ONE SQLite transaction;
-- the relay (internal/outbox) publishes unpublished rows to JetStream
-- DOMAIN_EVENTS and marks them published. Projections keep their synchronous
-- publish for low-latency SSE; the relay is the durability backstop that
-- closes the dual-write loss window (commit succeeded, publish crashed).
-- MsgID = event ID makes the double-publish idempotent within the stream's
-- duplicates window; downstream consumers stay idempotent regardless
-- (projections use ON CONFLICT, SSE morphs are naturally idempotent).

CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    published_at TEXT,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(published_at) WHERE published_at IS NULL;

-- Backfill: existing events predate the outbox, so enqueue them all.
-- They fall inside the 90d DOMAIN_EVENTS retention and give the new stream
-- full parity with SQLite from day one; MsgID dedup keeps relay reruns safe.
INSERT OR IGNORE INTO outbox (event_id) SELECT id FROM events;
