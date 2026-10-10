-- Global Event Sequence & Projection Checkpoints
-- Adds global catch-up checkpointing for event-sourced projections and consumers.

CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    last_position INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_projection_checkpoints_updated ON projection_checkpoints(updated_at DESC);
