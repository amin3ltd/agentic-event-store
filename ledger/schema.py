"""
PostgreSQL Event Store Schema

This module defines the append-only tables required for the event store.
Each table serves a specific purpose in the event sourcing infrastructure.
"""

# SQL for creating event store tables
# These tables form the foundation of The Ledger

CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id        TEXT NOT NULL,
    stream_position  BIGINT NOT NULL,
    global_position  BIGINT GENERATED ALWAYS AS IDENTITY,
    event_type       TEXT NOT NULL,
    event_version    SMALLINT NOT NULL DEFAULT 1,
    payload          JSONB NOT NULL,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    correlation_id   UUID,
    causation_id     UUID,
    CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);
"""

CREATE_EVENTS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events (stream_id, stream_position);",
    "CREATE INDEX IF NOT EXISTS idx_events_global_pos ON events (global_position);",
    "CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);",
    "CREATE INDEX IF NOT EXISTS idx_events_recorded ON events (recorded_at);",
    "CREATE INDEX IF NOT EXISTS idx_events_correlation ON events (correlation_id);",
    "CREATE INDEX IF NOT EXISTS idx_events_causation ON events (causation_id);",
]

CREATE_EVENT_STREAMS_TABLE = """
CREATE TABLE IF NOT EXISTS event_streams (
    stream_id        TEXT PRIMARY KEY,
    aggregate_type   TEXT NOT NULL,
    current_version  BIGINT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at      TIMESTAMPTZ,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);
"""

CREATE_PROJECTION_CHECKPOINTS_TABLE = """
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name  TEXT PRIMARY KEY,
    last_position    BIGINT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

CREATE_OUTBOX_TABLE = """
CREATE TABLE IF NOT EXISTS outbox (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         UUID NOT NULL REFERENCES events(event_id),
    destination      TEXT NOT NULL,
    payload          JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at     TIMESTAMPTZ,
    attempts         SMALLINT NOT NULL DEFAULT 0
);
"""

CREATE_OUTBOX_INDEX = "CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox (created_at) WHERE published_at IS NULL;"


def get_all_schema_sql() -> list[str]:
    """Returns all SQL statements needed to set up the event store schema."""
    statements = [
        CREATE_EVENTS_TABLE,
        CREATE_EVENT_STREAMS_TABLE,
        CREATE_PROJECTION_CHECKPOINTS_TABLE,
        CREATE_OUTBOX_TABLE,
    ]
    statements.extend(CREATE_EVENTS_INDEXES)
    statements.append(CREATE_OUTBOX_INDEX)
    return statements


# Schema version tracking for migrations
SCHEMA_VERSION = 1
