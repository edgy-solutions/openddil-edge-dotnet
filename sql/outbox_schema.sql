-- =============================================================================
-- OpenDDIL Edge — SQLite Outbox Schema
-- =============================================================================
-- Local write-side event spool for DDIL environments. Events are inserted here
-- by the Edge SDK and flushed to HQ (Redpanda) when connectivity is available.
--
-- This script is executed by OpenDdilOutboxManager on first initialization.
-- =============================================================================

CREATE TABLE IF NOT EXISTS outbox_events (
    -- Unique event ID (UUID v4 string). Matches the CloudEvent.id and the
    -- domain event's event_id for end-to-end idempotency.
    id              TEXT    PRIMARY KEY,

    -- CloudEvent type URI (e.g., "openddil.inventory.v1.ItemAllocatedEvent").
    -- Used by the Relay to route events and by consumers to deserialize.
    event_type      TEXT    NOT NULL,

    -- Serialized Protobuf binary of the full CloudEvent envelope.
    -- Contains both metadata and the packed domain event payload.
    payload_bytes   BLOB    NOT NULL,

    -- When this event was spooled into the Outbox (ISO 8601, UTC).
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),

    -- Whether the Relay has successfully flushed this event to HQ.
    -- 0 = pending, 1 = flushed and acknowledged.
    is_processed    INTEGER NOT NULL DEFAULT 0
);

-- Index for the Relay to efficiently query unflushed events in order.
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox_events (is_processed, created_at)
    WHERE is_processed = 0;
