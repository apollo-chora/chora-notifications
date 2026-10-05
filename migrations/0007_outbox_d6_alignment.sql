-- =============================================================================
-- chora-notifications : 0007_outbox_d6_alignment.sql
--
-- Aligns the chora-notifications outbox with the canonical D6.2 producer-side
-- transactional outbox shape per `feedback_d6_resilience_first_class` B.6.2
-- sub-deliverable (a) and the M12.3 outbox-templates plan (Wave 2 — w2d).
--
-- Mirrors (canonical references):
--   services/chora-guardrail/migrations/0004_outbox.sql
--   services/chora-a2a-gateway/migrations/0002_outbox.sql
--   services/chora-closure-orchestrator/migrations/0002_outbox.sql
--
-- Domain   : Notifications (supporting/platform — Team 3)
-- Database : chora_notifications
-- Date     : 2026-05-12
--
-- HARD INVARIANTS
--   * tenant_id is captured as a top-level UUID column (D6.3 multi-tenant
--     isolation indexing + RLS — production notification fan-out carries
--     events from many tenants through the same Pub/Sub pipe).
--   * idempotency_key + envelope mandatory per CLAUDE.md cross-cutting
--     rule (event envelope mandatory fields).
--   * Cross-DB queries remain forbidden — domain subscribers in other
--     chora-* services read events from Pub/Sub, never from this table.
--   * Topic taxonomy matches the chora.notifications.{aggregate}.{event_type}.v1
--     canonical form (already in internal/domain/events/events.go).
--
-- NOTE: This supersedes the pre-D6.2 generic outbox_events table created by
-- 0002_outbox.sql. That table used aggregate_type/aggregate_id columns but
-- lacked top-level tenant_id + RLS. No Go code reads or writes outbox_events
-- directly — the M12.3 W2d adapter package (internal/adapter/outbox/) targets
-- the canonical notifications_outbox_events shape from day-1.
-- =============================================================================

BEGIN;

-- Drop the pre-D6.2 generic outbox shape (no Go callers in production).
-- idempotency_keys is retained — separate concern (subscriber-side inbox
-- already landed in W1.7 via 0006_idempotency_keys.up.sql).
DROP TABLE IF EXISTS outbox_dead_letters;
DROP TABLE IF EXISTS outbox_poll_checkpoints;
DROP TABLE IF EXISTS outbox_events;

CREATE TABLE IF NOT EXISTS notifications_outbox_events (
    id              TEXT        PRIMARY KEY,                 -- UUIDv7 (event_id)
    tenant_id       UUID        NOT NULL,                    -- D6.3 isolation
    gcid            UUID,                                    -- subject (may be empty for system events)
    event_type      TEXT        NOT NULL,                    -- e.g. 'notifications.notification.created'
    topic           TEXT        NOT NULL,                    -- 'chora.notifications.{aggregate}.{event_type}.v1'
    payload         BYTEA       NOT NULL,                    -- JSON (POC) / Protobuf bytes
    envelope        JSONB       NOT NULL,                    -- full envelope: event_id,
                                                             -- idempotency_key, traceparent,
                                                             -- tracestate, source_project,
                                                             -- source_service, schema_version
    idempotency_key TEXT        NOT NULL,                    -- dedupe key (from envelope)
    occurred_at     TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','published','failed','deadlettered')),
    retry_count     INT         NOT NULL DEFAULT 0,
    last_error      TEXT        NOT NULL DEFAULT '',
    last_attempt_at TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dispatcher poll query — find next pending event by occurred_at.
CREATE INDEX IF NOT EXISTS notifications_outbox_events_pending_idx
    ON notifications_outbox_events (occurred_at ASC) WHERE status = 'pending';

-- D6.3 multi-tenant isolation lookup: dispatcher MAY filter per-tenant.
CREATE INDEX IF NOT EXISTS notifications_outbox_events_tenant_idx
    ON notifications_outbox_events (tenant_id, status, occurred_at);

-- Per-topic dispatcher worker mode.
CREATE INDEX IF NOT EXISTS notifications_outbox_events_topic_idx
    ON notifications_outbox_events (topic, status);

-- Idempotency dedupe — re-emission of the same event collapses on this
-- unique index. Unique because dedupe MUST be exact.
CREATE UNIQUE INDEX IF NOT EXISTS notifications_outbox_events_idempotency_idx
    ON notifications_outbox_events (idempotency_key);

-- Dispatcher checkpoint — at most one row per (worker_id, topic).
-- Tracks the last successfully published event so the worker can resume
-- after a pod-death without re-publishing already-acked events.
CREATE TABLE IF NOT EXISTS notifications_outbox_dispatch_checkpoints (
    worker_id                  TEXT        NOT NULL,
    topic                      TEXT        NOT NULL,
    last_processed_outbox_id   TEXT        NOT NULL,
    last_processed_occurred_at TIMESTAMPTZ NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (worker_id, topic)
);

CREATE INDEX IF NOT EXISTS notifications_outbox_dispatch_checkpoints_topic_idx
    ON notifications_outbox_dispatch_checkpoints (topic, updated_at DESC);

-- DLQ pointer — events that exceed max_retries land here. resolved_at
-- is set when an operator manually replays via the notifications runbook
-- (per B.6.2 sub-deliverable d "Orchestrator-side DLQ awareness").
CREATE TABLE IF NOT EXISTS notifications_outbox_dead_letters (
    outbox_event_id   TEXT        PRIMARY KEY REFERENCES notifications_outbox_events(id),
    failure_reason    TEXT        NOT NULL,
    attempt_count     INT         NOT NULL,
    worker_id         TEXT        NOT NULL,
    deadlettered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at       TIMESTAMPTZ,
    resolution_note   TEXT        NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS notifications_outbox_dead_letters_unresolved_idx
    ON notifications_outbox_dead_letters (deadlettered_at DESC) WHERE resolved_at IS NULL;

-- Multi-tenant RLS — same pattern as the guardrail + a2a outbox.
-- chora-notifications fan-out is inherently multi-tenant; production wires
-- the GUC via per-connection SET LOCAL app.current_tenant. RLS policy
-- remains permissive when the GUC is unset to keep the dispatcher
-- functional; per ADR-141 tenant isolation is also enforced at the
-- application + envelope layer.
ALTER TABLE notifications_outbox_events ENABLE ROW LEVEL SECURITY;

CREATE POLICY notifications_outbox_events_tenant_isolation ON notifications_outbox_events
    USING (
        current_setting('app.current_tenant', TRUE) IS NULL
        OR current_setting('app.current_tenant', TRUE) = ''
        OR tenant_id::TEXT = current_setting('app.current_tenant', TRUE)
    );

COMMIT;
