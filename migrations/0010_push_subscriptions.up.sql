-- =============================================================================
-- chora-notifications : 0010_push_subscriptions.up.sql
--
-- Web Push (FCM) registration tokens per (tenant, gcid). One user may register
-- many tokens (devices / browsers). The push dispatcher (ADR-172) looks these
-- up on chora.notifications.in_app.created.v1 and sends to each via FCM HTTP v1.
--
-- RLS tenant_isolation like every other notifications table; grants are
-- self-contained because 9999_grant_app_roles.sql (one-time GRANT ON ALL
-- TABLES) has already run and does not cover tables created afterwards.
--
-- Date : 2026-06-02
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS push_subscriptions (
    id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID         NOT NULL,
    gcid          UUID         NOT NULL,
    token         TEXT         NOT NULL,
    platform      TEXT         NOT NULL DEFAULT 'web',   -- web | android | ios
    user_agent    TEXT         NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_seen_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    deleted_at    TIMESTAMPTZ,
    -- A token is globally unique to one device; re-registration upserts.
    UNIQUE (token)
);

CREATE INDEX IF NOT EXISTS idx_push_subscriptions_tenant_gcid
    ON push_subscriptions (tenant_id, gcid) WHERE deleted_at IS NULL;

-- Idempotency guards (1c debt-pass item 3, 2026-06-11): this file was
-- applied out-of-band before the runner ever tracked it; inside the BEGIN
-- block an unguarded CREATE TRIGGER aborts the whole transaction on
-- re-run and the runner's lenient retry cannot recover (25P02 cascade).
CREATE OR REPLACE TRIGGER trg_push_subscriptions_updated_at
    BEFORE UPDATE ON push_subscriptions
    FOR EACH ROW EXECUTE FUNCTION notifications_set_updated_at();

ALTER TABLE push_subscriptions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON push_subscriptions;
CREATE POLICY tenant_isolation ON push_subscriptions
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- Self-contained grants (9999 already ran; new tables need explicit grants).
GRANT SELECT, INSERT, UPDATE, DELETE ON push_subscriptions TO chora_notifications_app_rw;
GRANT SELECT ON push_subscriptions TO chora_notifications_app_ro;

COMMIT;
