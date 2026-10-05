-- =============================================================================
-- chora-notifications : 0003_subscriber_preferences.sql
--
-- Subscriber preferences — per-(GCID, tenant) rich notification preferences
-- including digest mode, quiet hours, IANA timezone, and per-channel toggles.
--
-- Migrated from chora-communication.migrations/007_create_notification_preferences
-- during M12.2.E.5 consolidation.
--
-- Distinct from `subscription_preferences` (0001_initial.sql):
--   - subscription_preferences = per-topic-glob opt-in/out (lightweight, used
--     in the enqueue suppression hot path).
--   - subscriber_preferences   = per-(GCID, tenant) rich scheduling +
--     channel toggles + quiet hours (used by the digest scheduler).
--
-- Date     : 2026-05-12
-- =============================================================================

BEGIN;

CREATE TYPE digest_mode AS ENUM (
    'immediate',
    'hourly',
    'daily'
);

CREATE TABLE subscriber_preferences (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    gcid                UUID         NOT NULL,
    tenant_id           UUID         NOT NULL,
    digest_mode         digest_mode  NOT NULL DEFAULT 'immediate',
    quiet_hours_start   TEXT         NOT NULL DEFAULT '22:00',   -- "HH:MM"
    quiet_hours_end     TEXT         NOT NULL DEFAULT '07:00',   -- "HH:MM"
    timezone_strategy   TEXT         NOT NULL DEFAULT 'UTC',     -- IANA timezone
    in_app_enabled      BOOLEAN      NOT NULL DEFAULT true,
    push_enabled        BOOLEAN      NOT NULL DEFAULT true,
    email_enabled       BOOLEAN      NOT NULL DEFAULT true,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (gcid, tenant_id)
);

CREATE INDEX idx_subscriber_preferences_gcid        ON subscriber_preferences (gcid);
CREATE INDEX idx_subscriber_preferences_tenant      ON subscriber_preferences (tenant_id);
CREATE INDEX idx_subscriber_preferences_digest_mode ON subscriber_preferences (digest_mode)
    WHERE digest_mode != 'immediate';

CREATE TRIGGER trg_subscriber_prefs_updated_at
    BEFORE UPDATE ON subscriber_preferences
    FOR EACH ROW EXECUTE FUNCTION notifications_set_updated_at();

ALTER TABLE subscriber_preferences ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriber_preferences FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON subscriber_preferences
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMIT;
