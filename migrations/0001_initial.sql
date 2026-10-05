-- =============================================================================
-- chora-notifications : 0001_initial.sql
--
-- Domain        : Notifications (supporting/platform)
-- Database      : chora_notifications
-- Author        : agent-a5e52e89b73ede1d2 (db-migrations-11-services)
-- Date          : 2026-05-08
-- Architecture  : Architecture Review locked 2026-05-07 (Tier 1 D1)
--
-- Aggregates owned by this database:
--   - notifications (per-recipient per-channel delivery instance)
--   - templates (versioned, append-only handlebars-style templates)
--   - subscription_preferences (per-user opt-in/out, glob-matched topic)
--
-- Channel: email / push / in_app. SendGrid (Twilio) for email per CLAUDE.md §1.
-- Templates are APPEND-ONLY versions: NewVersion produces a fresh row carrying
-- version+1, never UPDATEs in place (mirrors AtomRevision invariant).
-- =============================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE OR REPLACE FUNCTION notifications_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- ENUMs
-- -----------------------------------------------------------------------------
CREATE TYPE notification_channel  AS ENUM ('email', 'push', 'in_app');
CREATE TYPE notification_status   AS ENUM ('queued', 'sent', 'failed', 'suppressed');
CREATE TYPE notification_priority AS ENUM ('low', 'normal', 'high', 'urgent');

-- -----------------------------------------------------------------------------
-- templates — APPEND-ONLY versioned content templates
--
-- NewVersion produces a fresh row carrying version+1 and a fresh template_id.
-- name + tenant_id + channel are stable across versions. The Go domain enforces
-- this; the schema preserves history rows by NEVER allowing UPDATE/DELETE.
-- -----------------------------------------------------------------------------
CREATE TABLE templates (
    template_id          UUID                 PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID                 NOT NULL,
    name                 VARCHAR(128)         NOT NULL,
    channel              notification_channel NOT NULL,
    subject_handlebars   TEXT                 NOT NULL DEFAULT '',
    body_handlebars      TEXT                 NOT NULL,
    version              INTEGER              NOT NULL DEFAULT 1 CHECK (version >= 1),
    registered_at        TIMESTAMPTZ          NOT NULL DEFAULT now(),
    created_at           TIMESTAMPTZ          NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name, version)
);

CREATE INDEX idx_templates_tenant_name ON templates (tenant_id, name, version DESC);
CREATE INDEX idx_templates_channel     ON templates (channel);

CREATE OR REPLACE FUNCTION enforce_templates_append_only()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'templates is append-only (version-based): % rejected', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_templates_no_update
    BEFORE UPDATE ON templates
    FOR EACH ROW EXECUTE FUNCTION enforce_templates_append_only();

CREATE TRIGGER trg_templates_no_delete
    BEFORE DELETE ON templates
    FOR EACH ROW EXECUTE FUNCTION enforce_templates_append_only();

ALTER TABLE templates ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON templates
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- notifications — per-recipient delivery instance
-- -----------------------------------------------------------------------------
CREATE TABLE notifications (
    notification_id      UUID                  PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID                  NOT NULL,
    recipient_gcid       UUID                  NOT NULL,
    channel              notification_channel  NOT NULL,
    template_id          UUID                  NOT NULL REFERENCES templates(template_id) ON DELETE RESTRICT,
    payload              JSONB                 NOT NULL DEFAULT '{}'::jsonb,
    priority             notification_priority NOT NULL DEFAULT 'normal',
    status               notification_status   NOT NULL DEFAULT 'queued',
    idempotency_key      VARCHAR(128)          NULL,
    retry_count          SMALLINT              NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    sent_at              TIMESTAMPTZ           NULL,
    read_at              TIMESTAMPTZ           NULL,
    created_at           TIMESTAMPTZ           NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ           NOT NULL DEFAULT now(),
    deleted_at           TIMESTAMPTZ           NULL
);

CREATE INDEX idx_notifications_tenant       ON notifications (tenant_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_notifications_recipient    ON notifications (recipient_gcid) WHERE deleted_at IS NULL;
CREATE INDEX idx_notifications_status       ON notifications (status) WHERE deleted_at IS NULL;
CREATE INDEX idx_notifications_priority     ON notifications (priority, created_at DESC) WHERE status = 'queued' AND deleted_at IS NULL;
CREATE UNIQUE INDEX idx_notifications_idem  ON notifications (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TRIGGER trg_notifications_updated_at
    BEFORE UPDATE ON notifications
    FOR EACH ROW EXECUTE FUNCTION notifications_set_updated_at();

ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON notifications
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- subscription_preferences — per-user opt-in/out, glob-matched topic
-- -----------------------------------------------------------------------------
CREATE TABLE subscription_preferences (
    preference_id        UUID                 PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID                 NOT NULL,
    gcid                 UUID                 NOT NULL,
    channel              notification_channel NOT NULL,
    topic_glob           VARCHAR(128)         NOT NULL,
    opted_in             BOOLEAN              NOT NULL DEFAULT TRUE,
    updated_at           TIMESTAMPTZ          NOT NULL DEFAULT now(),
    created_at           TIMESTAMPTZ          NOT NULL DEFAULT now(),
    UNIQUE (gcid, channel, topic_glob)
);

CREATE INDEX idx_subprefs_tenant_gcid ON subscription_preferences (tenant_id, gcid);
CREATE INDEX idx_subprefs_channel     ON subscription_preferences (channel);

CREATE TRIGGER trg_subprefs_updated_at
    BEFORE UPDATE ON subscription_preferences
    FOR EACH ROW EXECUTE FUNCTION notifications_set_updated_at();

ALTER TABLE subscription_preferences ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON subscription_preferences
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMIT;
