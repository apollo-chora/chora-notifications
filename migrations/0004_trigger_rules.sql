-- =============================================================================
-- chora-notifications : 0004_trigger_rules.sql
--
-- TriggerRule — maps domain events to notification deliveries via templates +
-- recipient roles + channels + optional schedule.
--
-- Migrated from chora-communication.migrations/004_create_trigger_rules_and_schedules
-- during M12.2.E.5 consolidation.
--
-- tenant_id is nullable: a NULL row is a platform-default rule applied to
-- every tenant unless overridden by a tenant-scoped rule with the same
-- source_event_type.
--
-- Date     : 2026-05-12
-- =============================================================================

BEGIN;

CREATE TYPE trigger_priority AS ENUM ('critical', 'high', 'normal', 'low');
CREATE TYPE trigger_category AS ENUM (
    'engagement', 'assessment', 'social', 'training',
    'gamification', 'account', 'system'
);

CREATE TABLE trigger_rules (
    id                          UUID                 PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                   UUID,                                  -- NULL = platform default
    name                        VARCHAR(255)         NOT NULL,
    source_event_type           VARCHAR(255)         NOT NULL,
    notification_title_template VARCHAR(500),
    notification_body_template  TEXT,
    priority                    trigger_priority     NOT NULL DEFAULT 'normal',
    category                    trigger_category     NOT NULL DEFAULT 'system',
    recipient_roles             TEXT[]               NOT NULL DEFAULT '{}',
    channels                    notification_channel[] NOT NULL DEFAULT '{}',
    is_active                   BOOLEAN              NOT NULL DEFAULT true,
    schedule                    JSONB,                                 -- {cron_expression, timezone_strategy, quiet_hours_*}
    created_at                  TIMESTAMPTZ          NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ          NOT NULL DEFAULT now(),
    deleted_at                  TIMESTAMPTZ                            -- Soft delete per ddd-enforcement #5
);

CREATE INDEX idx_trigger_rules_tenant       ON trigger_rules (tenant_id)
    WHERE deleted_at IS NULL;
CREATE INDEX idx_trigger_rules_event_type   ON trigger_rules (source_event_type)
    WHERE deleted_at IS NULL AND is_active = true;
CREATE INDEX idx_trigger_rules_active       ON trigger_rules (is_active)
    WHERE deleted_at IS NULL;

CREATE TRIGGER trg_trigger_rules_updated_at
    BEFORE UPDATE ON trigger_rules
    FOR EACH ROW EXECUTE FUNCTION notifications_set_updated_at();

ALTER TABLE trigger_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE trigger_rules FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON trigger_rules
    FOR ALL USING (
        tenant_id IS NULL
        OR tenant_id = current_setting('chora.tenant_id', true)::uuid
    );

COMMIT;
