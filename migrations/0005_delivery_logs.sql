-- =============================================================================
-- chora-notifications : 0005_delivery_logs.sql
--
-- DeliveryLog — append-only audit record for each per-channel delivery
-- attempt. Migrated from chora-communication.migrations/005_create_delivery_logs
-- during M12.2.E.5 consolidation.
--
-- Append-only: no UPDATE/DELETE allowed in domain code. A trigger here
-- enforces that schematically as well.
--
-- Date     : 2026-05-12
-- =============================================================================

BEGIN;

CREATE TYPE delivery_status AS ENUM (
    'pending', 'sent', 'delivered', 'bounced', 'failed'
);

CREATE TABLE delivery_logs (
    id                  UUID                  PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id     UUID                  NOT NULL,                  -- Cross-aggregate ref; no FK to preserve service independence
    channel             notification_channel  NOT NULL,
    status              delivery_status       NOT NULL DEFAULT 'pending',
    provider_message_id VARCHAR(500),
    error_message       TEXT,
    attempted_at        TIMESTAMPTZ           NOT NULL DEFAULT now(),
    delivered_at        TIMESTAMPTZ
);

CREATE INDEX idx_delivery_logs_notification ON delivery_logs (notification_id);
CREATE INDEX idx_delivery_logs_channel      ON delivery_logs (channel);
CREATE INDEX idx_delivery_logs_status       ON delivery_logs (status);
CREATE INDEX idx_delivery_logs_attempted    ON delivery_logs (attempted_at DESC);

CREATE OR REPLACE FUNCTION enforce_delivery_logs_append_only()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'delivery_logs is append-only: % rejected', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_delivery_logs_no_update
    BEFORE UPDATE ON delivery_logs
    FOR EACH ROW EXECUTE FUNCTION enforce_delivery_logs_append_only();

CREATE TRIGGER trg_delivery_logs_no_delete
    BEFORE DELETE ON delivery_logs
    FOR EACH ROW EXECUTE FUNCTION enforce_delivery_logs_append_only();

COMMIT;
