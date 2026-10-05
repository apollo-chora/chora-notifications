-- =============================================================================
-- chora-notifications : 0008_cicd_migration_trigger_pilot.down.sql
--
-- Rollback for 0008_cicd_migration_trigger_pilot.up.sql.
-- The runner SKIPS *.down.sql in forward apply (see runner.sh line ~442 +
-- ~488); this file exists only to satisfy the up/down pairing convention
-- adopted from 0006_idempotency_keys.up.sql onwards.
--
-- Rollback is itself a no-op — the up migration only set a metadata
-- COMMENT on the runner's own bookkeeping table.
-- =============================================================================

-- Clear the documentary comment set by the .up.sql.
COMMENT ON TABLE chora_runner_schema_migrations IS NULL;

SELECT 1;  -- explicit no-op marker
