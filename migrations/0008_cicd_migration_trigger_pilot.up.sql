-- =============================================================================
-- chora-notifications : 0008_cicd_migration_trigger_pilot.up.sql
--
-- CICD pilot migration — exercises the chora-migrations-apply-dev Cloud Build
-- trigger (which has NEVER fired per docs/cicd-audit-2026-05-21.md Tier 3 K).
--
-- This migration is intentionally a no-op against production data:
--   * NO table create / drop / alter
--   * NO data write to any domain table
--   * NO schema lock
--   * NO RLS policy change
--   * NO role grant change
--
-- It only sets a documentary COMMENT on the runner's own bookkeeping table
-- (chora_runner_schema_migrations, owned + created by the migrations-runner
-- itself, NOT by application code). Comments are metadata-only and incur
-- no row-level lock on production data.
--
-- Domain   : Notifications (supporting/platform — Team 3)
-- Database : chora_notifications
-- Date     : 2026-05-24
-- Tracker  : docs/cicd-audit-2026-05-21.md Tier 3 K (migration trigger pilot)
--
-- The runner falls through to "lenient apply" because this file is NOT in
-- chora-infra/scripts/migrations-runner/runner.sh SENTINEL_MAP. That is OK
-- — the apply itself is a single idempotent COMMENT ON statement, and the
-- runner records the filename in chora_runner_schema_migrations afterwards
-- so subsequent re-runs SKIP.
-- =============================================================================

-- COMMENT ON … IS requires a single string literal — `||` concatenation
-- expressions are rejected by the parser.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'chora_runner_schema_migrations') THEN
    COMMENT ON TABLE chora_runner_schema_migrations IS
        'Migration runner bookkeeping table. CICD pilot stamped 2026-05-24 by migration 0008_cicd_migration_trigger_pilot to exercise the chora-migrations-apply-dev Cloud Build trigger end-to-end.';
  END IF;
END $$;

-- No data changes, no DDL beyond the COMMENT above.
SELECT 1;  -- explicit no-op marker
