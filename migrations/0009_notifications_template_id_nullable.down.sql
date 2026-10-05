-- 0009_notifications_template_id_nullable.down.sql
--
-- Reverting requires that no NULL template_id rows exist (the NOT NULL re-add
-- would fail otherwise). Fan-out notifications carry NULL template_id, so a
-- down-migration must first decide their fate; here we simply re-assert the
-- constraint and let it fail loud if NULLs are present (operator backfills or
-- deletes them deliberately — never silently).
ALTER TABLE notifications ALTER COLUMN template_id SET NOT NULL;
