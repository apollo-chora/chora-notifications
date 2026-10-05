-- 0012_push_subscriptions_fcm.down.sql — reverse the FCM cutover back to
-- the raw-webpush shape (historical 0011). Rows registered under the FCM
-- shape are soft-deleted (token is meaningless to the webpush sender);
-- never hard-deleted.
BEGIN;

ALTER TABLE push_subscriptions DROP CONSTRAINT IF EXISTS push_subscriptions_token_key;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'push_subscriptions' AND column_name = 'token'
    ) THEN
        UPDATE push_subscriptions SET deleted_at = now() WHERE deleted_at IS NULL;
    END IF;
END $$;

ALTER TABLE push_subscriptions DROP COLUMN IF EXISTS token;
ALTER TABLE push_subscriptions ADD COLUMN IF NOT EXISTS endpoint TEXT;
ALTER TABLE push_subscriptions ADD COLUMN IF NOT EXISTS p256dh TEXT;
ALTER TABLE push_subscriptions ADD COLUMN IF NOT EXISTS auth TEXT;
ALTER TABLE push_subscriptions DROP CONSTRAINT IF EXISTS push_subscriptions_endpoint_key;
ALTER TABLE push_subscriptions ADD CONSTRAINT push_subscriptions_endpoint_key UNIQUE (endpoint);

COMMIT;
