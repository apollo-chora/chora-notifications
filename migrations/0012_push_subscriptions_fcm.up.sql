-- 0012_push_subscriptions_fcm.up.sql — FCM cutover for push_subscriptions
-- (ADR-172 v2 / NOTIF-FCM lane).
--
-- History: 0010 created the FCM token shape (token UNIQUE, platform). The
-- raw-RFC-8291 webpush pivot (historical 0011_push_subscriptions_webpush,
-- hand-applied to the LIVE chora_notifications DB, since superseded and
-- REMOVED from the repo) dropped `token` and added endpoint/p256dh/auth.
-- This migration transforms the live webpush shape back to the token shape
-- and is a clean no-op on a fresh-DB replay (0010 → 0012; 0011 never runs).
--
-- Rows present in the webpush shape are raw push subscriptions that cannot
-- be reused as FCM tokens — they are soft-deleted (never hard-deleted, per
-- ddd-enforcement). Browsers re-register via PushSubscriptionService.enable().
BEGIN;

ALTER TABLE push_subscriptions DROP CONSTRAINT IF EXISTS push_subscriptions_endpoint_key;

-- Retire webpush-era rows (only when the table is actually in webpush shape;
-- guarded so the fresh-DB replay touches nothing).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'push_subscriptions' AND column_name = 'endpoint'
    ) THEN
        UPDATE push_subscriptions SET deleted_at = now() WHERE deleted_at IS NULL;
    END IF;
END $$;

ALTER TABLE push_subscriptions DROP COLUMN IF EXISTS endpoint;
ALTER TABLE push_subscriptions DROP COLUMN IF EXISTS p256dh;
ALTER TABLE push_subscriptions DROP COLUMN IF EXISTS auth;

-- Restore the FCM token column + uniqueness. Nullable BY DESIGN on both
-- paths (fresh DBs converge via DROP NOT NULL): webpush-era soft-deleted
-- relics carry NULL tokens; the repository always writes a token.
ALTER TABLE push_subscriptions ADD COLUMN IF NOT EXISTS token TEXT;
ALTER TABLE push_subscriptions ALTER COLUMN token DROP NOT NULL;
ALTER TABLE push_subscriptions DROP CONSTRAINT IF EXISTS push_subscriptions_token_key;
ALTER TABLE push_subscriptions ADD CONSTRAINT push_subscriptions_token_key UNIQUE (token);

COMMIT;
