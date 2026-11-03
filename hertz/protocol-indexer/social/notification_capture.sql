-- Installed in the configured Social database, never the wallet/users database.
-- AFTER INSERT runs only after the processor's signature verification. Capture
-- commits atomically with the projection and never scans imported history.
CREATE SCHEMA IF NOT EXISTS social_notifications;
CREATE TABLE IF NOT EXISTS social_notifications.activation (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    since_ms BIGINT NOT NULL DEFAULT (extract(epoch FROM clock_timestamp()) * 1000)::bigint
);
INSERT INTO social_notifications.activation (singleton) VALUES (TRUE)
ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS social_notifications.outbox (
    id BIGSERIAL PRIMARY KEY,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('content', 'vote', 'follow')),
    transaction_id BYTEA NOT NULL,
    block_time BIGINT NOT NULL,
    retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts INTEGER NOT NULL DEFAULT 0,
    completed_at TIMESTAMPTZ,
    UNIQUE (source_kind, transaction_id)
);
CREATE INDEX IF NOT EXISTS social_outbox_pending
ON social_notifications.outbox (retry_at, id) WHERE completed_at IS NULL;

CREATE OR REPLACE FUNCTION social_notifications.xkey(key BYTEA) RETURNS BYTEA
LANGUAGE SQL IMMUTABLE STRICT AS $$
    SELECT CASE WHEN octet_length(key) = 32 THEN key
        WHEN octet_length(key) = 33 AND get_byte(key, 0) IN (2, 3)
        THEN substring(key FROM 2) END
$$;
CREATE INDEX IF NOT EXISTS social_blocks_identity
ON public.k_blocks (social_notifications.xkey(sender_pubkey),
                    social_notifications.xkey(blocked_user_pubkey));

CREATE OR REPLACE FUNCTION social_notifications.capture() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    -- Shared canonical replay sets this transaction-local context per native
    -- action. Its history epoch may be newer than this service's activation;
    -- the timestamp check alone cannot distinguish live from replay inserts.
    -- Legacy verified writers leave it unset and retain existing behavior.
    IF current_setting('kasanova.social_notify_allowed', true) = 'false' THEN
        RETURN NEW;
    END IF;
    IF NEW.block_time >= (SELECT since_ms FROM social_notifications.activation) THEN
        INSERT INTO social_notifications.outbox (source_kind, transaction_id, block_time)
        VALUES (TG_ARGV[0], NEW.transaction_id, NEW.block_time)
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NEW;
END
$$;
DROP TRIGGER IF EXISTS kasanova_notification_content ON public.k_contents;
CREATE TRIGGER kasanova_notification_content AFTER INSERT ON public.k_contents
FOR EACH ROW EXECUTE FUNCTION social_notifications.capture('content');
DROP TRIGGER IF EXISTS kasanova_notification_vote ON public.k_votes;
CREATE TRIGGER kasanova_notification_vote AFTER INSERT ON public.k_votes
FOR EACH ROW EXECUTE FUNCTION social_notifications.capture('vote');
DROP TRIGGER IF EXISTS kasanova_notification_follow ON public.k_follows;
CREATE TRIGGER kasanova_notification_follow AFTER INSERT ON public.k_follows
FOR EACH ROW EXECUTE FUNCTION social_notifications.capture('follow');
