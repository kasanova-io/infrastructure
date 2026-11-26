-- Installed by the owning Notifications service under its existing DML lease.
-- Room capture is independently activated; historical/replayed rows stay quiet.
CREATE TABLE IF NOT EXISTS social_notifications.public_channel_activation (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    since_ms BIGINT NOT NULL DEFAULT (extract(epoch FROM clock_timestamp())*1000)::bigint
);
INSERT INTO social_notifications.public_channel_activation(singleton) VALUES(TRUE)
ON CONFLICT DO NOTHING;
ALTER TABLE social_notifications.outbox DROP CONSTRAINT IF EXISTS outbox_source_kind_check;
ALTER TABLE social_notifications.outbox ADD CONSTRAINT outbox_source_kind_check
CHECK (source_kind IN ('content','vote','follow','tip','room'));
CREATE TABLE IF NOT EXISTS social_notifications.public_rooms (
    outbox_id BIGINT PRIMARY KEY REFERENCES social_notifications.outbox(id) ON DELETE CASCADE,
    after_device TEXT NOT NULL DEFAULT '',
    retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts INTEGER NOT NULL DEFAULT 0,
    retry_pass BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE OR REPLACE FUNCTION social_notifications.capture_public_room() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE captured BIGINT;
BEGIN
    IF TG_RELID <> 'public.kachat_broadcasts'::regclass OR TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Unexpected public room trigger source';
    END IF;
    IF current_setting('kasanova.social_notify_allowed',true) IS DISTINCT FROM 'true'
       OR NEW.channel NOT IN ('kaspa','kasanova') THEN
        RETURN NEW;
    END IF;
    IF NEW.block_time >= (SELECT since_ms FROM social_notifications.public_channel_activation) THEN
        INSERT INTO social_notifications.outbox(source_kind,transaction_id,block_time)
        VALUES('room',NEW.transaction_id,NEW.block_time)
        ON CONFLICT DO NOTHING RETURNING id INTO captured;
        IF captured IS NOT NULL THEN
            INSERT INTO social_notifications.public_rooms(outbox_id) VALUES(captured);
        END IF;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION social_notifications.capture_public_room() FROM PUBLIC;
DO $room_capture$
BEGIN
    IF to_regclass('public.kachat_broadcasts') IS NOT NULL THEN
        DROP TRIGGER IF EXISTS kasanova_notification_room ON public.kachat_broadcasts;
        CREATE TRIGGER kasanova_notification_room AFTER INSERT ON public.kachat_broadcasts
        FOR EACH ROW EXECUTE FUNCTION social_notifications.capture_public_room();
    END IF;
END
$room_capture$;
