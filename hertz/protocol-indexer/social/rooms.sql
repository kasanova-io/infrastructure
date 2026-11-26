-- Native KaChat room table shape from pinned e2 database.rs. No second scanner,
-- TTL purge, operator channel/art filter or notification writer is installed.
DO $$ BEGIN
  IF to_regclass('public.kachat_broadcasts') IS NOT NULL
     AND NOT EXISTS(SELECT 1 FROM social_shared.decoder_epochs WHERE kind='kchat:1:bcast') THEN
    IF EXISTS(SELECT 1 FROM public.kachat_broadcasts) THEN
      RAISE EXCEPTION 'unowned room history requires explicit reconciliation';
    END IF;
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS kachat_broadcasts (
  id bigserial PRIMARY KEY,transaction_id bytea UNIQUE NOT NULL,
  block_time bigint NOT NULL,channel varchar(36) NOT NULL,
  sender_address text NOT NULL,content text NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bcast_channel_time ON kachat_broadcasts(channel,block_time DESC,id DESC);
CREATE TABLE IF NOT EXISTS social_shared.room_facts (
  transaction_id bytea PRIMARY KEY REFERENCES social_shared.actions,
  revision bigint NOT NULL,sender_address text NOT NULL
);
INSERT INTO social_shared.decoder_epochs(kind) VALUES('kchat:1:bcast') ON CONFLICT DO NOTHING;
CREATE OR REPLACE VIEW social_shared.current_broadcasts AS
SELECT b.* FROM kachat_broadcasts b
JOIN protocol_indexer.canonical c USING(transaction_id)
JOIN social_shared.room_facts f ON f.transaction_id=b.transaction_id AND f.revision=c.revision AND f.sender_address=b.sender_address
WHERE c.accepting_block IS NOT NULL
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=c.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL);
