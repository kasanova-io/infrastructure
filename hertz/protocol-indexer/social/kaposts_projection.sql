-- Additive owning projection metadata; upstream native table shapes are retained.
DO $$
DECLARE table_name text; nonempty boolean;
BEGIN
  IF NOT EXISTS(SELECT 1 FROM social_shared.decoder_epochs WHERE kind='kchat:1:extensions') THEN
    FOREACH table_name IN ARRAY ARRAY['k_edits','k_deletes','kachat_delete_tombstones','k_polls','k_poll_votes'] LOOP
      IF to_regclass('public.'||table_name) IS NOT NULL THEN
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I)',table_name) INTO STRICT nonempty;
        IF nonempty THEN RAISE EXCEPTION 'unowned extension history requires reconciliation'; END IF;
      END IF;
    END LOOP;
  END IF;
END $$;
ALTER TABLE k_contents ADD COLUMN IF NOT EXISTS edited_at bigint;
DO $$ BEGIN
  IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='k_contents'::regclass
       AND conname='k_contents_content_type_check' AND pg_get_constraintdef(oid) LIKE '%poll%') THEN
    ALTER TABLE k_contents DROP CONSTRAINT IF EXISTS k_contents_content_type_check;
    ALTER TABLE k_contents ADD CONSTRAINT k_contents_content_type_check CHECK(content_type IN('post','reply','repost','quote','poll'));
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS k_edits (
  id bigserial PRIMARY KEY,transaction_id bytea UNIQUE NOT NULL,block_time bigint NOT NULL,
  sender_pubkey bytea NOT NULL,sender_signature bytea UNIQUE NOT NULL,post_id bytea NOT NULL,
  base64_encoded_message text NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_edits_post_time ON k_edits(post_id,block_time DESC,id DESC);
CREATE TABLE IF NOT EXISTS k_deletes (
  id bigserial PRIMARY KEY,transaction_id bytea UNIQUE NOT NULL,block_time bigint NOT NULL,
  sender_pubkey bytea NOT NULL,post_id bytea UNIQUE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_deletes_post ON k_deletes(post_id);
CREATE TABLE IF NOT EXISTS kachat_delete_tombstones (
  post_id bytea NOT NULL,sender_pubkey bytea NOT NULL,transaction_id bytea NOT NULL,block_time bigint NOT NULL,
  PRIMARY KEY(post_id,sender_pubkey)
);
CREATE INDEX IF NOT EXISTS kachat_delete_tombstones_tx ON kachat_delete_tombstones(transaction_id);
CREATE TABLE IF NOT EXISTS k_polls (post_id bytea PRIMARY KEY,options text NOT NULL,closes_at bigint NOT NULL,block_time bigint NOT NULL);
CREATE TABLE IF NOT EXISTS k_poll_votes (
  transaction_id bytea PRIMARY KEY,poll_id bytea NOT NULL,voter_pubkey bytea NOT NULL,
  option_index int NOT NULL,block_time bigint NOT NULL,sender_signature bytea UNIQUE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pollvotes_poll_voter_time ON k_poll_votes(poll_id,voter_pubkey,block_time DESC);

CREATE TABLE IF NOT EXISTS social_shared.content_keys (
  transaction_id bytea PRIMARY KEY REFERENCES social_shared.actions,
  signature bytea NOT NULL,author bytea NOT NULL,kind text NOT NULL,reference bytea,
  original_time bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS social_shared_content_signature ON social_shared.content_keys(signature,transaction_id);
CREATE INDEX IF NOT EXISTS social_shared_quote_relation ON social_shared.content_keys(author,reference,kind,signature);
CREATE INDEX IF NOT EXISTS social_shared_quote_range ON social_shared.content_keys(author,reference,original_time,transaction_id,signature);
CREATE TABLE IF NOT EXISTS social_shared.quote_withdrawals (
  sender bytea NOT NULL,reference bytea NOT NULL,transaction_id bytea NOT NULL,original_time bigint NOT NULL,
  PRIMARY KEY(sender,reference)
);
CREATE TABLE IF NOT EXISTS social_shared.ordinary_vote_targets (
  transaction_id bytea PRIMARY KEY REFERENCES social_shared.actions,post_id bytea NOT NULL
);
CREATE INDEX IF NOT EXISTS social_shared_ordinary_vote_target ON social_shared.ordinary_vote_targets(post_id,transaction_id);
CREATE TABLE IF NOT EXISTS social_shared.extensions (
  transaction_id bytea PRIMARY KEY REFERENCES social_shared.actions,
  revision bigint NOT NULL,original_time bigint NOT NULL,kind text NOT NULL,sender bytea NOT NULL,signature bytea NOT NULL,
  target bytea,message text,mentions jsonb NOT NULL,options text,closes_at bigint,option_index int,
  shape_valid boolean NOT NULL,target_signature bytea,target_author bytea,target_kind text,target_time bigint
);
CREATE INDEX IF NOT EXISTS social_shared_extension_target ON social_shared.extensions(target,kind,transaction_id);
CREATE INDEX IF NOT EXISTS social_shared_extension_signature ON social_shared.extensions(target_signature,kind,transaction_id);
CREATE INDEX IF NOT EXISTS social_shared_extension_identity ON social_shared.extensions(signature,transaction_id);
CREATE INDEX IF NOT EXISTS social_shared_extension_effective ON social_shared.extensions(target_signature,kind,original_time DESC,transaction_id DESC);
-- Keep immutable authorization predicates in the ordered index. Otherwise the
-- planner can underestimate sender=target_author and bitmap-scan/sort the entire
-- target history before its LIMIT, even when the newest edit is already valid.
CREATE INDEX IF NOT EXISTS social_shared_edit_effective ON social_shared.extensions(target_signature,original_time DESC,transaction_id DESC)
  WHERE kind='edit' AND shape_valid AND sender=target_author AND target_kind<>'poll'
    AND original_time>target_time AND original_time::numeric-target_time::numeric<=7200000;
CREATE INDEX IF NOT EXISTS social_shared_extension_relation ON social_shared.extensions(sender,target,kind,original_time DESC,transaction_id DESC);
CREATE TABLE IF NOT EXISTS social_shared.edit_mentions (
  transaction_id bytea NOT NULL REFERENCES social_shared.extensions,mentioned_pubkey bytea NOT NULL,
  PRIMARY KEY(transaction_id,mentioned_pubkey)
);
CREATE INDEX IF NOT EXISTS social_shared_edit_recipient ON social_shared.edit_mentions(mentioned_pubkey,transaction_id);
CREATE TABLE IF NOT EXISTS social_shared.extension_mentions (
  signature bytea NOT NULL,mentioned_pubkey bytea NOT NULL,id bigint NOT NULL,block_time bigint NOT NULL,
  PRIMARY KEY(signature,mentioned_pubkey)
);
CREATE TABLE IF NOT EXISTS social_shared.poll_vote_facts (
  transaction_id bytea PRIMARY KEY REFERENCES social_shared.extensions,
  revision bigint NOT NULL,post_signature bytea NOT NULL
);
-- Bootstrap only already-owned verified native records, not foreign history.
INSERT INTO social_shared.content_keys
SELECT t.transaction_id,t.sender_signature,t.sender_pubkey,t.content_type,t.referenced_content_id,a.original_time
FROM k_contents t JOIN social_shared.actions a USING(transaction_id) WHERE a.status='verified'
ON CONFLICT DO NOTHING;
INSERT INTO social_shared.ordinary_vote_targets
SELECT t.transaction_id,t.post_id FROM k_votes t JOIN social_shared.actions a USING(transaction_id) WHERE a.status='verified'
ON CONFLICT DO NOTHING;
INSERT INTO social_shared.decoder_epochs(kind) VALUES('kchat:1:extensions') ON CONFLICT DO NOTHING;
CREATE OR REPLACE VIEW social_shared.current_poll_votes AS
SELECT vote.*,post.transaction_id AS content_transaction_id FROM k_poll_votes vote
JOIN protocol_indexer.canonical c USING(transaction_id)
JOIN social_shared.poll_vote_facts fact ON fact.transaction_id=vote.transaction_id AND fact.revision=c.revision
JOIN social_shared.extensions e ON e.transaction_id=vote.transaction_id AND e.option_index=vote.option_index AND e.signature=vote.sender_signature
JOIN protocol_indexer.canonical target ON target.transaction_id=vote.poll_id
JOIN k_contents post ON post.sender_signature=fact.post_signature AND post.content_type='poll'
JOIN protocol_indexer.canonical representative ON representative.transaction_id=post.transaction_id
WHERE c.accepting_block IS NOT NULL AND target.accepting_block IS NOT NULL AND representative.accepting_block IS NOT NULL
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id IN(vote.transaction_id,vote.poll_id,post.transaction_id) AND p.acceptance_changed AND p.accepting_block IS NULL);
