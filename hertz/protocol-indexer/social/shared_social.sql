-- Installed only after the configured canonical source and empty/unowned legacy
-- projection checks pass. No upstream scanner table/cursor is replaced.
CREATE SCHEMA IF NOT EXISTS social_shared;
CREATE TABLE IF NOT EXISTS social_shared.config (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    network text NOT NULL,
    source_id text NOT NULL
);
CREATE TABLE IF NOT EXISTS social_shared.actions (
    transaction_id bytea PRIMARY KEY CHECK(octet_length(transaction_id)=32),
    revision bigint NOT NULL,
    subject text,
    native_table text,
    state_action text,
    source_payload text NOT NULL,
    original_time bigint NOT NULL,
    status text NOT NULL CHECK(status IN ('verified','unsupported_or_invalid')),
    notification_consumed boolean NOT NULL DEFAULT false,
    snapshot jsonb
);
CREATE INDEX IF NOT EXISTS social_shared_subject ON social_shared.actions(subject,original_time,transaction_id);
CREATE INDEX IF NOT EXISTS social_shared_effective ON social_shared.actions(subject,state_action,original_time,transaction_id);

-- Additive KaChat tip facts retain upstream column/uniqueness conventions.
-- Decoder epoch prevents an upgrade replay becoming a new live notification.
CREATE TABLE IF NOT EXISTS social_shared.decoder_epochs (
    kind text PRIMARY KEY,
    since_ms bigint NOT NULL DEFAULT (extract(epoch FROM clock_timestamp())*1000)::bigint
);
DO $$ BEGIN
    IF to_regclass('public.k_tips') IS NOT NULL
       AND NOT EXISTS(SELECT 1 FROM social_shared.decoder_epochs WHERE kind='kchat:1:tip') THEN
        IF EXISTS(SELECT 1 FROM public.k_tips) THEN
            RAISE EXCEPTION 'unowned tip history requires explicit reconciliation';
        END IF;
    END IF;
END $$;
CREATE TABLE IF NOT EXISTS public.k_tips (
    id BIGSERIAL NOT NULL,
    transaction_id BYTEA PRIMARY KEY,
    block_time BIGINT NOT NULL,
    sender_pubkey BYTEA NOT NULL,
    sender_signature BYTEA UNIQUE NOT NULL,
    post_id BYTEA NOT NULL,
    author_pubkey BYTEA NOT NULL,
    amount BIGINT NOT NULL CHECK(amount>0)
);
CREATE INDEX IF NOT EXISTS idx_tips_post_time ON public.k_tips(post_id,block_time DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_tips_sender ON public.k_tips(sender_pubkey);
INSERT INTO social_shared.decoder_epochs(kind) VALUES('kchat:1:tip') ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS social_shared.tip_targets (
    transaction_id bytea PRIMARY KEY REFERENCES social_shared.actions(transaction_id),
    post_id bytea NOT NULL CHECK(octet_length(post_id)=32),
    post_signature bytea
);
CREATE INDEX IF NOT EXISTS social_shared_tip_target ON social_shared.tip_targets(post_id,transaction_id);

CREATE TABLE IF NOT EXISTS social_shared.tip_payments (
    transaction_id bytea PRIMARY KEY REFERENCES social_shared.actions(transaction_id),
    revision bigint NOT NULL,
    amount bigint NOT NULL CHECK(amount>0),
    post_signature bytea NOT NULL
);

CREATE INDEX IF NOT EXISTS social_shared_tip_payment_signature
ON social_shared.tip_payments(post_signature,transaction_id);

-- Counts/readers use current accepted facts even before a queued revocation is
-- drained. This adds no public endpoint and preserves native table shapes.
CREATE OR REPLACE VIEW social_shared.current_tips AS
SELECT tip.*,post.transaction_id AS content_transaction_id FROM public.k_tips tip
JOIN protocol_indexer.canonical payment ON payment.transaction_id=tip.transaction_id
JOIN protocol_indexer.canonical target ON target.transaction_id=tip.post_id
JOIN social_shared.tip_payments fact ON fact.transaction_id=tip.transaction_id AND fact.revision=payment.revision
JOIN social_shared.tip_targets dependency ON dependency.transaction_id=tip.transaction_id
JOIN public.k_contents post ON post.sender_signature=fact.post_signature
JOIN protocol_indexer.canonical representative ON representative.transaction_id=post.transaction_id
WHERE payment.accepting_block IS NOT NULL AND target.accepting_block IS NOT NULL
AND representative.accepting_block IS NOT NULL AND tip.amount=fact.amount
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p
    WHERE p.transaction_id IN(tip.transaction_id,tip.post_id,post.transaction_id)
      AND p.acceptance_changed AND p.accepting_block IS NULL);
