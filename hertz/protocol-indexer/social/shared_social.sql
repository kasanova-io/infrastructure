-- Installed only after the configured canonical source and empty/unowned legacy
-- projection checks pass. No upstream scanner table/cursor is replaced.
CREATE SCHEMA IF NOT EXISTS social_shared;
CREATE TABLE IF NOT EXISTS social_shared.config (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    network text NOT NULL,
    source_id text NOT NULL
);
-- An operator-bound native snapshot is distinct from a canonical chain receipt.
-- The inventory covers the original tables, including notification state.
CREATE TABLE IF NOT EXISTS social_shared.retained_imports (
    import_id text PRIMARY KEY CHECK(import_id ~ '^[0-9a-f]{64}$'),
    network text NOT NULL, source_id text NOT NULL,
    protocol_hint text NOT NULL CHECK(protocol_hint IN ('k','kchat')),
    source_backup_sha256 text NOT NULL CHECK(source_backup_sha256 ~ '^[0-9a-f]{64}$'),
    inventory jsonb NOT NULL, native_records bigint NOT NULL CHECK(native_records>=0),
    imported_at timestamptz NOT NULL DEFAULT clock_timestamp()
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
ALTER TABLE social_shared.actions ADD COLUMN IF NOT EXISTS provenance text NOT NULL DEFAULT 'canonical'
    CHECK(provenance IN ('canonical','retained_native'));
ALTER TABLE social_shared.actions ADD COLUMN IF NOT EXISTS retained_import_id text REFERENCES social_shared.retained_imports(import_id);
CREATE TABLE IF NOT EXISTS social_shared.retained_conflicts (
    transaction_id bytea PRIMARY KEY REFERENCES social_shared.actions(transaction_id),
    canonical_revision bigint NOT NULL,
    reason text NOT NULL CHECK(reason='canonical_signing_fields_differ_from_retained_snapshot'),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- Accepted journal history replayed at migration is silent even when its
-- original live event predates this new projection. Future first acceptances
-- are not included in this fixed operator snapshot.
CREATE TABLE IF NOT EXISTS social_shared.retained_replay (
    transaction_id bytea PRIMARY KEY REFERENCES protocol_indexer.canonical(transaction_id),
    canonical_revision bigint NOT NULL,
    retained_import_id text NOT NULL REFERENCES social_shared.retained_imports(import_id)
);
-- No fabricated accepting block, original RPC transaction or canonical row.
-- Once ANY canonical record exists, the retained fallback is ineligible. A
-- formerly retained action must first match the actual SDK-verified fields.
CREATE OR REPLACE VIEW social_shared.source_facts AS
SELECT c.transaction_id,c.revision,
    substring(c.source_transaction->>'payload' FROM 3) AS verification_payload,
    COALESCE(a.original_time,(c.source_transaction->>'block_time')::bigint) AS original_time,
    (c.accepting_block IS NOT NULL AND
     NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=c.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) AND
     (a.retained_import_id IS NULL OR (a.provenance='canonical' AND a.status='verified'))) AS eligible,
    false AS retained_native
FROM protocol_indexer.canonical c LEFT JOIN social_shared.actions a USING(transaction_id)
UNION ALL
SELECT a.transaction_id,a.revision,a.source_payload,a.original_time,true,true
FROM social_shared.actions a JOIN social_shared.retained_imports i ON i.import_id=a.retained_import_id
JOIN social_shared.config cfg ON cfg.singleton AND cfg.network=i.network AND cfg.source_id=i.source_id
WHERE a.provenance='retained_native' AND a.status='verified'
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.canonical c WHERE c.transaction_id=a.transaction_id)
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL);

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
JOIN social_shared.source_facts target ON target.transaction_id=tip.post_id
JOIN social_shared.tip_payments fact ON fact.transaction_id=tip.transaction_id AND fact.revision=payment.revision
JOIN social_shared.tip_targets dependency ON dependency.transaction_id=tip.transaction_id
JOIN public.k_contents post ON post.sender_signature=fact.post_signature
JOIN social_shared.source_facts representative ON representative.transaction_id=post.transaction_id
WHERE payment.accepting_block IS NOT NULL AND target.eligible
AND representative.eligible AND tip.amount=fact.amount
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p
    WHERE p.transaction_id IN(tip.transaction_id,tip.post_id,post.transaction_id)
      AND p.acceptance_changed AND p.accepting_block IS NULL);
