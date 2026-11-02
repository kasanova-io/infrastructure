-- Existing native table shapes, filtered by the same native subject policy.
-- Installed only by the source-bound projection, never by an API read.
CREATE SCHEMA IF NOT EXISTS social_shared_api;

CREATE OR REPLACE FUNCTION social_shared_api.selected_k_contents(subject text)
RETURNS bytea LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp
AS $selection_k_contents$
SELECT picked.transaction_id FROM (SELECT a.transaction_id,a.source_payload,a.original_time,a.notification_consumed,c.revision,c.retained_native,a.snapshot::text AS snapshot FROM social_shared.actions a JOIN LATERAL (SELECT c.* FROM social_shared.source_facts c WHERE c.transaction_id=a.transaction_id OFFSET 0) c ON true WHERE a.subject=$1 AND c.eligible AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) AND NOT EXISTS(SELECT 1 FROM social_shared.extensions e WHERE e.transaction_id=a.transaction_id AND e.kind='poll' AND NOT e.shape_valid) ORDER BY EXISTS(SELECT 1 FROM k_contents native WHERE native.transaction_id=a.transaction_id) DESC,a.original_time,a.transaction_id LIMIT 1) picked
$selection_k_contents$;

CREATE OR REPLACE FUNCTION social_shared_api.selected_k_votes(subject text)
RETURNS bytea LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp
AS $selection_k_votes$
SELECT picked.transaction_id FROM (SELECT a.transaction_id,a.source_payload,a.original_time,a.notification_consumed,c.revision,c.retained_native,a.snapshot::text AS snapshot FROM social_shared.actions a JOIN LATERAL (SELECT c.* FROM social_shared.source_facts c WHERE c.transaction_id=a.transaction_id OFFSET 0) c ON true WHERE a.subject=$1 AND c.eligible AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) ORDER BY EXISTS(SELECT 1 FROM k_votes native WHERE native.transaction_id=a.transaction_id) DESC,a.original_time,a.transaction_id LIMIT 1) picked
$selection_k_votes$;

CREATE OR REPLACE FUNCTION social_shared_api.selected_k_broadcasts(subject text)
RETURNS bytea LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp
AS $selection_k_broadcasts$
SELECT picked.transaction_id FROM (SELECT a.transaction_id,a.source_payload,a.original_time,a.notification_consumed,c.revision,c.retained_native,a.snapshot::text AS snapshot FROM social_shared.actions a JOIN LATERAL (SELECT c.* FROM social_shared.source_facts c WHERE c.transaction_id=a.transaction_id OFFSET 0) c ON true WHERE a.subject=$1 AND c.eligible AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) ORDER BY a.original_time DESC,a.transaction_id DESC LIMIT 1) picked
$selection_k_broadcasts$;

CREATE OR REPLACE FUNCTION social_shared_api.selected_k_follows(subject text)
RETURNS bytea LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp
AS $selection_k_follows$
SELECT picked.transaction_id FROM (WITH reversal AS (SELECT a.original_time,a.transaction_id FROM social_shared.actions a JOIN LATERAL (SELECT c.* FROM social_shared.source_facts c WHERE c.transaction_id=a.transaction_id OFFSET 0) c ON true WHERE a.subject=$1 AND c.eligible AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) AND a.state_action='deactivate' ORDER BY a.original_time DESC,a.transaction_id DESC LIMIT 1) SELECT a.transaction_id,a.source_payload,a.original_time,a.notification_consumed,c.revision,c.retained_native,a.snapshot::text AS snapshot FROM social_shared.actions a JOIN LATERAL (SELECT c.* FROM social_shared.source_facts c WHERE c.transaction_id=a.transaction_id OFFSET 0) c ON true WHERE a.subject=$1 AND c.eligible AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) AND a.state_action='activate' AND (NOT EXISTS(SELECT 1 FROM reversal) OR (a.original_time,a.transaction_id)>(SELECT original_time,transaction_id FROM reversal)) ORDER BY a.original_time,a.transaction_id LIMIT 1) picked
$selection_k_follows$;

CREATE OR REPLACE FUNCTION social_shared_api.selected_k_blocks(subject text)
RETURNS bytea LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp
AS $selection_k_blocks$
SELECT picked.transaction_id FROM (WITH reversal AS (SELECT a.original_time,a.transaction_id FROM social_shared.actions a JOIN LATERAL (SELECT c.* FROM social_shared.source_facts c WHERE c.transaction_id=a.transaction_id OFFSET 0) c ON true WHERE a.subject=$1 AND c.eligible AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) AND a.state_action='deactivate' ORDER BY a.original_time DESC,a.transaction_id DESC LIMIT 1) SELECT a.transaction_id,a.source_payload,a.original_time,a.notification_consumed,c.revision,c.retained_native,a.snapshot::text AS snapshot FROM social_shared.actions a JOIN LATERAL (SELECT c.* FROM social_shared.source_facts c WHERE c.transaction_id=a.transaction_id OFFSET 0) c ON true WHERE a.subject=$1 AND c.eligible AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) AND a.state_action='activate' AND (NOT EXISTS(SELECT 1 FROM reversal) OR (a.original_time,a.transaction_id)>(SELECT original_time,transaction_id FROM reversal)) ORDER BY a.original_time,a.transaction_id LIMIT 1) picked
$selection_k_blocks$;

-- Bound each canonical/retained UNION lookup to its action BEFORE joins.
-- The offset preserves parameterization; eligibility and revision checks stay
-- identical while an ordered page avoids repeated whole-history hash builds.
CREATE OR REPLACE VIEW social_shared_api.current_actions AS
SELECT a.* FROM social_shared.actions a
JOIN LATERAL (SELECT c.* FROM social_shared.source_facts c
 WHERE c.transaction_id=a.transaction_id OFFSET 0) c ON true
WHERE a.status='verified' AND a.revision=c.revision AND c.eligible
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL);

-- Authorization is shared verbatim with the native edit selector. This view
-- retains ALL current suppliers: edit-derived mentions are cumulative upstream.
CREATE OR REPLACE VIEW social_shared_api.current_edits AS
SELECT e.* FROM social_shared.extensions e
JOIN protocol_indexer.canonical c USING(transaction_id)
JOIN LATERAL (SELECT t.* FROM social_shared.source_facts t
 WHERE t.transaction_id=e.target OFFSET 0) t ON true
JOIN social_shared_api.current_actions a ON a.transaction_id=e.transaction_id
JOIN social_shared_api.current_actions target_action ON target_action.transaction_id=e.target
WHERE e.revision=c.revision AND __AUTHORIZED_EDIT__;

CREATE OR REPLACE VIEW social_shared_api.k_contents AS
SELECT p.* FROM public.k_contents p
WHERE EXISTS (
SELECT 1 FROM LATERAL (SELECT a.* FROM social_shared_api.current_actions a
 WHERE a.transaction_id=p.transaction_id OFFSET 0) a
LEFT JOIN LATERAL (
 SELECT e.transaction_id,e.original_time,e.message FROM social_shared_api.current_edits e
 WHERE e.target_signature=p.sender_signature
 ORDER BY e.original_time DESC,e.transaction_id DESC LIMIT 1
) effective_edit ON true
WHERE a.transaction_id=p.transaction_id
AND p.transaction_id=social_shared_api.selected_k_contents(a.subject)
AND ((effective_edit.transaction_id IS NULL AND p.edited_at IS NULL)
 OR (p.edited_at=effective_edit.original_time AND p.base64_encoded_message=effective_edit.message))
AND NOT EXISTS(
 SELECT 1 FROM social_shared.extensions e
 JOIN social_shared_api.current_actions mutation ON mutation.transaction_id=e.transaction_id
 JOIN social_shared_api.current_actions target_action ON target_action.transaction_id=e.target
 WHERE e.kind='delete' AND e.shape_valid AND e.sender=e.target_author
 AND e.target_signature=p.sender_signature AND e.revision=mutation.revision
)
AND NOT EXISTS(
 SELECT 1 FROM social_shared.content_keys k
 JOIN social_shared.extensions e ON e.kind='unquote' AND e.sender=k.author AND e.target=k.reference
 JOIN social_shared_api.current_actions mutation ON mutation.transaction_id=e.transaction_id
 WHERE k.transaction_id=p.transaction_id AND k.kind IN('quote','repost')
 AND e.shape_valid AND e.revision=mutation.revision AND __UNQUOTE_ORDER__
)
-- Bound the native transaction lookup before evaluating the subject selector.
-- Keep the native ordered/indexed relation as the outer pagination scan.
-- OFFSET0 prevents flattening this per-row eligibility check into a ledger-first
-- join/sort of the entire history; it does not change result membership.
OFFSET 0
);

-- Poll read metadata belongs to the currently selected, eligible post and
-- the same verified extension revision. Never read unbound native options.
CREATE OR REPLACE VIEW social_shared_api.current_polls AS
SELECT p.post_id,p.options,p.closes_at FROM public.k_polls p
JOIN social_shared_api.k_contents post ON post.transaction_id=p.post_id AND post.content_type='poll'
JOIN social_shared_api.current_actions a ON a.transaction_id=p.post_id
JOIN social_shared.extensions e ON e.transaction_id=p.post_id AND e.revision=a.revision
WHERE e.kind='poll' AND e.shape_valid AND e.signature=post.sender_signature
AND e.sender=post.sender_pubkey AND e.message=post.base64_encoded_message
AND e.options=p.options AND e.closes_at=p.closes_at AND e.original_time=p.block_time;

CREATE OR REPLACE VIEW social_shared_api.k_broadcasts AS
SELECT native.* FROM public.k_broadcasts native
WHERE EXISTS (SELECT 1 FROM LATERAL (SELECT a.* FROM social_shared_api.current_actions a
 WHERE a.transaction_id=native.transaction_id OFFSET 0) a
 WHERE a.transaction_id=native.transaction_id
 AND native.transaction_id=social_shared_api.selected_k_broadcasts(a.subject)
 OFFSET 0);
CREATE OR REPLACE VIEW social_shared_api.k_follows AS
SELECT native.* FROM public.k_follows native
WHERE EXISTS (SELECT 1 FROM LATERAL (SELECT a.* FROM social_shared_api.current_actions a
 WHERE a.transaction_id=native.transaction_id OFFSET 0) a
 WHERE a.transaction_id=native.transaction_id
 AND native.transaction_id=social_shared_api.selected_k_follows(a.subject)
 OFFSET 0);
CREATE OR REPLACE VIEW social_shared_api.k_blocks AS
SELECT native.* FROM public.k_blocks native
WHERE EXISTS (SELECT 1 FROM LATERAL (SELECT a.* FROM social_shared_api.current_actions a
 WHERE a.transaction_id=native.transaction_id OFFSET 0) a
 WHERE a.transaction_id=native.transaction_id
 AND native.transaction_id=social_shared_api.selected_k_blocks(a.subject)
 OFFSET 0);

-- Preserve the signed literal target. Unknown targets keep upstream vote lookup
-- semantics; a KNOWN revoked/pending-removal target cannot be rescued by an alias.
CREATE OR REPLACE VIEW social_shared_api.k_votes AS
SELECT native.* FROM public.k_votes native
WHERE EXISTS (SELECT 1 FROM LATERAL (SELECT a.* FROM social_shared_api.current_actions a
 WHERE a.transaction_id=native.transaction_id OFFSET 0) a
LEFT JOIN social_shared.ordinary_vote_targets dependency USING(transaction_id)
WHERE a.transaction_id=native.transaction_id
AND native.transaction_id=social_shared_api.selected_k_votes(a.subject)
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.canonical target
 WHERE target.transaction_id=COALESCE(dependency.post_id,native.post_id) AND target.accepting_block IS NULL)
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending pending
 WHERE pending.transaction_id=COALESCE(dependency.post_id,native.post_id)
 AND pending.acceptance_changed AND pending.accepting_block IS NULL)
OFFSET 0);

-- Only DTO-supported content enters pagination; tip delivery belongs to #4.
-- The separate legacy contract below retains its historical set of kinds.
CREATE OR REPLACE VIEW social_shared_api.k_mentions AS
SELECT m.* FROM public.k_mentions m WHERE
(m.content_type IN('post','reply','quote','poll') AND EXISTS(
 SELECT 1 FROM social_shared_api.k_contents p
 JOIN social_shared_api.current_actions a ON a.transaction_id=p.transaction_id
 WHERE p.transaction_id=m.content_id AND (
  EXISTS(SELECT 1 FROM jsonb_array_elements(COALESCE(a.snapshot->'k_mentions','[]'::jsonb)) original
         WHERE original->>'mentioned_pubkey'='\x'||encode(m.mentioned_pubkey,'hex'))
  OR EXISTS(SELECT 1 FROM social_shared_api.current_edits e
            JOIN social_shared.edit_mentions supplier ON supplier.transaction_id=e.transaction_id
            WHERE e.target_signature=p.sender_signature AND supplier.mentioned_pubkey=m.mentioned_pubkey)
 )))
OR (m.content_type='vote' AND EXISTS(SELECT 1 FROM social_shared_api.k_votes vote WHERE vote.transaction_id=m.content_id));

CREATE OR REPLACE VIEW social_shared_api.k_hashtags AS
SELECT h.* FROM public.k_hashtags h
WHERE EXISTS(SELECT 1 FROM social_shared_api.k_contents content WHERE content.transaction_id=h.content_id OFFSET 0);
CREATE OR REPLACE VIEW social_shared_api.k_vars AS SELECT * FROM public.k_vars;

-- Preserve the existing K-only read contract separately from mixed reads.
-- This is legacy dataset membership, NOT proof of a retained row's wire root.
-- The mixed API must continue to report that root as unknown without evidence.
CREATE SCHEMA IF NOT EXISTS social_shared_legacy_api;
CREATE OR REPLACE FUNCTION social_shared_legacy_api.is_k_member(bytea)
RETURNS boolean LANGUAGE sql STABLE AS $legacy_k_member$
SELECT EXISTS (
 SELECT 1 FROM social_shared_api.current_actions a
 WHERE a.transaction_id=$1 AND (
  (a.provenance='canonical' AND lower(left(a.source_payload,8))='6b3a313a')
  OR (a.provenance='retained_native' AND EXISTS (
   SELECT 1 FROM social_shared.retained_imports i
   JOIN social_shared.config s ON s.singleton AND s.network=i.network
   WHERE i.import_id=a.retained_import_id AND i.protocol_hint='k'
  ))
 )
)
$legacy_k_member$;
CREATE OR REPLACE VIEW social_shared_legacy_api.k_contents AS
SELECT p.* FROM social_shared_api.k_contents p
WHERE p.content_type IN ('post','reply','quote')
AND social_shared_legacy_api.is_k_member(p.transaction_id);
CREATE OR REPLACE VIEW social_shared_legacy_api.k_votes AS
SELECT v.* FROM social_shared_api.k_votes v
WHERE social_shared_legacy_api.is_k_member(v.transaction_id);
-- Identity and privacy remain author-wide across the unified product.
CREATE OR REPLACE VIEW social_shared_legacy_api.k_broadcasts AS
SELECT * FROM social_shared_api.k_broadcasts;
CREATE OR REPLACE VIEW social_shared_legacy_api.k_follows AS
SELECT * FROM social_shared_api.k_follows;
CREATE OR REPLACE VIEW social_shared_legacy_api.k_blocks AS
SELECT * FROM social_shared_api.k_blocks;
CREATE OR REPLACE VIEW social_shared_legacy_api.k_mentions AS
SELECT m.* FROM social_shared_api.k_mentions m
WHERE m.content_type IN ('post','reply','quote','vote')
AND social_shared_legacy_api.is_k_member(m.content_id);
CREATE OR REPLACE VIEW social_shared_legacy_api.k_hashtags AS
SELECT h.* FROM social_shared_api.k_hashtags h
WHERE EXISTS (SELECT 1 FROM social_shared_legacy_api.k_contents p WHERE p.transaction_id=h.content_id);
CREATE OR REPLACE VIEW social_shared_legacy_api.k_vars AS
SELECT * FROM social_shared_api.k_vars;
