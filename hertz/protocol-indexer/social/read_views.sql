-- Existing native table shapes, filtered by the same native subject policy.
-- Installed only by the source-bound projection, never by an API read.
CREATE SCHEMA IF NOT EXISTS social_shared_api;

CREATE OR REPLACE FUNCTION social_shared_api.selected_k_contents(subject text)
RETURNS bytea LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp
AS $selection_k_contents$
SELECT picked.transaction_id FROM (SELECT a.transaction_id,a.source_payload,a.original_time,a.notification_consumed,c.revision,a.snapshot::text AS snapshot FROM social_shared.actions a JOIN protocol_indexer.canonical c USING(transaction_id) WHERE a.subject=$1 AND c.accepting_block IS NOT NULL AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) AND NOT EXISTS(SELECT 1 FROM social_shared.extensions e WHERE e.transaction_id=a.transaction_id AND e.kind='poll' AND NOT e.shape_valid) ORDER BY EXISTS(SELECT 1 FROM k_contents native WHERE native.transaction_id=a.transaction_id) DESC,a.original_time,a.transaction_id LIMIT 1) picked
$selection_k_contents$;

CREATE OR REPLACE FUNCTION social_shared_api.selected_k_votes(subject text)
RETURNS bytea LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp
AS $selection_k_votes$
SELECT picked.transaction_id FROM (SELECT a.transaction_id,a.source_payload,a.original_time,a.notification_consumed,c.revision,a.snapshot::text AS snapshot FROM social_shared.actions a JOIN protocol_indexer.canonical c USING(transaction_id) WHERE a.subject=$1 AND c.accepting_block IS NOT NULL AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) ORDER BY EXISTS(SELECT 1 FROM k_votes native WHERE native.transaction_id=a.transaction_id) DESC,a.original_time,a.transaction_id LIMIT 1) picked
$selection_k_votes$;

CREATE OR REPLACE FUNCTION social_shared_api.selected_k_broadcasts(subject text)
RETURNS bytea LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp
AS $selection_k_broadcasts$
SELECT picked.transaction_id FROM (SELECT a.transaction_id,a.source_payload,a.original_time,a.notification_consumed,c.revision,a.snapshot::text AS snapshot FROM social_shared.actions a JOIN protocol_indexer.canonical c USING(transaction_id) WHERE a.subject=$1 AND c.accepting_block IS NOT NULL AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) ORDER BY a.original_time DESC,a.transaction_id DESC LIMIT 1) picked
$selection_k_broadcasts$;

CREATE OR REPLACE FUNCTION social_shared_api.selected_k_follows(subject text)
RETURNS bytea LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp
AS $selection_k_follows$
SELECT picked.transaction_id FROM (WITH reversal AS (SELECT a.original_time,a.transaction_id FROM social_shared.actions a JOIN protocol_indexer.canonical c USING(transaction_id) WHERE a.subject=$1 AND c.accepting_block IS NOT NULL AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) AND a.state_action='deactivate' ORDER BY a.original_time DESC,a.transaction_id DESC LIMIT 1) SELECT a.transaction_id,a.source_payload,a.original_time,a.notification_consumed,c.revision,a.snapshot::text AS snapshot FROM social_shared.actions a JOIN protocol_indexer.canonical c USING(transaction_id) WHERE a.subject=$1 AND c.accepting_block IS NOT NULL AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) AND a.state_action='activate' AND (NOT EXISTS(SELECT 1 FROM reversal) OR (a.original_time,a.transaction_id)>(SELECT original_time,transaction_id FROM reversal)) ORDER BY a.original_time,a.transaction_id LIMIT 1) picked
$selection_k_follows$;

CREATE OR REPLACE FUNCTION social_shared_api.selected_k_blocks(subject text)
RETURNS bytea LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp
AS $selection_k_blocks$
SELECT picked.transaction_id FROM (WITH reversal AS (SELECT a.original_time,a.transaction_id FROM social_shared.actions a JOIN protocol_indexer.canonical c USING(transaction_id) WHERE a.subject=$1 AND c.accepting_block IS NOT NULL AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) AND a.state_action='deactivate' ORDER BY a.original_time DESC,a.transaction_id DESC LIMIT 1) SELECT a.transaction_id,a.source_payload,a.original_time,a.notification_consumed,c.revision,a.snapshot::text AS snapshot FROM social_shared.actions a JOIN protocol_indexer.canonical c USING(transaction_id) WHERE a.subject=$1 AND c.accepting_block IS NOT NULL AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) AND a.state_action='activate' AND (NOT EXISTS(SELECT 1 FROM reversal) OR (a.original_time,a.transaction_id)>(SELECT original_time,transaction_id FROM reversal)) ORDER BY a.original_time,a.transaction_id LIMIT 1) picked
$selection_k_blocks$;

CREATE OR REPLACE VIEW social_shared_api.current_actions AS
SELECT a.* FROM social_shared.actions a
JOIN protocol_indexer.canonical c USING(transaction_id)
WHERE a.status='verified' AND a.revision=c.revision AND c.accepting_block IS NOT NULL
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL);

-- Authorization is shared verbatim with the native edit selector. This view
-- retains ALL current suppliers: edit-derived mentions are cumulative upstream.
CREATE OR REPLACE VIEW social_shared_api.current_edits AS
SELECT e.* FROM social_shared.extensions e
JOIN protocol_indexer.canonical c USING(transaction_id)
JOIN protocol_indexer.canonical t ON t.transaction_id=e.target
JOIN social_shared_api.current_actions a ON a.transaction_id=e.transaction_id
JOIN social_shared_api.current_actions target_action ON target_action.transaction_id=e.target
WHERE e.revision=c.revision AND __AUTHORIZED_EDIT__;

CREATE OR REPLACE VIEW social_shared_api.k_contents AS
SELECT p.* FROM public.k_contents p
WHERE EXISTS (
SELECT 1 FROM social_shared_api.current_actions a
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
-- Keep the native ordered/indexed relation as the outer pagination scan.
-- OFFSET0 prevents flattening this per-row eligibility check into a ledger-first
-- join/sort of the entire history; it does not change result membership.
OFFSET 0
);

CREATE OR REPLACE VIEW social_shared_api.k_broadcasts AS
SELECT native.* FROM public.k_broadcasts native
WHERE EXISTS (SELECT 1 FROM social_shared_api.current_actions a
 WHERE a.transaction_id=native.transaction_id
 AND native.transaction_id=social_shared_api.selected_k_broadcasts(a.subject)
 OFFSET 0);
CREATE OR REPLACE VIEW social_shared_api.k_follows AS
SELECT native.* FROM public.k_follows native
WHERE EXISTS (SELECT 1 FROM social_shared_api.current_actions a
 WHERE a.transaction_id=native.transaction_id
 AND native.transaction_id=social_shared_api.selected_k_follows(a.subject)
 OFFSET 0);
CREATE OR REPLACE VIEW social_shared_api.k_blocks AS
SELECT native.* FROM public.k_blocks native
WHERE EXISTS (SELECT 1 FROM social_shared_api.current_actions a
 WHERE a.transaction_id=native.transaction_id
 AND native.transaction_id=social_shared_api.selected_k_blocks(a.subject)
 OFFSET 0);

-- Preserve the signed literal target. Unknown targets keep upstream vote lookup
-- semantics; a KNOWN revoked/pending-removal target cannot be rescued by an alias.
CREATE OR REPLACE VIEW social_shared_api.k_votes AS
SELECT native.* FROM public.k_votes native
WHERE EXISTS (SELECT 1 FROM social_shared_api.current_actions a
LEFT JOIN social_shared.ordinary_vote_targets dependency USING(transaction_id)
WHERE a.transaction_id=native.transaction_id
AND native.transaction_id=social_shared_api.selected_k_votes(a.subject)
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.canonical target
 WHERE target.transaction_id=COALESCE(dependency.post_id,native.post_id) AND target.accepting_block IS NULL)
AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending pending
 WHERE pending.transaction_id=COALESCE(dependency.post_id,native.post_id)
 AND pending.acceptance_changed AND pending.accepting_block IS NULL)
OFFSET 0);

-- This shadows the LEGACY reader's mention table. Keep only kinds its DTO
-- dispatch supports before SQL pagination; tip DTO delivery belongs to #4.
CREATE OR REPLACE VIEW social_shared_api.k_mentions AS
SELECT m.* FROM public.k_mentions m WHERE
(m.content_type IN('post','reply','quote') AND EXISTS(
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
