-- One-time legacy -> shared Chats handoff, never an ordinary restart step.
-- Caller must fence the legacy writer, prove the current original store has
-- neither shared ledger nor binding partitions, and hash that proof. A stale
-- backup or zero SQL receipts alone does not establish first activation.
-- Supply psql variables network, journal_id, expected_epoch_ms,
-- store_proof_sha256. Run with ON_ERROR_STOP=1 using the owning source account.
BEGIN;
SET LOCAL statement_timeout='5s';
SET LOCAL lock_timeout='1s';
-- A reconciler can acquire this same transaction lock before freshly reading
-- the audit/epoch after a lost client response. Never infer COMMIT from stdout.
DO $$ BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('kasanova:first-native-chat-activation-v1',0));
END $$;
CREATE TEMP TABLE first_shared_activation_input ON COMMIT DROP AS
SELECT :'network'::text AS network, :'journal_id'::uuid AS journal_id,
       :'expected_epoch_ms'::bigint AS expected_epoch_ms,
       :'store_proof_sha256'::text AS store_proof_sha256;

-- This audit is independent of delivery receipts, which may legitimately be
-- empty. Its primary key makes a retry refuse to move an activated boundary.
CREATE TABLE IF NOT EXISTS protocol_indexer.native_activation_handoffs (
    consumer text PRIMARY KEY,
    network text NOT NULL,
    journal_id uuid NOT NULL,
    previous_epoch_ms bigint NOT NULL,
    activated_at_ms bigint NOT NULL,
    store_proof_sha256 text NOT NULL CHECK (store_proof_sha256 ~ '^[0-9a-f]{64}$')
);
LOCK TABLE protocol_indexer.notification_consumers IN SHARE ROW EXCLUSIVE MODE;
DO $$
DECLARE input record; old_epoch bigint; new_epoch bigint;
BEGIN
    SELECT * INTO STRICT input FROM first_shared_activation_input;
    IF input.store_proof_sha256 !~ '^[0-9a-f]{64}$'
       OR NOT EXISTS (SELECT 1 FROM protocol_indexer.config
                      WHERE network=input.network AND journal_id=input.journal_id)
       OR EXISTS (SELECT 1 FROM protocol_indexer.native_activation_handoffs
                  WHERE consumer='native-chat-push-v1')
       OR EXISTS (SELECT 1 FROM protocol_indexer.notification_consumers
                  WHERE consumer='native-address-push-v1')
       OR EXISTS (SELECT 1 FROM protocol_indexer.deliveries
                  WHERE consumer='native-chat-push-v1') THEN
        RAISE EXCEPTION 'First shared Chats activation preconditions failed';
    END IF;
    SELECT activated_at_ms INTO STRICT old_epoch
    FROM protocol_indexer.notification_consumers
    WHERE consumer='native-chat-push-v1' FOR UPDATE;
    IF old_epoch IS DISTINCT FROM input.expected_epoch_ms THEN
        RAISE EXCEPTION 'First shared Chats activation epoch changed';
    END IF;
    new_epoch := (extract(epoch FROM clock_timestamp())*1000)::bigint;
    IF new_epoch <= old_epoch THEN
        RAISE EXCEPTION 'First shared Chats activation must advance';
    END IF;
    INSERT INTO protocol_indexer.native_activation_handoffs
    VALUES ('native-chat-push-v1',input.network,input.journal_id,
            old_epoch,new_epoch,input.store_proof_sha256);
    UPDATE protocol_indexer.notification_consumers SET activated_at_ms=new_epoch
    WHERE consumer='native-chat-push-v1';
END $$;
SELECT row_to_json(t) FROM protocol_indexer.native_activation_handoffs t
WHERE consumer='native-chat-push-v1';
COMMIT;
