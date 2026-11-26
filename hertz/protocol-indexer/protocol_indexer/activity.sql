-- Ordinary Address Activity shares the raw collector, not the protocol parser.
-- State is bounded by raw retention; acceptance-before-body wakeups have no FK.
CREATE TABLE IF NOT EXISTS protocol_indexer.activity_transactions (
    transaction_id bytea PRIMARY KEY REFERENCES public.transactions ON DELETE CASCADE,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    handled boolean NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS protocol_indexer.activity_pending (
    transaction_id bytea PRIMARY KEY CHECK (octet_length(transaction_id)=32),
    queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS activity_pending_ready ON protocol_indexer.activity_pending(available_at,transaction_id);

CREATE OR REPLACE FUNCTION protocol_indexer.queue_activity()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE txid bytea;
BEGIN
    IF TG_TABLE_NAME='transactions' THEN
        txid:=NEW.transaction_id;
        INSERT INTO protocol_indexer.activity_transactions(transaction_id) VALUES(txid)
        ON CONFLICT DO NOTHING;
    ELSE
        IF TG_OP='DELETE' THEN txid:=OLD.transaction_id; ELSE txid:=NEW.transaction_id; END IF;
        IF current_setting('protocol_indexer.archive_seed',true)=encode(txid,'hex')
            OR (TG_OP='DELETE' AND current_setting('protocol_indexer.activity_pruning',true)='true') THEN
            RETURN NULL;
        END IF;
    END IF;
    INSERT INTO protocol_indexer.activity_pending(transaction_id) VALUES(txid)
    ON CONFLICT(transaction_id) DO UPDATE SET available_at=clock_timestamp();
    -- queued_at and observed_at stay fixed across revision/acceptance changes.
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS protocol_activity_body ON public.transactions;
CREATE TRIGGER protocol_activity_body AFTER INSERT OR UPDATE ON public.transactions
FOR EACH ROW EXECUTE FUNCTION protocol_indexer.queue_activity();
DROP TRIGGER IF EXISTS protocol_activity_acceptance ON public.transactions_acceptances;
CREATE TRIGGER protocol_activity_acceptance AFTER INSERT OR UPDATE OR DELETE ON public.transactions_acceptances
FOR EACH ROW EXECUTE FUNCTION protocol_indexer.queue_activity();
