-- Additive to Simply Kaspa schema 23. No upstream tables or cursors are replaced.
CREATE SCHEMA IF NOT EXISTS protocol_indexer;

CREATE TABLE IF NOT EXISTS protocol_indexer.config (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    network text NOT NULL,
    source_revision text NOT NULL,
    journal_id uuid NOT NULL DEFAULT gen_random_uuid(),
    activated_at_ms bigint NOT NULL
);
-- Process/container restarts retain the epoch; recreating a journal resets it.
ALTER TABLE protocol_indexer.config ADD COLUMN IF NOT EXISTS journal_id uuid NOT NULL DEFAULT gen_random_uuid();

CREATE TABLE IF NOT EXISTS protocol_indexer.pending (
    transaction_id bytea PRIMARY KEY CHECK (octet_length(transaction_id) = 32),
    acceptance_changed boolean NOT NULL DEFAULT false,
    accepting_block bytea,
    queued_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS protocol_indexer.canonical (
    transaction_id bytea PRIMARY KEY CHECK (octet_length(transaction_id) = 32),
    revision bigint NOT NULL,
    family text,
    source_transaction jsonb NOT NULL,
    accepting_block bytea,
    ever_accepted boolean NOT NULL,
    first_observed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS canonical_family_id ON protocol_indexer.canonical (family, transaction_id);

-- Archive sentinels must yield to the first genuine live INSERT, including a
-- concurrent writer whose command snapshot predates the sentinel's commit.
-- Keep provenance on the locked conflicting tuple, not in a side-table lookup.
ALTER TABLE public.transactions_acceptances ADD COLUMN IF NOT EXISTS is_archive_seed boolean NOT NULL DEFAULT false;

CREATE OR REPLACE FUNCTION protocol_indexer.insert_source_acceptances(transactions bytea[],blocks bytea[])
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE inserted bigint;
BEGIN
    IF cardinality(transactions) IS DISTINCT FROM cardinality(blocks) THEN
        RAISE EXCEPTION 'acceptance arrays differ';
    END IF;
    INSERT INTO public.transactions_acceptances(transaction_id,block_hash,is_archive_seed)
    SELECT DISTINCT ON (transaction_id) transaction_id,block_hash,false
    FROM unnest(transactions,blocks) WITH ORDINALITY AS values_(transaction_id,block_hash,ordinal)
    ORDER BY transaction_id,ordinal
    ON CONFLICT (transaction_id) DO UPDATE SET block_hash=EXCLUDED.block_hash,is_archive_seed=false
    WHERE public.transactions_acceptances.is_archive_seed;
    GET DIAGNOSTICS inserted = ROW_COUNT;
    RETURN inserted;
END;
$$;

-- A removal must survive even when the affected transaction is not discovered
-- until a later archive backfill. Importers and the live source lock this same
-- block row before touching acceptance rows, serializing that boundary.
CREATE TABLE IF NOT EXISTS protocol_indexer.acceptance_blocks (
    block_hash bytea PRIMARY KEY CHECK (octet_length(block_hash)=32),
    last_removed_at timestamptz
);

CREATE OR REPLACE FUNCTION protocol_indexer.remove_source_acceptances(blocks bytea[])
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE block bytea; removed bigint;
BEGIN
    FOR block IN SELECT DISTINCT unnest(blocks) ORDER BY 1 LOOP
        INSERT INTO protocol_indexer.acceptance_blocks(block_hash,last_removed_at)
        VALUES (block,clock_timestamp()) ON CONFLICT (block_hash)
        DO UPDATE SET last_removed_at=EXCLUDED.last_removed_at;
    END LOOP;
    DELETE FROM public.transactions_acceptances WHERE block_hash=ANY(blocks);
    GET DIAGNOSTICS removed = ROW_COUNT;
    RETURN removed;
END;
$$;

-- One durable wakeup per transaction. Producers never update/lock an existing
-- job; the consumer fences its acknowledgement against the canonical revision.
CREATE TABLE IF NOT EXISTS protocol_indexer.consumer_jobs (
    consumer text NOT NULL,
    transaction_id bytea NOT NULL REFERENCES protocol_indexer.canonical,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_error text,
    PRIMARY KEY (consumer, transaction_id)
);
CREATE INDEX IF NOT EXISTS consumer_jobs_ready ON protocol_indexer.consumer_jobs (consumer, available_at);

-- The file cursor acknowledges durable staging, never a transient HTTP result.
-- Deferred history survives process exit and a caller advancing to the next page.
CREATE TABLE IF NOT EXISTS protocol_indexer.archive_jobs (
    batch_sha256 text NOT NULL CHECK (batch_sha256 ~ '^[0-9a-f]{64}$'),
    line_number bigint NOT NULL CHECK (line_number>0),
    receipt jsonb NOT NULL,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_error text,
    PRIMARY KEY(batch_sha256,line_number)
);
CREATE INDEX IF NOT EXISTS archive_jobs_ready ON protocol_indexer.archive_jobs(batch_sha256,available_at,line_number);

CREATE TABLE IF NOT EXISTS protocol_indexer.evidence (
    transaction_id bytea PRIMARY KEY REFERENCES protocol_indexer.canonical,
    receipt jsonb NOT NULL,
    acquired_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS protocol_indexer.events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id bytea NOT NULL REFERENCES protocol_indexer.canonical,
    revision bigint NOT NULL,
    family text,
    kind text NOT NULL CHECK (kind IN ('observed', 'accepted', 'updated', 'revoked')),
    source_transaction jsonb NOT NULL,
    accepting_block bytea,
    notify_allowed boolean NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (transaction_id, revision)
);
CREATE INDEX IF NOT EXISTS events_family_id ON protocol_indexer.events (family, event_id);

-- Acknowledgements, not a MAX(event_id) cursor: sequence allocation is not commit
-- order. A slow transaction committing behind a fast one must remain deliverable.
CREATE TABLE IF NOT EXISTS protocol_indexer.deliveries (
    consumer text NOT NULL,
    event_id bigint NOT NULL REFERENCES protocol_indexer.events,
    PRIMARY KEY (consumer, event_id)
);

-- Poll only outstanding work, rather than repeatedly anti-joining a family's
-- entire acknowledged history. Inserted atomically with each new event.
CREATE TABLE IF NOT EXISTS protocol_indexer.delivery_pending (
    consumer text NOT NULL,
    event_id bigint NOT NULL REFERENCES protocol_indexer.events,
    transaction_id bytea NOT NULL,
    revision bigint NOT NULL,
    PRIMARY KEY (consumer, event_id),
    UNIQUE (consumer, transaction_id, revision)
);

-- Adapter output is an ingestion inbox, not an authenticated Social/Chat model.
-- Existing protocol consumers must validate signatures, recipients and payments.
CREATE TABLE IF NOT EXISTS protocol_indexer.records (
    family text NOT NULL,
    transaction_id bytea NOT NULL,
    revision bigint NOT NULL,
    root text NOT NULL,
    version integer,
    action text NOT NULL,
    tail bytea NOT NULL,
    source_transaction jsonb NOT NULL,
    accepting_block bytea,
    mode text NOT NULL CHECK (mode IN ('scan', 'backfill')),
    PRIMARY KEY (family, transaction_id)
);

CREATE OR REPLACE FUNCTION protocol_indexer.is_candidate(payload bytea)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT COALESCE(substring(payload FROM 1 FOR 6) = convert_to('kchat:', 'UTF8')
        OR substring(payload FROM 1 FOR 9) = convert_to('ciph_msg:', 'UTF8')
        OR substring(payload FROM 1 FOR 2) = convert_to('k:', 'UTF8'), false);
$$;

CREATE OR REPLACE FUNCTION protocol_indexer.queue_transaction()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF protocol_indexer.is_candidate(NEW.payload) THEN
        INSERT INTO protocol_indexer.pending (transaction_id) VALUES (NEW.transaction_id)
        ON CONFLICT (transaction_id) DO UPDATE SET queued_at = clock_timestamp();
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION protocol_indexer.queue_acceptance()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE txid bytea; accepting bytea;
BEGIN
    IF TG_OP = 'DELETE' THEN
        txid := OLD.transaction_id; accepting := NULL;
    ELSE
        txid := NEW.transaction_id; accepting := NEW.block_hash;
    END IF;
    -- Only an importer's own acceptance sentinel is quiet. Other transactions
    -- and every independent live-source connection retain normal wakeups.
    IF current_setting('protocol_indexer.archive_seed',true)=encode(txid,'hex') THEN
        RETURN NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM public.transactions t WHERE t.transaction_id = txid
               AND protocol_indexer.is_candidate(t.payload))
       -- A concurrent body INSERT may not be visible yet. Queue absent bodies
       -- too; otherwise its queue can be acknowledged before this acceptance
       -- commits, permanently losing the accepted transition.
       OR NOT EXISTS (SELECT 1 FROM public.transactions t WHERE t.transaction_id = txid)
       OR EXISTS (SELECT 1 FROM protocol_indexer.canonical c WHERE c.transaction_id = txid)
       OR EXISTS (SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id = txid) THEN
        INSERT INTO protocol_indexer.pending (transaction_id, acceptance_changed, accepting_block)
        VALUES (txid, true, accepting)
        ON CONFLICT (transaction_id) DO UPDATE
            SET acceptance_changed = true, accepting_block = EXCLUDED.accepting_block,
                queued_at = clock_timestamp();
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS protocol_queue_transaction ON public.transactions;
CREATE TRIGGER protocol_queue_transaction AFTER INSERT OR UPDATE ON public.transactions
FOR EACH ROW EXECUTE FUNCTION protocol_indexer.queue_transaction();
DROP TRIGGER IF EXISTS protocol_queue_acceptance ON public.transactions_acceptances;
CREATE TRIGGER protocol_queue_acceptance AFTER INSERT OR UPDATE OR DELETE ON public.transactions_acceptances
FOR EACH ROW EXECUTE FUNCTION protocol_indexer.queue_acceptance();
