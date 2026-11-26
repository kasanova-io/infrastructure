-- The common journal owns this fence for every source INSERT, including archive
-- import and ordinary SQL upserts. Native retained-target checks use the same
-- transaction key before testing absence. Existing-row UPDATE/revocation is
-- already protected by the native canonical row fence. INSERT-only avoids
-- taking an advisory lock after an UPDATE tuple lock (the reverse of UPSERT).
-- No scanner or event policy changes.
LOCK TABLE protocol_indexer.canonical IN SHARE ROW EXCLUSIVE MODE;
CREATE OR REPLACE FUNCTION protocol_indexer.fence_source_identity()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog
AS $source_fence$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(encode(NEW.transaction_id,'hex'),736125));
    RETURN NEW;
END;
$source_fence$;
DROP TRIGGER IF EXISTS protocol_source_identity_fence ON protocol_indexer.canonical;
CREATE TRIGGER protocol_source_identity_fence
BEFORE INSERT ON protocol_indexer.canonical
FOR EACH ROW EXECUTE FUNCTION protocol_indexer.fence_source_identity();
