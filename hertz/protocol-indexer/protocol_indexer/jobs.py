"""Claim existing consumer work without putting live traffic behind backfill."""


def claim_ready(conn, consumer):
    # Priority changes scheduling only. The owning consumer still checks the
    # current source, proof, native admission and revision before acknowledging.
    # Revocations must also reach native storage promptly, even for old history.
    # Lock only the job: holding canonical across archive/native HTTP would block
    # live capture and reverse the established acknowledgement lock order.
    return conn.execute("""SELECT j.* FROM protocol_indexer.consumer_jobs j
        JOIN protocol_indexer.canonical c USING(transaction_id)
        CROSS JOIN protocol_indexer.config cfg
        WHERE j.consumer=%s AND j.available_at<=clock_timestamp()
        ORDER BY ((c.accepting_block IS NULL AND c.ever_accepted)
                  OR COALESCE((c.source_transaction->>'block_time')::bigint >= cfg.activated_at_ms,false)) DESC,
                 j.available_at,j.transaction_id
        LIMIT 1 FOR UPDATE OF j SKIP LOCKED""", (consumer,)).fetchone()
