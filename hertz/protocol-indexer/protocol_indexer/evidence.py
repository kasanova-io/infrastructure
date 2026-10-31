"""Durable output-proof acquisition from the shared journal, without a scanner."""

from .chats import EvidencePending
from .archive import HEX64
from .routing import route
from psycopg.pq import TransactionStatus

CONSUMER = 'output-evidence-v1'


class EvidenceConsumer:
    def __init__(self, network, source_id, resolve, resolve_sender=None):
        self.network, self.source_id, self.resolve = network, source_id, resolve
        self.resolve_sender = resolve_sender

    def admitted(self, conn, row):
        ready = conn.execute("SELECT to_regclass('social_shared.config') IS NOT NULL AND to_regclass('social_shared.actions') IS NOT NULL AS ready").fetchone()['ready']
        if not ready:
            raise EvidencePending('native_projection_not_ready')
        config = conn.execute('SELECT network,source_id FROM social_shared.config WHERE singleton').fetchone()
        if not config or config['network'] != self.network or config['source_id'] != self.source_id:
            raise ValueError('output evidence native source/network mismatch')
        native = conn.execute("SELECT last_error FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1' AND transaction_id=%s", (row['transaction_id'],)).fetchone()
        if native and isinstance(native['last_error'], str):
            parts = native['last_error'].split(':')
            try:
                classified = route(bytes.fromhex(row['source_transaction']['payload'][2:]))
            except (ValueError, TypeError, KeyError):
                classified = None
            expected = None
            if classified and classified.version == 1:
                if classified.root == 'kchat' and classified.action == 'tip':
                    expected = 'verified_tip_output_pending'
                elif classified.root in {'kchat', 'ciph_msg'} and classified.action == 'bcast':
                    expected = 'parsed_room_sender_pending'
            if (expected and len(parts) in (2, 3) and parts[:2] == [expected, str(row['revision'])]
                    and (len(parts) == 2 or HEX64.fullmatch(parts[2]))):
                return ('room' if expected == 'parsed_room_sender_pending' else 'tip',
                        parts[2] if len(parts) == 3 else None)
        if native is None and conn.execute('SELECT 1 FROM social_shared.actions WHERE transaction_id=%s AND revision=%s',
                                           (row['transaction_id'], row['revision'])).fetchone():
            # Exact native terminal decision: invalid signature, invalid
            # target/author, or already materialized. None needs archive I/O.
            return False
        raise EvidencePending('native_signature_or_target_pending')

    def run_one(self, conn):
        if not conn.autocommit or conn.info.transaction_status != TransactionStatus.IDLE:
            raise ValueError('output evidence requires an idle autocommit connection')
        wake = None
        with conn.transaction():
            config = conn.execute('SELECT network,journal_id::text AS source_id FROM protocol_indexer.config').fetchone()
            if not config or config['network'] != self.network or config['source_id'] != self.source_id:
                raise ValueError('output evidence journal network/identity mismatch')
            job = conn.execute('''SELECT * FROM protocol_indexer.consumer_jobs
                WHERE consumer=%s AND available_at<=clock_timestamp()
                ORDER BY available_at,transaction_id LIMIT 1 FOR UPDATE SKIP LOCKED''', (CONSUMER,)).fetchone()
            if job is None:
                return None
            row = conn.execute('SELECT * FROM protocol_indexer.canonical WHERE transaction_id=%s',
                               (job['transaction_id'],)).fetchone()
            try:
                settled = False
                if row['accepting_block'] is not None:
                    admission = self.admitted(conn, row)
                    if admission:
                        kind, fingerprint = admission
                        resolver = self.resolve_sender if kind == 'room' else self.resolve
                        if resolver is None:
                            raise EvidencePending('room_sender_resolver_not_configured')
                        if fingerprint is None:
                            resolver(conn, row)
                        else:
                            resolver(conn, row, fingerprint)
                        wake = job['transaction_id']
                    else:
                        settled = True
            except EvidencePending:
                conn.execute('''UPDATE protocol_indexer.consumer_jobs
                    SET available_at=clock_timestamp()+interval '5 seconds',last_error='EvidencePending'
                    WHERE consumer=%s AND transaction_id=%s''', (CONSUMER, job['transaction_id']))
                return 'deferred'
            # No canonical lock during archive I/O. Producers never update an
            # existing consumer job, so their revision can advance freely.
            latest = conn.execute('SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=%s FOR UPDATE',
                                  (job['transaction_id'],)).fetchone()
            if latest['revision'] == row['revision']:
                conn.execute('DELETE FROM protocol_indexer.consumer_jobs WHERE consumer=%s AND transaction_id=%s',
                             (CONSUMER, job['transaction_id']))
                result = 'stored' if wake else 'settled' if settled else 'inactive'
            else:
                conn.execute('''UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp(),last_error=NULL
                    WHERE consumer=%s AND transaction_id=%s''', (CONSUMER, job['transaction_id']))
                result = 'changed'
        # Native locks its own job before canonical. Wake only AFTER committing
        # the proof/canonical fence, avoiding the reverse lock order. A crash
        # between commit and wake only leaves the native job's bounded retry.
        if wake:
            conn.execute('''UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp(),last_error=NULL
                WHERE consumer='native-social-v1' AND transaction_id=%s''', (wake,))
        return result
