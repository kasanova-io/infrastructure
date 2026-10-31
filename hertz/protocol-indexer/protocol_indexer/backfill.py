"""Quiet archival body import into the same live journal, with a VCP block fence.

The caller must verify the hooked source binding and fetch current acceptance
from the configured archive, never a cached accepted flag. A trusted archive is
still the authority for pre-activation history; this is not consensus validation.
"""
from dataclasses import dataclass
from datetime import timedelta

from psycopg.types.json import Jsonb

from .archive import checked_receipt, integer, raw_hex, require
from .chats import EvidencePending
from .journal import write_revision
from .routing import route
from .source import SIMPLY_REVISION


@dataclass(frozen=True)
class AcceptanceObservation:
    network: str
    transaction_id: str
    payload: str
    accepting_block: str | None

    def checked(self, receipt, network):
        require(self.network == network and self.transaction_id == receipt['transaction_id']
                and raw_hex(self.payload).hex() == receipt['payload'], 'current archive observation identity mismatch')
        if self.accepting_block is not None:
            value = raw_hex(self.accepting_block)
            require(len(value) == 32, 'current accepting block must be a full hash')
            return value
        return None


def source_body(receipt):
    raw = receipt['rpc_transaction']
    def bytea(value): return '\\x' + raw_hex(value).hex()
    inputs = []
    for index, item in enumerate(raw['inputs']):
        prior = item['previousOutpoint']
        inputs.append({'index': index, 'previous_outpoint_hash': bytea(prior['transactionId']),
            'previous_outpoint_index': integer(prior['index']), 'signature_script': bytea(item['signatureScript']),
            'sig_op_count': item.get('sigOpCount'), 'previous_outpoint_script': None,
            'previous_outpoint_amount': None, 'compute_budget': None, 'covenant_id': None})
    outputs = []
    for index, item in enumerate(raw['outputs']):
        address = (item.get('verboseData') or {}).get('scriptPublicKeyAddress')
        outputs.append({'index': index, 'amount': integer(item['amount']),
            'script_public_key': bytea(item['scriptPublicKey']['scriptPublicKey']),
            'script_public_key_address': address.split(':', 1)[1] if address else None,
            'covenant_authorizing_input': None, 'covenant_id': None})
    return {'transaction_id': bytea(receipt['transaction_id']), 'subnetwork_id': None, 'hash': None, 'mass': None,
            'payload': bytea(receipt['payload']), 'block_hash': bytea(receipt['containing_block']),
            'block_time': integer(receipt['block_time']), 'version': integer(raw.get('version', 0), 65535),
            'inputs': inputs, 'outputs': outputs}


def seed(conn, txid, statement, params):
    """Suppress only this transaction's own sentinel trigger, transaction-locally."""
    previous = conn.execute("SELECT current_setting('protocol_indexer.archive_seed',true) AS value").fetchone()['value']
    conn.execute("SELECT set_config('protocol_indexer.archive_seed',%s,true)", (txid.hex(),))
    result = conn.execute(statement, params)
    conn.execute("SELECT set_config('protocol_indexer.archive_seed',%s,true)", (previous or '',))
    return result


def check_context(conn, network):
    config = conn.execute('SELECT network,source_revision FROM protocol_indexer.config').fetchone()
    require(config is not None and config['network'] == network and config['source_revision'] == SIMPLY_REVISION,
            'journal network/source mismatch')


def import_receipt(conn, receipt, network, observe):
    receipt = checked_receipt(receipt, receipt['transaction_id'], network)
    require(route(bytes.fromhex(receipt['payload'])) is not None, 'unsupported historical protocol')
    body = source_body(receipt)
    txid = bytes.fromhex(receipt['transaction_id'])
    hint = observe().checked(receipt, network)
    with conn.transaction():
        check_context(conn, network)
        removed = False
        if hint is not None:
            conn.execute('INSERT INTO protocol_indexer.acceptance_blocks(block_hash) VALUES (%s) ON CONFLICT DO NOTHING', (hint,))
            guard = conn.execute('SELECT last_removed_at FROM protocol_indexer.acceptance_blocks WHERE block_hash=%s FOR UPDATE', (hint,)).fetchone()
            removed = guard['last_removed_at'] is not None
        created = False
        if hint is not None and not removed:
            created = seed(conn, txid, '''INSERT INTO public.transactions_acceptances(transaction_id,block_hash,is_archive_seed)
                VALUES (%s,%s,true) ON CONFLICT DO NOTHING RETURNING transaction_id''', (txid, hint)).fetchone() is not None
        acceptance = conn.execute('SELECT block_hash FROM public.transactions_acceptances WHERE transaction_id=%s FOR UPDATE', (txid,)).fetchone()
        conn.execute('INSERT INTO protocol_indexer.pending(transaction_id) VALUES (%s) ON CONFLICT DO NOTHING', (txid,))
        pending = conn.execute('SELECT * FROM protocol_indexer.pending WHERE transaction_id=%s FOR UPDATE', (txid,)).fetchone()
        old = conn.execute('SELECT * FROM protocol_indexer.canonical WHERE transaction_id=%s', (txid,)).fetchone()
        source = conn.execute('SELECT payload FROM public.transactions WHERE transaction_id=%s', (txid,)).fetchone()

        def unseed():
            if created:
                seed(conn, txid, 'DELETE FROM public.transactions_acceptances WHERE transaction_id=%s', (txid,))

        if old or source:
            require((old['source_transaction']['payload'] if old else '\\x' + source['payload'].hex()) == body['payload'],
                    'archive payload conflicts with live journal')
            unseed()
            # Preserve the live wakeup; capture_one owns every existing source
            # body/canonical revision and any acceptance event arriving later.
            return 'existing' if old else 'live_pending'
        if acceptance and not created:
            block = acceptance['block_hash']
        elif pending['acceptance_changed'] and pending['accepting_block'] is not None and pending['accepting_block'] != hint:
            raise EvidencePending('live_acceptance_changed_during_archive_import')
        elif removed:
            # A trusted current VCP removal beats a lagging public archive.
            block = None
        elif pending['acceptance_changed']:
            block = pending['accepting_block']
            if block is None:
                unseed()
        else:
            # The sentinel is locked before this fresh HTTP observation. Any
            # later live VCP removal waits, then queues the next revision.
            current = observe().checked(receipt, network)
            if current != hint:
                raise EvidencePending('archive_acceptance_changed_during_import')
            block = current
        write_revision(conn, txid, body, block, None, quiet=True, historically_accepted=True)
        conn.execute('INSERT INTO protocol_indexer.evidence(transaction_id,receipt) VALUES (%s,%s)', (txid, Jsonb(receipt)))
        conn.execute('DELETE FROM protocol_indexer.pending WHERE transaction_id=%s', (txid,))
        return 'imported'


def stage_receipt(conn, batch, line, receipt, network):
    receipt = checked_receipt(receipt, receipt['transaction_id'], network)
    with conn.transaction():
        check_context(conn, network)
        conn.execute('''INSERT INTO protocol_indexer.archive_jobs(batch_sha256,line_number,receipt)
            VALUES (%s,%s,%s) ON CONFLICT DO NOTHING''', (batch, line, Jsonb(receipt)))
        saved = conn.execute('SELECT receipt FROM protocol_indexer.archive_jobs WHERE batch_sha256=%s AND line_number=%s', (batch, line)).fetchone()
        require(saved['receipt'] == receipt, 'immutable archive batch/line conflict')


def deliver_one(conn, batch, network, observe):
    with conn.transaction():
        check_context(conn, network)
        job = conn.execute('''SELECT * FROM protocol_indexer.archive_jobs
            WHERE batch_sha256=%s AND available_at<=clock_timestamp()
            ORDER BY available_at,line_number LIMIT 1 FOR UPDATE SKIP LOCKED''', (batch,)).fetchone()
        if job is None:
            return None
        try:
            outcome = import_receipt(conn, job['receipt'], network, lambda: observe(job['receipt']))
        except (EvidencePending, ValueError) as exc:
            conn.execute('''UPDATE protocol_indexer.archive_jobs SET available_at=clock_timestamp()+%s,last_error=%s
                WHERE batch_sha256=%s AND line_number=%s''',
                (timedelta(seconds=5), type(exc).__name__, batch, job['line_number']))
            outcome = 'deferred'
        else:
            conn.execute('DELETE FROM protocol_indexer.archive_jobs WHERE batch_sha256=%s AND line_number=%s', (batch, job['line_number']))
        return {'line': job['line_number'], 'transaction_id': job['receipt']['transaction_id'], 'status': outcome}
