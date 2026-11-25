"""Actual tip/source views in an owning disposable PostgreSQL database.

Fixtures model already materialized native facts, not SDK or chain acceptance.
The cost case protects a 20-card count query from unrelated journal growth.
"""

import hashlib
import json
import os
from pathlib import Path
import unittest

from protocol_indexer.journal import connect


DSN = os.environ.get("PROTOCOL_TEST_DATABASE_URL")
SOCIAL_SQL = (Path(__file__).parents[1] / "social/shared_social.sql").read_text()
SOURCE = "a179f285-19a7-4646-9eef-cf1bf2c20a56"
IMPORT = "1" * 64


def txid(number):
    return hashlib.sha256(f"tip-read-fixture:{number}".encode()).digest()


def signature(number):
    return hashlib.sha512(f"tip-read-fixture:{number}".encode()).digest()


@unittest.skipUnless(DSN, "set PROTOCOL_TEST_DATABASE_URL to an owning disposable database")
class TipReadBounds(unittest.TestCase):
    def setUp(self):
        self.db = connect(DSN)
        if not self.db.info.dbname.startswith("protocol_test_"):
            self.db.close()
            raise ValueError("tip tests require an owning protocol_test_* database")
        self.db.execute("DROP SCHEMA IF EXISTS social_shared CASCADE")
        self.db.execute("DROP SCHEMA IF EXISTS protocol_indexer CASCADE")
        self.db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        # Only native prerequisites used by the production source/tip views.
        self.db.execute("""
            CREATE SCHEMA protocol_indexer;
            CREATE TABLE protocol_indexer.canonical (
                transaction_id bytea PRIMARY KEY, revision bigint,
                source_transaction jsonb, accepting_block bytea);
            CREATE TABLE protocol_indexer.pending (
                transaction_id bytea PRIMARY KEY, acceptance_changed boolean,
                accepting_block bytea);
            CREATE TABLE public.k_contents (
                transaction_id bytea PRIMARY KEY, sender_signature bytea NOT NULL);
            CREATE INDEX fixture_content_signature ON public.k_contents(sender_signature);
        """)
        self.db.execute(SOCIAL_SQL)
        self.db.execute("INSERT INTO social_shared.config(network,source_id) VALUES('testnet-10',%s)", (SOURCE,))
        self.db.execute("""INSERT INTO social_shared.retained_imports
            (import_id,network,source_id,protocol_hint,source_backup_sha256,inventory,native_records)
            VALUES(%s,'testnet-10',%s,'kchat',%s,'{}',1)""", (IMPORT, SOURCE, "2" * 64))
        for number in range(1, 21):
            self.action(number)
            self.db.execute("INSERT INTO public.k_contents VALUES(%s,%s)", (txid(number), signature(number)))
            self.action(1000 + number, table="k_tips")
            self.db.execute("""INSERT INTO public.k_tips
                (transaction_id,block_time,sender_pubkey,sender_signature,post_id,author_pubkey,amount)
                VALUES(%s,%s,%s,%s,%s,%s,11)""",
                (txid(1000 + number), number, b"\x02" + b"a" * 32,
                 signature(1000 + number), txid(number), b"\x03" + b"b" * 32))
            self.db.execute("INSERT INTO social_shared.tip_targets VALUES(%s,%s,%s)",
                            (txid(1000 + number), txid(number), signature(number)))
            self.db.execute("INSERT INTO social_shared.tip_payments VALUES(%s,1,11,%s)",
                            (txid(1000 + number), signature(number)))
        self.db.execute("SET jit=off")

    def tearDown(self):
        self.db.close()

    def action(self, number, *, table="k_contents", retained=False):
        if not retained:
            self.db.execute("INSERT INTO protocol_indexer.canonical VALUES(%s,1,'{}',%s)",
                            (txid(number), b"a" * 32))
        self.db.execute("""INSERT INTO social_shared.actions
            (transaction_id,revision,subject,native_table,source_payload,original_time,status,
             provenance,retained_import_id)
            VALUES(%s,1,%s,%s,'6b636861743a313a66697874757265',%s,'verified',%s,%s)""",
            (txid(number), f"fixture:{number}", table, number,
             "retained_native" if retained else "canonical", IMPORT if retained else None))

    def visible(self, missing=()):
        rows = self.db.execute("""SELECT transaction_id,content_transaction_id,amount
            FROM social_shared.current_tips""").fetchall()
        expected = {txid(1000 + n): (txid(n), 11) for n in range(1, 21) if n not in missing}
        self.assertEqual({bytes(row["transaction_id"]): (bytes(row["content_transaction_id"]), row["amount"])
                          for row in rows}, expected)

    def test_current_acceptance_payment_revision_amount_and_pending_fences(self):
        self.visible()
        for identity in [txid(1001), txid(1)]:
            self.db.execute("UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=%s", (identity,))
            self.visible([1])
            self.db.execute("UPDATE protocol_indexer.canonical SET accepting_block=%s WHERE transaction_id=%s", (b"a" * 32, identity))
            self.db.execute("INSERT INTO protocol_indexer.pending VALUES(%s,true,NULL)", (identity,))
            self.visible([1])
            self.db.execute("DELETE FROM protocol_indexer.pending WHERE transaction_id=%s", (identity,))
        self.db.execute("UPDATE protocol_indexer.canonical SET revision=2 WHERE transaction_id=%s", (txid(1001),))
        self.visible([1])
        self.db.execute("UPDATE protocol_indexer.canonical SET revision=1 WHERE transaction_id=%s", (txid(1001),))
        self.db.execute("UPDATE social_shared.tip_payments SET amount=12 WHERE transaction_id=%s", (txid(1001),))
        self.visible([1])
        self.db.execute("UPDATE social_shared.tip_payments SET amount=11 WHERE transaction_id=%s", (txid(1001),))
        self.db.execute("DELETE FROM social_shared.tip_targets WHERE transaction_id=%s", (txid(1001),))
        self.visible([1])
        self.db.execute("INSERT INTO social_shared.tip_targets VALUES(%s,%s,%s)", (txid(1001), txid(1), signature(1)))
        self.visible()

    def test_retained_target_and_representative_keep_source_and_revocation_policy(self):
        self.action(3001, retained=True)
        self.db.execute("UPDATE public.k_tips SET post_id=%s WHERE transaction_id=%s", (txid(3001), txid(1001)))
        self.db.execute("UPDATE social_shared.tip_targets SET post_id=%s WHERE transaction_id=%s", (txid(3001), txid(1001)))
        self.visible()
        self.db.execute("INSERT INTO protocol_indexer.pending VALUES(%s,true,NULL)", (txid(3001),))
        self.visible([1])
        self.db.execute("DELETE FROM protocol_indexer.pending WHERE transaction_id=%s", (txid(3001),))
        self.db.execute("UPDATE social_shared.retained_imports SET source_id='other-source'")
        self.visible([1])
        self.db.execute("UPDATE social_shared.retained_imports SET source_id=%s", (SOURCE,))
        # An unaccepted canonical row suppresses the retained fallback.
        self.db.execute("INSERT INTO protocol_indexer.canonical VALUES(%s,1,'{}',NULL)", (txid(3001),))
        self.visible([1])
        self.db.execute("DELETE FROM protocol_indexer.canonical WHERE transaction_id=%s", (txid(3001),))
        self.db.execute("DELETE FROM protocol_indexer.canonical WHERE transaction_id=%s", (txid(1),))
        self.db.execute("UPDATE social_shared.actions SET provenance='retained_native',retained_import_id=%s WHERE transaction_id=%s", (IMPORT, txid(1)))
        self.visible()
        self.db.execute("INSERT INTO protocol_indexer.pending VALUES(%s,true,NULL)", (txid(1),))
        self.visible([1])

    def test_twenty_card_counts_stay_bounded_with_unrelated_journal_history(self):
        self.db.execute("""INSERT INTO protocol_indexer.canonical
            SELECT decode(md5('unrelated:'||n)||md5('unrelated-tail:'||n),'hex'),1,
                jsonb_build_object('payload','0x6b3a313a66697874757265','block_time',n),decode(repeat('aa',32),'hex')
            FROM generate_series(1,60000) n""")
        self.db.execute("""INSERT INTO social_shared.actions
            (transaction_id,revision,subject,native_table,source_payload,original_time,status)
            SELECT transaction_id,1,'unrelated:'||encode(transaction_id,'hex'),'k_contents',
                '6b3a313a66697874757265',1,'verified'
            FROM protocol_indexer.canonical WHERE source_transaction->>'payload' IS NOT NULL""")
        self.db.execute("ANALYZE protocol_indexer.canonical; ANALYZE social_shared.actions")
        self.db.execute("ANALYZE public.k_contents; ANALYZE public.k_tips")
        self.db.execute("ANALYZE social_shared.tip_payments; ANALYZE social_shared.tip_targets")
        self.db.execute("SET statement_timeout='2s'")
        plan = self.db.execute("""EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON,TIMING FALSE)
            SELECT a.transaction_id,(SELECT count(*) FROM social_shared.current_tips tip
                WHERE tip.content_transaction_id=a.transaction_id) AS tips_count
            FROM social_shared.actions a WHERE a.transaction_id=ANY(%s)""",
            ([txid(n) for n in range(1, 21)],)).fetchone()["QUERY PLAN"][0]
        self.assertEqual(plan["Plan"]["Actual Rows"], 20)
        buffers = plan["Plan"].get("Shared Hit Blocks", 0) + plan["Plan"].get("Shared Read Blocks", 0)
        self.assertLess(buffers, 10000, "a page must not repeatedly scan unrelated source history")
        counts = self.db.execute("""SELECT (SELECT count(*) FROM social_shared.current_tips tip
            WHERE tip.content_transaction_id=a.transaction_id) AS tips_count
            FROM social_shared.actions a WHERE a.transaction_id=ANY(%s)""",
            ([txid(n) for n in range(1, 21)],)).fetchall()
        self.assertEqual([row["tips_count"] for row in counts], [1] * 20)
        print(json.dumps({"tip_page_rows": 20, "unrelated_canonical_rows": 60000,
                          "execution_ms": plan["Execution Time"], "buffers": buffers}))


if __name__ == "__main__":
    unittest.main()
