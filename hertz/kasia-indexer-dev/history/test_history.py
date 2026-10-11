import concurrent.futures
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import collect
import prepare


class HistoryCollectionTests(unittest.TestCase):
    def test_binary_envelopes_preserve_colons_and_non_utf8(self):
        for prefix in (b"ciph_msg", b"kchat"):
            body = b"\xff\x00:\x81"
            root, kind, sealed, fields = prepare.decode((prefix+b":1:handshake:"+body).hex())
            self.assertEqual(root,prefix.decode())
            self.assertEqual(kind,"handshake")
            self.assertEqual(sealed,body)

    def test_long_and_empty_aliases_match_native_lookup_keys(self):
        raw=b"abcdefghijklmnopqrstuvwxyz0123456789_suffix"
        _, kind, sealed, fields=prepare.decode((b"kchat:1:comm:"+raw+b":body").hex())
        self.assertEqual(fields["alias"],b"abcdefghijklmnop".hex())
        self.assertEqual(fields["wire_alias"],raw.hex())
        self.assertEqual(sealed,b"body")
        self.assertEqual(prepare.decode(b"ciph_msg:1:comm::body".hex())[3]["alias"],"")

    def test_page_checkpoint_resumes_without_restarting_history(self):
        a={"transaction_id":"a","payload":b"kchat:1:handshake:aa".hex()}
        b={"transaction_id":"b","payload":b"ciph_msg:1:handshake:bb".hex()}
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with patch.object(collect,"request",side_effect=[([a],{"X-Next-Page-Before":"99"}),RuntimeError("interrupted")]):
                with self.assertRaises(RuntimeError):collect.collect_explorer("kaspa:fixture",root)
            with patch.object(collect,"request",return_value=([b],{})) as req:
                result=collect.collect_explorer("kaspa:fixture",root)
            self.assertEqual({row["transaction_id"] for row in result["rows"]},{"a","b"})
            self.assertIn("before=99",req.call_args.args[0])
            with patch.object(collect,"request",side_effect=AssertionError("must use completed checkpoint")):
                self.assertEqual(collect.collect_explorer("kaspa:fixture",root)["pages"],2)

    def test_binary_scope_remains_bytes_in_hex(self):
        _, kind, sealed, fields = prepare.decode(b"kchat:1:self_stash:\xff:encrypted".hex())
        self.assertEqual((kind,sealed,fields),("self_stash",b"encrypted",{"scope":"ff"}))

    def test_inclusive_indexer_cursor_deduplicates_boundary(self):
        page=[{"tx_id":str(i),"block_time":i+1,"message_payload":"aa"} for i in range(50)]
        responses=[(page,{}),([page[-1],{"tx_id":"next","block_time":51,"message_payload":"bb"}],{})]
        with patch.object(collect,"request",side_effect=responses) as req:
            result=collect.collect_indexer("kasia","kaspa:fixture","/handshakes/by-sender")
        self.assertEqual(len(result["rows"]),51)
        self.assertIn("block_time=50",req.call_args.args[0])

    def test_saturated_timestamp_is_a_failure(self):
        page=[{"tx_id":str(i),"block_time":10,"message_payload":"aa"} for i in range(50)]
        with patch.object(collect,"request",return_value=(page,{})):
            with self.assertRaisesRegex(ValueError,"saturated"):
                collect.collect_indexer("kasia","kaspa:fixture","/handshakes/by-sender")

    def test_explorer_follows_header_even_for_short_page(self):
        a={"transaction_id":"a","payload":b"ciph_msg:1:handshake:aa".hex()}
        b={"transaction_id":"b","payload":b"kchat:1:comm:ab:bb".hex()}
        with patch.object(collect,"request",side_effect=[([a],{"X-Next-Page-Before":"99"}),([b],{})]) as req:
            result=collect.collect_explorer("kaspa:fixture")
        self.assertEqual(len(result["rows"]),2)
        self.assertIn("before=99",req.call_args.args[0])

    def test_concurrent_cache_writes_are_atomic(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"cache.json.gz"
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(lambda i:collect.atomic_json(path,{"value":i}),range(100)))
            with gzip.open(path,"rt") as stream:
                self.assertIn(json.load(stream)["value"],range(100))


if __name__=="__main__":unittest.main()
