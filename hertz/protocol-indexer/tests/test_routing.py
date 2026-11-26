import unittest

from protocol_indexer.routing import Family, native_chat, route


class RoutingTests(unittest.TestCase):
    def test_dm_native_dispatch_validates_tag_without_reencoding_binary_tail(self):
        for sealed in (bytes(range(256)), b'\xff\x00:binary', b'deadbeef'):
            tail = b'ab' * 16 + b':alias:' + sealed
            classified = route(b'kchat:1:dm:' + tail)
            self.assertEqual(classified.tail, tail)
            self.assertTrue(native_chat(classified))
        for wire in (b'ciph_msg:1:dm:' + b'ab' * 16 + b':alias:sealed',
                     b'kchat:2:dm:' + b'ab' * 16 + b':alias:sealed',
                     b'kchat:1:dm:' + b'ab' * 16 + b':alias:',
                     b'kchat:1:dm:' + b'AB' * 16 + b':alias:sealed'):
            self.assertFalse(native_chat(route(wire)))

    def test_exact_protocol_action_dispatch(self):
        cases = {
            b"kchat:1:post:key:sig:body:[]": Family.KACHAT_POSTS,
            b"kchat:1:reply:key:sig:parent:body:[]": Family.KACHAT_POSTS,
            b"kchat:1:poll:key:sig:body": Family.KACHAT_POSTS,
            b"kchat:1:comm:alias:sealed": Family.KACHAT_MESSAGES,
            b"kchat:1:dm:tag:alias:sealed": Family.KACHAT_MESSAGES,
            b"kchat:1:bcast:kaspa:hello:with:colons": Family.KACHAT_MESSAGES,
            b"ciph_msg:1:bcast:kaspa:hello:with:colons": Family.KACHAT_MESSAGES,
            b"kchat:1:tip:key:sig:parent:author:sealed": Family.KACHAT_ACTIONS,
            b"kchat:1:vote:key:sig:parent:unvote:author": Family.KACHAT_ACTIONS,
            b"ciph_msg:1:comm:alias:sealed": Family.KASIA_MESSAGES,
            b"ciph_msg:1:payment:sealed": Family.KASIA_MESSAGES,
            b"ciph_msg:1:gctl:sealed": Family.KASIA_MESSAGES,
            b"k:1:post:key:sig:body:[]": Family.KSOCIAL_POSTS,
            b"k:1:quote:key:sig:parent:body:author": Family.KSOCIAL_POSTS,
            b"k:1:vote:key:sig:parent:upvote:author": Family.KSOCIAL_ACTIONS,
            b"k:1:broadcast:key:sig:nick:image:profile": Family.KSOCIAL_ACTIONS,
            b"kchat:1:broadcast:key:sig:nick:image:profile": Family.KACHAT_ACTIONS,
        }
        for payload, expected in cases.items():
            with self.subTest(payload=payload):
                self.assertEqual(route(payload).family, expected)

    def test_binary_handshake_tail_is_preserved(self):
        tail = b"\xff\x00\x80:first:second\xfe"
        for root in (b"kchat", b"ciph_msg"):
            parsed = route(root + b":1:handshake:" + tail)
            self.assertEqual(parsed.tail, tail)
            self.assertEqual(parsed.action, "handshake")

    def test_unknown_versions_and_actions_do_not_become_generic_chat(self):
        for payload in (
            b"kchat:2:comm:sealed", b"kchat:01:post:body", b"ciph_msg:2:comm:sealed",
            b"kchat:1:anything:sealed", b"kchat:1:unvote:sealed", b"kchat:1:unfollow:key",
            b"ciph_msg:1:post:body", b"ciph_msg:1:tip:body", b"k:1:tip:body",
            b"k:1:comm:body", b"k:1:bcast:body", b"kchat:1:comm:", b"kchat:1:post",
            b"kchat:", b"other:kchat:1:post:body", b"kchat:1:profile:body",
        ):
            with self.subTest(payload=payload):
                self.assertIsNone(route(payload))

    def test_versionless_chat_history_is_still_routed(self):
        for root, family in ((b"ciph_msg", Family.KASIA_MESSAGES), (b"kchat", Family.KACHAT_MESSAGES)):
            for ciphertext in (b"abcdef1234", b"123456"):
                parsed = route(root + b":" + ciphertext)
                self.assertEqual(parsed.family, family)
                self.assertIsNone(parsed.version)
                self.assertEqual(parsed.tail, ciphertext)
        self.assertIsNone(route(b"ciph_msg:odd"))


if __name__ == "__main__":
    unittest.main()
