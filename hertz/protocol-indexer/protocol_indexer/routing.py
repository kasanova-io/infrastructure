"""Byte-preserving protocol dispatch used identically by scan and backfill.

This identifies a parser, not a valid signature, supported UI feature, or payment.
In particular an accepted Kaspa transaction can still carry an invalid Social action.
No encrypted/binary tail is decoded or reconstructed at this boundary.
"""

from dataclasses import dataclass
from enum import StrEnum


class Family(StrEnum):
    KACHAT_POSTS = "kachat_posts"
    KACHAT_MESSAGES = "kachat_messages"
    KACHAT_ACTIONS = "kachat_tips_and_co"
    KASIA_MESSAGES = "kasia_messages"
    KSOCIAL_POSTS = "ksocial_posts"
    KSOCIAL_ACTIONS = "ksocial_co"


# Action names, not substring tests. `unvote` and `unfollow` are values inside
# their parent actions, not top-level wire actions. `bcast` is a public chat;
# `broadcast` is a signed Social profile action.
CHAT = frozenset({b"comm", b"dm", b"handshake", b"self_stash", b"gcomm", b"gctl", b"pay", b"payment"})
KASIA_CHAT = CHAT - {b"dm"}
K_POSTS = frozenset({b"post", b"reply", b"quote"})
K_ACTIONS = frozenset({b"vote", b"follow", b"block", b"broadcast"})
KCHAT_POSTS = K_POSTS | {b"edit", b"delete", b"poll"}
KCHAT_ACTIONS = K_ACTIONS | {b"tip", b"unquote", b"pollvote"}


@dataclass(frozen=True)
class Route:
    family: Family
    root: str
    version: int | None
    action: str
    tail: bytes


def native_chat(classified: Route | None) -> bool:
    """Current native projection, not the broader retained protocol inventory."""
    return classified is not None and (
        (classified.family in {Family.KASIA_MESSAGES, Family.KACHAT_MESSAGES}
         and classified.action not in {"dm", "bcast"})
        or (classified.family == Family.KACHAT_ACTIONS and classified.action == "tip")
    )


def native_social(classified: Route | None) -> bool:
    """Public protocol work, independent of private encrypted-message storage.

    This queues the native decoder; it does not assert signature/payment validity
    or that every routed extension already has a public projection.
    """
    return classified is not None and (
        classified.family in {Family.KACHAT_POSTS, Family.KACHAT_ACTIONS,
                              Family.KSOCIAL_POSTS, Family.KSOCIAL_ACTIONS}
        or (classified.family == Family.KACHAT_MESSAGES and classified.action == 'bcast')
    )


def route(payload: bytes) -> Route | None:
    """Select exactly one family; unknown versions/actions stay unprojected.

    Retain Kasia's versionless sealed envelope for the existing legacy decoder.
    An explicitly numeric unknown version must never fall back to that decoder.
    """
    if not isinstance(payload, bytes):
        raise TypeError("payload must be raw transaction bytes")
    root, separator, remaining = payload.partition(b":")
    if not separator or root not in {b"kchat", b"ciph_msg", b"k"} or not remaining:
        return None
    version, separator, rest = remaining.partition(b":")
    if version != b"1":
        # The legacy envelope is hex ciphertext, not an unknown protocol version.
        if root in {b"ciph_msg", b"kchat"} and not separator and len(remaining) % 2 == 0:
            try:
                bytes.fromhex(remaining.decode("ascii"))
            except (ValueError, UnicodeDecodeError):
                return None
            family = Family.KASIA_MESSAGES if root == b"ciph_msg" else Family.KACHAT_MESSAGES
            return Route(family, root.decode(), None, "legacy", remaining)
        return None
    if not separator:
        return None
    action, separator, tail = rest.partition(b":")
    if not separator or not tail:
        return None
    family = None
    if root == b"kchat":
        if action in CHAT | {b"bcast"}:
            family = Family.KACHAT_MESSAGES
        elif action in KCHAT_POSTS:
            family = Family.KACHAT_POSTS
        elif action in KCHAT_ACTIONS:
            family = Family.KACHAT_ACTIONS
    elif root == b"ciph_msg":
        if action == b"bcast":
            # KaChat public rooms used this root before the rename. Their
            # original bytes/root stay intact; they are still room messages.
            family = Family.KACHAT_MESSAGES
        elif action in KASIA_CHAT:
            family = Family.KASIA_MESSAGES
    elif root == b"k":
        if action in K_POSTS:
            family = Family.KSOCIAL_POSTS
        elif action in K_ACTIONS:
            family = Family.KSOCIAL_ACTIONS
    if family is None:
        return None
    return Route(family, root.decode("ascii"), 1, action.decode("ascii"), tail)
