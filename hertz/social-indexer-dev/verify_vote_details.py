#!/usr/bin/env python3
"""Read-only acceptance of exact indexed vote confirmation; never creates a vote."""
import argparse
import json
from verify_web_compat import request


def verify(container, vote_id, requester, parent, vote_type):
    expected = {"id": vote_id, "userPublicKey": requester,
                "parentPostId": parent, "voteType": vote_type}
    checks = []
    for identifier, viewer in [(vote_id, requester), (vote_id.upper(), requester.upper())]:
        status, body = request(container, "/get-vote-details", {"id": identifier, "requesterPubkey": viewer})
        assert status == 200 and body == {"vote": expected}, (status, body)
        checks.append({"case": "exact-vote", "status": status, "vote": body["vote"]})
    wrong_viewer = ("02" if requester.startswith("03") else "03") + requester[2:]
    for identifier, viewer, label in [(vote_id, wrong_viewer, "other-requester"),
                                      ("0" * 64, requester, "unknown-vote"),
                                      (parent, requester, "post-is-not-a-vote")]:
        status, _ = request(container, "/get-vote-details", {"id": identifier, "requesterPubkey": viewer})
        assert status == 404, (label, status)
        checks.append({"case": label, "status": status})
    invalid = [({}, "missing-both"), ({"id": vote_id}, "missing-requester"),
               ({"requesterPubkey": requester}, "missing-id"),
               ({"id": "a" * 63, "requesterPubkey": requester}, "short-id"),
               ({"id": "g" * 64, "requesterPubkey": requester}, "nonhex-id"),
               ({"id": vote_id, "requesterPubkey": requester[2:]}, "xonly-requester"),
               ({"id": vote_id, "requesterPubkey": "04" + requester[2:]}, "bad-key-prefix"),
               ({"id": vote_id, "requesterPubkey": "02" + "g" * 64}, "nonhex-key")]
    for params, label in invalid:
        status, _ = request(container, "/get-vote-details", params)
        assert status == 400, (label, status)
        checks.append({"case": label, "status": status})
    status, _ = request(container, "/get-post-details", {"id": vote_id, "requesterPubkey": requester})
    assert status == 404, status
    checks.append({"case": "vote-is-not-content", "status": status})
    status, body = request(container, "/get-post-details", {"id": parent, "requesterPubkey": requester})
    assert status == 200 and body["post"]["id"] == parent, (status, body)
    checks.append({"case": "target-post-still-readable", "status": status})
    return {"checks": checks, "passed": len(checks)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ["container", "vote-id", "requester", "parent", "vote-type"]:
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.container, args.vote_id, args.requester, args.parent, args.vote_type), indent=2))
