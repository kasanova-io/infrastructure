#!/usr/bin/env python3
"""Discover conversation/scoped lookups from public transaction envelopes."""
import argparse
from pathlib import Path
import json
from collect import SOURCES, run_jobs
from prepare import load, decode


def main():
    parser=argparse.ArgumentParser();parser.add_argument("directory",type=Path);args=parser.parse_args()
    root=args.directory;jobs=set()
    for path in (root/"results").glob("*.json.gz"):
        result=load(path)
        if result["job"][0]!="explorer":continue
        for tx in result["rows"]:
            try:
                _,kind,_,fields=decode(tx["payload"])
                inputs=sorted(tx.get("inputs",[]),key=lambda row:row["index"])
                sender=inputs[0].get("previous_outpoint_address") if inputs else None
                if not sender or not sender.startswith("kaspa:"):continue
                if kind=="comm": endpoint="/contextual-messages/by-sender";extra={"alias":fields["alias"]}
                elif kind=="self_stash": endpoint="/self-stash/by-owner";extra={"owner":sender,"scope":fields["scope"]}
                elif kind=="gctl": endpoint="/group-control/by-sender";extra={"sender":sender}
                elif kind=="gcomm": endpoint="/group-messages/by-blinded-group-id";extra={"blinded_group_id":fields["blinded_group_id"]}
                else:continue
                for source in SOURCES:
                    if kind in ("gctl","gcomm") and source=="kasia":continue
                    jobs.add((source,sender,endpoint,json.dumps(extra,sort_keys=True)))
            except (ValueError,KeyError):
                # Preparation tracks unsupported/malformed records; this discovery
                # pass must not invent a query for an undecodable envelope.
                continue
    expanded=[(source,address,endpoint,json.loads(extra)) for source,address,endpoint,extra in sorted(jobs)]
    print(json.dumps({"supplemental_queries":len(expanded)}),flush=True)
    failed=run_jobs(root,expanded,2)
    (root/"supplement-status.json").write_text(json.dumps({"queries":len(expanded),"failed":failed}))
    return bool(failed)


if __name__=="__main__":raise SystemExit(main())
