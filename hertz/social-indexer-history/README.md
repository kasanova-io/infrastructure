# Social history recovery

This tooling discovers retained public content from a configured Social indexer,
verifies the original accepted transactions against a configured archive, and
replays immutable batches through the **unchanged production processor** in an
independent staging database. It never connects staging to the live database,
chain node, shared network or public proxy. It does not broadcast transactions.

## Collect and verify

From the infrastructure checkout, use a durable directory outside Git:

```sh
python3 hertz/social-indexer-history/history.py discover "$HISTORY_DIRECTORY" \
  --source "$SOCIAL_SOURCE" --archive "$ARCHIVE_SOURCE" --network "$SOCIAL_NETWORK"
python3 hertz/social-indexer-history/history.py verify "$HISTORY_DIRECTORY" \
  --source "$SOCIAL_SOURCE" --archive "$ARCHIVE_SOURCE" --network "$SOCIAL_NETWORK"
```

`--limit N` bounds discovery pages or archive-verification records for a pilot.
Both commands resume. Discovery prioritizes global posts/quotes, current profiles,
then authored replies, then exhaustive parent threads. Source responses and cursor
progress are durable. Verification includes IDs discovered while it runs; it stops when its pending
queue is empty. Reconcile again after discovery finishes in case new IDs arrived
after the verifier drained its queue. Quarantined records are
explicit failures; `--retry-quarantine` retries them after investigation. Current
SQLite status is authoritative; prior failure artifacts remain as evidence after
a successful retry. `--txid` selects an already-discovered transaction for a
bounded verification; `discover --parent` targets a known thread through the same
checkpointed collector when reply-count reconciliation finds a gap.

The implementation imports the existing Kasia history migration's atomic gzip
writer, archive rate buckets, Retry-After handling and retries. Social discovery
uses one request/second; archive detail requests share a conservative 0.6-second
bucket, supplementing Chats’ own 0.4-second floor and shared 429 cooldown. A task
operator can atomically set `archive-interval.txt` in the content inventory to a
value from 0.4 to 60 seconds; use a longer interval when provider cooldowns appear.
`verify --workers 4` overlaps archive latency while retaining the same shared
request rate and 429 cooldown. Only the coordinator writes the SQLite ledger;
worker results and source evidence remain atomic and resumable.
Commands hold per-phase locks. Source/archive/network configuration cannot change
inside an existing inventory. Collection stops before less than 2 GiB remain.

Verification checks accepted status, original payload, source signer/signature and
content, accepting block identity, and exact original transaction inclusion in a
full containing block. The verified containing block supplies the timestamp;
source and representative archive timestamps can differ legitimately. This is
independent archive evidence, not a locally established consensus proof. Native
signature validation happens during staging replay, not the Python verifier.

`status` reports discovery/verification counts. Staging is tracked separately;
verification is not import. `export --output <new-file.jsonl>` emits a new immutable,
chronologically sorted batch. It refuses to overwrite a previous batch.

## Isolated staging

Create a new private server directory containing `compose.yaml`, `init.sql`,
`stage.py` and the exported batch. Supply a mode-0600 `.env`:

- `HISTORY_PROJECT=social_history_<unique_batch_name>`
- `HISTORY_DB_PASSWORD=<new random password>`
- `SOCIAL_NETWORK=<matching configured network>`
- `SOCIAL_PROCESSOR_IMAGE=<exact existing immutable image ID>`
- `SOCIAL_WEB_IMAGE=<exact existing immutable image ID>`

Run `docker compose config --quiet`, then `docker compose up -d`. The database
creates a staging marker and raw transaction table; the original processor
creates its own production schema and signature-verifying pipeline. It runs one
worker. There is no chain ingestion service or raw-data pruning.

```sh
python3 stage.py batch.jsonl --project "$HISTORY_PROJECT" --network "$SOCIAL_NETWORK"
```

The runner keeps one PostgreSQL client session and verifies actual Docker project
ownership, database name, staging marker,
processor network and immutable batch hash. It inserts one verified raw transaction,
waits for the native parser's observable effect, then records its replay ledger
before advancing. On interruption an unledgered transaction is notified again;
the native parser owns deduplication. An invalid signature, conflicting signature
deduplication or missing effect stops the batch, rather than counting a successful
function return as import. A resumed completed batch is a no-op.

A batch is bound permanently to that staging database. Do not append newly
collected older records to it; create a fresh staging project and replay the
expanded batch from its beginning. Default batches permit only posts, quotes,
replies and broadcasts. Explicit current relationship projections use the separate
workflow below; inverse actions are excluded.
Equal-time conflicting profile transitions require an explicit ordering decision;
the exporter rejects them. Current profile discovery is a snapshot, not all revisions.

Read back every staged record through the existing web service, checking ID,
content, signer, signature and timestamp. The pilot evidence also verifies
idempotent replay. Keep actual accepted source evidence and rejection counts.
Never run `down -v` as cleanup without an explicit decision to discard evidence.

## Coverage and delivery boundary

The upstream API does not retain removed relationships or superseded profiles and
filters notification votes. Discovery can miss orphan replies by undiscovered
participants. This phase recovers useful content and current profiles, not every
historical event. Full coverage needs archive-address expansion or a fuller
archival source. Relation snapshots must not be replayed as complete histories.

There is deliberately **no live cutover command**. Review the exact batch,
coverage, parser/readback evidence, live-tail boundary and rollback before adding
history to the running indexer. DEV and PROD ingestion remain independent.

Tests: `python3 -m unittest discover -s hertz/social-indexer-history -p 'test_*.py' -v`.

Historical keys: the production parser supports both 32-byte x-only and 33-byte
compressed signers. Preserve their exact bytes; never manufacture a parity byte.
The public upstream source's user-specific HTTP routes still reject 64-hex x-only
keys, so discovery uses parent-thread traversal for those authors. Private staging
that retains the original web image may use the global profile list for readback.
Our DEV and PROD web services now use the reviewed compatibility patch described
in `../social-indexer-dev/README.md`: author queries accept exact 64-hex x-only or
66-hex compressed keys; requester keys remain compressed. This deployment fixes
our HTTP author-query limitation without changing the upstream discovery source,
stored signer bytes, or original processor/signature-validation semantics.

## Current relationship projections

`relationships.py` uses a **separate** inventory and shares request rate/cooldown
files with `--content <content-inventory>`. It seeds known compressed authors and
traverses following, follower and block lists; new related users extend the graph.
`--owner <key>` bounds a pilot to that owner's three lists. Follower rows name the
original sender, while following/block rows name the target: the collector records
that distinction and verifies the exact original sender, target, action and
signature against the archive. It accepts only active `follow`/`block`, never
`unfollow`/`unblock`, in this projection. Raw source pages remain evidence.

It supports `discover`, `verify`, `status` and `export` with the same source,
archive, network, limit and worker options. First complete a bounded owner pilot,
then omit `--owner` to traverse all known users. Current state can change during
collection; there is no source snapshot token. Reconcile counts and a fresh
relationship tail before any live migration. Unknown disconnected users, x-only
owners rejected by upstream, and deleted relationships remain explicit gaps.

Exported relation records are marked `current-relationship-projection`. Merge
only verified relation and content records, sort by `(block_time,transaction_id)`,
and use a **fresh** private staging database. `stage.py` and `verify_stage.py`
require `--allow-relationship-snapshot` for such a batch. Native signature replay
and complete relationship API readback remain mandatory. The flag does not
assert complete historical coverage or authorize a live import.

`complete_stage.py` can watch both inventories until discovery jobs and pending
archive verification are exhausted. It then holds all four collector locks,
exports accepted records, checks exact verified-ID coverage, and replays a fresh
combined batch in a new private project. Supply explicit immutable processor/web
image digests, source/archive/network, inventory/output paths and SSH host. Its
staging tools are frozen and hashed at launch; resumption preserves the same
batch and tools. It never performs live cutover. Quarantines remain excluded and
listed in coverage evidence. A failed collector, failed parser or failed HTTP
readback stops progress without claiming completion.

## Resumed staging provenance

`complete_stage.py` freezes `provenance.py` with the replay tools. Before startup,
existing private `.env` project/network/image pins must exactly match the requested
configuration; mismatches fail without rewriting credentials. Before replay and
after API verification, actual Docker processor/web image IDs, project/service
labels, private network, running state, and processor network are checked and
saved in `provenance-before.json` / `provenance-after.json`. Signature validation
still belongs to the unchanged pinned native processor.

An older four-file frozen snapshot is rejected before any tool is copied into it.
Use a fresh output directory and a reviewed complete source snapshot (including
the unchanged Kasia collector helper), preserving original logs and manifests.
Do not describe old pilot results as having passed a subsequently added guard.

For KSNV-411 the waiting pre-guard watcher was replaced at a verified safe boundary;
collectors were not restarted or modified. The fresh `full-stage-guarded` output
uses an independent private project and the reviewed compatible web image. Its
source hashes, PID, waiting checkpoint and preserved old snapshot are recorded
in the task's `watcher-guard-transition.json`. This is still a waiting collector
completion workflow, not a completed full-stage replay or a live import.

## Preserving the live content/vote overlay

`live_overlay.py` verifies an immutable read-only live database projection in a
separate inventory, using the same archive acceptance, network, original payload
and containing-block checks as historical recovery. It shares the original
collector's pacing/cooldown files. The snapshot binds exact IDs, signer and
signature, content/parent, and vote target/value; changing the snapshot or source
requires another inventory. It performs no database writes outside its local
evidence directory. Export its verified records using `History.export` only after
every selected record is resolved; preserve exclusions explicitly.

Staging and API verification accept votes only with `--allow-live-overlay` and the
explicit `live-content-and-vote-projection` record scope. The original processor
still verifies the signature and populates `k_votes`; readback uses the real
`/get-vote-details` route, checking exact transaction, sender, parent and value.
Use a separate marked private project with provenance checks before/after replay,
and repeat the immutable batch to verify idempotence. Historical votes are not
globally recovered by this limited live projection.

`build_candidate.py --history <batch> --overlay <batch> --network <network>
--output <new-directory>` combines completed immutable batches. Duplicate IDs
must have identical archive evidence, including chain timestamps and payload;
only recovery scope may differ. It preserves historical rows and records every
added and duplicate ID plus input/output hashes. The combined batch requires a
fresh guarded native replay with both explicit scope flags, and full API
verification. Its manifest intentionally records native combined verification as
false until that separate proof exists. Neither overlay verification nor batch
combination authorizes live cutover or claims a complete intervening event tail.
