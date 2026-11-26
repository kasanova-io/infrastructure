# Mainnet Chats history migration

The migration collects public encrypted transaction data for registered
production wallets, then imports both `ciph_msg:` and `kchat:` through the
same indexer parser and native database partitions. It does not decrypt,
re-encrypt, broadcast transactions, or notify users.

## Address snapshot

Read `postgres_prod`, database `kasanova_users_prod`, using a read-only query
that unions mainnet addresses from:

- `kasanova.user_profiles.address`
- `notifications.subscriptions.address` where `is_tracked = false`
- `kasanova.user_behavior_snapshots.address`
- `kasanova.vault_backups.owner_address`

Filter `kaspa:` addresses and deduplicate. The 2026-09-15 UTC snapshot contains
2,169 distinct addresses. These are addresses, not a user count. The production
profile table at the count check contains 2,326 profiles, 2,161 with mainnet
addresses (2,160 distinct), and 165 without an address.

Keep the address snapshot and all encrypted records on Hertz in a mode-0700
migration directory outside Git. Never commit profile data, address lists,
transaction payloads, or connection credentials.

## Collection and verification

Run these scripts from that private directory with `addresses.txt` present:

```sh
python3 collect.py .
python3 supplement.py .
python3 prepare.py .
python3 import_history.py .
python3 verify.py .
```

Scripts and their imports must be copied together from this directory.
Each phase is resumable. Re-run collection until its failed-job count is zero;
then rerun supplementation and preparation against the complete collection.
Do not declare completion from an earlier partial snapshot or an import count.

- `collect.py` reads handshakes and payments from both the official Kasia and
  KaChat APIs, and all matching transactions from the archival explorer. It
  follows the explorer's pagination headers without the upstream tool's
  20,000-transaction cap. Full timestamp ties fail explicitly instead of
  skipping messages. One hop of handshake counterparties is also scanned
  because incoming contextual messages are self-send transactions.
- `supplement.py` discovers aliases and self-stash scopes in the on-chain
  envelopes and queries their histories from both indexers. Group routes are
  queried on KaChat, whose API supports them.
- `prepare.py` cross-checks source ciphertext with archival transaction bytes,
  resolves the actual sender from the first input's previous output, selects
  the recipient using the upstream output-selection rule, and preserves
  confirmation metadata. Non-chat social operations are counted separately.
- `import_history.py` writes bounded atomic batches through the private
  `/history-import` route. A persistent transaction-ID ledger makes retries
  idempotent. Different ciphertext or metadata for an already imported ID is
  rejected. Separate transactions with equal message bodies remain separate.
- `verify.py` reads each imported record through the client REST routes and
  compares ciphertext, sender/recipient, timestamp, payment amount where
  applicable, and acceptance block/DAA. It records missing rows or stalled
  pagination as failures.

`collect.py` shares cross-process request pacing for the archival API and
honors rate-limit cooldowns. Keep `CHATS_RATE_DIR` (default current directory)
shared by all migration processes. Successful job results are immutable cache
files; failures remain outstanding until retried successfully. Atomic cache
writes use unique temporary files so concurrent lookups cannot corrupt them.

## Import service

`history.rs` is a small addition to the pinned upstream chat indexer. The
Dockerfile applies `history.patch` and runs the complete locked workspace tests
before building the image. It uses the existing parser and partition APIs for
handshakes, contextual messages, payments, self-stash and group operations.
Every batch commits its records and deduplication ledger together. Malformed
records or conflicts abort the whole batch; the runner then isolates failures.
Historical import never changes chain synchronization cursors or emits pushes.

All cursor-aware history routes use an exact lookahead row and expose their
continuation contract without changing the JSON list body:

- `X-Kasia-Has-More` is always `true` or `false`.
- `X-Kasia-Next-Cursor` is present only when another page exists, and matches
  the cursor on the last returned row.

The cursor is opaque, exclusive, versioned, and bound to its route and query.
Clients must fail the history load if these headers are absent, malformed, or
contradict the returned rows; a partial page must never be reported as complete.

The route is disabled by default. It requires both `NETWORK_TYPE=mainnet` and
`KASANOVA_HISTORY_IMPORT_ENABLED=true`. The temporary production Compose
migration override enables it on Hertz loopback port 18082. Public Caddy
configuration denies this route and all upstream maintenance routes.

After complete collection, preparation, import and read verification:

1. Record all phase summaries, source/address snapshot identities, image ID,
   infrastructure revision, and counts by prefix/type.
2. Remove the migration override and recreate the service with graceful
   shutdown. Preserve the database volume and migration evidence.
3. Repeat all read checks after restart and verify the import route is disabled.
4. Verify the public production endpoint and continuous mainnet advancement.

A migration of registered wallet history does not establish complete history
for every address on mainnet. Offline-only wallet addresses absent from our
production records cannot be enumerated by this server-side snapshot.

## Tests

```sh
python3 -m unittest discover -s . -p 'test_*.py' -v
```

The Rust tests additionally cover both prefixes, each personal message type,
atomic failure, conflicting duplicates, binary ciphertext, acceptance metadata,
and persistence across restart. Runtime verification must still read the
actual migrated records; unit tests are not migration acceptance.

## Shared-native DM candidate

`shared-dm.patch` narrowly backports the upstream KaChat DM type and inbox
partition from `e2f462171bee80ff26c6fe14ddafe06614f05efa` (ISC notice retained
in `LICENSE.KaChat-Indexer`). The original standalone parser/actors keep their
grammar; only the shared importer enables canonical `kchat:1:dm`. It reuses the
same contextual ciphertext and sender index, adding the native inbox index in
the existing transaction with acceptance and durable journal acknowledgement.
No actor partition IDs, personal filtering or push routing change.

The inbox GET uses the established cursor/header contract; the standalone
route returns404 so it cannot advertise unsupported DM ingestion. Shared mode
denies contextual/import, import-file, personal/purge-all and self-stash GC
before request-body extraction, preserving the canonical writer's ownership.
Push configuration remains independent. Pair this image with its reviewed
shared adapter and replay retained DM inventory before evaluating cutover.
`shared-dm-source.lock` identifies the tested candidate. Its bounded private
activation delivered three retained real tip jobs to the original native store;
a corrected copy-only follow-up verified durable revisions, payment API data,
old records and the original cursor. Both earlier verifier failures remain
retained.

A subsequent bounded run delivered seven naturally captured accepted Kasia rows:
two `ciph_msg:1:comm`, two handshake and three self-stash. All 44 operational
checks passed. A fresh immutable cold backup preceded original-store delivery;
separate baseline/audit copies preserved the backup. Ten exact durable
acknowledgements (seven new plus three inherited tips) were verified only on a
fresh marked copy. Nine native REST route/record pairs matched payload bytes,
source acceptance and sender/owner, with exclusive cursors. All nine previous
exported key/value records and the original payment/cursor remained; final 25 exported key/value records survived
restart on the copy. All nine owned services stopped with exit 0; the temporary egress
bridge was removed. The first preflight's malformed diagnostic image literal
failed closed before any resource or original-store open and remains FAIL.

No new transaction was sent by that run, and no plaintext decryption or push
routing was tested. The lock's `genuine_new_dm_acceptance` remains false for
canonical `kchat:1:dm`/inbox, distinct from the accepted classic Kasia records.
KaChat dm/inbox, room/extension transactions, external backfill, broader history
migration and the complete six-family public cutover remain separate gates.


## Shared runtime: full DEV history preservation

The shared scanner replacement is tested against a writable clone of the
complete verified DEV snapshot. The live service and immutable backup stay
untouched. The current native image starts without loading the legacy scanner
cache or starting independent chain maintenance.

The complete before/after scan preserves all 27 partition fingerprints:
7,615,542 key/value records, including 14,183 contextual message index records
and 71 live device registration keys. The earlier snapshot reported a metric
of 91 registrations. That metric uses Fjall's approximate length; it is not a
live-key count. The new runtime also reported 94 while the 71-key registration
fingerprint remained identical. Neither metric subtraction nor the old failed
91-row assertion proves a registration delta. The original failure remains
recorded separately from the subsequent count reconciliation.

A 60-request corpus from 20 actual history query groups covers handshake and
payment sender/receiver routes and contextual messages by sender. Complete DTOs,
pagination headers and exclusive cursors match the existing live service,
then remain identical across the shared process restart. Starts took 8.29 and
8.74 seconds, below the unchanged 40-second bound. All private actors stopped
cleanly. Raw queries, bodies, registration values, ciphertext and logs stay on
the host; source metadata records counts and hashes only.

This establishes full-copy idle/runtime/API preservation. Source-connected
replay, a verified current-source delta, live notification continuity and the
reviewed DEV cutover remain separate gates. The immutable snapshot is never
opened by Fjall; only the marked writable clone is opened. No new transaction
or public traffic switch is part of this acceptance.

## Shared live chat notification candidate

Journal imports remain quiet. After a successful import, the connector requests
notification admission separately through the same private authenticated route.
The native handler validates its configured source binding and PostgreSQL
identity, current journal/revision/payload, real acceptance and pending changes.
It holds its own source and canonical guards through admission to the existing
`PushDispatcher`, so delayed HTTP work rechecks authority after a caller timeout.
Existing device, recipient, privacy and self-send filtering remains there.
Notification aliases and encrypted payment payloads keep the live actor's wire
representation; storage normalization does not change push matching or content.

The common schema persists a separate notification-consumer activation epoch.
Both connector and native admission require an eligible event and transaction
time after that boundary. Reinstall/restart preserves it, and an upgrade cannot
notify old, previously quiet history. Native SQL delivery receipts make normal
HTTP retries and restarts idempotent. Queue admission precedes receipt commit:
external delivery retains the existing best-effort semantics, with no claim of
crash-exactly-once delivery. Missing source configuration denies notification
admission while quiet import and standalone behavior remain available.

The pinned client lock adds PostgreSQL support while preserving all 505 prior
package/source/checksum pins; it adds 17 packages, with some resolved feature
edges changing. The final image remains the minimal runtime. The explicit
`notification-contract` target executes 77 ordinary native cases in their
standalone fixture context, then all nine PostgreSQL/HTTP cases with the shared
test source configured. All 86 unique cases passed. The initial runner mixed
these contexts and failed nine existing fixtures; that result remains adverse.
The corrected runner changes no Rust feature code or test assertions.
The retry persistence case reopens the native store; a process restart with the
live notification pipeline still needs operational acceptance.

`shared-dm-source.lock` records this separate candidate and its scope. These are
synthetic source/authorization checks, not real chain or APNs acceptance. Live
source catch-up, current-history reconciliation, ordinary-transfer Address
Activity continuity and real notification delivery remain pre-cutover gates.


## Shared ordinary Address Activity

Ordinary payments now have durable raw-source activity state and pending work,
separate from protocol parsing. The native worker reuses the registered watch
union, existing Funds event factory and dispatcher. A watched candidate fetches
its containing block through the existing RPC client; SDK transaction ID, full
script versions, amounts, outpoints and header context must match. Current SQL
body, real nonarchive acceptance, native/source identity and epoch are checked
again under locks before queue admission and acknowledgement. Shared alerts wait
for acceptance; standalone block-sighting behavior is unchanged.

First actual native activation creates a persistent epoch after schema readiness
and native binding. Old observed/queued/body times stay quiet. Failed queue
admission retains work; handled state prevents normal replay/reacceptance from
notifying twice. Retention protects pending/unhandled raw rows. Only its own
acceptance deletion suppresses wakeups. Worker shutdown joins before SyncAll.
External delivery retains the dispatcher's existing best-effort semantics.

The current candidate passes 91 unique native cases (78 ordinary plus 13
PostgreSQL), including all previous Chat authorization cases, and 131 common
Python/PostgreSQL cases. Its compiled source fixture is the exact concatenation
of Simply schema 23, the common schema and activity SQL. The earlier compile and
quiet-batch failures remain adverse. Live worker restart, real payment/provider
delivery, current-source reconciliation and DEV cutover remain unverified.
The candidate is not activated; `shared-dm-source.lock` records exact images.
