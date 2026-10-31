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
retained. Consumers stopped cleanly afterwards. No new DM transaction was sent;
genuine DM chain/decryption, broader history migration and the complete
six-family public cutover remain separate acceptance requirements.
