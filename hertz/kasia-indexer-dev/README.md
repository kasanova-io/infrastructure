# Hertz DEV Kasia indexer

This Compose project runs the Kasanova-owned TN10 Kasia indexer used by
Chats end-to-end testing. It is independent of the external
K-Kluster GitOps deployment.

## Runtime contract

- Host: `hertz`
- Compose project: `chats_indexer_dev`
- Container: `chats_indexer_dev`
- Kaspa source: `ws://kaspad-testnet10:17210` on `caddy_caddy_net`
- Public route: `https://dev-indexer.kasanova.io`
- Health path: `https://dev-indexer.kasanova.io/healthz`
- Persistent volume: the existing, verified migrated volume supplied as
  `CHATS_INDEXER_DB_VOLUME` (`chats_indexer_dev_preserved_20260915` on Hertz)
- Indexer image: tested immutable image ID supplied as `CHATS_INDEXER_IMAGE`

The replacement has its own project and container name. Starting it must not
recreate the original `kasia_indexer_dev` service or modify its original volume.
The external volume declaration fails if the named migrated database is absent;
it cannot silently start with an empty history.

Set `DEPLOYMENT_REVISION` to the exact infrastructure commit before rendering
or starting the Compose project. The container label
`io.kasanova.source-revision` records that deployment identity.

The active Hertz Caddyfile must contain the exact managed block from
`Caddyfile`. Validate the full configuration before reloading Caddy.

## Shared Chats image

The Dockerfile builds only the chat service from
[KaspaSilver/KaChat-Indexer](https://github.com/KaspaSilver/KaChat-Indexer/tree/879d34a0840c3af0117bc6de7f0ffeeda53c9338/kasia-indexer),
pinned to `879d34a0840c3af0117bc6de7f0ffeeda53c9338`. Its locked Rust workspace
tests run during the image build. Both `ciph_msg:1` and `kchat:1` enter the same
parser. Kasanova continues writing `ciph_msg:1`.

Push mutations require the current wallet-signed contract
(`PUSH_AUTH_MODE=strict`). Both deployments mount the Kasanova Firebase service
account read-only and route Android data messages through FCM. The sender
project is fixed to `kasanova-io`, matching the native Firebase app that issues
the installed wallet's Android registration tokens. DEV and PROD chain
environments use that same sender project; a testnet deployment must not change
it to `kasanova-io-dev`. Set only `FCM_SERVICE_ACCOUNT_HOST_PATH` in the private
deployment environment, and never store the service-account JSON in this
repository.

Apple delivery uses the team-scoped `Kasanova Chats Push` APNs key for bundle
ID `io.kasanova.wallet`. Set `APNS_KEY_HOST_PATH` to its mode-`0600` `.p8` file
in the private deployment environment. DEV defaults unknown iOS tokens to the
sandbox APNs host; the indexer retries production once on `BadDeviceToken` and
persists the learned environment per device. PROD reverses that default. Never
store the `.p8` file in this repository.

For direct Chats events, the indexer includes the canonical `recipient` wallet
address from the matched device's authenticated primary registration in both
APNs and FCM payloads. The wallet binds the notification route to that exact
address, so a delayed tap cannot open after the user switches to another wallet
on the same network. Transaction-bound Chats routes without a recipient are
rejected by the app.

Build on Hertz, then record the resulting image ID, test output, upstream
revision, and infrastructure revision in the deployment evidence:

```sh
docker build --progress=plain -t kasanova/chats-indexer:879d34a0840c .
docker image inspect kasanova/chats-indexer:879d34a0840c --format '{{.Id}}'
```

Set `CHATS_INDEXER_IMAGE` to that exact `sha256:...` ID in the deployment
environment. `pull_policy: never` prevents a tag update from substituting a
different build. DEV and PROD must use the same tested image. A different host
needs the image transferred or published by digest before deployment.

## DEV migration and cutover

Hertz already has a consistent copy, completed upstream migration, verified
history, and a running dual-prefix writer. Do not repeat the original DEV
snapshot or stop the original public writer for this rollout.

1. Use `chats_indexer_dev_preserved_20260915` as `CHATS_INDEXER_DB_VOLUME`.
   Check the running writer's volume and image against `VERIFICATION.md`.
   The disposable empty-volume `compose.candidate.yaml` is only a build smoke
   test; it is not the public replacement and does not contain the history.
2. After review, stop only `chats_indexer_dev_cursor_v2` gracefully, then start
   this Compose project with the same migrated volume and tested image. Keep
   its stopped predecessors stopped: one writer per database. Original public
   DEV continues serving throughout replacement startup and catch-up.
3. Wait for `/metrics` on the replacement, advancing chain processing, and
   preserved history. Compare the original and replacement APIs again. Verify
   native and `kchat:1` transactions on the replacement before routing traffic.
4. Apply this directory's managed Caddy block to the existing full configuration,
   validate it, and reload Caddy. It targets `chats_indexer_dev:8080`. Verify
   public health, both-prefix reads, complete history, and Flutter decryption.
   Verify maintenance and internal-push routes return 404 at public ingress.
5. Record the exact infrastructure revision, image ID, volume, comparison,
   transaction IDs, and device evidence. The old service remains running during
   acceptance so a route rollback does not require its long cold startup.
   Retire it only after accepted cutover and a separately authorized cleanup.

For a first deployment on another host, create a consistent backup with its
writer gracefully stopped, restore into a separate volume, run the migration,
and verify complete history and restart persistence before step 2. Never copy a
live Fjall directory or attach two writers to one volume.

For routing rollback, validate and reload Caddy with the saved original upstream.
The original indexer does not read `kchat:1`; returning traffic to it temporarily
loses visibility of those messages. Preserve the upgraded volume and reconcile
all new records before any subsequent database restoration. Never run the old
image on the migrated database or overwrite either database with a stale copy.

The candidate, manifests, or image build alone do not constitute deployed DEV
acceptance or authorize a wallet release.

## Shared-ingestion consumer candidate

In shared mode, the Chats service retains the existing message and push-registry
partitions and API, while skipping the standalone chain cache and its block,
virtual-chain, pruning and ticker workers. It does not scan or delete retained
chain history. One captured mode governs RPC connect, disconnect, stale
notifications and shutdown, so reconnects cannot fill unused scanner queues.
Shutdown drains the API and node tasks and persists the native keyspace.
Standalone mode retains its existing cache, processors and shutdown behavior.

The startup change passed 74 locked workspace tests, including seven storage,
standalone-cache and lifecycle regressions. A populated private-copy check also
preserved twelve durable acknowledgements, thirty exported records and existing
message/payment cursors while leaving 20,001 synthetic legacy-cache entries
unchanged. These checks do not establish migration or cutover of the current
public DEV store; its complete history and push registrations still require
the consistent-copy and API verification workflow above.

### Cold snapshot for shared-ingestion migration

`history/cold_snapshot.py` prepares a fresh full-filesystem backup of an existing
native store. This is a separate operation from the completed September migration
above. A message-only `/export` omits the push registration and watch partitions;
it cannot replace this backup. The helper reads files without opening Fjall.

First run its read-only preflight with the full source container ID, immutable
source/helper image IDs, and actual configured data volume:

```sh
python3 history/cold_snapshot.py preflight \
  --container "$SOURCE_CONTAINER_ID" --image "$SOURCE_IMAGE_ID" \
  --volume "$SOURCE_DATA_VOLUME" --utility-image "$PYTHON_HELPER_IMAGE_ID" \
  --output "$PREFLIGHT_RECEIPT"
```

The preflight binds the sole healthy writer, full deployment configuration,
capacity, and durable message/registration counters. It creates no Docker
resource and stops no service. Current bounds are32GiB/32768entries and64GiB
free for a backup plus separate working clone. A bound failure needs inspection,
not an automatic increase. Do not execute the snapshot while another owner holds
the shared DEV deployment or service; coordinate a concrete writer-stop slot.

```sh
python3 history/cold_snapshot.py snapshot \
  --preflight "$PREFLIGHT_RECEIPT" --preflight-sha256 "$PREFLIGHT_SHA256" \
  --backup-volume "$NEW_BACKUP_VOLUME" --helper "$NEW_HELPER_NAME" \
  --receipt "$NEW_SNAPSHOT_RECEIPT"
```

Use a durable host process independent of the SSH connection. The snapshot uses
the source's configured SIGINT with Docker's unlimited graceful-stop timeout;
it never forces the original writer to exit. Only after a clean shutdown does a
network-isolated helper copy the read-only source to a new run-labelled volume,
preserve ownership/modes, fsync it, and compare all file hashes three ways.
Every failure or handled terminal signal still attempts recovery of the exact
original container/image/configuration/volume. An own helper's read-only source
attachment cannot block recovery after a cleanup failure. Foreign attachments
are preserved and reported, never stopped. Successful recovery requires a new
process start, health, nondecreasing durable counters and advancing chain work.

The local stop observation is bounded at3minutes, copy at30minutes and recovery
at60minutes. These are failure ceilings, not a short-outage promise: the existing
legacy writer previously took38minutes to recover its chain cache. Retain the
host receipt and helper logs even after failure; they distinguish a complete
backup from an incomplete volume and record whether source recovery was proven.
No proxy route, environment, image, QA pin or original data is replaced.

Never open the backup with Fjall. Create a separate writable working clone for
candidate/history/API/push-registry checks. This staging snapshot alone cannot
become a later cutover source: preserve every intervening message, acceptance
change and registration through a fresh coordinated snapshot or verified delta
before routing traffic. Old image/database rollback needs the same reconciliation.

Local regressions: `python3 -B -m unittest discover -s history -p 'test_cold_snapshot.py' -v`.

The private `/history-import` transport now accepts a versioned shared-journal
object as well as the established legacy array. Both paths reuse the same native
parser, transactional Fjall insertions and existing read APIs; there is no second
message database/schema. The candidate mode is opt-in through the following
configuration, with no change to the current DEV/PROD deployments:

- `KASANOVA_SHARED_INGESTION_ENABLED=true`
- `KASANOVA_SHARED_NETWORK=<exact configured full network ID>`
- `KASANOVA_SHARED_INGEST_TOKEN=<private random bearer secret, at least32bytes>`
- Existing `NETWORK_TYPE` and `KASPA_NODE_WBORSH_URL` must agree with that network.

Shared requests require `Authorization: Bearer ...`, `schema_version:1`, the
journal's persistent UUID `source_id`, its exact `network`, and `transactions`
containing a positive `revision` plus the existing `HistoryTx` transaction body.
The new journal UUID is stored in `protocol_indexer.config.journal_id`; a process
restart retains it, whereas a recreated journal must be reconciled explicitly.
The native database binds the first network/source identity and rejects a changed
source, conflicting body or conflicting duplicate revision. Older revisions are
acknowledged as stale without applying them. A revoked record clears native
acceptance; reacceptance updates acceptance while preserving existing pagination
keys. Writes, revision receipt and acceptance changes commit atomically, with
`SyncAll` before acknowledgement, including retries of a duplicate.

When shared mode is enabled, legacy arrays are rejected even if an old migration
flag remains enabled. Native BlockAdded/VCP ingestion is disabled; ordinary RPC
connection/request dispatch remains active. API metrics remain available; shared
mode skips the standalone periodic, compaction and cursor-pruning workers. Existing public ingress must
continue hiding private import/export/maintenance routes. The token is neither a
replacement for that boundary nor permission to expose this endpoint publicly.

The deployed `kchat:1:tip` uses64-byte Schnorr signatures. Its ASCII hex sealed
memo is normalized to raw bytes at the native storage boundary; normal pay/payment
bytes remain unchanged even if they happen to look like ASCII hex. Existing
payment API serialization therefore returns tip memos exactly once as hex.
The original serialized transaction remains intact in the provenance ledger.

This is still a **consumer candidate**. The shared source connector and independent
sender/payment/signature validation have private DEV acceptance. KaChat `dm`
inbox tags enter the existing native store, and public `bcast` rooms enter the
Social projection. Full public-history migration, client decryption and cutover
remain pending. No push is emitted by this import path; the existing notification
pipeline is integrated in the next ordered goal stage.
Do not replace the current public writer with this candidate yet.

The consumer candidate also registers shutdown signals before startup/API serving,
retaining the signal future until the existing shutdown select. This fixes an
observed baseline race where an immediate stop after an acknowledged restart could
be lost during initial node connection. Unix and Windows shutdown routing remain
the same; the Linux build includes a real signal-before-await regression.
