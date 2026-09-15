# Candidate verification — 2026-09-15 UTC

Infrastructure branch: `feat/chats-indexer-20260914`, uncommitted changes on
`6c3b51bb1e74aa0978e1e87eab7e01cc3f0da5cf`.

Upstream: `KaspaSilver/KaChat-Indexer` at
`018882c448d248ca96d51dac107a8b0ed44fb101`.

## Executed on Hertz

- Docker build completed with exit 0. `cargo test --locked --release
  --workspace`: 41 passed, 0 failed, 0 ignored. Release binary built.
- Image ID:
  `sha256:699ec52713c79e6c182426364f215bc762b067393560cc8a6bc2afa7f34fc502`.
- Build log:
  `/home/ren/Kasanova/staging/chats-indexer-018882c448d2/build.log`.
- DEV, PROD, and candidate Compose configurations rendered successfully with
  the tested image ID and an explicitly marked working-candidate revision.
- Candidate `chats_indexer_candidate-indexer-1` started with its own empty
  volume and loopback port 18080. Connected to `kaspad-testnet10:17210`.
  `/metrics` showed processed blocks rising from 19,905 to 45,586, with no
  database errors. Initial sync started from the node's pruning point.
- Production Caddy block validated in a disposable Caddy container.
- DEV Caddy block exercised in a disposable proxy on loopback port 18081,
  changing only the listener and upstream to target the candidate. Health
  and metrics returned 200. Export, import-file, purge, self-stash GC,
  contextual-message import, and internal-push test paths returned 404.
  The disposable proxy was stopped and removed afterward.

## Not yet verified

- Real encrypted transactions under both prefixes through the candidate.
- Migration of a consistent copy of the existing DEV database, complete
  message preservation, and restart persistence.
- Complete historical mainnet seed or archival replay source.
- Live Caddy cutover, PROD deployment, Flutter endpoint change, wallet/device
  acceptance, review, or release.

## DEV snapshot restart incident — 2026-09-15

The original DEV writer was stopped for a consistent database copy, then
restarted at 15:16:24 UTC. The original volume remains `kasia_indexer_dev_data`.
The copy is `chats_indexer_dev_preserved_20260915`.

The original API has not yet recovered. Its deployed source
`K-Kluster/kasia-indexer@e43123ac995788a8300e5fdf2851a157f39ea054` loads up
to three million compact headers before starting the listener. Process I/O
confirms active startup reads. A running container is not API readiness.

The copy container `chats_indexer_dev_migration` is paused to prioritize
original DEV recovery. No DEV image or public routing change was applied.
Before resuming it, verify `/healthz`, real message routes, preserved history
counts, and advancing block metrics on the original service.

Pre-restart counts: 760 outgoing handshakes, 522 unique incoming handshakes,
873 contextual messages, and two payments in each direction. The original
metrics snapshot is preserved on Hertz in the staging directory.

These checks do not establish release readiness.


### DEV restored

The original API reopened at 15:54:52 UTC; public `/healthz` returned 200 at
15:55:08 UTC. All five saved message counters matched the pre-restart values.
Blocks advanced from 3,023,433 to 3,023,615 between samples, the node connection
succeeded, and gap filling completed. The process initializes its block counter
from retained headers on restart, so it is not directly comparable with the
pre-restart lifetime counter. Android Chats resumed successful handshake
retrieval at 15:55:22 UTC and loaded its inbox at 15:55:24 UTC.

The migration copy was resumed with a one-CPU limit at 15:50 UTC. It remains
private; original DEV keeps its original image, volume, and public route.
Restoration metrics are on Hertz as `dev-restored-first-metrics.json` and
`dev-restored-second-metrics.json` in the image staging directory.


### Production backlog verification — 16:06 UTC

The importer added 54,040 prepared records with zero errors, bringing the
journal to 146,993. Public verification through `https://indexer.kasanova.io`
checked all journaled records across 5,916 read groups with zero errors.
`verified_read_records` is 156,110 because records queried in both sender and
receiver groups are checked more than once; it is not the import count.
Collection and preparation of the remaining source histories are unfinished.

Flutter implementation is committed as `cd254b0a` in app PR #396; all app,
core, design-system, and analysis commit hooks passed. Funded device
interoperability acceptance remains pending. Backend endpoint routing is in
public-service PR #222 and has not been promoted or deployed.

## Current verified status — after 2026-09-15 recovery

The earlier sections are timestamped execution history. The current final
results supersede their pending-state descriptions:

- All 311,689 accepted, prepared mainnet records were imported with zero import
  errors and verified through the public PROD API: 6,961 read groups, 321,661
  route observations, zero verification errors. Seven source records remain
  quarantined: four absent from the archive and three not accepted. No
  acceptance metadata was fabricated.
- Personal-route cursors now cover all six personal routes. They are exclusive,
  versioned, and bound to route/address/scope. Deduplication precedes the cursor
  boundary; supplying both cursor and block_time is rejected.
- The final image passed 48 Rust workspace tests. Eight migration-tool tests
  passed locally. Original DEV versus upgraded preserved-copy comparisons
  passed 1,038 real HTTP queries, covering 1,723 read records with no missing
  or changed records. The complete raw export comparison covered 3,803 records.
- Current private DEV copy: `chats_indexer_dev_cursor_v2`, loopback port 32770,
  volume `chats_indexer_dev_preserved_20260915`, image
  `sha256:a0e3f6b741400e912456bd6438edfbba998078bf4b8dcef753ca029a2be971db`.
  It continues TN10 indexing. Do not start its stopped predecessor containers
  concurrently; they share this volume.
- Real Flutter DEV/TN10 handshake and message transactions from dedicated
  Dave/Eve wallets were observed on the upgraded private copy. The public DEV
  service still uses its original image and volume. No personal-cursor cutover
  has occurred on public DEV or PROD.

Final public recovery evidence is on Hertz under
`/home/ren/Kasanova/staging/chats-history-20260915/final-public-verification.log`.
The personal cursor image still needs reviewed deployment. No full Flutter
release gate or wallet promotion is established by these backend checks.
