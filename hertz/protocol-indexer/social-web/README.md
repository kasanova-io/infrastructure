# Private shared Social API overlay

This reuses pinned K-webserver917863 and the existing historical-author-key,
exact-vote and literal-content-search patches. It adds the upstream KaChat room
reader to the same Axum server, rate limits, pool and middleware. It does not
replace the full webserver with KaChat's application-specific services. The
original ISC notice is retained in both images. Exact input/provenance/image
hashes and the dependency lock are recorded in `SOURCE.lock`.

This is an unactivated private candidate for the existing configured indexer
origins, `dev-indexer.kasanova.io` and `indexer.kasanova.io`. It changes no Caddy,
Public routing, deployment or rollout setting. Broader existing Social reads
still need canonical filtering/history/API reconciliation; adding room methods
does not establish that those older routes are ready on a shared database.

## Shared binding and room reads

The existing database/server CLI arguments are preserved. Opt-in room mode also
requires BOTH `--shared-source-id <journal UUID>` and `--shared-network <full
configured network>`. A half-specified binding fails CLI parsing. Startup checks
the source journal, native projection ownership, native network and current room
view. Every room read repeats that check on the same connection and repeatable
read, read-only transaction as its data query. Wrong or absent ownership/view
fails503 without leaking database details. The existing server request budget
also sets a transaction-local PostgreSQL statement timeout; it does not leak
into later pooled sessions. HTTP timeout alone is not relied on to cancel SQL.
Without opt-in mode neither room route is attached, preserving legacy404 even
for malformed room query parameters.

`GET /get-broadcasts?channel=&limit=&before=` preserves upstream field names and
behavior: `messages`, `hasMore`, `txId`, `channel`, `senderAddress`, `content`,
`blockTime`; default200/clamp1–500; trimmed/lowercase channel; missing/unknown
channel yields an empty page; legacy `before` is a strictly older timestamp.
Reads use the native `social_shared.current_broadcasts` view, which excludes
unaccepted, stale-revision and pending-removal rows. Content bytes remain intact.

An additive `nextCursor` is returned only when lookahead finds another row.
Return it as `cursor` for exclusive `(block_time DESC,id DESC)` pagination when
timestamps tie. Cursor version, route, normalized channel, source UUID and full
network must match. `before` plus `cursor` is invalid. The cursor represents the
last returned row, never the lookahead row. Like existing native pagination,
it is a continuation key, not an authorization token or frozen multi-page
snapshot: intervening acceptance/reorgs may change page membership.

`GET /broadcast-channels?limit=&cursor=` reports **observed current channels**,
with `channels`, `hasMore`, and optional `nextCursor`. It uses the same default200
and cap500 and a separately bound continuation token. Revoking the last current
row may remove a channel. It is not an operator allowlist or a promise of
retention/push. No default rooms are invented. Response size and query duration
are bounded, but the DISTINCT view query still requires representative history
query-plan/cost acceptance before public cutover.

## Reproducible private build

From this directory, prepare a new context containing only the ten reviewed
inputs (including the unchanged owned compatibility patches and native room
schema):

```sh
python3 prepare_context.py --output /tmp/new-shared-social-web-context
```

The preparer refuses an existing directory or changed input hash; it does not
copy environment files, data, unrelated source or credentials. Build `Dockerfile`
in that context using the immutable `RUST_IMAGE`, `RUNTIME_IMAGE` and
`UPSTREAM_ARCHIVE_SHA256` values from `SOURCE.lock`. Cargo uses the committed
lock. The `lock-export` stage is preparation, not acceptance. Both test and
runtime builds refresh own source/manifest mtimes inside their locked target
cache before compiling, then copy artifacts out of that same build step.

The `contract` image packages native tests. Shared room PostgreSQL tests require
an explicit `SOCIAL_SHARED_WEB_TEST_DATABASE_URL` whose database name starts with
`protocol_test_social_web`; they reset only that owning disposable database. The
unchanged search contract separately requires `SOCIAL_SEARCH_TEST_DATABASE_URL`
pointing to `social_search_contract` and uses connection-local temporary tables.
Each router contract must run in its own process: the unchanged upstream
Prometheus recorder is global. Default Rust unit tests do not need either DB.
The owner runner inventories eight exact cases and requires all eight individual
PASS results plus exit0 and complete owned-process/network cleanup.

Nine native unit and eight actual PostgreSQL/router contracts passed on the
recorded frozen image. Cases include523 equal-time rows, cross-route/channel/
network cursors, pending removals/revision mismatch, source/view failure, SQL
cancellation/pool hygiene, legacy404, MAINNET configuration and the original
search regression. Fixtures are explicit isolated materialization records,
not chain transactions or SDK sender proofs. Previous combined-process tests
failed on the global recorder and remain adverse evidence.

Source-connected preservation and real DEV/TN10 protocol acceptance, broader
Social/history/cursor reconciliation, query-plan acceptance and complete
six-family cutover remain required. These tests do not establish public route
activation, a visible tip counter, notification delivery or native-wallet QA.
