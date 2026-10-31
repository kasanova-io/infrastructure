# Private shared Social API overlay

This reuses pinned K-webserver917863 and the existing historical-author-key,
exact-vote and literal-content-search patches. It adds the upstream KaChat room
reader to the same Axum server, rate limits, pool and middleware. It does not
replace the full webserver with KaChat's application-specific services. The
original ISC notice is retained in both images. Exact input/provenance/image
hashes and the dependency lock are recorded in `SOURCE.lock`.

This private candidate passed bounded acceptance against the retained DEV source;
all temporary readers are stopped. The configured origins remain
`dev-indexer.kasanova.io` and `indexer.kasanova.io`. Public routing, Caddy,
deployment and rollout settings are unchanged. Existing Social routes now use
canonical-filtered native views in shared mode, preserving their SQL, DTOs and
cursors. Source-connected history reconciliation and six-family acceptance
remain required before those origins can switch to this candidate.

## Shared binding and room reads

The existing database/server CLI arguments are preserved. Opt-in room mode also
requires BOTH `--shared-source-id <journal UUID>` and `--shared-network <full
configured network>`. A half-specified binding fails CLI parsing. Startup checks
the source journal, native projection ownership, native network and all required
native read views. Each reader repeats its binding and required-view checks on
the same connection and repeatable-read, read-only transaction as its data query. Wrong or absent ownership/view
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

From this directory, prepare a new context containing only the eighteen reviewed
inputs (including unchanged compatibility patches, native schemas and shared
read authorization predicates):

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
The owner runner inventories twenty exact cases and requires every individual
PASS result plus exit0 and complete owned-process/network cleanup.

Nine native unit and twenty actual PostgreSQL/router contracts passed on the
recorded frozen image. Cases include523 equal-time rows, cross-route/channel/
network cursors, pending removals/revision mismatch, source/view failure, SQL
cancellation/pool hygiene, legacy404, MAINNET configuration and the original
search regression. Fixtures are explicit isolated materialization records,
not chain transactions or SDK sender proofs. Previous combined-process tests
failed on the global recorder and remain adverse evidence.

Twelve of the database contracts exercise existing Social reads: source/view
failures even on empty results, current revisions and pending removals, native
profile/relationship selection, literal edit/unquote/delete targets, cumulative
mentions, exact legacy feed/search JSON and tied cursors, ordinary vote lookup,
notification pagination, hashtags, both configured networks and snapshot/pool
hygiene. The legacy notification DTO has no tip variant, so its mention view
excludes tips before LIMIT and counting; tip notifications belong to the owning
notification implementation.

One repeatable-read/read-only transaction covers binding and all queries in a
method, including fallbacks. All eight views are resolved and locked before
setting a transaction-local search path with no public fallback. Legacy mode
still uses a plain pool checkout. The source-bound native writer installs these
views; API reads never create or repair a schema.

On 10,000 explicit materialized posts, the ordered view page uses 409 shared
buffers (4.361 ms execution); the ACTUAL feed DAO uses 561 buffers and returns 21
rows for legacy 20+lookahead pagination. Both bounded-cost assertions pass. The
first candidate failed these gates and two native concurrency regressions; those
results remain retained. Synthetic cost coverage does not establish production
history/query-plan acceptance or live protocol delivery.

Source-connected preservation passed 24 checks: the original post, native rows,
three retained tip facts, notification state and journal ownership were preserved.
Eight legacy/current API responses, including both original cursor directions,
were byte-identical. This single retained-post sample does not prove broader
public history reconciliation. The notification viewer is an unrelated read key;
empty-equivalence is not recipient or tip-notification delivery acceptance.

Supervised test containers use Docker init to forward signals. Native exits 0,
readers 143 and wrong-source startup 1 were verified; the first unsupervised reader
ignored SIGTERM at PID 1 and was forcibly killed 137. That failed run and its
cleanup failure remain retained. Public deployment lifecycle is unverified.

Genuine new DEV/TN10 protocol acceptance, broader historical Social/API
reconciliation, representative-history query-plan acceptance and complete
six-family cutover remain required. These tests do not establish public route
activation, a visible tip counter, notification delivery or native-wallet QA.
