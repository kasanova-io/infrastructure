# Social indexer on Hertz

Dedicated DEV Compose project alongside `hertz/kasia-indexer-dev`. Upstream source is pinned by `vendor/k-indexer`; processor and web images build from that source. Separate PostgreSQL volume; no host database ports, wallets, signing keys, push credentials, or changes to Kasia.

Copy the repository (including initialized submodule) to an isolated deployment directory. Create a mode-0600 `.env` beside this Compose file on the server with `DB_PASSWORD` (random hex), `SOURCE_REVISION` (exact submodule SHA), `CHAIN_INDEXER_IMAGE` (resolved immutable digest of upstream's `supertypo/simply-kaspa-indexer:v2.3.0`), `SOCIAL_NETWORK` (`testnet-10` or `mainnet`) `KASPA_NODE_URL` (matching existing node), and `SOCIAL_WEB_IMAGE` (reviewed immutable web image ID). Do not commit this file or print expanded Compose config.

Build a web candidate with `python3 prepare-web-build.py` and `docker build -t <candidate-tag> .web-build`; verify it before selecting its immutable image ID in `SOCIAL_WEB_IMAGE`. Prepare and test the profile-guard processor through `../social-indexer-processor/README.md`, preserving its real dependency lock and choosing its tested immutable runtime ID in `SOCIAL_PROCESSOR_IMAGE`. Then run `docker compose config --quiet` and `docker compose up -d --no-build --wait`. Verify `/health`, `/stats`, and a paginated feed via the Caddy route before activation. Caddy should proxy `/social-api/*` on the environment's miniapps host using `handle_path` to `social_indexer_dev_web:3001`. Its config must validate before reload. Public backend's `social_indexer` vendor URL owns client routing.

Upstream's minimal ingestion configuration does not track transaction acceptance or reorgs. This feed displays indexed public content, not proof of finality. The retained social tables start with available node history; a fresh node/indexer does not recreate pruned historical posts. No content-remover, personal cleanup service, membership filter or app-specific filter is deployed.

Rollback: remove the managed Social Caddy block, validate/reload Caddy, and run `docker compose down` in this directory. Keep `social_data` for recovery; never use `down -v`. Stop only this Compose project. App/public activation remains subject to the existing DEV release pin.

## Initial DEV deployment (2026-10-08)

Runtime root: `/home/ren/Kasanova/deployments/social-indexer-dev` on Hertz; Compose runs from `hertz/social-indexer-dev` beneath it. `.env` is mode 0600 and configured for existing `kaspad-testnet10:17210`, network `testnet-10`. Chain image: `supertypo/simply-kaspa-indexer@sha256:f28478e046af83b8d490984ca76bd75c663a71e242feba51a176e5ab6ce0a62f`.

All four health checks pass. Public API: `https://dev-miniapps.kasanova.io/social-api`; `/health` reports healthy TN10. Initial `/stats` contains zero social posts; this is an empty live index, not a seeded demonstration or historical mainnet import. The miniapp static files are hosted at `/social/`. App/public activation is still held by the existing release pin.

The Caddyfile is a file mount, so validate a copied temporary candidate *inside* the Caddy container before replacing the host file. Back up the original and compare for concurrent edits; reload only after validation. The deployed managed block is included beside this README.

## Superseded temporary mainnet switch

Ren initially requested a temporary DEV mainnet reader, then corrected the direction to keep TN10 running and publish production separately. DEV has been restored to its original TN10 configuration and database. The independent production recipe is in `../social-indexer-prod`. Neither indexer stack has signing keys; transaction authorization remains in the native wallet.

Set `SOCIAL_DATABASE_VOLUME` to a distinct named volume per network. The initial TN10 default remains `social_indexer_dev_social_data`; mainnet uses `social_indexer_dev_mainnet_data`. Never reuse one database across networks. The server retains mode-0600 `.env.tn10` and `.env.mainnet` configurations. Stop only this Compose project's services before switching `.env`, validate configuration, and bring the same project back up with health checks. Neither environment file belongs in Git.

To restore TN10, from this deployment's Compose directory run `docker compose stop --timeout 60`, copy `.env.tn10` to `.env`, then `docker compose config --quiet` and `docker compose up -d --wait --wait-timeout 240`. Keep both database volumes. The `/social-api` URL stays stable. No Caddy, wallet-network, app/public release pin or production deployment change is needed for this reader switch.

## Historical author key compatibility candidate

The pinned upstream checkout remains unchanged. `prepare-web-build.py` verifies its exact SHA and clean tracked source, then creates an ignored minimal `.web-build` context containing only the workspace manifest, web manifest/source, `Dockerfile.web`, and the historical-author-key and exact-vote patches. No `.env`, processor, history corpus, or unrelated workspace content enters this build context. Run this preparer before Compose builds; direct isolated candidate builds use `docker build -t <candidate-tag> .web-build`.

The patch accepts historical 64-character hexadecimal author keys and compressed 66-character `02`/`03` keys in posts, replies, mentions, profile details, followers, following, and author search. Requester keys remain compressed. SQL receives exact decoded author bytes. Search now matches the exact author key, rather than stripping its parity prefix. Quotes use the existing posts/details/replies endpoints and retain their original identity. There are no transaction-processor, schema, payload-validation, or signing changes.

`Dockerfile.web` runs the production Rust validator tests before building. `python3 -m unittest discover -s . -p 'test_web_compat.py' -v` applies the patch to temporary source and checks the actual Rust validator plus handler coverage (requires a working Rust toolchain). `verify_web_compat.py --container <isolated-candidate> --author <staged-historical-key> --quote-id <staged-quote-id>` exercises the real HTTP handlers using read-only requests. Candidate web containers may join the isolated history network; do not replace the original staged web or mutate its database.

Both environments select an explicitly tested immutable `SOCIAL_WEB_IMAGE`. This recipe does not deploy or promote either environment. Capture the candidate image ID, patch hash, complete test result and staged-data counts before any separate rollout approval/review.

Candidate validation on 2026-10-09: image `sha256:674f2549181268a8a44908e5225374dedaf1a77a1141a74905ccce732e382ba1`, compatibility patch SHA256 `ef2eed4659c5a52c9674843f9123c57fb60a88dec3acbb31fe5d2a7716c1e020`. Both production Rust validator tests passed during build; all 45 isolated HTTP checks passed. The real staged x-only author `330a856d1fbc1b6312141d4535af72c01f5bcefa3930e120069e8cde0c6742b9` returned its two posts, while both compressed representations remained empty. The staged corpus has no x-only profile/reply/relationship record, so those target routes were verified with valid empty/placeholder responses, not invented historical data. Original stage counts stayed 999 contents, 41 broadcasts, zero follows and zero blocks; original web image `sha256:b666b8950019c441bf7ec945644987aa04e720a602509aaf412b2c65a956c7e9` stayed running. This is an isolated candidate result, not DEV/PROD deployment acceptance. Build/HTTP logs are in the task evidence folder `working_docs/KSNV-411/social-web-compat-20261009` at the workspace root.


## Exact indexed votes (2026-10-09)

`patches/exact-vote-details.patch` adds GET `/get-vote-details?id=<txid>&requesterPubkey=<compressed-key>`. The query selects the exact transaction and sender from `k_votes`, returning `vote.id`, `vote.userPublicKey`, `vote.parentPostId`, and `vote.voteType`. Missing/malformed parameters return 400; absent or other-requester votes return 404. The existing content-only post-details route remains unchanged. This lets the wallet confirm its exact submitted vote, including a self-vote, without confusing a vote transaction with a content transaction.

The build runs all four production Rust tests. `verify_vote_details.py` performs 15 HTTP assertions against a specified existing vote, never submitting a transaction. Candidate checks use read-only database sessions and retain unchanged table fingerprints; the 45 historical-key HTTP regressions also pass.

Both DEV/TN10 and PROD/mainnet now run web image `sha256:fb35e68bd388ae4444cd382856998392335a2b5eb35347237540883fdaf54ef5`. Only web containers changed; ingestion/database container IDs stayed identical. Previous image `674f2549` remains available with private `.env`/Compose backups suffixed `.before-exact-vote-20261009`. Restore both files and recreate only web with `--no-deps --no-build --wait` for rollback. Exact source hashes, Rust/HTTP logs, live readbacks, and rollback records are in `working_docs/KSNV-411/social-vote-api-20261009` at the workspace root. No historical live import occurred.

## Content search candidate

`patches/content-search.patch` adds `GET /search-posts?query=<text>&requesterPubkey=<compressed-key>&limit=10`, returning the existing `{posts,pagination}` feed response. It searches post and quote text, with the same viewer block rules, author/quote metadata, counts, vote state and numeric row cursors as the public feed. Query text is trimmed and bounded to 2–200 Unicode characters; limit is 1–100 (default 10). Optional `before` or `after` must be a valid timestamp/numeric-row cursor; malformed or simultaneous cursors return 400. Search is a case-insensitive literal substring: percent and underscore have no wildcard meaning. No schema or ingestion change is required.

The search predicate runs before pagination and binds query text as a SQL parameter. Nested CASE guards validate Base64 and PostgreSQL-compatible non-NUL UTF-8 before decoding to text; malformed historical content is omitted rather than failing the entire query. Other feed paths pass no search predicate. This patch does not scan content on the client.

`test_web_compat.py` applies all three patches and compiles the actual production validators. For the full production Rust/API/SQL contract, create a separate disposable PostgreSQL database named `social_search_contract`, set `SOCIAL_SEARCH_TEST_DATABASE_URL` to it, and run `python3 run-search-contract.py`. The runner verifies the pinned clean upstream source, applies patches in temporary source, runs all Rust tests with the `search-contract` feature, and builds the complete release binary. Supply isolated `CARGO_HOME`, `RUSTUP_HOME`, and `CARGO_TARGET_DIR` when required by the task environment. It neither provisions nor alters deployment databases; SQL fixtures use only connection-local temporary tables. The database test fails when the explicit connection or expected database name is absent. It exercises the actual Axum router, API handler and database query, including malformed encodings, Unicode boundaries, literal metacharacters, blocked authors, enriched results and tied-timestamp pagination.

The Docker build runs the default Rust tests, including search validators, but cannot replace this separate real-database contract. A local release build is also distinct from a tested runtime image and live environment acceptance. No search image is selected or deployed by this source change.
