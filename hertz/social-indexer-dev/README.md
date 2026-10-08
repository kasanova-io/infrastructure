# Social indexer on Hertz

Dedicated DEV Compose project alongside `hertz/kasia-indexer-dev`. Upstream source is pinned by `vendor/k-indexer`; processor and web images build from that source. Separate PostgreSQL volume; no host database ports, wallets, signing keys, push credentials, or changes to Kasia.

Copy the repository (including initialized submodule) to an isolated deployment directory. Create a mode-0600 `.env` beside this Compose file on the server with `DB_PASSWORD` (random hex), `SOURCE_REVISION` (exact submodule SHA), `CHAIN_INDEXER_IMAGE` (resolved immutable digest of upstream's `supertypo/simply-kaspa-indexer:v2.3.0`), `SOCIAL_NETWORK` (`testnet-10` or `mainnet`) and `KASPA_NODE_URL` (matching existing node). Do not commit this file or print expanded Compose config.

Run `docker compose config --quiet`, `docker compose build`, then `docker compose up -d --wait`. Verify `/health`, `/stats`, and a paginated feed via the Caddy route before activation. Caddy should proxy `/social-api/*` on the environment's miniapps host using `handle_path` to `social_indexer_dev_web:3001`. Its config must validate before reload. Public backend's `social_indexer` vendor URL owns client routing.

Upstream's minimal ingestion configuration does not track transaction acceptance or reorgs. This feed displays indexed public content, not proof of finality. The retained social tables start with available node history; a fresh node/indexer does not recreate pruned historical posts. No content-remover, personal cleanup service, membership filter or app-specific filter is deployed.

Rollback: remove the managed Social Caddy block, validate/reload Caddy, and run `docker compose down` in this directory. Keep `social_data` for recovery; never use `down -v`. Stop only this Compose project. App/public activation remains subject to the existing DEV release pin.

## Initial DEV deployment (2026-10-08)

Runtime root: `/home/ren/Kasanova/deployments/social-indexer-dev` on Hertz; Compose runs from `hertz/social-indexer-dev` beneath it. `.env` is mode 0600 and configured for existing `kaspad-testnet10:17210`, network `testnet-10`. Chain image: `supertypo/simply-kaspa-indexer@sha256:f28478e046af83b8d490984ca76bd75c663a71e242feba51a176e5ab6ce0a62f`.

All four health checks pass. Public API: `https://dev-miniapps.kasanova.io/social-api`; `/health` reports healthy TN10. Initial `/stats` contains zero social posts; this is an empty live index, not a seeded demonstration or historical mainnet import. The miniapp static files are hosted at `/social/`. App/public activation is still held by the existing release pin.

The Caddyfile is a file mount, so validate a copied temporary candidate *inside* the Caddy container before replacing the host file. Back up the original and compare for concurrent edits; reload only after validation. The deployed managed block is included beside this README.
