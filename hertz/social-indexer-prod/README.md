# Social production reader

Independent mainnet deployment authorized by Ren on 2026-10-08: keep DEV/TN10 running and publish production separately. Same pinned upstream source and tested images as the DEV deployment; no wallets or signing keys. The first Social miniapp is read-only and unfiltered.

The Compose project `social_indexer_prod`, web container `social_indexer_prod_web`, private network and `social_indexer_prod_mainnet_data` database volume do not overlap DEV. Web source builds remain documented in `../social-indexer-dev`; the profile-guard processor uses `../social-indexer-processor`. Both verify the same `vendor/k-indexer` pin and require tested immutable runtime IDs before activation.

At `/home/ren/Kasanova/deployments/social-indexer-prod/hertz/social-indexer-prod`, create a mode-0600 `.env` with a fresh random-hex `DB_PASSWORD`, `SOCIAL_NETWORK=mainnet`, `KASPA_NODE_URL=ws://kaspad_mainnet:17110`, `SOCIAL_DATABASE_VOLUME=social_indexer_prod_mainnet_data`, and tested immutable image IDs for `CHAIN_INDEXER_IMAGE`, `SOCIAL_PROCESSOR_IMAGE`, and `SOCIAL_WEB_IMAGE`. Do not commit or print credentials.

Validate with `docker compose config --quiet`; start with `docker compose up -d --wait --wait-timeout 240`. Caddy's managed block routes `https://miniapps.kasanova.io/social-api/*` to the production web container. Validate the full candidate in the Caddy container before replacing/reloading its file-mounted config. Publish the miniapp only to `/opt/caddy/sites/miniapps.kasanova.io/social/`, preserving other miniapps.

Verify `/health` reports mainnet, all four containers are healthy, ingestion approaches live time, and the actual feed contract works. Fresh-node history coverage is limited by retained node history. Upstream's minimal ingestion configuration does not establish accepted-chain/reorg finality. Normal app/public activation, review and release controls still apply; hosting these independent services does not release the wallet.

Rollback: stop only this Compose project and remove only the production Social Caddy block, validating before reload. Preserve its data volume. DEV/TN10 stays running on its own database and endpoint.
