# Hertz production Chats indexer

This service owns mainnet indexing for Chats inside Kasanova. Its build is
defined alongside [DEV](../kasia-indexer-dev/README.md), with independent runtime
state. The public DEV service still runs its previous image; the replacement
TN10 candidate is separate.

| Setting | Production value |
| --- | --- |
| Host | `hertz` |
| Compose project and container | `kasia_indexer_prod` |
| Kaspa node | `ws://kaspad_mainnet:17110` |
| Network | `mainnet` |
| Database volume | `kasia_indexer_prod_data` |
| Public endpoint | `https://indexer.kasanova.io` |
| Health endpoint | `/healthz` |

Set `CHATS_INDEXER_IMAGE` to the exact tested image ID and
`DEPLOYMENT_REVISION` to the reviewed infrastructure commit. The manifest
does not publish a host port. Public traffic enters through the managed Caddy
block, which excludes upstream maintenance and internal-push endpoints.

## Live activation: 2026-09-15

Ren explicitly authorized public activation while historical recovery continues.
The full existing Hertz Caddy configuration plus this service's block passed
validation and was gracefully reloaded at 13:28 UTC. DNS resolves to Hertz,
HTTPS health returns 200, and HTTP redirects to HTTPS. Public maintenance
routes, including GET and POST `/history-import`, return 404.
Public read-back smoke verification passed for 12 recovered records across both
prefixes and all six operation types: 16 route checks, zero errors, including a
35-page contextual-message lookup. Ciphertext and transaction metadata matched
the prepared records. This sample is separate from full recovery verification.

The running PROD image is
`sha256:f6ee2f4897ad93b4b78723a12f23d8a583c2b3e8caef82d785be0d6400f0f71f`.
It is a tested working build, not yet a reviewed infrastructure commit.
Its independent mainnet database is persistent and regular indexing continues.
The temporary migration override keeps import access bound to localhost;
public Caddy ingress excludes that route.

The subsequent pagination patch fixes duplicate group transactions crossing
page boundaries when multiple blocks contain the same transaction. Group
message and group-control reads register earlier sightings before applying
cursor/time filters. This preserves the stored block evidence and returns each
transaction once. The 45 Rust workspace tests passed, including a real database
and API-handler regression covering page sizes 1–3, equal timestamps, later
block sightings, and timestamp-only resume. Group reads currently scan the
group/sender/recipient range from its beginning to establish those prior
sightings; this trades additional scanning on later pages for correct results.
After deployment, the previously failing public group read returned 151 unique
transactions over four pages with no duplicates. All 151 imported records in
that group passed ciphertext and metadata verification. Evidence on Hertz:
`group-pagination-fix-verification.json` and `group-pagination-public-pages.json`.

History recovery from both public indexers and archival transaction records
completed for 311,689 verified accepted records using
[the migration tools](../kasia-indexer-dev/history/README.md). All imported
records passed public REST read-back verification across 6,961 read groups,
with zero import or verification errors. Seven source records remain
quarantined: four missing from the archive and three not accepted.
These records were not assigned invented confirmation metadata. This recovery
result does not establish Flutter acceptance or release readiness.

Operational evidence is stored on Hertz under
`/home/ren/Kasanova/staging/chats-history-20260915`; do not commit address lists
or encrypted user records. The pre-activation Caddy backup is
`/opt/caddy/config/Caddyfile.before-prod-chats-20260915T132845Z`.

After migration, remove the temporary override, verify the importer is disabled,
and verify persistent history after restart. Complete DEV replacement and real
Flutter device acceptance separately.

No permanent second indexer fallback is part of this design. Migration must
preserve history in the new service. Until verified, the current Flutter
production endpoint stays in place. The public indexer is deployed; the Flutter
production configuration has not been switched by this activation.

## Upstream backfill limits

At the pinned revision, `kachat-admin/src/main.rs` implements address-based
explorer import, but filters only `ciph_msg:` and stops after 40 pages of 500
transactions. Its target imports contextual messages only. This is not a
complete history migration for both prefixes or all message types. The
`/contextual-messages/import` route is also excluded from public ingress.
