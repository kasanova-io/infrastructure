# Hertz DEV Kasia indexer

This Compose project runs the Kasanova-owned TN10 Kasia indexer used by
KSNV-304 end-to-end recovery testing. It is independent of the external
K-Kluster GitOps deployment.

## Runtime contract

- Host: `hertz`
- Compose project: `kasia_indexer_dev`
- Container: `kasia_indexer_dev`
- Kaspa source: `ws://kaspad-testnet10:17210` on `caddy_caddy_net`
- Public route: `https://dev-indexer.kasanova.io`
- Health path: `https://dev-indexer.kasanova.io/healthz`
- Persistent volume: `kasia_indexer_dev_data`
- Indexer image: tested immutable image ID supplied as `CHATS_INDEXER_IMAGE`

Set `DEPLOYMENT_REVISION` to the exact infrastructure commit before rendering
or starting the Compose project. The container label
`io.kasanova.source-revision` records that deployment identity.

The active Hertz Caddyfile must contain the exact managed block from
`Caddyfile`. Validate the full configuration before reloading Caddy.

## Shared Chats image

The Dockerfile builds only the chat service from
[KaspaSilver/KaChat-Indexer](https://github.com/KaspaSilver/KaChat-Indexer/tree/018882c448d248ca96d51dac107a8b0ed44fb101/kasia-indexer),
pinned to `018882c448d248ca96d51dac107a8b0ed44fb101`. Its locked Rust workspace
tests run during the image build. Both `ciph_msg:1` and `kchat:1` enter the same
parser. Kasanova continues writing `ciph_msg:1`.

Build on Hertz, then record the resulting image ID, test output, upstream
revision, and infrastructure revision in the deployment evidence:

```sh
docker build --progress=plain -t kasanova/chats-indexer:018882c448d2 .
docker image inspect kasanova/chats-indexer:018882c448d2 --format '{{.Id}}'
```

Set `CHATS_INDEXER_IMAGE` to that exact `sha256:...` ID in the deployment
environment. `pull_policy: never` prevents a tag update from substituting a
different build. DEV and PROD must use the same tested image. A different host
needs the image transferred or published by digest before deployment.

## DEV migration and cutover

1. Start `compose.candidate.yaml` against its separate empty volume. Its HTTP
   port is bound only to Hertz loopback at `18080`. Verify node connection,
   increasing metrics, and real TN10 ingestion under both prefixes.
2. Capture existing message API pages and transaction IDs as the history
   baseline. Record the running image ID and deployment configuration.
3. Take a consistent backup with the old writer stopped gracefully. Do not
   copy the Fjall directory while it is being written. Preserve the original
   volume and the existing recovery backup. Never attach two indexer processes
   to the same writable database.
4. Restore the backup into a separate candidate volume, then run the new image
   there. The upstream v0-to-v1 migration changes DAA header keys; the original
   backup must remain untouched. Verify all baseline message pages and IDs,
   chain catch-up, restart persistence, and ingestion under both prefixes.
5. Apply the managed Caddy block and validate the complete Caddy configuration.
   Verify maintenance and internal-push routes return 404 at public ingress.
   Export/import, purge, and garbage collection are private maintenance APIs.
6. Deploy the reviewed infrastructure revision only after migration and real
   DEV/TN10 acceptance pass. Stop writers before the final consistent copy and
   switch; verify public health, both-prefix reads, and preserved history.

For rollback, stop the new writer before restoring the saved original DB and
image. Do not run the old image against a database migrated by the new image.
Preserve any new records written since cutover for reconciliation before
rollback; a stale snapshot alone does not preserve those messages.

The candidate, manifests, or image build alone do not constitute deployed DEV
acceptance or authorize a wallet release.
