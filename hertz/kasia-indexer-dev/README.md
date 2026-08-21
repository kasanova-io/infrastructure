# Hertz DEV Kasia indexer

This Compose project runs the Kasanova-owned TN10 Kasia indexer used by
KSNV-304 end-to-end recovery testing. It is independent of the external
K-Kluster GitOps deployment.

## Runtime contract

- Host: `hertz`
- Compose project: `kasia_indexer_dev`
- Container: `kasia_indexer_dev`
- Kaspa source: `ws://kaspad-testnet10:17210` on `caddy_caddy_net`
- Public route: `https://dev-indexer.kasia.fyi`
- Health path: `https://dev-indexer.kasia.fyi/healthz`
- Persistent volume: `kasia_indexer_dev_data`
- Indexer image: immutable digest in `compose.yaml`

Set `DEPLOYMENT_REVISION` to the exact infrastructure commit before rendering
or starting the Compose project. The container label
`io.kasanova.source-revision` records that deployment identity.

The active Hertz Caddyfile must contain the exact managed block from
`Caddyfile`. Validate the full configuration before reloading Caddy.
