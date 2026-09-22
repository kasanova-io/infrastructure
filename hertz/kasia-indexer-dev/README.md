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
