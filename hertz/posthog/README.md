# PostHog on Hertz

This standalone Compose project installs the official PostHog hobby stack behind
the existing Hertz Caddy gateway at `https://posthog.kasanova.io`. It owns its
Postgres, ClickHouse, Kafka, Redis and object storage volumes. The wallet analytics
connector and archived Amplitude exports are separate and remain unchanged.

Upstream source is pinned in `prepare.py`; pristine upstream deployment inputs
are in `upstream/`. `prepare.py` generates private, durable secrets only on a fresh
installation and preserves them on retries. Do not regenerate the Django secret,
database credentials or encryption salt on an existing installation.

## Install

Copy this directory to `/home/ren/Kasanova/tools/posthog` on `hertz`. Set
`DEPLOYMENT_REVISION` to the exact infrastructure source commit, then run
`python3 prepare.py`. Install `kasanova-posthog.slice` under
`/etc/systemd/system`, reload systemd and start that slice. The shared budget is
four CPU cores, 22 GiB memory and 2 GiB swap; it applies only to this stack.

Run Compose with the generated upstream file and the Hertz overlay:

```sh
docker compose -f compose.upstream.yaml -f compose.hertz.yaml config --quiet
docker compose -f compose.upstream.yaml -f compose.hertz.yaml pull --quiet
```

Record each pulled image's immutable repository digest in `images.lock.yaml`,
with a `services` mapping that overrides every service's `image`. Start with
all three files and `up -d --no-build`. Continue using the lock file for all
subsequent commands; upgrading requires a new explicit source/image selection
and a backup first. Run `prepare.py` again after the gateway starts, then repeat
`up -d --no-build`; this binds Django's trusted proxy chain to the actual gateway
IP addresses. Repeat this after recreating either gateway.
Install the managed block in `Caddyfile` only after bootstrap
and local acceptance; validate the entire live Caddy configuration before reload.
Create the DNS A record for this hostname pointing to Hertz.

Only the PostHog gateway joins `caddy_caddy_net`. Databases and supporting APIs
have no published ports. The gateway also binds `127.0.0.1:18084` for host health
checks. `/_health` and `/login` must respond successfully over public HTTPS.
Verify a dedicated setup project can ingest and query a diagnostic event, and
verify DEV and PROD project keys and data are separate before app integration.

## Data and operations

Compose project: `kasanova_posthog`. Docker volumes retain data across container
recreation. Never use `down --volumes`, Docker volume pruning or the generic
upstream installer in this directory. Its host package and shared port changes
are unsuitable for this server.

Store bootstrap credentials outside Git under the existing private Kasanova
secret directory. Before importing source data, implement and verify consistent
database/object-storage backups and restore. The original Amplitude archives
remain the source of truth until backfill reconciliation succeeds.

PostHog describes this self-hosted deployment as unsupported and offers no data
loss guarantee. Installation acceptance does not certify migration parity,
historical recording playback or completion of the PROD Amplitude archive.
