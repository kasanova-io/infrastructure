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
with a `services` mapping that overrides every service's `image`. On a fresh
instance, start `web proxy kafka-init` with all three files and `up -d --no-build`,
let the initial migrations finish, then start the entire stack. Continue using the lock file for all
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
Verify DEV can ingest and query a diagnostic event, and
verify DEV and PROD project keys and data are separate before app integration.
The installed hobby build refuses additional projects through its normal API.
The existing PROD and DEV projects have passed capture/query isolation checks;
this does not establish that multiple projects are a supported free deployment.
Resolve that deployment constraint before connector cutover. Do not enable paid
features by changing license data or create a paid subscription implicitly.

## Data and operations

Compose project: `kasanova_posthog`. Docker volumes retain data across container
recreation. Never use `down --volumes`, Docker volume pruning or the generic
upstream installer in this directory. Its host package and shared port changes
are unsuitable for this server.

Store bootstrap credentials outside Git under the existing private Kasanova
secret directory. Before importing source data, implement and verify consistent
database/object-storage backups and restore. The original Amplitude archives
remain the source of truth until backfill reconciliation succeeds.

Before the first PROD import, run `sudo python3 backup.py`. This takes only this
PostHog instance offline, checkpoints every exclusively owned volume (including
anonymous volumes), runtime configuration and private bootstrap credentials,
and restarts the original running containers. Backups are private under
`/home/ren/kasanova-archives/posthog/<UTC timestamp>`.
Run `sudo python3 verify_backup_restore.py <checkpoint path>` to restore all
volume copies, compare content and ownership, and query isolated PostgreSQL,
ClickHouse and ZooKeeper containers without connecting to live data mounts.
Copy the sealed checkpoint to independent storage and verify its hashes too.

This checkpoint command requires an empty PROD project. It does not certify
steady-state ingestion draining or recurring backup after importing history or
connecting clients; those remain requirements before connector cutover.
The generated SeaweedFS bucket bootstrap wrapper forwards shutdown signals to
the storage process so its persistent volume can be checkpointed cleanly.
Rust services replace their entrypoint shell with the actual binary using `exec`;
this delivers Docker's stop signal to the service instead of forcing termination
after a shell ignores it. Allow 90 seconds for their coordinated shutdown.

## Prepare and validate historical events

`prepare_amplitude_import.py` prepares a new private import directory from sealed
PROD source components and the audited canonical identity map. It checks original
export hashes, record counts and UUID uniqueness, and sorts events chronologically.
It preserves event names, UUIDs, timestamps and original typed event properties.
Each envelope includes the complete original record bytes and their checksum;
event-time user properties and original session/device fields remain recoverable.
Native session IDs are deterministic UUIDv7 values. Preparation never sends data.

`validate_amplitude_import.py` captures a representative sample and compares the
persisted UUID, name, timestamp, identity, source bytes, event/user properties and
session mapping. A separate project is preferred but the hobby API currently
rejects that creation. `--existing-dev-fixtures` requires explicit fixture markers
and separate test identities; fixture UUIDs must also be distinct from source
UUIDs. Those fixtures still contain private original source records. Keep their
files and evidence private, and record that DEV contains migration test fixtures.
The validator checks that fixture UUIDs are absent from PROD and does not resend
an acknowledged sample when resumed with the same evidence directory.

Run the offline contract checks with:

```sh
python3 -m unittest discover -s . -p test_amplitude_import.py -v
```

The 121-event DEV validation covers all 120 observed source event types and the
newest event boundary. It does not certify the full PROD backfill, current-profile
restoration, future SDK identity continuity, chart/cohort parity or replay playback.

PostHog describes this self-hosted deployment as unsupported and offers no data
loss guarantee. Installation acceptance does not certify migration parity,
historical recording playback or completion of the PROD Amplitude archive.
