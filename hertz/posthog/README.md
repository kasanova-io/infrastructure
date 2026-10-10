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

The initial checkpoint requires an empty PROD project. After a fully reconciled
offline event backfill, `--offline-import-reconciliation <reconciliation.json>`
allows that imported history to be checkpointed too. It stops the public gateway
and capture services, requires clean capture/ingestion exits and zero lag for
all consumers that commit storage offsets, and rejects events that change during
the drain. The pinned Go live-preview consumers deliberately disable offset
commits, so their reported broker lag cannot prove or disprove ingestion drain.
Their queue bytes and offsets are still preserved in the Kafka/Redis checkpoint.
It does not certify
steady-state ingestion draining or recurring backup after importing history or
connecting clients; those remain requirements before connector cutover.
The generated SeaweedFS bucket bootstrap wrapper forwards shutdown signals to
the storage process so its persistent volume can be checkpointed cleanly.
Rust services replace their entrypoint shell with the actual binary using `exec`;
this delivers Docker's stop signal to the service instead of forcing termination
after a shell ignores it. Docker's init process forwards signals and reaps child
processes, including auxiliary binaries without a custom SIGTERM handler. Allow
90 seconds for coordinated shutdown.

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

`import_amplitude_events.py <prepared directory> <evidence directory>` performs
an offline PROD backfill. It checks the entire compressed source before sending,
requires empty PROD on the first invocation, retains acknowledged offsets, bounds
ingestion backlog and stops on uncertain delivery. Resume with the identical
source and evidence directory; deterministic source UUIDs support deduplication.
It then compares every logical persisted event with its original envelope through
the owned ClickHouse container, including exact original record bytes, typed
properties and microsecond timestamps. `--verify-only` repeats comparison without
capturing events. The importer and backup share an exclusive runtime lock.
`--repair-unaccepted-backfill` is allowed only after every original event was
acknowledged, the recorded count matches, no successful reconciliation exists,
and the corrected capture image is running. It reuses source event identities;
ClickHouse's newer `_timestamp` versions replace the earlier event properties.
Validate that behavior on DEV first; never delete the original source archives.
Repair backlog checks count newly persisted ingestion versions, since an unchanged
UUID count alone cannot show whether replacement batches have arrived. Run
`verify_native_event_counts.py <prepared directory> <evidence directory>` after
complete field reconciliation. It uses the normal authenticated PostHog query API
to compare every event type's count, total rows, UUIDs and absence of DEV fixtures;
extra physical versions must finish merging before normal query counts can pass.

Run the offline contract checks with:

```sh
python3 -m unittest discover -s . -p 'test_*.py' -v
```

The 121-event DEV validation covers all 120 observed source event types and the
newest event boundary. It does not certify the full PROD backfill, current-profile
restoration, future SDK identity continuity, chart/cohort parity or replay playback.

## Numeric precision

The installed capture image uses `serde_json` 1.0.149 with its default best-effort
float parser. Complete historical comparison found a one-step binary float change;
the original record bytes still matched. A synthetic 60,000-number regression
reproduced 8,719 value changes with the default parser and zero with
`serde_json/float_roundtrip`. Keep the full comparison strict: a one-step change
fails, and there is no numerical tolerance. JSON permits `1` and `1.0` to represent
the same numeric value; booleans, strings, arrays and objects remain distinct.
Original numeric text remains available in the unchanged original record bytes.

`build_precise_capture.py` builds the exact capture revision with that Cargo
feature enabled. It keeps upstream source unchanged, pins the compiler and runtime
base images, limits compilation to two cores/eight GiB, mounts no app credentials,
and does not deploy the result. Validate the resulting image on DEV, preserve its
image archive and immutable ID, then repair and reconcile the entire offline PROD
backfill. Full original-field comparison passed for all 556,777 events after the
correction. Event backfill acceptance alone does not authorize connector cutover;
the full archive, profile/identity/reporting/replay parity and live backup remain
separate acceptance requirements.

`verify_capture_precision.py <evidence directory>` first captures 600 synthetic DEV
events with the original image and records the drift. After compilation,
`deploy_precise_capture.py` verifies executable loading, exports the exact image
under `custom-images/`, retains its checksum and restore instructions, and changes
only the capture service's immutable image pin. That image archive is included in
the runtime checkpoint; isolated restore checks its identity and checksum too.
Run the DEV validator again with `--corrected`: the same UUIDs must retain every
number exactly and acquire the new validation phase. The verified live DEV test
found 74 changed values before the correction and zero after it. Synthetic fixtures
remain in DEV; no original PROD UUIDs are used in the precision test.

Four archived events have no source IP address. The capture HTTP endpoint replaces
an omitted, null, false or empty `$ip` with the migration server address. The
DEV-only `verify_missing_ip.py` reproduces that behavior. `repair_missing_ip.py`
tests a new explicit fixture through the normal historical queue envelope before
`--prod` can correct just the four original UUIDs, retaining every source field and
original record byte. Its empty transport IP prevents the Node normalizer's
fallback. Source timestamps must fit the installed millisecond ingestion
precision; the four affected source timestamps do. The synthetic test also
verifies the original source bytes and typed properties. No source archive or
project privacy setting is changed.

Background ReplacingMergeTree merges do not guarantee normal query deduplication.
After full field reconciliation, `merge_offline_event_versions.py <evidence>`
merges the affected monthly partitions sequentially. Before and after it computes
a cryptographic fingerprint over UUID, name, identity, exact timestamp and every
property byte, and verifies both environments' unique UUID counts. Fingerprinting
one month at a time keeps reads within one GiB. Verify normal API counts after the
merge; the 14-month offline maintenance preserved all reconciled native fields.

PostHog describes this self-hosted deployment as unsupported and offers no data
loss guarantee. Installation acceptance does not certify migration parity,
historical recording playback or completion of the PROD Amplitude archive.
