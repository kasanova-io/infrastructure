# Native Social projection foundation

This component reuses the pinned K-social processor's parser, signature verifier,
storage statements and schema. The patch accepts both `k:1` and `kchat:1` for the
existing signed actions without KaChat's exclusivity/personal-mode filters.
Signed content, including the KaChat marker, remains unchanged.

`verify_transaction` uses Kaspa wallet-core and returns an opaque envelope owning
the exact transaction. `apply_verified_in` requires the caller's SQL transaction;
private storage helpers never check out another connection. The established
`process_k_transaction` entry point keeps its API and commits those same native
effects atomically. Invalid signatures/key forms never receive an envelope.

The existing standalone listener retains its original K-social inputs. Opt-in
shared mode uses the common canonical journal, with revision fencing, native
state reconciliation, stable cursors and quiet replay described below. This is
an unactivated private candidate. Legacy history reconciliation, remaining
KaChat extensions and six-family acceptance precede public cutover. No Python
replacement signature parser is used.

`SOURCE.lock` records upstream/SDK/patch/lock/image identities. The SDK revision
matches the KaChat reference lock; the genuine historical K-post fixture verifies
backward signature compatibility. Its prefixed KaChat counterpart is explicitly
a synthetic compatibility vector, not an additional chain transaction.

Build `Dockerfile` from this directory with explicit immutable `RUST_IMAGE`,
`RUNTIME_IMAGE` and `UPSTREAM_ARCHIVE_SHA256`. Dependencies are frozen in Cargo.lock;
the `lock` target is preparation only, not the tested build. The final build runs
the complete native unit suite. `contract` additionally packages the compiled
PostgreSQL test for a separate disposable database, without Cargo/network access.
Its `SOCIAL_TEST_DATABASE_URL` is mandatory and the database name must begin with
`protocol_test_social` before any schema reset. Never point it at a shared source
or live Social database. A fresh isolated test network/volume needs no host port.

The PostgreSQL contract uses deterministic public test keys, synthetic payloads
and the actual native SQL schema/statements. It proves atomic rollback, legacy
commit behavior and cross-root signature deduplication. It neither signs with a
wallet fixture nor broadcasts a transaction; it is not real-chain acceptance.
The original ISC notice is retained in source and both runtime images.

## Shared canonical consumer

`--shared-source-id <journal UUID>` selects the canonical consumer before pool
startup and the legacy listener/reindex path. Shutdown is registered before
connecting. Supply the existing explicit native network/database arguments; the
journal UUID and network must match the persistent common source and native
schema v2 on every iteration. Shared mode requires an existing reconciled native
schema and rejects schema-reset/upgrade flags. Fresh schema provisioning reuses
the pinned upstream schema; migration of existing public history remains a
separate prerequisite. Empty ownership metadata does not authorize adoption of
nonempty legacy tables.

The worker drains durable `native-social-v1` jobs. Signature verification, an
owned immutable action ledger, native effects, notification opportunity and job
acknowledgement share one SQL transaction. Current revisions are locked; another
selected action is share-locked without waiting and its revision compared before
projection. A contended/changed selected action rolls back and defers the current
job after releasing subject/current locks. Missing source timestamps defer for
five seconds. Correctly signed malformed native hex fields are retained with
explicit unsupported/invalid status instead of poisoning progress. SQL/storage
outages remain operational errors.

Effective state is selected using the native uniqueness rules: retain an accepted
content/vote representative, latest accepted profile, or the first activation
after the latest accepted relationship reversal. One original native action is
applied per affected subject. Unrelated history remains untouched. Persisted
snapshots restore native content, mention, hashtag and relationship cursor IDs
and original times after metadata refresh or reacceptance. Unsupported actions
remain in the raw journal and ledger for inventory replay after decoder upgrades.

A present Notifications capture function must match the approved actual function
body fingerprint in `notification_capture.sha256`; comments are insufficient for
this check. `notification_capture.sql` is the exact approved contract fixture from
Notifications commit8216791a786a730ea95ef9793a888f78da12d36e. The consumer acquires
ROW EXCLUSIVE on the same three tables locked SHARE ROW EXCLUSIVE by the owning
installer before reading the fingerprint, so a rollback installer cannot replace
the body between capability check and native insertion. An absent outbox has no
push capture and is allowed. Every applied action gets transaction-local quiet or
eligible context; only fenced evaluated opportunities are consumed. Previously
accepted history/reacceptance and duplicate signed actions stay quiet.

The initial source-binding refusal deliberately preserves existing public
history; it is not an automatic migration/cutover tool. Remaining KaChat
extensions (including verified public tips/rooms), private DM/inbox upgrades,
legacy reconciliation and six-family real acceptance still precede public
activation. Existing public consumers keep running throughout private testing.


## Verified tip facts

The tip parser, signing string and version-0 Schnorr p2pk output sum reuse pinned
KaChat source e2f462. Native Kaspa SDK verification produces an opaque owned tip;
output evidence binds its ID/payload/network and every output amount/script to
the canonical source. The fee-paying input may use a different key or script;
it is not substituted for the signed tipper. Memo ciphertext stays opaque.

Each accepted payment gets a positive, signed-BIGINT-range fact before signature
deduplication. A zero-paying or unavailable earlier copy cannot poison a later
verified paying copy. The exact accepted target is verified through the existing
SDK and its author must match. Current/selected tip and target revisions are
fenced without waiting behind a subject owner. Missing target/proof defers the
job; SQL/storage failures stay operational. Native tip+mention writes reuse the
upstream CTE in the same transaction, with the existing snapshot helpers.

`social_shared.current_tips` exposes verified current facts for later API/count
integration. It checks canonical acceptance/pending removals of the payment,
literal target and visible native representative. Signature aliases across the
two post roots share a representative without changing the original tip target.
A parent revocation hides counts immediately and removes/requeues dependent
facts during native reconciliation. Tip/mention IDs and original times survive
reacceptance. Decoder epoch is persistent; newly supported history stays quiet.

This component supplies storage, not the app counter or notification rollout.
The existing capture function has no tip trigger, so zero real outbox rows do not
prove tip-notification suppression; an append-only positive-control contract
checks eligibility until Notifications is extended. Live output-proof harvesting
and the remaining KaChat extensions/DM/history/API/counter/UI integration remain
required before public cutover. The genuine private tip sample uses an already
retained full receipt, without injecting a source/revision/evidence row or sending
another transaction.
