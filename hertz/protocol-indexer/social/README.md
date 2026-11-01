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
state reconciliation, stable cursors and quiet replay described below. The
private candidate passed a bounded upgrade on retained genuine post/tip history;
the worker stopped cleanly afterwards. The current canonical-read candidate also
passed bounded view installation and API/history preservation against that same
source. Its test worker is stopped; the lock retains the earlier SDK acceptance
identity separately. Broader history/API reconciliation and six-family real-chain
acceptance precede public cutover. No Python replacement
signature parser is used.

`SOURCE.lock` records upstream/SDK/patch/lock/image identities. The SDK revision
matches the KaChat reference lock; the genuine historical K-post fixture verifies
backward signature compatibility. Its prefixed KaChat counterpart is explicitly
a synthetic compatibility vector, not an additional chain transaction.

Prepare a fresh context using the existing shared-component build convention:

```sh
python3 prepare_context.py --output "$NATIVE_BUILD_CONTEXT"
```

The preparer verifies every frozen input in `SOURCE.lock`, including the single
journal-owned `../protocol_indexer/source_fence.sql` asset. Build `Dockerfile`
from that prepared context with explicit immutable `RUST_IMAGE`,
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

## Offline retained signing fields

The existing processor also has an explicit offline audit mode:

```sh
social-processor --network "$CONFIGURED_NETWORK" \
  --audit-retained-history "$NATIVE_FIELD_SNAPSHOT" \
  --audit-retained-history-report "$NEW_PRIVATE_REPORT"
```

The two paths are required together and conflict with schema initialization,
schema upgrade and the shared worker. This mode returns before pool connection,
schema changes, listeners or reindexing. It builds only a lazy, unacquired pool
using the existing configured options, then reuses the original native parser
and Kaspa SDK verifier. It creates a new mode-0600 metadata report and refuses
to overwrite any existing report. No new dependency, signature algorithm,
public endpoint or source ownership is introduced.

Input schema `kasanova-native-social-fields-v1` contains the exact configured
network, native schema version `2`, an explicit `assumed_root` (`k` or `kchat`),
a caller-declared snapshot digest, counts, and retained PostgreSQL JSON rows for
`public.k_contents`, `k_votes`, `k_broadcasts`, `k_follows`, `k_blocks` and
`k_mentions`. All six names must be present in both maps. The regular input file
is bounded to64MiB/100000rows. A wrong network, count, missing table or duplicate
native identity fails the audit. Mentions are ordered by their retained native
IDs; a missing or ambiguous single mention is not guessed from another author.

The report says only whether retained fields reproduce a signing string accepted
by the existing SDK. Both32-byte and valid33-byte signer forms keep the original
verifier's rules. Unrecoverable fields remain listed and cause a nonzero exit
after the report is persisted; they do not prove an invalid original transaction.
The report does not expose message bodies, public keys or signatures.

Original wire spelling/root, transaction identity, chain acceptance, source
provenance and complete history are explicitly **not verified**. In particular,
the wire root is outside these signing strings and a copied signature can verify
under another transaction ID. The declared digest is not independently checked by
this CLI: its caller must bind the input to an accepted native snapshot. Never
use this report as a canonical receipt or automatic permission to adopt legacy
rows. Native ownership/history reconciliation remains a separate reviewed step.

The `retained_signature_audit_candidate` entry in `SOURCE.lock` records the
current source inputs and offline image separately from the earlier accepted
shared writer at the top of that file. The new image passed 54 native unit tests
and a private offline check of all 26 retained native records (14 contents and
12 votes, with 15 mention rows). All signing fields were recoverable. Original
native IDs and timestamps were preserved, and wrong-network, overwrite and
schema-initialization attempts were rejected. The 39 PostgreSQL contracts were
not rerun for this isolated mode; storage, SQL and the verifier are unchanged.
All test actors stopped. No public worker, routing or ownership was activated.

## Explicit retained-history adoption

`--adopt-retained-history <inventory.json> --shared-source-id <journal UUID>` is
an operator migration mode. It exits after migration and never starts the legacy
listener, scanner or shared worker. It conflicts with initialization, upgrade
and the offline audit. Ordinary shared startup still refuses unowned history.

Quiesce the old native writer before migration and keep it fenced after adoption.
Table locks protect the migration transaction; they cannot prevent an old writer
resuming afterward. The common journal installer must first install its source
identity fence. Native migration/projection checks the actual function body and
unconditional enabled INSERT trigger under the installer's table lease. Native
retained-target checks take the matching nonblocking transaction lock before
testing canonical absence. Existing canonical revisions retain their row fences.

Inventory schema `kasanova-retained-native-inventory-v1` declares the configured
network, native schema version `2`, explicit `protocol_hint`, backup digest and
counts/row hashes for all eight native tables, plus both notification activation
and outbox tables when present. The hashes cover PostgreSQL's ordered JSONB rows,
including original IDs, times and notification state. The manifest is an
operator-bound source attestation; the CLI does not authenticate a backup file
from a digest supplied by that same caller. It checks actual target table hashes
and SDK signing-field recovery before installing ownership, in one transaction.
A failed inventory, source binding or signature check rolls back the migration.

Retained actions carry separate provenance and quiet notification state. They
never create a canonical journal row, RPC transaction or accepting block. The
reader/selector can use those facts only while no canonical row or pending
removal exists. A real canonical body immediately suppresses that fallback and
is reconciled through the normal durable queue. Matching SDK fields promote the
actual wire payload while preserving native IDs/times; contradictions are
recorded and quarantined. Existing canonical overlaps are queued atomically.
Accepted journal history at import is replayed quietly; future first acceptances
retain normal notification eligibility. Repeating the identical import is a no-op.

Tip payments still require the original canonical transaction and output proof.
The parent may be an explicitly adopted native fact; current canonical or pending
revocation takes precedence over it. This does not infer chain proof from a
retained signature or permit a missing payment proof.

`retained_history_candidate` records the new build separately from earlier
accepted runtimes. It passed 55 native units and all 50 PostgreSQL contracts,
including concurrent insert barriers, promotion/revocation/reacceptance, stable
IDs/times, quiet replay, conflicting history, retry fairness and retained-parent
tip authorization. The common Python suite passed all 117 cases (67 PostgreSQL
and 50 unit/HTTP), with no skips. Genuine retained-parent tip acceptance, full
legacy API preservation and public cutover remain pending. No public adoption
is recorded.

A complete private DEV source copy also passed actual legacy adoption: all 26
action snapshots and all 14 original post rows preserved their native IDs, times
and fields. The prepared target already had the nullable `edited_at` extension;
all original fields matched the old dump, that column was null, and the strict
import inventory bound the complete target shape. Replay preserved canonical
facts, proofs, the chain cursor and notification state. The three existing tips
reverified against their exact receipts and amounts. Their parent already had a
canonical fact, so this does not establish a new tip to a retained-only parent.
The shared reader then passed 51 real HTTP requests, preserving the original
signed fields, IDs and times in all 14 post-detail responses, all 12 original
vote DTOs and original cursor tokens. Its current
10-record feed includes one canonical KaChat post, one canonical K-Social post
and eight retained native posts. Membership, counts and ordering use current
accepted state; valid additions are allowed. Complete current responses and all
protected rows remain identical after reader and database restarts. This is API
presentation acceptance against accepted native views; signature/eligibility
checks belong to the separately recorded native contracts and adoption. Full
native Chats history preservation and public cutover remain pending.

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
history; it is not an automatic migration/cutover tool. The source includes
verified tips, all five KaPosts extension forms and room projection. Its bounded
private source-connected upgrade preserved existing native rows, IDs, times and
notification state, and all three retained real tips pass SDK/payment validation
in the current-tip view. New extension/room transactions, native DM integration,
broader legacy history/API reconciliation and six-family real acceptance remain
required before public activation. Existing public consumers keep running
throughout private testing.


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
checks eligibility until Notifications is extended. Source-connected proof harvesting, genuine extension/DM/room acceptance and
history/API/counter/UI integration remain required before public cutover. The genuine private tip sample uses an already
retained full receipt, without injecting a source/revision/evidence row or sending
another transaction.


## Public room broadcasts and shared transaction identity

Both `kchat:1:bcast:<channel>:<content>` and `ciph_msg:1:bcast:...` use the pinned
KaChat public-room envelope and pure self-send rule: the address derived from
input0's spent output must equal the address derived from output0. Rooms have no
payload author signature. The original ISC notice is byte-identical to the
retained K-indexer notice. Exact upstream source hashes are in `SOURCE.lock`.
Channel trimming/lowercasing and the native 36-character bound are preserved;
content, including colons, is stored verbatim. Invalid UTF-8/NUL is terminal.
Operator language/art/personal filters and upstream RPC singletons are not
installed in shared infrastructure.

The configured-network SDK recomputes current and previous transaction IDs and
extracts both script addresses. The previous transaction must match the exact
canonical input0 outpoint. Canonical input indexes/outpoints/signature bytes,
optional resolved amount/script, and every current output are bound to the
receipt. Missing verbose address metadata is allowed; a supplied sender claim
must match the SDK address. Complete mismatched or non-addressable scripts
receive no room attribution. Private Chats keeps its existing strict sender
address contract. Transaction identity is not an independent consensus, UTXO or
signature check; current acceptance remains owned by the canonical journal.
The same current-ID check strengthens tip output evidence. Only three already
transitive SDK packages become direct dependencies, with versions/sources and
checksums unchanged in the locked graph.

Missing room proof rolls back projection and durably admits the existing bounded
proof worker as `parsed_room_sender_pending:<revision>`. An SDK-rejected stored
proof additionally carries its exact PostgreSQL receipt fingerprint, for both
room and tip admission. Forced recovery bypasses current/prior body cache files
without replacing them, keeps the rejected SQL receipt as an audit artifact,
and stores a fresh proof only through an atomic SQL fingerprint comparison.
If another writer wins during HTTP, its proof is revalidated and reused or the
job defers. No newer proof, canonical history or native fact is overwritten by
an old rejection. Missing/stale/wrong-kind admission never authorizes HTTP.

Room effects and acknowledgements are atomic. Revocation removes the native
row; reacceptance restores its original ID/time. `social_shared.current_broadcasts`
also hides pending removals and stale revisions before reconciliation. New
nonempty unowned room history is refused. Rooms remain quiet, without adding
notifications, retention jobs, another scanner or public endpoints. This view
is internal storage, not a claim of public room API or history compatibility.


## Existing Social read compatibility

The bound writer installs `social_shared_api` views over the existing native
shapes. The existing K-webserver can use them through a local read-only search
path, without new DTOs, cursor IDs or native row copies. Current acceptance,
revision and pending removal are checked at read time before asynchronous
reconciliation. Selection functions mirror native subject policy; a unit check
requires their normalized SQL to match. Edit authorization and unquote tuple
ordering are shared source predicates rather than parallel implementations.

Writer selection retains immutable SDK verification when canonical metadata
advances and fences the latest canonical revision before applying the selected
action. It does not require that candidate's ledger metadata refresh to have
already run. API eligibility still requires current ledger/canonical revisions.
Native concurrency tests retain selected-row contention and canonical-fence
checks unchanged. The frozen candidate passes 47 unit and 39 actual PostgreSQL
contracts; this is isolated SQL acceptance, not new genuine chain acceptance.


Bounded source-connected read acceptance passed 24 checks on the retained private
DEV source: normal native bind installed ten views, all native/outbox/config rows
were unchanged, and the new reader returned identical JSON for eight existing
API requests, including the original post and both cursor directions. One post
and three current verified tip facts totaling 1.2 KAS remain visible. This bind did
not reverify those historical payments; their prior SDK acceptance is retained.
All temporary services stopped. No public route, genuine new DM/room/extension,
recipient tip notification or displayed counter acceptance is implied.
