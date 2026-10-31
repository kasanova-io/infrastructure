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
