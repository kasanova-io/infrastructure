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

The existing standalone listener still selects its original K-social inputs.
This is a tested primitive, **not an activated shared Social consumer**. Canonical
revision binding, ordering/reorg undo, original cursor/history preservation,
historical notification suppression and remaining KaChat actions/rooms must be
integrated before public cutover. No Python replacement signature parser is used.

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
