# syntax=docker/dockerfile:1
ARG RUST_IMAGE
ARG RUNTIME_IMAGE
FROM ${RUST_IMAGE} AS source
RUN apk add --no-cache musl-dev protobuf-dev g++ clang18-dev linux-headers openssl-dev postgresql-dev curl patch git python3
ARG UPSTREAM_REVISION=917863f49623aa6802e5ad4c0e246581470d93cb
ARG UPSTREAM_ARCHIVE_SHA256
ADD https://github.com/thesheepcat/K-indexer/archive/${UPSTREAM_REVISION}.tar.gz /tmp/upstream.tar.gz
RUN test -n "$UPSTREAM_ARCHIVE_SHA256" && echo "$UPSTREAM_ARCHIVE_SHA256  /tmp/upstream.tar.gz" | sha256sum -c -
WORKDIR /src
RUN tar -xzf /tmp/upstream.tar.gz --strip-components=1
COPY prepare_manifests.py /tmp/prepare_manifests.py
RUN python3 /tmp/prepare_manifests.py
COPY transactional-protocol.patch /tmp/transactional-protocol.patch
RUN patch -p1 < /tmp/transactional-protocol.patch
COPY shared_social.rs shared_social.sql notification_capture.sql notification_capture.sha256 /src/K-transaction-processor/src/
COPY transaction_tests.rs /src/K-transaction-processor/src/shared_protocol_tests.rs
COPY real_kpost.json /src/K-transaction-processor/src/real_kpost.json
ENV CARGO_BUILD_JOBS=3 RUSTFLAGS="-C link-args=-s"

# Preparation only: extract/review this lock before the tested immutable build.
FROM source AS lock
RUN --mount=type=cache,id=kasanova-social-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=kasanova-social-git,target=/usr/local/cargo/git \
    cargo generate-lockfile

FROM source AS build
COPY Cargo.lock /src/Cargo.lock
RUN --mount=type=cache,id=kasanova-social-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=kasanova-social-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=kasanova-social-target-rust196-sdk01b532,target=/src/target,sharing=locked \
    touch /src/K-transaction-processor/src/*.rs && \
    (cargo test --locked --release --workspace --features shared-postgres-contract --no-run --message-format=json > /tmp/test-artifacts.jsonl || { python3 -c 'import json,pathlib; rows=[json.loads(v) for v in pathlib.Path("/tmp/test-artifacts.jsonl").read_text().splitlines()]; [print(v.get("message",{}).get("rendered","")) for v in rows if v.get("reason")=="compiler-message"]'; exit 1; }) && \
    python3 -c 'import json,pathlib,shutil; rows=[json.loads(v) for v in pathlib.Path("/tmp/test-artifacts.jsonl").read_text().splitlines()]; paths=[v["executable"] for v in rows if v.get("reason")=="compiler-artifact" and v.get("profile",{}).get("test") and v.get("executable")]; assert len(paths)==1; pathlib.Path("/out").mkdir(exist_ok=True); shutil.copyfile(paths[0],"/out/social-tests"); pathlib.Path("/out/social-tests").chmod(0o755)'
RUN /out/social-tests --test-threads=1 pure_
RUN --mount=type=cache,id=kasanova-social-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=kasanova-social-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=kasanova-social-target-rust196-sdk01b532,target=/src/target,sharing=locked \
    touch /src/K-transaction-processor/src/*.rs && cargo build --locked --release --bin K-transaction-processor && cp /src/target/release/K-transaction-processor /out/social-processor
RUN /out/social-tests --test-threads=1 --skip postgres_

FROM ${RUNTIME_IMAGE} AS runtime
RUN apk add --no-cache ca-certificates libssl3 libcrypto3 libgcc libstdc++
WORKDIR /app
COPY --from=source /src/LICENSE /app/LICENSE.K-indexer
USER 10001:10001

FROM runtime AS contract
COPY --from=build /out/social-tests /app/social-tests
ENTRYPOINT ["/app/social-tests"]
CMD ["--test-threads=1", "postgres_"]

FROM runtime AS final
COPY --from=build /out/social-processor /app/social-processor
ENTRYPOINT ["/app/social-processor"]
