# syntax=docker/dockerfile:1
ARG RUST_IMAGE
ARG RUNTIME_IMAGE
FROM ${RUST_IMAGE} AS source
RUN apk add --no-cache musl-dev pkgconfig openssl-dev patch curl python3
ARG UPSTREAM_REVISION=917863f49623aa6802e5ad4c0e246581470d93cb
ARG UPSTREAM_ARCHIVE_SHA256
ADD https://github.com/thesheepcat/K-indexer/archive/${UPSTREAM_REVISION}.tar.gz /tmp/upstream.tar.gz
RUN test -n "$UPSTREAM_ARCHIVE_SHA256" && echo "$UPSTREAM_ARCHIVE_SHA256  /tmp/upstream.tar.gz" | sha256sum -c -
WORKDIR /src
RUN tar -xzf /tmp/upstream.tar.gz --strip-components=1
COPY prepare_web.py historical-author-keys.patch exact-vote-details.patch content-search.patch shared-api.patch shared-read-api.patch /inputs/
RUN python3 /inputs/prepare_web.py
COPY shared_rooms.rs shared_room_tests.rs shared_reads.rs shared_read_tests.rs /src/K-webserver/src/
COPY shared_room_schema.sql shared_read_schema.sql shared_edit_authorization.sql shared_unquote_order.sql shared_social_schema.sql shared_kaposts_schema.sql /src/K-webserver/src/
RUN cp /src/K-transaction-processor/src/migrations/schema/up.sql /src/K-webserver/src/native_schema.sql
ENV CARGO_BUILD_JOBS=3 RUSTFLAGS="-C link-args=-s"
FROM source AS lock
RUN --mount=type=cache,id=kasanova-social-registry,target=/usr/local/cargo/registry cargo generate-lockfile
FROM scratch AS lock-export
COPY --from=lock /src/Cargo.lock /Cargo.lock
FROM source AS build
COPY Cargo.lock /src/Cargo.lock
RUN --mount=type=cache,id=kasanova-social-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=kasanova-shared-web-rust196,target=/src/target,sharing=locked \
    touch /src/K-webserver/src/*.rs /src/K-webserver/src/*.sql /src/K-webserver/Cargo.toml /src/Cargo.toml && \
    (cargo test --locked --release --bin K-webserver --features shared-web-contract,search-contract --no-run --message-format=json > /tmp/test-artifacts.jsonl || { python3 -c 'import json,pathlib; rows=[json.loads(v) for v in pathlib.Path("/tmp/test-artifacts.jsonl").read_text().splitlines()]; [print(v.get("message",{}).get("rendered","")) for v in rows if v.get("reason")=="compiler-message"]'; exit 1; }) && \
    python3 -c 'import json,pathlib,shutil; rows=[json.loads(v) for v in pathlib.Path("/tmp/test-artifacts.jsonl").read_text().splitlines()]; paths=[v["executable"] for v in rows if v.get("reason")=="compiler-artifact" and v.get("profile",{}).get("test") and v.get("executable")]; assert len(paths)==1; pathlib.Path("/out").mkdir(exist_ok=True); shutil.copyfile(paths[0],"/out/web-tests"); pathlib.Path("/out/web-tests").chmod(0o755)'
RUN /out/web-tests --test-threads=1 --skip postgres_ --skip search_contract_tests
RUN --mount=type=cache,id=kasanova-social-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=kasanova-shared-web-rust196,target=/src/target,sharing=locked \
    touch /src/K-webserver/src/*.rs /src/K-webserver/src/*.sql /src/K-webserver/Cargo.toml /src/Cargo.toml && \
    cargo build --locked --release --bin K-webserver && cp /src/target/release/K-webserver /out/webserver
FROM ${RUNTIME_IMAGE} AS runtime
RUN apk add --no-cache ca-certificates libssl3 libcrypto3 libgcc libstdc++
WORKDIR /app
COPY --from=source /src/LICENSE /app/LICENSE.K-indexer
USER 10001:10001
FROM runtime AS contract
COPY --from=build /out/web-tests /app/web-tests
ENTRYPOINT ["/app/web-tests"]
CMD ["--test-threads=1","postgres_"]
FROM runtime AS final
COPY --from=build /out/webserver /app/webserver
ENTRYPOINT ["/app/webserver"]
