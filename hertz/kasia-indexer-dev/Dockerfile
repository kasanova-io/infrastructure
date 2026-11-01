# syntax=docker/dockerfile:1
# Build the chat service from a fixed upstream source revision. The social-feed
# processor, webserver and admin control panel are separate services upstream.
FROM rust:1.89.0-alpine3.20@sha256:8476b180807ca64ba3af8ddaaf4197d2238e0607a18628d797b3d2ed3d8ba527 AS build
RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static curl
ARG UPSTREAM_REVISION=879d34a0840c3af0117bc6de7f0ffeeda53c9338
ADD https://github.com/KaspaSilver/KaChat-Indexer/archive/${UPSTREAM_REVISION}.tar.gz /tmp/upstream.tar.gz
RUN echo "33cdbcee4fa45cb691d55d8136fc37499a42ffd300f2311bba1b70f15b27d4d4  /tmp/upstream.tar.gz" | sha256sum -c -
WORKDIR /src
RUN tar -xzf /tmp/upstream.tar.gz --strip-components=2 \
    "KaChat-Indexer-${UPSTREAM_REVISION}/kasia-indexer"
# Use the upstream lockfile, including the pinned Kaspa/fjall git dependencies.
ENV CARGO_BUILD_JOBS=3
COPY upstream-current.patch /tmp/upstream-current.patch
RUN apk add --no-cache patch && patch -p1 < /tmp/upstream-current.patch
COPY history/shared-source.patch /tmp/shared-source.patch
RUN patch -p1 < /tmp/shared-source.patch
RUN cargo test --locked --release --workspace
RUN cargo build --locked --release -p indexer

COPY history/history.rs /src/indexer/src/api/v1/history.rs
COPY history/shared_ingestion.rs /src/indexer/src/api/v1/shared_ingestion.rs
COPY history/history.patch /tmp/history.patch
RUN patch -p1 < /tmp/history.patch
COPY history/group-pagination.patch /tmp/group-pagination.patch
RUN patch -p1 < /tmp/group-pagination.patch
COPY history/pagination.rs /src/indexer/src/api/v1/pagination.rs
COPY history/personal_pagination_tests.rs /src/indexer/src/api/v1/personal_pagination_tests.rs
COPY history/personal-pagination.patch /tmp/personal-pagination.patch
RUN patch -p1 < /tmp/personal-pagination.patch
COPY history/explicit-pagination.patch /tmp/explicit-pagination.patch
RUN patch -p1 < /tmp/explicit-pagination.patch
COPY history/shared-lifecycle.patch /tmp/shared-lifecycle.patch
RUN patch -p1 < /tmp/shared-lifecycle.patch
COPY history/shared-dm.patch /tmp/shared-dm.patch
RUN patch -p1 < /tmp/shared-dm.patch
COPY history/inbox.rs /src/indexer/src/api/v1/inbox.rs
COPY history/shared_mutator_tests.rs /src/indexer/src/api/v1/shared_mutator_tests.rs
COPY history/shared_runtime.rs /src/indexer/src/shared_runtime.rs
COPY history/shared-runtime.patch /tmp/shared-runtime.patch
RUN patch -p1 < /tmp/shared-runtime.patch
COPY history/shared_notifications.rs /src/indexer/src/api/v1/shared_notifications.rs
COPY history/shared_notification_tests.rs /src/indexer/src/api/v1/shared_notification_tests.rs
COPY history/shared-notifications.patch /tmp/shared-notifications.patch
RUN patch -p1 < /tmp/shared-notifications.patch
COPY history/shared_activity.rs /src/indexer/src/api/v1/shared_activity.rs
COPY history/shared_activity_tests.rs /src/indexer/src/api/v1/shared_activity_tests.rs
COPY history/shared_activity_schema.sql /src/indexer/src/api/v1/shared_activity_schema.sql
COPY history/shared-activity.patch /tmp/shared-activity.patch
RUN patch -p1 < /tmp/shared-activity.patch
COPY history/notification-Cargo.toml /src/indexer/Cargo.toml
COPY history/notification-Cargo.lock /src/Cargo.lock
RUN cargo test --locked --release --workspace
RUN cargo build --locked --release -p indexer

FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc AS runtime
RUN apk add --no-cache ca-certificates libssl3 libcrypto3 libgcc
ARG UPSTREAM_REVISION=879d34a0840c3af0117bc6de7f0ffeeda53c9338
LABEL org.opencontainers.image.source="https://github.com/KaspaSilver/KaChat-Indexer" \
      org.opencontainers.image.revision="${UPSTREAM_REVISION}"
WORKDIR /app
COPY --from=build /src/target/release/indexer /app/indexer
COPY history/LICENSE.KaChat-Indexer /app/LICENSE.KaChat-Indexer
EXPOSE 8080
# Upstream flushes its embedded database on SIGINT. Allow it to finish.
STOPSIGNAL SIGINT
ENTRYPOINT ["/app/indexer"]

# Explicit QA target uses the exact compiled workspace test binaries. The normal
# final image remains the minimal runtime and contains no compiler/test harness.
FROM build AS notification-contract
RUN mkdir /qa && cargo test --locked --release --workspace --no-run --message-format=json > /qa/artifacts.json \
    && sed -n 's/.*"executable":"\([^"]*\/target\/release\/deps\/[^"]*\)".*/\1/p' /qa/artifacts.json > /qa/paths \
    && test "$(wc -l < /qa/paths)" -eq 4 \
    && while IFS= read -r executable; do \
        file="${executable##*/}"; package="${file%%-*}"; \
        case "$package" in indexer|indexer_db|indexer_actors|protocol) ;; *) exit 1 ;; esac; \
        test ! -e "/qa/${package}.test" && cp "$executable" "/qa/${package}.test"; \
    done < /qa/paths
COPY history/run_notification_contracts.sh /qa/run-tests.sh
RUN chmod 755 /qa/run-tests.sh
USER 10001:10001
ENTRYPOINT ["/qa/run-tests.sh"]

FROM runtime AS release
