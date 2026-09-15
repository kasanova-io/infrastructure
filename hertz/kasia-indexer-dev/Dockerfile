# syntax=docker/dockerfile:1
# Build the chat service from a fixed upstream source revision. The social-feed
# processor, webserver and admin control panel are separate services upstream.
FROM rust:1.89.0-alpine3.20 AS build
RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static curl
ARG UPSTREAM_REVISION=018882c448d248ca96d51dac107a8b0ed44fb101
ADD https://github.com/KaspaSilver/KaChat-Indexer/archive/${UPSTREAM_REVISION}.tar.gz /tmp/upstream.tar.gz
WORKDIR /src
RUN tar -xzf /tmp/upstream.tar.gz --strip-components=2 \
    "KaChat-Indexer-${UPSTREAM_REVISION}/kasia-indexer"
# Use the upstream lockfile, including the pinned Kaspa/fjall git dependencies.
ENV CARGO_BUILD_JOBS=3
RUN cargo test --locked --release --workspace
RUN cargo build --locked --release -p indexer

COPY history/history.rs /src/indexer/src/api/v1/history.rs
COPY history/history.patch /tmp/history.patch
RUN apk add --no-cache patch && patch -p1 < /tmp/history.patch
COPY history/group-pagination.patch /tmp/group-pagination.patch
RUN patch -p1 < /tmp/group-pagination.patch
COPY history/pagination.rs /src/indexer/src/api/v1/pagination.rs
COPY history/personal_pagination_tests.rs /src/indexer/src/api/v1/personal_pagination_tests.rs
COPY history/personal-pagination.patch /tmp/personal-pagination.patch
RUN patch -p1 < /tmp/personal-pagination.patch
RUN cargo test --locked --release --workspace
RUN cargo build --locked --release -p indexer

FROM alpine:3.20
RUN apk add --no-cache ca-certificates libssl3 libcrypto3 libgcc
ARG UPSTREAM_REVISION=018882c448d248ca96d51dac107a8b0ed44fb101
LABEL org.opencontainers.image.source="https://github.com/KaspaSilver/KaChat-Indexer" \
      org.opencontainers.image.revision="${UPSTREAM_REVISION}"
WORKDIR /app
COPY --from=build /src/target/release/indexer /app/indexer
EXPOSE 8080
# Upstream flushes its embedded database on SIGINT. Allow it to finish.
STOPSIGNAL SIGINT
ENTRYPOINT ["/app/indexer"]
