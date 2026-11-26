#!/bin/sh
# Prebuilt native workspace tests: no Cargo writes or network are needed here.
set -eu
count=0
printf 'kasanova-test-phase=ordinary\n'
for binary in /qa/*.test; do
    test -f "$binary"
    count=$((count + 1))
    # Existing fixtures deliberately exercise standalone MAINNET contexts.
    # The following PG phase supplies the shared TESTNET handler environment.
    env -u KASANOVA_SHARED_INGESTION_ENABLED -u KASANOVA_SHARED_NETWORK \
        -u NETWORK_TYPE -u KASANOVA_SHARED_INGEST_TOKEN -u PROTOCOL_DATABASE_URL \
        -u PROTOCOL_NATIVE_NOTIFICATION_TEST_DSN -u PROTOCOL_SOURCE_BINDING \
        "$binary" --test-threads=1
done
test "$count" -eq 4
printf 'kasanova-test-phase=postgresql\n'
/qa/indexer.test --ignored --test-threads=1
