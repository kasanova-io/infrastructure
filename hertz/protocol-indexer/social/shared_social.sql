-- Installed only after the configured canonical source and empty/unowned legacy
-- projection checks pass. No upstream scanner table/cursor is replaced.
CREATE SCHEMA IF NOT EXISTS social_shared;
CREATE TABLE IF NOT EXISTS social_shared.config (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    network text NOT NULL,
    source_id text NOT NULL
);
CREATE TABLE IF NOT EXISTS social_shared.actions (
    transaction_id bytea PRIMARY KEY CHECK(octet_length(transaction_id)=32),
    revision bigint NOT NULL,
    subject text,
    native_table text,
    state_action text,
    source_payload text NOT NULL,
    original_time bigint NOT NULL,
    status text NOT NULL CHECK(status IN ('verified','unsupported_or_invalid')),
    notification_consumed boolean NOT NULL DEFAULT false,
    snapshot jsonb
);
CREATE INDEX IF NOT EXISTS social_shared_subject ON social_shared.actions(subject,original_time,transaction_id);
CREATE INDEX IF NOT EXISTS social_shared_effective ON social_shared.actions(subject,state_action,original_time,transaction_id);
