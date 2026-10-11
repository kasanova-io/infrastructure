-- Only the independent history staging PostgreSQL service mounts this file.
CREATE TABLE transactions (transaction_id BYTEA PRIMARY KEY, payload BYTEA NOT NULL, block_time BIGINT NOT NULL);
CREATE TABLE history_stage_guard (singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton), purpose TEXT NOT NULL CHECK(purpose='isolated-social-history'));
INSERT INTO history_stage_guard VALUES (TRUE, 'isolated-social-history');
CREATE TABLE history_replay (transaction_id BYTEA PRIMARY KEY, record JSONB NOT NULL, ordinal BIGINT NOT NULL UNIQUE);
CREATE TABLE history_batch (singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton), sha256 TEXT NOT NULL, network TEXT NOT NULL);
CREATE TABLE history_pending_undo (transaction_id BYTEA PRIMARY KEY, record JSONB NOT NULL, predecessor JSONB NOT NULL);
CREATE TABLE history_lineage (sha256 TEXT PRIMARY KEY, parent_sha256 TEXT UNIQUE, manifest JSONB NOT NULL, complete BOOLEAN NOT NULL DEFAULT FALSE);
