//! Offline recoverability of retained native signing fields, not chain evidence.
//!
//! A native projection loses original wire spelling, root and transaction body.
//! Reconstruct only an SDK signature-check candidate from retained fields. Never
//! turn this report into a canonical receipt or write native/source ownership.
use crate::{config::AppConfig, database::Transaction, k_protocol::KProtocolProcessor};
use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{collections::{BTreeMap, BTreeSet}, fs::{self, OpenOptions}, io::{Read, Write}, path::Path};

const MAX_INPUT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ROWS: usize = 100_000;
const TABLES: [&str; 6] = [
    "public.k_contents", "public.k_votes", "public.k_broadcasts",
    "public.k_follows", "public.k_blocks", "public.k_mentions",
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    schema: String,
    network: String,
    native_schema_version: String,
    assumed_root: String,
    declared_snapshot_sha256: String,
    table_counts: BTreeMap<String, usize>,
    tables: BTreeMap<String, Vec<Value>>,
}

#[derive(Serialize, Debug)]
pub(crate) struct Record {
    pub native_table: String,
    pub native_id: i64,
    pub transaction_id: String,
    pub original_native_time: i64,
    pub signing_fields_recoverable: bool,
    pub reason: Option<&'static str>,
}

#[derive(Serialize, Debug)]
pub(crate) struct Report {
    pub schema: &'static str,
    pub network: String,
    pub assumed_root: String,
    pub declared_snapshot_sha256: String,
    pub caller_snapshot_provenance_verified: bool,
    pub original_wire_verified: bool,
    pub original_transaction_identity_verified: bool,
    pub chain_acceptance_verified: bool,
    pub complete_history_verified: bool,
    pub recoverable_records: usize,
    pub unrecoverable_records: usize,
    pub retained_mention_rows: usize,
    pub records: Vec<Record>,
}

fn number(row: &Value, field: &str) -> Result<i64> {
    row[field].as_i64().ok_or_else(|| anyhow::anyhow!("invalid retained integer field"))
}

fn text<'a>(row: &'a Value, field: &str) -> Result<&'a str> {
    let value = row[field].as_str().ok_or_else(|| anyhow::anyhow!("missing retained text field"))?;
    ensure!(!value.contains(':') && !value.chars().any(|c| c.is_control() && !matches!(c, '\n'|'\r'|'\t')),
        "retained field cannot be reproduced by the native wire parser");
    Ok(value)
}

fn bytes(row: &Value, field: &str) -> Result<String> {
    let raw = row[field].as_str().and_then(|s| s.strip_prefix("\\x"))
        .ok_or_else(|| anyhow::anyhow!("invalid retained bytea field"))?;
    Ok(hex::encode(hex::decode(raw)?))
}

fn single_mention(hints: &[Value], kind: &str) -> Result<String> {
    let mut values = BTreeSet::new();
    for hint in hints {
        ensure!(text(hint, "content_type")? == kind, "mixed retained mention kinds");
        values.insert(bytes(hint, "mentioned_pubkey")?);
    }
    ensure!(values.len() == 1, "retained single-mention field is absent or ambiguous");
    Ok(values.into_iter().next().unwrap())
}

fn candidate(root: &str, table: &str, row: &Value, hints: &[Value], id: &str, time: i64) -> Result<Transaction> {
    let key = bytes(row, "sender_pubkey")?;
    let signature = bytes(row, "sender_signature")?;
    let (kind, tail) = match table {
        "public.k_contents" => {
            let message = text(row, "base64_encoded_message")?;
            match text(row, "content_type")? {
                kind @ ("post" | "reply") => {
                    let mut mentions = Vec::new();
                    for hint in hints {
                        ensure!(text(hint, "content_type")? == kind, "mixed retained mention kinds");
                        mentions.push(bytes(hint, "mentioned_pubkey")?);
                    }
                    let mentions = serde_json::to_string(&mentions)?;
                    if kind == "post" {
                        ensure!(row["referenced_content_id"].is_null(), "retained post has a reference");
                        ("post", format!("{message}:{mentions}"))
                    } else {
                        let parent = bytes(row, "referenced_content_id")?;
                        ("reply", format!("{parent}:{message}:{mentions}"))
                    }
                }
                "quote" => {
                    let parent = bytes(row, "referenced_content_id")?;
                    let mention = single_mention(hints, "quote")?;
                    ("quote", format!("{parent}:{message}:{mention}"))
                }
                _ => bail!("unsupported retained content kind"),
            }
        }
        "public.k_votes" => {
            let parent = bytes(row, "post_id")?;
            let vote = text(row, "vote")?;
            let mention = single_mention(hints, "vote")?;
            ("vote", format!("{parent}:{vote}:{mention}"))
        }
        "public.k_broadcasts" => {
            let nickname = text(row, "base64_encoded_nickname")?;
            let image = if row["base64_encoded_profile_image"].is_null() { "" }
                else { text(row, "base64_encoded_profile_image")? };
            let message = text(row, "base64_encoded_message")?;
            ("broadcast", format!("{nickname}:{image}:{message}"))
        }
        "public.k_follows" => {
            ensure!(text(row, "following_action")? == "follow", "not a retained active follow");
            ("follow", format!("follow:{}", bytes(row, "followed_user_pubkey")?))
        }
        "public.k_blocks" => {
            ensure!(text(row, "blocking_action")? == "block", "not a retained active block");
            ("block", format!("block:{}", bytes(row, "blocked_user_pubkey")?))
        }
        _ => bail!("unsupported retained native table"),
    };
    Ok(Transaction { transaction_id: id.into(), block_time: Some(time),
        payload: Some(hex::encode(format!("{root}:1:{kind}:{key}:{signature}:{tail}"))) })
}

pub(crate) fn audit(processor: &KProtocolProcessor, configured_network: &str, data: &[u8]) -> Result<Report> {
    ensure!(data.len() as u64 <= MAX_INPUT_BYTES, "retained input exceeds bound");
    let snapshot: Snapshot = serde_json::from_slice(data)?;
    ensure!(snapshot.schema == "kasanova-native-social-fields-v1" && snapshot.native_schema_version == "2", "unsupported retained snapshot schema");
    ensure!(snapshot.network == configured_network, "retained snapshot/configured network mismatch");
    ensure!(matches!(snapshot.assumed_root.as_str(), "k"|"kchat"), "unsupported root assumption");
    ensure!(snapshot.declared_snapshot_sha256.len() == 64 && snapshot.declared_snapshot_sha256.bytes().all(|b| b.is_ascii_hexdigit()), "invalid caller snapshot digest");
    let expected: BTreeSet<_> = TABLES.iter().map(|s| s.to_string()).collect();
    ensure!(snapshot.tables.keys().cloned().collect::<BTreeSet<_>>() == expected && snapshot.table_counts.keys().cloned().collect::<BTreeSet<_>>() == expected, "retained table inventory incomplete or unknown");
    let mut total = 0;
    for (table, rows) in &snapshot.tables {
        ensure!(snapshot.table_counts[table] == rows.len(), "retained table count mismatch");
        total += rows.len();
        ensure!(total <= MAX_ROWS, "retained row inventory exceeds bound");
    }
    let mut mentions: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut mention_ids = BTreeSet::new();
    for hint in &snapshot.tables["public.k_mentions"] {
        let id = number(hint, "id")?;
        ensure!(id > 0 && mention_ids.insert(id), "invalid or duplicate retained mention ID");
        mentions.entry(bytes(hint, "content_id")?).or_default().push(hint.clone());
    }
    for rows in mentions.values_mut() { rows.sort_by_key(|row| row["id"].as_i64().unwrap()); }
    let mut records = Vec::new();
    let mut transactions = BTreeSet::new();
    for table in TABLES.iter().filter(|&&t| t != "public.k_mentions") {
        let mut ids = BTreeSet::new();
        for row in &snapshot.tables[*table] {
            let id = bytes(row, "transaction_id")?;
            let native_id = number(row, "id")?;
            let time = number(row, "block_time")?;
            ensure!(id.len() == 64 && transactions.insert(id.clone()) && native_id > 0 && ids.insert(native_id) && time >= 0,
                "invalid or duplicate retained native identity");
            let hints = mentions.get(&id).map(Vec::as_slice).unwrap_or_default();
            let recovered = candidate(&snapshot.assumed_root, table, row, hints, &id, time)
                .ok().and_then(|tx| processor.verify_transaction(&tx)).is_some();
            records.push(Record { native_table: (*table).into(), native_id, transaction_id: id,
                original_native_time: time, signing_fields_recoverable: recovered,
                reason: if recovered { None } else { Some("retained fields did not reproduce an SDK-verifiable signing string; original transaction validity is undetermined") } });
        }
    }
    records.sort_by(|a,b| a.native_table.cmp(&b.native_table).then(a.native_id.cmp(&b.native_id)));
    let recoverable_records = records.iter().filter(|r| r.signing_fields_recoverable).count();
    Ok(Report { schema: "kasanova-native-social-signature-audit-v1", network: snapshot.network,
        assumed_root: snapshot.assumed_root, declared_snapshot_sha256: snapshot.declared_snapshot_sha256,
        caller_snapshot_provenance_verified: false, original_wire_verified: false,
        original_transaction_identity_verified: false, chain_acceptance_verified: false, complete_history_verified: false,
        unrecoverable_records: records.len() - recoverable_records, recoverable_records,
        retained_mention_rows: snapshot.tables["public.k_mentions"].len(), records })
}

pub(crate) async fn run(config: &AppConfig, input: &Path, output: &Path) -> Result<()> {
    ensure!(fs::symlink_metadata(input)?.is_file(), "retained input must be a regular file");
    let mut data = Vec::new();
    fs::File::open(input)?.take(MAX_INPUT_BYTES + 1).read_to_end(&mut data)?;
    ensure!(data.len() as u64 <= MAX_INPUT_BYTES, "retained input exceeds bound");
    // The verifier requires the existing processor type, but this mode never
    // acquires its pool. Keep connection options configured, not a new endpoint.
    let options = PgConnectOptions::new().host(&config.database.host).port(config.database.port)
        .database(&config.database.database).username(&config.database.username).password(&config.database.password);
    let pool = PgPoolOptions::new().max_connections(1).min_connections(0).connect_lazy_with(options);
    let processor = KProtocolProcessor::new(pool.clone());
    let report = audit(&processor, &config.network, &data)?;
    let mut options = OpenOptions::new(); options.write(true).create_new(true);
    #[cfg(unix)] {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(output)?;
    file.write_all(&serde_json::to_vec_pretty(&report)?)?; file.write_all(b"\n")?; file.sync_all()?;
    pool.close().await;
    println!("{}", serde_json::json!({"schema":report.schema,"recoverable_records":report.recoverable_records,"unrecoverable_records":report.unrecoverable_records,"chain_acceptance_verified":false}));
    ensure!(report.unrecoverable_records == 0, "some retained signing fields were not recoverable; inspect the new report");
    Ok(())
}
