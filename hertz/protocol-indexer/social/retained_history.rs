//! Explicit native-snapshot migration. Retained facts never enter the canonical
//! journal and never supply payment proofs or fabricated accepting blocks.
use crate::{config::AppConfig, database::Transaction, k_protocol::{KProtocolProcessor, VerifiedAction}, shared_social::SharedSocial};
use anyhow::{Result, ensure, Context};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{Postgres, Row};
use std::{collections::{BTreeMap, BTreeSet}, fs, io::Read, path::Path};

const NATIVE: [&str; 8] = ["public.k_contents", "public.k_votes", "public.k_broadcasts",
    "public.k_follows", "public.k_blocks", "public.k_mentions", "public.k_hashtags", "public.k_vars"];
const NOTIFICATIONS: [&str; 2] = ["social_notifications.activation", "social_notifications.outbox"];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Inventory {
    schema: String,
    network: String,
    native_schema_version: String,
    protocol_hint: String,
    source_backup_sha256: String,
    tables: BTreeMap<String, Digest>,
    #[serde(default)]
    signing_recovery: BTreeMap<String, crate::retained_signature_audit::SigningRecovery>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Digest { count: usize, rows_sha256: String }

fn digest(value: &str) -> bool {
    value.len()==64 && value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn inventory(data: &[u8], network: &str) -> Result<Inventory> {
    ensure!(data.len()<=64*1024*1024, "retained manifest exceeds bound");
    let value: Inventory=serde_json::from_slice(data)?;
    ensure!(value.schema=="kasanova-retained-native-inventory-v1" && value.native_schema_version=="2", "unsupported retained inventory");
    ensure!(value.network==network && matches!(value.protocol_hint.as_str(),"k"|"kchat"), "retained network/root hint mismatch");
    ensure!(digest(&value.source_backup_sha256), "invalid retained source backup digest");
    let tables: BTreeSet<_>=value.tables.keys().map(String::as_str).collect();
    let native: BTreeSet<_>=NATIVE.into_iter().collect();
    let all: BTreeSet<_>=NATIVE.into_iter().chain(NOTIFICATIONS).collect();
    ensure!(tables==native || tables==all, "retained table inventory is incomplete or unknown");
    let mut count=0usize;
    for table in value.tables.values() {
        count=count.checked_add(table.count).context("retained count overflow")?;
        ensure!(count<=100_000 && digest(&table.rows_sha256), "retained table digest/count invalid");
    }
    Ok(value)
}

/// Identical authorization fields, while retaining native byte-key forms.
/// Hex spelling is lossy in PostgreSQL bytea; base64/text spelling is not.
pub(crate) fn normalized_fields(value: &VerifiedAction) -> Result<Value> {
    fn normalize(value: &mut Value, field: &str) {
        let hex_field=matches!(field,"sender_pubkey"|"sender_signature"|"post_id"|"content_id"|
            "followed_user_pubkey"|"blocked_user_pubkey"|"mentioned_pubkey"|"mentioned_pubkeys");
        match value {
            Value::Object(map)=>for (name,value) in map {normalize(value,name)},
            Value::Array(rows)=>for row in rows {normalize(row,field)},
            Value::String(text) if hex_field=>*text=text.to_ascii_lowercase(),
            _=>{},
        }
    }
    let mut fields=serde_json::to_value(value.action())?;
    normalize(&mut fields,"");
    Ok(fields)
}

/// Hold the installer's table lease while verifying its actual function body
/// and unconditional row trigger. A comment/capability name is insufficient.
pub(crate) async fn require_source_fence(tx: &mut sqlx::Transaction<'_,Postgres>) -> Result<()> {
    sqlx::query("LOCK TABLE protocol_indexer.canonical IN ROW EXCLUSIVE MODE").execute(&mut **tx).await?;
    let body=include_str!("source_fence.sql").split("$source_fence$").nth(1).context("missing common source fence body")?;
    let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_proc f ON f.oid=t.tgfoid WHERE t.tgrelid='protocol_indexer.canonical'::regclass AND t.tgname='protocol_source_identity_fence' AND t.tgenabled='O' AND t.tgtype=7 AND t.tgqual IS NULL AND t.tgnargs=0 AND t.tgattr=''::int2vector AND f.oid=to_regprocedure('protocol_indexer.fence_source_identity()') AND f.prosrc=$1 AND NOT f.prosecdef AND f.proconfig=ARRAY['search_path=pg_catalog'])")
        .bind(body).fetch_one(&mut **tx).await?;
    ensure!(valid,"common journal must enforce the retained-source identity fence before adoption or projection");
    Ok(())
}

pub(crate) async fn require_if_adopted(tx: &mut sqlx::Transaction<'_,Postgres>) -> Result<()> {
    let exists:bool=sqlx::query_scalar("SELECT to_regclass('social_shared.retained_imports') IS NOT NULL").fetch_one(&mut **tx).await?;
    if exists {
        let adopted:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM social_shared.retained_imports)").fetch_one(&mut **tx).await?;
        if adopted {require_source_fence(tx).await?;}
    }
    Ok(())
}

pub(crate) enum Observation {
    Ordinary,
    Busy,
    Conflict { subject: String, table: String },
}

/// A genuine canonical body supersedes the root assumption only after matching
/// the original native SDK fields. Contradictions are durable and fail closed.
pub(crate) async fn observe(
    tx: &mut sqlx::Transaction<'_,Postgres>, processor: &KProtocolProcessor,
    current: &Transaction, verified: Option<&VerifiedAction>, revision: i64,
) -> Result<Observation> {
    let id=hex::decode(&current.transaction_id)?;
    let previous=sqlx::query("SELECT source_payload,original_time,subject,native_table FROM social_shared.actions WHERE transaction_id=$1 AND provenance='retained_native'")
        .bind(&id).fetch_optional(&mut **tx).await?;
    let Some(previous)=previous else {return Ok(Observation::Ordinary)};
    let subject:String=previous.get("subject"); let table:String=previous.get("native_table");
    let locked:bool=sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1,736124))")
        .bind(&subject).fetch_one(&mut **tx).await?;
    if !locked {return Ok(Observation::Busy)};
    let retained=Transaction {transaction_id:current.transaction_id.clone(),
        payload:Some(previous.get("source_payload")),block_time:Some(previous.get("original_time"))};
    let old=processor.verify_transaction(&retained).context("retained signing fields no longer verify")?;
    let matches=match verified {Some(new)=>normalized_fields(new)?==normalized_fields(&old)?,None=>false};
    if matches {
        sqlx::query("UPDATE social_shared.actions SET provenance='canonical',source_payload=$2 WHERE transaction_id=$1")
            .bind(&id).bind(current.payload.as_ref()).execute(&mut **tx).await?;
        return Ok(Observation::Ordinary);
    }
    sqlx::query("UPDATE social_shared.actions SET status='unsupported_or_invalid' WHERE transaction_id=$1")
        .bind(&id).execute(&mut **tx).await?;
    sqlx::query("INSERT INTO social_shared.retained_conflicts(transaction_id,canonical_revision,reason) VALUES($1,$2,'canonical_signing_fields_differ_from_retained_snapshot') ON CONFLICT(transaction_id) DO UPDATE SET canonical_revision=EXCLUDED.canonical_revision")
        .bind(&id).bind(revision).execute(&mut **tx).await?;
    Ok(Observation::Conflict{subject,table})
}

/// Fence a real canonical revision, or the explicit retained action if there is
/// no canonical row. Read views independently suppress a concurrent revocation.
pub(crate) async fn fence(tx: &mut sqlx::Transaction<'_,Postgres>, id: &[u8], expected: i64, expected_retained: bool) -> Result<bool> {
    if !expected_retained {
        let canonical:Option<i64>=sqlx::query_scalar("SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=$1 FOR SHARE SKIP LOCKED")
            .bind(id).fetch_optional(&mut **tx).await?;
        return Ok(canonical==Some(expected));
    }
    let locked:bool=sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1,736125))")
        .bind(hex::encode(id)).fetch_one(&mut **tx).await?;
    if !locked {return Ok(false)}
    let retained:Option<i64>=sqlx::query_scalar("SELECT a.revision FROM social_shared.actions a WHERE a.transaction_id=$1 AND a.provenance='retained_native' AND a.status='verified' AND NOT EXISTS(SELECT 1 FROM protocol_indexer.canonical c WHERE c.transaction_id=a.transaction_id) AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=a.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) FOR SHARE OF a SKIP LOCKED")
        .bind(id).fetch_optional(&mut **tx).await?;
    Ok(retained==Some(expected))
}

pub(crate) async fn adopt(pool: sqlx::PgPool, network: &str, source_id: &str, data: &[u8]) -> Result<Value> {
    let manifest=inventory(data,network)?;
    let mut tx=pool.begin().await?;
    SharedSocial::check_source(&mut tx,network,source_id).await?;
    require_source_fence(&mut tx).await?;
    sqlx::query("SELECT pg_advisory_xact_lock(736124,31)").execute(&mut *tx).await?;
    let import_id:String=sqlx::query_scalar("SELECT encode(sha256($1),'hex')").bind(data).fetch_one(&mut *tx).await?;
    let exists:bool=sqlx::query_scalar("SELECT to_regclass('social_shared.retained_imports') IS NOT NULL")
        .fetch_one(&mut *tx).await?;
    if exists {
        let same:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM social_shared.retained_imports i JOIN social_shared.config c ON c.singleton AND c.network=i.network AND c.source_id=i.source_id WHERE i.import_id=$1 AND i.network=$2 AND i.source_id=$3)")
            .bind(&import_id).bind(network).bind(source_id).fetch_one(&mut *tx).await?;
        if same {tx.rollback().await?;return Ok(json!({"import_id":import_id,"already_imported":true,"canonical_receipts_created":0}))}
    }
    let owned:bool=sqlx::query_scalar("SELECT to_regclass('social_shared.config') IS NOT NULL").fetch_one(&mut *tx).await?;
    if owned {
        let nonempty:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM social_shared.config)").fetch_one(&mut *tx).await?;
        ensure!(!nonempty,"retained import requires an unowned native projection");
    }
    for table in NOTIFICATIONS {
        let exists:bool=sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL").bind(table).fetch_one(&mut *tx).await?;
        ensure!(exists==manifest.tables.contains_key(table),"retained notification inventory differs from database");
    }
    // Existing writers cannot change any retained table after its hash is read.
    sqlx::query(&format!("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE",manifest.tables.keys().cloned().collect::<Vec<_>>().join(",")))
        .execute(&mut *tx).await?;
    sqlx::query("SET LOCAL bytea_output='hex'").execute(&mut *tx).await?;
    let mut rows=BTreeMap::new();
    let mut inventory_json=serde_json::Map::new();
    for (table,expected) in &manifest.tables {
        let raw:String=sqlx::query_scalar(&format!("SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM {table} t"))
            .fetch_one(&mut *tx).await?;
        ensure!(raw.len()<=64*1024*1024,"retained native table exceeds bound");
        let hash:String=sqlx::query_scalar("SELECT encode(sha256(convert_to($1,'UTF8')),'hex')").bind(&raw).fetch_one(&mut *tx).await?;
        let value:Vec<Value>=serde_json::from_str(&raw)?;
        ensure!(value.len()==expected.count && hash==expected.rows_sha256,"retained native table inventory mismatch");
        inventory_json.insert(table.clone(),json!({"count":expected.count,"rows_sha256":hash}));
        rows.insert(table.clone(),value);
    }
    let variables:BTreeMap<_,_>=rows["public.k_vars"].iter().filter_map(|r|Some((r["key"].as_str()?,r["value"].as_str()?))).collect();
    ensure!(variables.get("network")==Some(&network) && variables.get("schema_version")==Some(&"2"),"retained native configuration mismatch");
    let tables:BTreeMap<_,_>=NATIVE.into_iter().filter(|t|!matches!(*t,"public.k_vars"|"public.k_hashtags")).map(|t|(t,rows[t].clone())).collect();
    let snapshot=json!({"schema":"kasanova-native-social-fields-v1","network":network,"native_schema_version":"2",
        "assumed_root":manifest.protocol_hint,"declared_snapshot_sha256":manifest.source_backup_sha256,
        "table_counts":tables.iter().map(|(k,v)|(*k,v.len())).collect::<BTreeMap<_,_>>(),"tables":tables,
        "signing_recovery":manifest.signing_recovery});
    let processor=KProtocolProcessor::new(pool.clone());
    let (report,actions)=crate::retained_signature_audit::recovered_fields(&processor,network,&serde_json::to_vec(&snapshot)? )?;
    ensure!(report.unrecoverable_records==0,"retained signing fields require explicit recovery before adoption");
    SharedSocial::install_in(&mut tx,network,source_id).await?;
    // Partially populated metadata never silently takes ownership of old rows.
    let clean:bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM social_shared.actions) AND NOT EXISTS(SELECT 1 FROM social_shared.retained_imports)")
        .fetch_one(&mut *tx).await?;
    ensure!(clean,"unowned shared metadata requires reconciliation");
    sqlx::query("INSERT INTO social_shared.retained_imports(import_id,network,source_id,protocol_hint,source_backup_sha256,inventory,native_records) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7)")
        .bind(&import_id).bind(network).bind(source_id).bind(&manifest.protocol_hint).bind(&manifest.source_backup_sha256)
        .bind(Value::Object(inventory_json).to_string()).bind(actions.len() as i64).execute(&mut *tx).await?;
    let mut native_rows=BTreeMap::new();
    for table in ["k_contents","k_votes","k_broadcasts","k_follows","k_blocks"] {
        for row in &rows[&format!("public.{table}")] {
            native_rows.insert((table,row["transaction_id"].as_str().context("retained transaction ID missing")?),row);
        }
    }
    let mut children:BTreeMap<(&str,&str),Vec<&Value>>=BTreeMap::new();
    for child in ["k_mentions","k_hashtags"] {
        for row in &rows[&format!("public.{child}")] {
            let content=row["content_id"].as_str().context("retained child content ID missing")?;
            children.entry((child,content)).or_default().push(row);
        }
    }
    for action in &actions {
        let value=action.transaction();let id=hex::decode(&value.transaction_id)?;
        let (subject,table)=crate::shared_social::subject(action);
        let native_id=format!("\\x{}",value.transaction_id);
        let native=native_rows.get(&(table,native_id.as_str())).context("retained action lost native row")?;
        let mut saved=json!({"main":native});
        for child in ["k_mentions","k_hashtags"] {
            saved[child]=json!(children.get(&(child,native_id.as_str())).cloned().unwrap_or_default());
        }
        sqlx::query("INSERT INTO social_shared.actions(transaction_id,revision,subject,native_table,state_action,source_payload,original_time,status,notification_consumed,snapshot,provenance,retained_import_id) VALUES($1,0,$2,$3,$4,$5,$6,'verified',true,$7::jsonb,'retained_native',$8)")
            .bind(&id).bind(subject).bind(table).bind(crate::shared_social::operation(action))
            .bind(value.payload.as_ref()).bind(value.block_time).bind(saved.to_string()).bind(&import_id).execute(&mut *tx).await?;
        crate::kaposts_projection::record_core(&mut tx,action).await?;
    }
    // Reuse the existing native consumer queue; do not fabricate revisions or
    // change old event notification eligibility. Only accepted-at-import rows
    // receive a durable quiet marker. Later first acceptances remain live.
    let families="(c.family IN ('kachat_posts','kachat_tips_and_co','ksocial_posts','ksocial_co') OR (c.family='kachat_messages' AND substring(c.source_transaction->>'payload' FROM 3 FOR 28)='6b636861743a313a62636173743a') OR EXISTS(SELECT 1 FROM social_shared.actions a WHERE a.transaction_id=c.transaction_id AND a.retained_import_id=$1))";
    sqlx::query(&format!("INSERT INTO social_shared.retained_replay SELECT c.transaction_id,c.revision,$1 FROM protocol_indexer.canonical c WHERE c.accepting_block IS NOT NULL AND {families} ON CONFLICT DO NOTHING"))
        .bind(&import_id).execute(&mut *tx).await?;
    sqlx::query(&format!("INSERT INTO protocol_indexer.consumer_jobs(consumer,transaction_id) SELECT 'native-social-v1',c.transaction_id FROM protocol_indexer.canonical c WHERE {families} ON CONFLICT DO NOTHING"))
        .bind(&import_id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(json!({"import_id":import_id,"already_imported":false,"retained_native_records":actions.len(),
        "canonical_receipts_created":0,"original_wire_verified":false,"chain_acceptance_verified":false,"notification_replays":0}))
}

pub(crate) async fn run(config: &AppConfig, input: &Path, source_id: &str) -> Result<()> {
    ensure!(fs::symlink_metadata(input)?.is_file(),"retained manifest must be a regular file");
    let mut data=Vec::new();fs::File::open(input)?.take(64*1024*1024+1).read_to_end(&mut data)?;
    inventory(&data,&config.network)?;
    let pool=crate::database::create_pool(config).await?;
    let result=adopt(pool.clone(),&config.network,source_id,&data).await?;
    pool.close().await;println!("{}",result);Ok(())
}

/// Complete RPC material supplies a payload root, not a new chain receipt.
/// The caller names an existing import; retained fields come from that store.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RootEvidence {
    schema: String,
    network: String,
    source_id: String,
    retained_import_id: String,
    transactions: BTreeMap<String, Value>,
}

fn root_evidence(data: &[u8], network: &str, source_id: &str) -> Result<RootEvidence> {
    ensure!(data.len() <= 64 * 1024 * 1024, "retained root material exceeds bound");
    let input: RootEvidence = serde_json::from_slice(data)?;
    ensure!(input.schema == "kasanova-retained-root-evidence-v1", "unsupported retained root material");
    ensure!(input.network == network && input.source_id == source_id, "retained root source/network mismatch");
    ensure!(digest(&input.retained_import_id), "invalid retained import identity");
    ensure!(!input.transactions.is_empty() && input.transactions.len() <= 256, "retained root batch must contain 1 to 256 transactions");
    ensure!(input.transactions.keys().all(|id| digest(id)), "invalid retained transaction identity");
    Ok(input)
}

/// Persist only SDK-verified root metadata. The transaction/subject fences are
/// the same ones used by native replay and common canonical insertion. No row
/// in actions, canonical, pending, jobs, native tables or notifications changes.
pub(crate) async fn recover_roots(
    pool: sqlx::PgPool, network: &str, source_id: &str, data: &[u8],
) -> Result<Value> {
    let input = root_evidence(data, network, source_id)?;
    let processor = KProtocolProcessor::new(pool.clone());
    let mut tx = pool.begin().await?;
    sqlx::raw_sql("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='30s'")
        .execute(&mut *tx).await?;
    SharedSocial::check_source(&mut tx, network, source_id).await?;
    require_source_fence(&mut tx).await?;
    let owned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM social_shared.retained_imports i JOIN social_shared.config cfg ON cfg.singleton AND cfg.network=i.network AND cfg.source_id=i.source_id WHERE i.import_id=$1 AND i.network=$2 AND i.source_id=$3)")
        .bind(&input.retained_import_id).bind(network).bind(source_id)
        .fetch_one(&mut *tx).await?;
    ensure!(owned, "retained import does not belong to the configured source");
    for (id, rpc) in &input.transactions {
        let bytes = hex::decode(id)?;
        let subject: String = sqlx::query_scalar("SELECT subject FROM social_shared.actions WHERE transaction_id=$1 AND retained_import_id=$2 AND provenance='retained_native' AND status='verified'")
            .bind(&bytes).bind(&input.retained_import_id).fetch_one(&mut *tx).await?;
        let subject_locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1,736124))")
            .bind(&subject).fetch_one(&mut *tx).await?;
        ensure!(subject_locked, "retained subject is busy; retry the same material later");
        // The source INSERT trigger takes this same transaction-ID lock. Hold
        // it before checking absence so concurrent canonical capture cannot be
        // mistaken for retained-only eligibility.
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1,736125))")
            .bind(id).fetch_one(&mut *tx).await?;
        ensure!(locked, "retained source identity is busy; retry later");
        let row = sqlx::query("SELECT a.source_payload,a.original_time,a.revision FROM social_shared.actions a JOIN social_shared.source_facts fact USING(transaction_id) WHERE a.transaction_id=$1 AND a.retained_import_id=$2 AND a.provenance='retained_native' AND a.status='verified' AND a.snapshot IS NOT NULL AND fact.eligible AND fact.retained_native AND fact.revision=a.revision FOR SHARE OF a SKIP LOCKED")
            .bind(&bytes).bind(&input.retained_import_id).fetch_optional(&mut *tx).await?
            .context("retained action is busy, changed or no longer eligible")?;
        let original = Transaction {
            transaction_id: id.clone(), payload: Some(row.get("source_payload")),
            block_time: Some(row.get("original_time")),
        };
        let retained = processor.verify_transaction(&original)
            .context("retained signing fields no longer verify")?;
        let root = crate::retained_signature_audit::checked_wire_root(
            &processor, id, original.block_time.unwrap(), &retained, rpc,
        )?;
        let result = sqlx::query("INSERT INTO social_shared.retained_wire_roots AS proof(transaction_id,retained_import_id,action_revision,original_time,source_payload_sha256,root,rpc_body_sha256,sdk_revision) SELECT transaction_id,retained_import_id,revision,original_time,sha256(convert_to(source_payload,'UTF8')),$3,sha256(convert_to($4,'UTF8')),'01b532e8b553523216471682649693af92f0fd16' FROM social_shared.actions WHERE transaction_id=$1 AND retained_import_id=$2 ON CONFLICT(transaction_id) DO UPDATE SET retained_import_id=EXCLUDED.retained_import_id,action_revision=EXCLUDED.action_revision,original_time=EXCLUDED.original_time,source_payload_sha256=EXCLUDED.source_payload_sha256,rpc_body_sha256=EXCLUDED.rpc_body_sha256 WHERE proof.root=EXCLUDED.root")
            .bind(&bytes).bind(&input.retained_import_id).bind(&root)
            .bind(serde_json::to_string(rpc)?).execute(&mut *tx).await?;
        ensure!(result.rows_affected() == 1, "conflicting retained payload-root evidence");
    }
    tx.commit().await?;
    Ok(json!({"schema":"kasanova-retained-root-recovery-v1",
        "verified_root_records":input.transactions.len(),"canonical_receipts_created":0,
        "chain_acceptance_verified":false,"native_rows_changed":0,"notification_rows_changed":0}))
}

pub(crate) async fn run_root_recovery(config: &AppConfig, input: &Path, source_id: &str) -> Result<()> {
    ensure!(fs::symlink_metadata(input)?.is_file(), "retained root material must be a regular file");
    let mut data = Vec::new();
    fs::File::open(input)?.take(64 * 1024 * 1024 + 1).read_to_end(&mut data)?;
    root_evidence(&data, &config.network, source_id)?;
    let pool = crate::database::create_pool(config).await?;
    let result = recover_roots(pool.clone(), &config.network, source_id, &data).await;
    pool.close().await;
    println!("{}", result?);
    Ok(())
}
