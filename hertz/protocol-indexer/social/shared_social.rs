//! Existing native projections, driven by the common accepted-chain journal.
//! No scanner, HTTP, replacement signature implementation, or direct push.
use crate::database::Transaction;
use crate::k_protocol::{KActionType, KProtocolProcessor, VerifiedAction};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Row};

#[derive(Clone)]
pub struct SharedSocial {
    pool: PgPool,
    network: String,
    source_id: String,
}

fn subject(action: &VerifiedAction) -> (String, &'static str) {
    // Match the original native byte-key/signature uniqueness, including 32/33
    // byte author forms. Do not silently change existing relationship semantics.
    let (table, key) = match action.action() {
        KActionType::Post(v) => ("k_contents", v.sender_signature.to_lowercase()),
        KActionType::Reply(v) => ("k_contents", v.sender_signature.to_lowercase()),
        KActionType::Quote(v) => ("k_contents", v.sender_signature.to_lowercase()),
        KActionType::Vote(v) => ("k_votes", v.sender_signature.to_lowercase()),
        KActionType::Broadcast(v) => ("k_broadcasts", v.sender_pubkey.to_lowercase()),
        KActionType::Follow(v) => (
            "k_follows",
            format!("{}:{}", v.sender_pubkey, v.followed_user_pubkey).to_lowercase(),
        ),
        KActionType::Block(v) => (
            "k_blocks",
            format!("{}:{}", v.sender_pubkey, v.blocked_user_pubkey).to_lowercase(),
        ),
        KActionType::Unknown(_) => unreachable!(),
    };
    (format!("{table}:{key}"), table)
}

fn operation(action: &VerifiedAction) -> &'static str {
    match action.action() {
        KActionType::Follow(v) => {
            if v.following_action == "follow" {
                "activate"
            } else {
                "deactivate"
            }
        }
        KActionType::Block(v) => {
            if v.blocking_action == "block" {
                "activate"
            } else {
                "deactivate"
            }
        }
        KActionType::Broadcast(_) => "replace",
        _ => "signed",
    }
}

pub(crate) fn selection_sql(table: &str) -> String {
    let fields = "a.transaction_id,a.source_payload,a.original_time,a.notification_consumed,c.revision,a.snapshot::text AS snapshot";
    let base = "FROM social_shared.actions a JOIN protocol_indexer.canonical c USING(transaction_id) WHERE a.subject=$1 AND c.accepting_block IS NOT NULL";
    match table {
        "k_broadcasts" => format!(
            "SELECT {fields} {base} ORDER BY a.original_time DESC,a.transaction_id DESC LIMIT 1"
        ),
        "k_follows" | "k_blocks" => format!(
            "WITH reversal AS (SELECT a.original_time,a.transaction_id {base} AND a.state_action='deactivate' ORDER BY a.original_time DESC,a.transaction_id DESC LIMIT 1) SELECT {fields} {base} AND a.state_action='activate' AND (NOT EXISTS(SELECT 1 FROM reversal) OR (a.original_time,a.transaction_id)>(SELECT original_time,transaction_id FROM reversal)) ORDER BY a.original_time,a.transaction_id LIMIT 1"
        ),
        "k_contents" | "k_votes" => format!(
            "SELECT {fields} {base} ORDER BY EXISTS(SELECT 1 FROM {table} native WHERE native.transaction_id=a.transaction_id) DESC,a.original_time,a.transaction_id LIMIT 1"
        ),
        _ => unreachable!("native table is a closed verified action mapping"),
    }
}

impl SharedSocial {
    pub async fn bind(pool: PgPool, network: &str, source_id: &str) -> Result<Self> {
        let mut tx = pool.begin().await?;
        // Read/bind BEFORE schema writes. UUID is compared as server-rendered
        // text; malformed/foreign identities cannot initialize a projection.
        Self::check_source(&mut tx, network, source_id).await?;
        sqlx::query("SELECT pg_advisory_xact_lock(736124, 31)")
            .execute(&mut *tx)
            .await?;
        let existing: Option<String> =
            sqlx::query_scalar("SELECT to_regclass('social_shared.config')::text")
                .fetch_one(&mut *tx)
                .await?;
        let owned = if existing.is_some() {
            sqlx::query("SELECT network,source_id FROM social_shared.config WHERE singleton")
                .fetch_optional(&mut *tx)
                .await?
                .is_some()
        } else {
            false
        };
        if !owned {
            // Migration of existing public history is a separate reviewed step.
            // Never erase/adopt unverifiable legacy rows to start a new writer.
            for table in [
                "k_contents",
                "k_votes",
                "k_broadcasts",
                "k_follows",
                "k_blocks",
                "k_mentions",
                "k_hashtags",
            ] {
                let nonempty: bool =
                    sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {table})"))
                        .fetch_one(&mut *tx)
                        .await?;
                ensure!(
                    !nonempty,
                    "legacy Social history needs explicit reconciliation before shared ownership"
                );
            }
        }
        sqlx::raw_sql(include_str!("shared_social.sql"))
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO social_shared.config(network,source_id) VALUES($1,$2) ON CONFLICT DO NOTHING")
            .bind(network).bind(source_id).execute(&mut *tx).await?;
        let row = sqlx::query("SELECT network,source_id FROM social_shared.config WHERE singleton")
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            row.get::<String, _>("network") == network
                && row.get::<String, _>("source_id") == source_id,
            "Social projection belongs to another source/network"
        );
        tx.commit().await?;
        Ok(Self {
            pool,
            network: network.into(),
            source_id: source_id.into(),
        })
    }

    async fn check_source(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        network: &str,
        source_id: &str,
    ) -> Result<()> {
        let row = sqlx::query("SELECT network,journal_id::text AS source_id FROM protocol_indexer.config WHERE singleton FOR SHARE")
            .fetch_one(&mut **tx).await?;
        ensure!(
            row.get::<String, _>("network") == network
                && row.get::<String, _>("source_id") == source_id,
            "canonical Social source/network mismatch"
        );
        let legacy: String = sqlx::query_scalar("SELECT value FROM k_vars WHERE key='network'")
            .fetch_one(&mut **tx)
            .await?;
        ensure!(legacy == network, "native Social network mismatch");
        let schema: String =
            sqlx::query_scalar("SELECT value FROM k_vars WHERE key='schema_version'")
                .fetch_one(&mut **tx)
                .await?;
        ensure!(schema == "2", "unsupported native Social schema");
        // The existing Notifications installer takes SHARE ROW EXCLUSIVE on
        // these tables before replacing capture/triggers. Hold its compatible
        // DML lease BEFORE reading the approved function body, so an older
        // installer cannot slip between capability check and native INSERT.
        sqlx::query(
            "LOCK TABLE public.k_contents,public.k_votes,public.k_follows IN ROW EXCLUSIVE MODE",
        )
        .execute(&mut **tx)
        .await?;
        let quiet_capable:bool=sqlx::query_scalar("SELECT CASE WHEN to_regprocedure('social_notifications.capture()') IS NULL THEN true ELSE EXISTS(SELECT 1 FROM pg_proc WHERE oid=to_regprocedure('social_notifications.capture()') AND encode(sha256(convert_to(prosrc,'UTF8')),'hex')=$1) END")
            .bind(include_str!("notification_capture.sha256").trim()).fetch_one(&mut **tx).await?;
        ensure!(
            quiet_capable,
            "Social notification capture must support canonical quiet replay before projection"
        );
        Ok(())
    }

    /// One canonical revision, ledger update, affected-subject projection and
    /// durable acknowledgement commit together. No network I/O under locks.
    pub async fn run_one(&self) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        Self::check_source(&mut tx, &self.network, &self.source_id).await?;
        let job = sqlx::query("SELECT transaction_id FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1' AND available_at<=clock_timestamp() ORDER BY available_at,transaction_id LIMIT 1 FOR UPDATE SKIP LOCKED")
            .fetch_optional(&mut *tx).await?;
        let Some(job) = job else { return Ok(false) };
        let id: Vec<u8> = job.get("transaction_id");
        let row = sqlx::query("SELECT revision,source_transaction::text AS body,accepting_block FROM protocol_indexer.canonical WHERE transaction_id=$1 FOR UPDATE")
            .bind(&id).fetch_one(&mut *tx).await?;
        let revision: i64 = row.get("revision");
        let body: Value = serde_json::from_str(row.get::<&str, _>("body"))?;
        let payload = body["payload"]
            .as_str()
            .and_then(|v| v.strip_prefix("\\x"))
            .context("canonical payload is not bytea JSON")?;
        let Some(time) = body["block_time"].as_i64() else {
            // Normal source enrichment may lag body discovery. Keep this job
            // durable without starving other ready subjects or crashing daemon.
            sqlx::query("UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp()+interval '5 seconds',last_error='canonical_block_time_pending' WHERE consumer='native-social-v1' AND transaction_id=$1")
                .bind(&id).execute(&mut *tx).await?;
            tx.commit().await?;
            return Ok(true);
        };
        let transaction = Transaction {
            transaction_id: hex::encode(&id),
            payload: Some(payload.into()),
            block_time: Some(time),
        };
        let processor = KProtocolProcessor::new(self.pool.clone());
        let verified = processor.verify_transaction(&transaction);
        let key = verified.as_ref().map(subject);
        if let Some((key, _)) = &key {
            // All actions for one signature/author/relation serialize; unrelated
            // subjects can progress. Rebuild never locks other canonical rows.
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,736124))")
                .bind(key)
                .execute(&mut *tx)
                .await?;
        }
        let old = sqlx::query(
            "SELECT source_payload,subject FROM social_shared.actions WHERE transaction_id=$1",
        )
        .bind(&id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(old) = old {
            ensure!(
                old.get::<String, _>("source_payload") == payload,
                "immutable Social transaction payload changed"
            );
            if let Some(prior) = old.get::<Option<String>, _>("subject") {
                ensure!(
                    key.as_ref().map(|v| v.0.as_str()) == Some(prior.as_str()),
                    "verified subject changed"
                );
            }
        }
        sqlx::query("INSERT INTO social_shared.actions(transaction_id,revision,subject,native_table,source_payload,original_time,status,state_action) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(transaction_id) DO UPDATE SET revision=EXCLUDED.revision,subject=EXCLUDED.subject,native_table=EXCLUDED.native_table,status=EXCLUDED.status,state_action=EXCLUDED.state_action")
            .bind(&id).bind(revision).bind(key.as_ref().map(|v|v.0.as_str())).bind(key.as_ref().map(|v|v.1))
            .bind(payload).bind(time).bind(if verified.is_some() {"verified"} else {"unsupported_or_invalid"})
             .bind(verified.as_ref().map(operation)).execute(&mut *tx).await?;
        if let Some((key, table)) = key {
            if !self.rebuild(&mut tx, &processor, &key, table, &id).await? {
                tx.rollback().await?;
                // A different worker/source writer owns the selected row.
                // Never wait behind it while holding this subject/current row:
                // that worker may itself be waiting for our subject lock.
                sqlx::query("UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp()+interval '50 milliseconds',last_error='subject_revision_busy' WHERE consumer='native-social-v1' AND transaction_id=$1")
                    .bind(&id).execute(&self.pool).await?;
                return Ok(true);
            }
        }
        // Held canonical row fence means a later writer sees no existing job
        // and queues its new revision after this commit. Producer never locks
        // an existing job, preserving its established lock order.
        sqlx::query("DELETE FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1' AND transaction_id=$1")
            .bind(&id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn rebuild(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        processor: &KProtocolProcessor,
        key: &str,
        table: &str,
        current: &[u8],
    ) -> Result<bool> {
        // Original timestamps persist across source metadata revisions. Native
        // signature identity chooses one accepted representative across roots.
        // Select effective state in SQL; never cryptographically reverify and
        // rewrite an author's full history for every metadata update. Native
        // pair semantics keep the first activation after the last deactivation.
        let selected = selection_sql(table);
        let active = sqlx::query(&selected)
            .bind(key)
            .fetch_all(&mut **tx)
            .await?;
        let mut evaluated = vec![current.to_vec()];
        for selected in &active {
            let id: Vec<u8> = selected.get("transaction_id");
            let locked:Option<i64>=sqlx::query_scalar("SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=$1 FOR SHARE SKIP LOCKED")
                .bind(&id).fetch_optional(&mut **tx).await?;
            if locked != Some(selected.get::<i64, _>("revision")) {
                return Ok(false);
            }
            evaluated.push(id);
        }
        // Delete only the affected owned subject. Unrelated history stays put;
        // no growing in-memory array of all historical transaction IDs.
        sqlx::query("DELETE FROM k_mentions m USING social_shared.actions a WHERE a.subject=$1 AND m.content_id=a.transaction_id")
            .bind(key).execute(&mut **tx).await?;
        sqlx::query(&format!("DELETE FROM {table} t USING social_shared.actions a WHERE a.subject=$1 AND t.transaction_id=a.transaction_id"))
            .bind(key).execute(&mut **tx).await?;
        for row in &active {
            let id: Vec<u8> = row.get("transaction_id");
            let transaction = Transaction {
                transaction_id: hex::encode(&id),
                payload: Some(row.get("source_payload")),
                block_time: Some(row.get("original_time")),
            };
            let verified = processor
                .verify_transaction(&transaction)
                .context("stored verified Social action no longer verifies")?;
            ensure!(
                subject(&verified) == (key.into(), table),
                "stored Social subject mismatch"
            );
            let notify: bool = if !row.get::<bool, _>("notification_consumed") {
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM protocol_indexer.events WHERE transaction_id=$1 AND revision<=$2 AND notify_allowed)")
                    .bind(&id).bind(row.get::<i64,_>("revision")).fetch_one(&mut **tx).await?
            } else {
                false
            };
            sqlx::query("SELECT set_config('kasanova.social_notify_allowed',$1,true)")
                .bind(if notify { "true" } else { "false" })
                .execute(&mut **tx)
                .await?;
            processor.apply_verified_in(tx, &verified).await?;
            // Restore stable existing cursor IDs, including mention/hashtag
            // cursors, after original native SQL recreated an accepted row.
            let previous: Option<String> = row.get("snapshot");
            if let Some(previous) = previous {
                let saved: Value = serde_json::from_str(&previous)?;
                if let Some(saved_id) = saved["main"]["id"].as_i64() {
                    sqlx::query(&format!("UPDATE {table} SET id=$1 WHERE transaction_id=$2"))
                        .bind(saved_id)
                        .bind(&id)
                        .execute(&mut **tx)
                        .await?;
                }
                for child in ["k_mentions", "k_hashtags"] {
                    if let Some(rows) = saved[child].as_array() {
                        sqlx::query(&format!("DELETE FROM {child} WHERE content_id=$1"))
                            .bind(&id)
                            .execute(&mut **tx)
                            .await?;
                        sqlx::query(&format!("INSERT INTO {child} SELECT * FROM json_populate_recordset(NULL::{child},$1::json)"))
                            .bind(serde_json::to_string(rows)?).execute(&mut **tx).await?;
                    }
                }
            }
            let main: Option<String> = sqlx::query_scalar(&format!(
                "SELECT row_to_json(t)::text FROM {table} t WHERE transaction_id=$1"
            ))
            .bind(&id)
            .fetch_optional(&mut **tx)
            .await?;
            if let Some(main) = main {
                let mut saved = json!({"main":serde_json::from_str::<Value>(&main)?});
                for child in ["k_mentions", "k_hashtags"] {
                    let rows: String = sqlx::query_scalar(&format!("SELECT COALESCE(json_agg(t),'[]'::json)::text FROM {child} t WHERE content_id=$1"))
                        .bind(&id).fetch_one(&mut **tx).await?;
                    saved[child] = serde_json::from_str(&rows)?;
                }
                sqlx::query(
                    "UPDATE social_shared.actions SET snapshot=$1::jsonb WHERE transaction_id=$2",
                )
                .bind(saved.to_string())
                .bind(&id)
                .execute(&mut **tx)
                .await?;
            }
        }
        // Only selected/current opportunities were evaluated and their exact
        // canonical rows are fenced. Another pending subject action may accept
        // or revoke concurrently; never silently consume that unseen change.
        sqlx::query("UPDATE social_shared.actions a SET notification_consumed=true FROM protocol_indexer.canonical c WHERE a.transaction_id=ANY($1) AND NOT a.notification_consumed AND a.transaction_id=c.transaction_id AND c.accepting_block IS NOT NULL")
            .bind(&evaluated).execute(&mut **tx).await?;
        // Leave no accidental live eligibility for a later statement in caller.
        sqlx::query("SELECT set_config('kasanova.social_notify_allowed','false',true)")
            .execute(&mut **tx)
            .await?;
        Ok(true)
    }
}

pub async fn run(pool: PgPool, network: &str, source_id: &str) -> Result<()> {
    let consumer = SharedSocial::bind(pool, network, source_id).await?;
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            result = consumer.run_one() => {
                if !result? {
                    tokio::select! {
                        _ = &mut shutdown => return Ok(()),
                        _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {}
                    }
                }
            }
        }
    }
}
