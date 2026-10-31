//! Canonical KaPosts state layered onto the existing native content projection.
//! Pure protocol rules/SQL shapes follow pinned KaChat; no RPC or direct push.
use crate::database::Transaction;
use crate::k_protocol::{KActionType, KProtocolProcessor, VerifiedAction, VerifiedKaPosts};
use crate::kaposts::{KPoll, KaPostsAction};
use crate::shared_social::{capture_snapshot, restore_snapshot};
use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;
use sqlx::{Postgres, Row};

type Tx<'a> = sqlx::Transaction<'a, Postgres>;
type Rows = Vec<sqlx::postgres::PgRow>;

pub fn decoded_message(value: &str) -> Option<String> {
    String::from_utf8(STANDARD.decode(value).ok()?).ok()
}

pub fn content_identity(
    processor: &KProtocolProcessor,
    value: &Transaction,
) -> Option<(String, String)> {
    if let Some(core) = processor.verify_transaction(value) {
        return match core.action() {
            KActionType::Post(v) => Some((v.sender_pubkey.clone(), v.sender_signature.clone())),
            KActionType::Reply(v) => Some((v.sender_pubkey.clone(), v.sender_signature.clone())),
            KActionType::Quote(v) => Some((v.sender_pubkey.clone(), v.sender_signature.clone())),
            _ => None,
        };
    }
    match processor.verify_kaposts_transaction(value)?.action() {
        KaPostsAction::Poll(v) if value.block_time.is_some_and(|time| valid_poll(v, time)) => {
            Some((v.sender_pubkey.clone(), v.sender_signature.clone()))
        }
        _ => None,
    }
}

pub fn valid_poll(poll: &KPoll, time: i64) -> bool {
    if decoded_message(&poll.base64_encoded_question).is_none()
        || poll.closes_at_ms <= time
        || time
            .checked_add(604_800_000)
            .is_none_or(|end| poll.closes_at_ms > end)
    {
        return false;
    }
    let encoded: Vec<_> = poll.options_b64_csv.split(',').collect();
    if !(2..=4).contains(&encoded.len()) {
        return false;
    }
    let mut seen = std::collections::HashSet::new();
    encoded.into_iter().all(|option| {
        decoded_message(option.trim())
            .is_some_and(|text| (1..=40).contains(&text.chars().count()) && seen.insert(text))
    })
}

pub fn subject(value: &VerifiedKaPosts) -> (String, &'static str) {
    let (signature, _) = value.action().signature_fields();
    if matches!(value.action(), KaPostsAction::Poll(_)) {
        (
            format!("k_contents:{}", signature.to_lowercase()),
            "k_contents",
        )
    } else {
        (
            format!(
                "kaposts:{}:{}",
                kind(value.action()),
                signature.to_lowercase()
            ),
            "kaposts",
        )
    }
}

fn kind(value: &KaPostsAction) -> &'static str {
    match value {
        KaPostsAction::Unquote(_) => "unquote",
        KaPostsAction::Edit(_) => "edit",
        KaPostsAction::Delete(_) => "delete",
        KaPostsAction::Poll(_) => "poll",
        KaPostsAction::PollVote(_) => "pollvote",
    }
}

pub fn relation_lock(
    core: Option<&VerifiedAction>,
    extension: Option<&VerifiedKaPosts>,
) -> Option<String> {
    if let Some(value) = core {
        if let KActionType::Quote(v) = value.action() {
            return Some(
                format!("quote_relation:{}:{}", v.sender_pubkey, v.content_id).to_lowercase(),
            );
        }
    }
    if let Some(value) = extension {
        if let KaPostsAction::Unquote(v) = value.action() {
            return Some(
                format!("quote_relation:{}:{}", v.sender_pubkey, v.content_id).to_lowercase(),
            );
        }
    }
    None
}

pub async fn record_core(tx: &mut Tx<'_>, value: &VerifiedAction) -> Result<()> {
    let (signature, author, kind, reference) = match value.action() {
        KActionType::Post(v) => (&v.sender_signature, &v.sender_pubkey, "post", None),
        KActionType::Reply(v) => (
            &v.sender_signature,
            &v.sender_pubkey,
            "reply",
            Some(v.post_id.as_str()),
        ),
        KActionType::Quote(v) => (
            &v.sender_signature,
            &v.sender_pubkey,
            "quote",
            Some(v.content_id.as_str()),
        ),
        KActionType::Vote(v) => {
            sqlx::query("INSERT INTO social_shared.ordinary_vote_targets VALUES($1,$2) ON CONFLICT DO NOTHING")
                .bind(hex::decode(&value.transaction().transaction_id)?).bind(hex::decode(&v.post_id)?).execute(&mut **tx).await?;
            return Ok(());
        }
        _ => return Ok(()),
    };
    record_content(tx, value.transaction(), signature, author, kind, reference).await
}

async fn record_content(
    tx: &mut Tx<'_>,
    value: &Transaction,
    signature: &str,
    author: &str,
    kind: &str,
    reference: Option<&str>,
) -> Result<()> {
    let inserted=sqlx::query("INSERT INTO social_shared.content_keys(transaction_id,signature,author,kind,reference,original_time) SELECT transaction_id,$2,$3,$4,$5,original_time FROM social_shared.actions WHERE transaction_id=$1 ON CONFLICT DO NOTHING")
        .bind(hex::decode(&value.transaction_id)?).bind(hex::decode(signature)?).bind(hex::decode(author)?).bind(kind)
        .bind(reference.map(hex::decode).transpose()?).execute(&mut **tx).await?.rows_affected();
    if inserted > 0 {
        sqlx::query("UPDATE social_shared.extensions e SET target_signature=k.signature,target_author=k.author,target_kind=k.kind,target_time=k.original_time FROM social_shared.content_keys k WHERE k.transaction_id=$1 AND e.target=k.transaction_id AND (e.target_signature,e.target_author,e.target_kind,e.target_time) IS DISTINCT FROM (k.signature,k.author,k.kind,k.original_time)")
            .bind(hex::decode(&value.transaction_id)?).execute(&mut **tx).await?;
    }
    Ok(())
}

pub async fn record_extension(
    tx: &mut Tx<'_>,
    value: &VerifiedKaPosts,
    revision: i64,
) -> Result<()> {
    let id = hex::decode(&value.transaction().transaction_id)?;
    let time: i64 = sqlx::query_scalar(
        "SELECT original_time FROM social_shared.actions WHERE transaction_id=$1",
    )
    .bind(&id)
    .fetch_one(&mut **tx)
    .await?;
    let (signature, sender) = value.action().signature_fields();
    let mut target = None;
    let mut message = None;
    let mut mentions: &[String] = &[];
    let mut options = None;
    let mut closes = None;
    let mut index = None;
    let mut valid = true;
    match value.action() {
        KaPostsAction::Unquote(v) => target = Some(v.content_id.clone()),
        KaPostsAction::Delete(v) => {
            target = Some(v.post_id.clone());
            valid = v.post_id.len() == 64;
        }
        KaPostsAction::Edit(v) => {
            target = Some(v.post_id.clone());
            message = Some(v.base64_encoded_message.as_str());
            mentions = &v.mentioned_pubkeys;
            valid = v.post_id.len() == 64 && decoded_message(&v.base64_encoded_message).is_some();
        }
        KaPostsAction::Poll(v) => {
            message = Some(v.base64_encoded_question.as_str());
            mentions = &v.mentioned_pubkeys;
            options = Some(v.options_b64_csv.as_str());
            closes = Some(v.closes_at_ms);
            valid = valid_poll(v, time);
            record_content(tx, value.transaction(), signature, sender, "poll", None).await?;
        }
        KaPostsAction::PollVote(v) => {
            target = Some(v.poll_id.clone());
            index = Some(v.option_index);
            valid = v.poll_id.len() == 64 && v.option_index >= 0;
        }
    }
    sqlx::query("INSERT INTO social_shared.extensions(transaction_id,revision,kind,sender,signature,target,message,mentions,options,closes_at,option_index,shape_valid,original_time) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12,$13) ON CONFLICT(transaction_id) DO UPDATE SET revision=EXCLUDED.revision,shape_valid=EXCLUDED.shape_valid")
        .bind(&id).bind(revision).bind(kind(value.action())).bind(hex::decode(sender)?).bind(hex::decode(signature)?)
        .bind(target.map(hex::decode).transpose()?).bind(message).bind(serde_json::to_string(mentions)?).bind(options).bind(closes).bind(index).bind(valid).bind(time).execute(&mut **tx).await?;
    if matches!(value.action(), KaPostsAction::Edit(_)) {
        for pubkey in mentions {
            sqlx::query(
                "INSERT INTO social_shared.edit_mentions VALUES($1,$2) ON CONFLICT DO NOTHING",
            )
            .bind(&id)
            .bind(hex::decode(pubkey)?)
            .execute(&mut **tx)
            .await?;
        }
    }
    Ok(())
}

struct Target {
    signature: Vec<u8>,
    author: Vec<u8>,
    kind: &'static str,
    time: i64,
    poll: Option<KPoll>,
    accepted: bool,
}

enum TargetState {
    Ready(Target),
    Missing,
    Busy,
    Invalid,
}

async fn target(tx: &mut Tx<'_>, processor: &KProtocolProcessor, id: &[u8]) -> Result<TargetState> {
    let row=sqlx::query("SELECT c.source_transaction::text AS body,c.accepting_block,COALESCE(a.original_time,(c.source_transaction->>'block_time')::bigint) AS original_time FROM protocol_indexer.canonical c LEFT JOIN social_shared.actions a USING(transaction_id) WHERE c.transaction_id=$1 FOR SHARE OF c SKIP LOCKED")
        .bind(id).fetch_optional(&mut **tx).await?;
    let Some(row) = row else {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM protocol_indexer.canonical WHERE transaction_id=$1)",
        )
        .bind(id)
        .fetch_one(&mut **tx)
        .await?;
        return Ok(if exists {
            TargetState::Busy
        } else {
            TargetState::Missing
        });
    };
    let body: Value = serde_json::from_str(row.get::<&str, _>("body"))?;
    let time: Option<i64> = row.get("original_time");
    let Some(time) = time else {
        return Ok(TargetState::Missing);
    };
    let transaction = Transaction {
        transaction_id: hex::encode(id),
        payload: body["payload"]
            .as_str()
            .and_then(|s| s.strip_prefix("\\x"))
            .map(str::to_owned),
        block_time: Some(time),
    };
    let accepted = row.get::<Option<Vec<u8>>, _>("accepting_block").is_some();
    let fields = if let Some(core) = processor.verify_transaction(&transaction) {
        match core.action() {
            KActionType::Post(v) => Some((
                v.sender_signature.clone(),
                v.sender_pubkey.clone(),
                "post",
                None,
            )),
            KActionType::Reply(v) => Some((
                v.sender_signature.clone(),
                v.sender_pubkey.clone(),
                "reply",
                None,
            )),
            KActionType::Quote(v) => Some((
                v.sender_signature.clone(),
                v.sender_pubkey.clone(),
                "quote",
                None,
            )),
            _ => None,
        }
    } else if let Some(extension) = processor.verify_kaposts_transaction(&transaction) {
        match extension.action() {
            KaPostsAction::Poll(v) if valid_poll(v, time) => Some((
                v.sender_signature.clone(),
                v.sender_pubkey.clone(),
                "poll",
                Some(v.clone()),
            )),
            _ => None,
        }
    } else {
        None
    };
    Ok(if let Some((signature, author, kind, poll)) = fields {
        TargetState::Ready(Target {
            signature: hex::decode(signature)?,
            author: hex::decode(author)?,
            kind,
            time,
            poll,
            accepted,
        })
    } else {
        TargetState::Invalid
    })
}

pub struct Plan {
    pub content_keys: Vec<String>,
    pub pending: bool,
    pub busy: bool,
}

pub async fn plan(
    tx: &mut Tx<'_>,
    processor: &KProtocolProcessor,
    value: &VerifiedKaPosts,
) -> Result<Plan> {
    let mut result = Plan {
        content_keys: vec![],
        pending: false,
        busy: false,
    };
    let id = hex::decode(&value.transaction().transaction_id)?;
    let row = sqlx::query(
        "SELECT shape_valid,target FROM social_shared.extensions WHERE transaction_id=$1",
    )
    .bind(&id)
    .fetch_one(&mut **tx)
    .await?;
    if !row.get::<bool, _>("shape_valid") {
        return Ok(result);
    }
    if matches!(value.action(), KaPostsAction::Poll(_)) {
        result.content_keys.push(subject(value).0);
        return Ok(result);
    }
    let reference: Vec<u8> = row.get("target");
    if matches!(value.action(), KaPostsAction::Delete(_)) {
        let (_, sender) = value.action().signature_fields();
        let candidates=sqlx::query("SELECT e.transaction_id,a.original_time,c.revision FROM social_shared.extensions e JOIN social_shared.actions a USING(transaction_id) JOIN protocol_indexer.canonical c USING(transaction_id) WHERE e.kind='delete' AND e.target=$1 AND e.sender=$2 AND e.shape_valid AND c.accepting_block IS NOT NULL ORDER BY a.original_time,e.transaction_id LIMIT 1")
            .bind(&reference).bind(hex::decode(sender)?).fetch_all(&mut **tx).await?;
        if !fenced(tx, &candidates).await? {
            result.busy = true;
            return Ok(result);
        }
        sqlx::query("DELETE FROM kachat_delete_tombstones WHERE post_id=$1 AND sender_pubkey=$2")
            .bind(&reference)
            .bind(hex::decode(sender)?)
            .execute(&mut **tx)
            .await?;
        if let Some(candidate) = candidates.first() {
            sqlx::query("INSERT INTO kachat_delete_tombstones VALUES($1,$2,$3,$4)")
                .bind(&reference)
                .bind(hex::decode(sender)?)
                .bind(candidate.get::<Vec<u8>, _>("transaction_id"))
                .bind(candidate.get::<i64, _>("original_time"))
                .execute(&mut **tx)
                .await?;
        }
    }
    if matches!(value.action(), KaPostsAction::Unquote(_)) {
        let (_, sender) = value.action().signature_fields();
        let sender = hex::decode(sender)?;
        let selected=sqlx::query("SELECT e.transaction_id,e.original_time,c.revision FROM social_shared.extensions e JOIN protocol_indexer.canonical c USING(transaction_id) WHERE e.kind='unquote' AND e.sender=$1 AND e.target=$2 AND e.shape_valid AND c.accepting_block IS NOT NULL AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=e.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) ORDER BY e.original_time DESC,e.transaction_id DESC LIMIT 1")
            .bind(&sender).bind(&reference).fetch_all(&mut **tx).await?;
        if !fenced(tx, &selected).await? {
            result.busy = true;
            return Ok(result);
        }
        let old=sqlx::query("SELECT transaction_id,original_time FROM social_shared.quote_withdrawals WHERE sender=$1 AND reference=$2").bind(&sender).bind(&reference).fetch_optional(&mut **tx).await?;
        let previous = old.as_ref().map(|r| {
            (
                r.get::<i64, _>("original_time"),
                r.get::<Vec<u8>, _>("transaction_id"),
            )
        });
        let next = selected.first().map(|r| {
            (
                r.get::<i64, _>("original_time"),
                r.get::<Vec<u8>, _>("transaction_id"),
            )
        });
        if previous == next {
            return Ok(result);
        }
        let (lower, upper) = if previous < next {
            (previous, next.clone().unwrap())
        } else {
            (next.clone(), previous.unwrap())
        };
        let keys:Vec<Vec<u8>>=sqlx::query_scalar("SELECT DISTINCT signature FROM social_shared.content_keys WHERE kind IN('quote','repost') AND author=$1 AND reference=$2 AND ($3::bigint IS NULL OR (original_time,transaction_id)>($3,$4)) AND (original_time,transaction_id)<=($5,$6) ORDER BY signature")
            .bind(&sender).bind(&reference).bind(lower.as_ref().map(|v|v.0)).bind(lower.as_ref().map(|v|v.1.as_slice())).bind(upper.0).bind(upper.1).fetch_all(&mut **tx).await?;
        if let Some((time, id)) = next {
            sqlx::query("INSERT INTO social_shared.quote_withdrawals VALUES($1,$2,$3,$4) ON CONFLICT(sender,reference) DO UPDATE SET transaction_id=EXCLUDED.transaction_id,original_time=EXCLUDED.original_time")
                .bind(sender).bind(reference).bind(id).bind(time).execute(&mut **tx).await?;
        } else {
            sqlx::query(
                "DELETE FROM social_shared.quote_withdrawals WHERE sender=$1 AND reference=$2",
            )
            .bind(sender)
            .bind(reference)
            .execute(&mut **tx)
            .await?;
        }
        result.content_keys = keys
            .into_iter()
            .map(|s| format!("k_contents:{}", hex::encode(s)))
            .collect();
        return Ok(result);
    }
    match target(tx, processor, &reference).await? {
        TargetState::Missing => result.pending = true,
        TargetState::Busy => result.busy = true,
        TargetState::Invalid => {}
        TargetState::Ready(parent) => {
            sqlx::query("UPDATE social_shared.extensions SET target_signature=$2,target_author=$3,target_kind=$4,target_time=$5 WHERE transaction_id=$1")
                .bind(&id).bind(&parent.signature).bind(&parent.author).bind(parent.kind).bind(parent.time).execute(&mut **tx).await?;
            let acquired: bool =
                sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1,736124))")
                    .bind(format!("k_contents:{}", hex::encode(&parent.signature)))
                    .fetch_one(&mut **tx)
                    .await?;
            if !acquired {
                result.busy = true;
                return Ok(result);
            }
            if parent.accepted {
                let captured:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM social_shared.content_keys WHERE transaction_id=$1)").bind(&reference).fetch_one(&mut **tx).await?;
                if !captured {
                    sqlx::query("INSERT INTO protocol_indexer.consumer_jobs(consumer,transaction_id) VALUES('native-social-v1',$1) ON CONFLICT DO NOTHING").bind(&reference).execute(&mut **tx).await?;
                    result.pending = true;
                    return Ok(result);
                }
            }
            if matches!(value.action(), KaPostsAction::PollVote(_)) {
                if !apply_poll_vote(tx, processor, value, &parent).await? {
                    result.busy = true;
                }
            } else {
                let (_, sender) = value.action().signature_fields();
                if parent.author == hex::decode(sender)? {
                    let audit_ready = if matches!(value.action(), KaPostsAction::Edit(_)) {
                        edit_audit(tx, &hex::decode(value.action().signature_fields().0)?).await?
                    } else {
                        delete_audit(tx, &reference).await?
                    };
                    if !audit_ready {
                        result.busy = true;
                        return Ok(result);
                    }
                    result
                        .content_keys
                        .push(format!("k_contents:{}", hex::encode(parent.signature)));
                    // Index the owning target through its normal existing job;
                    // metadata can hide delete-before-post during first insert.
                }
            }
        }
    }
    Ok(result)
}

async fn fenced(tx: &mut Tx<'_>, rows: &Rows) -> Result<bool> {
    for row in rows {
        let revision:Option<i64>=sqlx::query_scalar("SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=$1 FOR SHARE SKIP LOCKED")
            .bind(row.get::<Vec<u8>,_>("transaction_id")).fetch_optional(&mut **tx).await?;
        if revision
            != Some(
                row.try_get("canonical_revision")
                    .unwrap_or_else(|_| row.get("revision")),
            )
        {
            return Ok(false);
        }
        if let Some(target) = row.try_get::<Option<Vec<u8>>, _>("target").ok().flatten() {
            let target_revision: Option<i64> = row.try_get("target_revision").ok().flatten();
            if let Some(expected) = target_revision {
                let actual:Option<i64>=sqlx::query_scalar("SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=$1 FOR SHARE SKIP LOCKED").bind(target).fetch_optional(&mut **tx).await?;
                if actual != Some(expected) {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

pub struct ContentPolicy {
    hidden: bool,
    edits: Rows,
    deletes: Rows,
    withdraws: Rows,
    original: String,
}

pub fn source_message<'a>(
    core: Option<&'a VerifiedAction>,
    extension: Option<&'a VerifiedKaPosts>,
) -> Option<&'a str> {
    if let Some(core) = core {
        return match core.action() {
            KActionType::Post(v) => Some(&v.base64_encoded_message),
            KActionType::Reply(v) => Some(&v.base64_encoded_message),
            KActionType::Quote(v) => Some(&v.base64_encoded_message),
            _ => None,
        };
    }
    match extension?.action() {
        KaPostsAction::Poll(v) => Some(&v.base64_encoded_question),
        _ => None,
    }
}

// Immutable SDK fields survive canonical metadata revisions. Current acceptance
// and exact selected revision fences come from the journal, never that cache.
pub(crate) const AUTHORIZED_EDIT: &str = include_str!("edit_authorization.sql");
pub(crate) const UNQUOTE_ORDER: &str = include_str!("unquote_order.sql");
fn selected_fields() -> &'static str {
    "e.*,e.mentions::text AS mentions_text,a.snapshot::text AS snapshot,c.revision AS canonical_revision,t.revision AS target_revision"
}
fn mutation_join() -> &'static str {
    "FROM social_shared.extensions e JOIN social_shared.actions a USING(transaction_id) JOIN protocol_indexer.canonical c USING(transaction_id) JOIN protocol_indexer.canonical t ON t.transaction_id=e.target"
}

pub(crate) fn edit_selection_sql() -> String {
    format!(
        "SELECT {} {} WHERE e.target_signature=$1 AND {} ORDER BY e.original_time DESC,e.transaction_id DESC LIMIT 1",
        selected_fields(),
        mutation_join(),
        AUTHORIZED_EDIT
    )
}

pub async fn content_policy(
    tx: &mut Tx<'_>,
    signature: &[u8],
    base_id: &[u8],
    original: &str,
) -> Result<Option<ContentPolicy>> {
    let edits = sqlx::query(&edit_selection_sql())
        .bind(signature)
        .fetch_all(&mut **tx)
        .await?;
    let deletes=sqlx::query(&format!("SELECT {} {} WHERE e.target_signature=$1 AND e.kind='delete' AND e.shape_valid AND e.sender=e.target_author AND c.accepting_block IS NOT NULL AND t.accepting_block IS NOT NULL AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id IN(e.transaction_id,e.target) AND p.acceptance_changed AND p.accepting_block IS NULL) ORDER BY e.original_time,e.transaction_id LIMIT 1",selected_fields(),mutation_join()))
        .bind(signature).fetch_all(&mut **tx).await?;
    let withdraws=sqlx::query(&format!("SELECT e.*,c.revision AS canonical_revision FROM social_shared.content_keys k JOIN social_shared.extensions e ON e.kind='unquote' AND e.sender=k.author AND e.target=k.reference JOIN protocol_indexer.canonical c ON c.transaction_id=e.transaction_id WHERE k.transaction_id=$1 AND k.kind IN('quote','repost') AND e.shape_valid AND c.accepting_block IS NOT NULL AND {} AND NOT EXISTS(SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id=e.transaction_id AND p.acceptance_changed AND p.accepting_block IS NULL) ORDER BY e.original_time DESC,e.transaction_id DESC LIMIT 1", UNQUOTE_ORDER))
        .bind(base_id).fetch_all(&mut **tx).await?;
    if !fenced(tx, &edits).await? || !fenced(tx, &deletes).await? || !fenced(tx, &withdraws).await?
    {
        return Ok(None);
    }
    Ok(Some(ContentPolicy {
        hidden: !deletes.is_empty() || !withdraws.is_empty(),
        edits,
        deletes,
        withdraws,
        original: original.into(),
    }))
}

pub fn hidden(policy: &ContentPolicy) -> bool {
    policy.hidden
}

pub async fn clear_content(tx: &mut Tx<'_>, signature: &[u8]) -> Result<()> {
    sqlx::query("DELETE FROM k_mentions WHERE content_id IN(SELECT transaction_id FROM k_contents WHERE sender_signature=$1)").bind(signature).execute(&mut **tx).await?;
    sqlx::query("DELETE FROM k_polls WHERE post_id IN(SELECT transaction_id FROM social_shared.content_keys WHERE signature=$1)").bind(signature).execute(&mut **tx).await?;
    sqlx::query("DELETE FROM k_contents WHERE sender_signature=$1")
        .bind(signature)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn eligible(tx: &mut Tx<'_>, row: &sqlx::postgres::PgRow) -> Result<bool> {
    sqlx::query_scalar("SELECT NOT a.notification_consumed AND a.original_time>=d.since_ms AND NOT EXISTS(SELECT 1 FROM social_shared.extensions other JOIN social_shared.actions done USING(transaction_id) WHERE other.signature=$3 AND done.notification_consumed) AND EXISTS(SELECT 1 FROM protocol_indexer.events WHERE transaction_id=a.transaction_id AND revision<=$2 AND notify_allowed) FROM social_shared.actions a CROSS JOIN social_shared.decoder_epochs d WHERE a.transaction_id=$1 AND d.kind='kchat:1:extensions'")
        .bind(row.get::<Vec<u8>,_>("transaction_id")).bind(row.try_get::<i64,_>("canonical_revision").unwrap_or_else(|_|row.get("revision"))).bind(row.get::<Vec<u8>,_>("signature")).fetch_one(&mut **tx).await.map_err(Into::into)
}

async fn restore_audit(tx: &mut Tx<'_>, table: &str, row: &sqlx::postgres::PgRow) -> Result<()> {
    let id: Vec<u8> = row.get("transaction_id");
    let snapshot: Option<String> = if let Some(saved) = row.get::<Option<String>, _>("snapshot") {
        Some(saved)
    } else {
        sqlx::query_scalar("SELECT a.snapshot::text FROM social_shared.extensions e JOIN social_shared.actions a USING(transaction_id) WHERE e.signature=$1 AND a.snapshot IS NOT NULL ORDER BY e.original_time,e.transaction_id LIMIT 1")
            .bind(row.get::<Vec<u8>,_>("signature")).fetch_optional(&mut **tx).await?
    };
    restore_snapshot(tx, table, &id, snapshot.as_deref()).await?;
    capture_snapshot(tx, table, &id).await?;
    Ok(())
}

async fn edit_audit(tx: &mut Tx<'_>, signature: &[u8]) -> Result<bool> {
    let selected=sqlx::query(&format!("SELECT {} {} WHERE e.signature=$1 AND {} ORDER BY EXISTS(SELECT 1 FROM k_edits WHERE transaction_id=e.transaction_id) DESC,e.original_time,e.transaction_id LIMIT 1",selected_fields(),mutation_join(),AUTHORIZED_EDIT))
        .bind(signature).fetch_all(&mut **tx).await?;
    if !fenced(tx, &selected).await? {
        return Ok(false);
    }
    let old: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT transaction_id FROM k_edits WHERE sender_signature=$1")
            .bind(signature)
            .fetch_optional(&mut **tx)
            .await?;
    if old.as_ref()
        == selected
            .first()
            .map(|r| r.get::<&[u8], _>("transaction_id"))
            .map(|r| r.to_vec())
            .as_ref()
    {
        return Ok(true);
    }
    sqlx::query("DELETE FROM k_edits WHERE sender_signature=$1")
        .bind(signature)
        .execute(&mut **tx)
        .await?;
    if let Some(row) = selected.first() {
        sqlx::query("INSERT INTO k_edits(transaction_id,block_time,sender_pubkey,sender_signature,post_id,base64_encoded_message) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(row.get::<Vec<u8>,_>("transaction_id")).bind(row.get::<i64,_>("original_time")).bind(row.get::<Vec<u8>,_>("sender")).bind(signature).bind(row.get::<Vec<u8>,_>("target")).bind(row.get::<String,_>("message")).execute(&mut **tx).await?;
        restore_audit(tx, "k_edits", row).await?;
    }
    Ok(true)
}

async fn delete_audit(tx: &mut Tx<'_>, post: &[u8]) -> Result<bool> {
    let selected=sqlx::query(&format!("SELECT {} {} WHERE e.target=$1 AND e.kind='delete' AND e.shape_valid AND e.sender=e.target_author AND c.accepting_block IS NOT NULL AND t.accepting_block IS NOT NULL ORDER BY EXISTS(SELECT 1 FROM k_deletes WHERE transaction_id=e.transaction_id) DESC,e.original_time,e.transaction_id LIMIT 1",selected_fields(),mutation_join()))
        .bind(post).fetch_all(&mut **tx).await?;
    if !fenced(tx, &selected).await? {
        return Ok(false);
    }
    let old: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT transaction_id FROM k_deletes WHERE post_id=$1")
            .bind(post)
            .fetch_optional(&mut **tx)
            .await?;
    if old.as_ref()
        == selected
            .first()
            .map(|r| r.get::<Vec<u8>, _>("transaction_id"))
            .as_ref()
    {
        return Ok(true);
    }
    sqlx::query("DELETE FROM k_deletes WHERE post_id=$1")
        .bind(post)
        .execute(&mut **tx)
        .await?;
    if let Some(row) = selected.first() {
        sqlx::query("INSERT INTO k_deletes(transaction_id,block_time,sender_pubkey,post_id) VALUES($1,$2,$3,$4)")
            .bind(row.get::<Vec<u8>,_>("transaction_id")).bind(row.get::<i64,_>("original_time")).bind(row.get::<Vec<u8>,_>("sender")).bind(post).execute(&mut **tx).await?;
        restore_audit(tx, "k_deletes", row).await?;
    }
    Ok(true)
}

async fn reconcile_mentions(
    tx: &mut Tx<'_>,
    base: &[u8],
    signature: &[u8],
    current: &[u8],
    effective: Option<&[u8]>,
    all: bool,
) -> Result<bool> {
    let pubkeys: Vec<Vec<u8>> = if all {
        sqlx::query_scalar(&format!("SELECT DISTINCT m.mentioned_pubkey FROM social_shared.edit_mentions m JOIN social_shared.extensions e USING(transaction_id) JOIN protocol_indexer.canonical c USING(transaction_id) JOIN protocol_indexer.canonical t ON t.transaction_id=e.target WHERE e.target_signature=$1 AND {}",AUTHORIZED_EDIT))
            .bind(signature).fetch_all(&mut **tx).await?
    } else {
        sqlx::query_scalar(
            "SELECT DISTINCT mentioned_pubkey FROM social_shared.edit_mentions WHERE transaction_id=$1 OR transaction_id=$2",
        )
        .bind(current)
        .bind(effective)
        .fetch_all(&mut **tx)
        .await?
    };
    for pubkey in pubkeys {
        let original:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM social_shared.actions a CROSS JOIN LATERAL jsonb_array_elements(a.snapshot->'k_mentions') m WHERE a.transaction_id=$1 AND m->>'mentioned_pubkey'=$2)")
            .bind(base).bind(format!("\\x{}",hex::encode(&pubkey))).fetch_one(&mut **tx).await?;
        if original {
            continue;
        }
        let selected=sqlx::query(&format!("SELECT {} {} JOIN social_shared.edit_mentions m ON m.transaction_id=e.transaction_id WHERE e.target_signature=$1 AND m.mentioned_pubkey=$2 AND {} ORDER BY e.original_time,e.transaction_id LIMIT 1",selected_fields(),mutation_join(),AUTHORIZED_EDIT))
            .bind(signature).bind(&pubkey).fetch_all(&mut **tx).await?;
        if !fenced(tx, &selected).await? {
            return Ok(false);
        }
        let existing: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM k_mentions WHERE content_id=$1 AND mentioned_pubkey=$2 LIMIT 1",
        )
        .bind(base)
        .bind(&pubkey)
        .fetch_optional(&mut **tx)
        .await?;
        if selected.is_empty() {
            if let Some(id) = existing {
                sqlx::query("DELETE FROM k_mentions WHERE content_id=$1 AND id=$2")
                    .bind(base)
                    .bind(id)
                    .execute(&mut **tx)
                    .await?;
            }
            continue;
        }
        if existing.is_some() {
            continue;
        }
        let row = &selected[0];
        let time: i64 = row.get("original_time");
        let sender: Vec<u8> = row.get("sender");
        let saved=sqlx::query("SELECT id,block_time FROM social_shared.extension_mentions WHERE signature=$1 AND mentioned_pubkey=$2").bind(signature).bind(&pubkey).fetch_optional(&mut **tx).await?;
        let kind:String=sqlx::query_scalar("SELECT CASE WHEN content_type IN('reply','quote') THEN content_type ELSE 'post' END FROM k_contents WHERE transaction_id=$1").bind(base).fetch_one(&mut **tx).await?;
        // The stable recipient cache also records first evaluation, including
        // quiet replay. A different edit may supply this recipient before its
        // own job runs; restoring the base must not notify that supplier twice
        // or consume its other, as-yet unevaluated recipients.
        let notify = saved.is_none() && eligible(tx, row).await?;
        sqlx::query("SELECT set_config('kasanova.social_notify_allowed',$1,true)")
            .bind(if notify { "true" } else { "false" })
            .execute(&mut **tx)
            .await?;
        if let Some(saved) = saved {
            sqlx::query("INSERT INTO k_mentions(id,content_id,content_type,mentioned_pubkey,block_time,sender_pubkey) VALUES($1,$2,$3,$4,$5,$6)")
                .bind(saved.get::<i64,_>("id")).bind(base).bind(kind).bind(pubkey).bind(saved.get::<i64,_>("block_time")).bind(sender).execute(&mut **tx).await?;
        } else {
            let id:i64=sqlx::query_scalar("INSERT INTO k_mentions(content_id,content_type,mentioned_pubkey,block_time,sender_pubkey) VALUES($1,$2,$3,$4,$5) RETURNING id")
                .bind(base).bind(kind).bind(&pubkey).bind(time).bind(sender).fetch_one(&mut **tx).await?;
            sqlx::query("INSERT INTO social_shared.extension_mentions VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING").bind(signature).bind(pubkey).bind(id).bind(time).execute(&mut **tx).await?;
        }
    }
    Ok(true)
}

pub async fn apply_policy(
    tx: &mut Tx<'_>,
    policy: ContentPolicy,
    base: &[u8],
    signature: &[u8],
    current: &[u8],
    fresh: bool,
) -> Result<bool> {
    // Audit only current/selected effective mutations; never rewrite historical
    // audit/ledger rows while applying a new edit of one post.
    if let Some(row) = policy.edits.first() {
        if !edit_audit(tx, &row.get::<Vec<u8>, _>("signature")).await? {
            return Ok(false);
        }
    }
    if let Some(row) = policy.deletes.first() {
        if !delete_audit(tx, &row.get::<Vec<u8>, _>("target")).await? {
            return Ok(false);
        }
    }
    if !policy.hidden {
        let (message, time) = if let Some(row) = policy.edits.first() {
            (
                row.get::<String, _>("message"),
                Some(row.get::<i64, _>("original_time")),
            )
        } else {
            (policy.original, None)
        };
        sqlx::query("UPDATE k_contents SET base64_encoded_message=$2,edited_at=$3 WHERE transaction_id=$1 AND (base64_encoded_message IS DISTINCT FROM $2 OR edited_at IS DISTINCT FROM $3)")
            .bind(base).bind(message).bind(time).execute(&mut **tx).await?;
        let effective = policy
            .edits
            .first()
            .map(|row| row.get::<Vec<u8>, _>("transaction_id"));
        if !reconcile_mentions(tx, base, signature, current, effective.as_deref(), fresh).await? {
            return Ok(false);
        }
    } else {
        for table in ["k_votes", "k_poll_votes"] {
            sqlx::query(&format!("DELETE FROM {table} WHERE {} IN(SELECT transaction_id FROM social_shared.content_keys WHERE signature=$1)",if table=="k_votes" {"post_id"} else {"poll_id"})).bind(signature).execute(&mut **tx).await?;
        }
    }
    for row in policy
        .edits
        .iter()
        .chain(policy.deletes.iter())
        .chain(policy.withdraws.iter())
    {
        sqlx::query("UPDATE social_shared.actions SET notification_consumed=true WHERE transaction_id=$1 AND NOT notification_consumed").bind(row.get::<Vec<u8>,_>("transaction_id")).execute(&mut **tx).await?;
    }
    sqlx::query("UPDATE social_shared.actions a SET notification_consumed=true FROM social_shared.extensions e JOIN protocol_indexer.canonical c USING(transaction_id) JOIN protocol_indexer.canonical t ON t.transaction_id=e.target WHERE a.transaction_id=$1 AND e.transaction_id=a.transaction_id AND NOT a.notification_consumed AND e.shape_valid AND e.sender=e.target_author AND c.accepting_block IS NOT NULL AND t.accepting_block IS NOT NULL AND (e.kind='delete' OR (e.kind='edit' AND e.target_kind<>'poll' AND e.original_time>e.target_time AND e.original_time::numeric-e.target_time::numeric<=7200000))")
        .bind(current).execute(&mut **tx).await?;
    sqlx::query("SELECT set_config('kasanova.social_notify_allowed','false',true)")
        .execute(&mut **tx)
        .await?;
    Ok(true)
}

async fn apply_poll_vote(
    tx: &mut Tx<'_>,
    _processor: &KProtocolProcessor,
    value: &VerifiedKaPosts,
    parent: &Target,
) -> Result<bool> {
    let KaPostsAction::PollVote(v) = value.action() else {
        return Ok(true);
    };
    let signature = hex::decode(&v.sender_signature)?;
    let id = hex::decode(&value.transaction().transaction_id)?;
    let visible: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM k_contents WHERE sender_signature=$1 AND content_type='poll')",
    )
    .bind(&parent.signature)
    .fetch_one(&mut **tx)
    .await?;
    let time: i64 = sqlx::query_scalar(
        "SELECT original_time FROM social_shared.actions WHERE transaction_id=$1",
    )
    .bind(hex::decode(&value.transaction().transaction_id)?)
    .fetch_one(&mut **tx)
    .await?;
    let accepted:bool=sqlx::query_scalar("SELECT accepting_block IS NOT NULL FROM protocol_indexer.canonical WHERE transaction_id=$1").bind(hex::decode(&value.transaction().transaction_id)?).fetch_one(&mut **tx).await?;
    let valid = parent.accepted
        && visible
        && accepted
        && parent.poll.as_ref().is_some_and(|poll| {
            v.option_index >= 0
                && (v.option_index as usize) < poll.options_b64_csv.split(',').count()
                && time < poll.closes_at_ms
        });
    if valid {
        sqlx::query("INSERT INTO social_shared.poll_vote_facts SELECT transaction_id,revision,$2 FROM protocol_indexer.canonical WHERE transaction_id=$1 ON CONFLICT(transaction_id) DO UPDATE SET revision=EXCLUDED.revision,post_signature=EXCLUDED.post_signature")
            .bind(&id).bind(&parent.signature).execute(&mut **tx).await?;
    } else {
        sqlx::query("DELETE FROM social_shared.poll_vote_facts WHERE transaction_id=$1")
            .bind(&id)
            .execute(&mut **tx)
            .await?;
    }
    let selected=sqlx::query("SELECT e.*,a.original_time,c.revision AS canonical_revision,t.revision AS target_revision FROM social_shared.poll_vote_facts f JOIN social_shared.extensions e USING(transaction_id) JOIN social_shared.actions a USING(transaction_id) JOIN protocol_indexer.canonical c USING(transaction_id) JOIN protocol_indexer.canonical t ON t.transaction_id=e.target WHERE e.signature=$1 AND f.revision=c.revision AND c.accepting_block IS NOT NULL AND t.accepting_block IS NOT NULL AND EXISTS(SELECT 1 FROM k_contents WHERE sender_signature=f.post_signature AND content_type='poll') ORDER BY EXISTS(SELECT 1 FROM k_poll_votes WHERE transaction_id=e.transaction_id) DESC,a.original_time,e.transaction_id LIMIT 1")
        .bind(&signature).fetch_all(&mut **tx).await?;
    if !fenced(tx, &selected).await? {
        return Ok(false);
    }
    sqlx::query("DELETE FROM k_poll_votes WHERE sender_signature=$1")
        .bind(&signature)
        .execute(&mut **tx)
        .await?;
    if let Some(selected) = selected.first() {
        sqlx::query("INSERT INTO k_poll_votes(transaction_id,poll_id,voter_pubkey,option_index,block_time,sender_signature) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(selected.get::<Vec<u8>,_>("transaction_id")).bind(selected.get::<Vec<u8>,_>("target")).bind(selected.get::<Vec<u8>,_>("sender")).bind(selected.get::<i32,_>("option_index")).bind(selected.get::<i64,_>("original_time")).bind(signature).execute(&mut **tx).await?;
    }
    Ok(true)
}

pub async fn ordinary_vote_hidden(
    tx: &mut Tx<'_>,
    processor: &KProtocolProcessor,
    value: &VerifiedAction,
) -> Result<Option<bool>> {
    let KActionType::Vote(v) = value.action() else {
        return Ok(Some(false));
    };
    match target(tx, processor, &hex::decode(&v.post_id)?).await? {
        TargetState::Busy => Ok(None),
        TargetState::Ready(parent) => {
            let acquired: bool =
                sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1,736124))")
                    .bind(format!("k_contents:{}", hex::encode(&parent.signature)))
                    .fetch_one(&mut **tx)
                    .await?;
            if !acquired {
                return Ok(None);
            }
            let deleted:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM social_shared.extensions e JOIN protocol_indexer.canonical c USING(transaction_id) JOIN protocol_indexer.canonical t ON t.transaction_id=e.target WHERE e.kind='delete' AND e.shape_valid AND e.sender=$2 AND (e.target=$1 OR e.target_signature=$3) AND c.accepting_block IS NOT NULL AND t.accepting_block IS NOT NULL)")
                .bind(hex::decode(&v.post_id)?).bind(parent.author).bind(parent.signature).fetch_one(&mut **tx).await?;
            Ok(Some(deleted))
        }
        _ => Ok(Some(false)),
    }
}

pub async fn content_reconciled(tx: &mut Tx<'_>, signature: &[u8]) -> Result<()> {
    // Producers never update existing jobs. Dependencies retry through their
    // own transaction/SDK path, avoiding reverse consumer-job lock order.
    sqlx::query("INSERT INTO protocol_indexer.consumer_jobs(consumer,transaction_id) SELECT 'native-social-v1',d.transaction_id FROM social_shared.ordinary_vote_targets d JOIN social_shared.content_keys k ON k.transaction_id=d.post_id WHERE k.signature=$1 UNION SELECT 'native-social-v1',e.transaction_id FROM social_shared.extensions e LEFT JOIN social_shared.content_keys k ON k.transaction_id=e.target WHERE e.kind IN('pollvote','edit','delete') AND (k.signature=$1 OR e.target_signature=$1) ON CONFLICT DO NOTHING")
        .bind(signature).execute(&mut **tx).await?;
    Ok(())
}
