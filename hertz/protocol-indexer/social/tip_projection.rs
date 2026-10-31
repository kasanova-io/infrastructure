//! Current accepted tip facts, reusing the native SDK and upstream payment rule.
use crate::database::Transaction;
use crate::k_protocol::{KActionType, KProtocolProcessor, VerifiedTip};
use crate::shared_social::{capture_snapshot, restore_snapshot};
use crate::tip_evidence::TipOutputEvidence;
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{Postgres, Row};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TipReconcile {
    Applied,
    Busy,
    Pending,
    ProofPending,
}

async fn clear_subject(tx: &mut sqlx::Transaction<'_, Postgres>, subject: &str) -> Result<()> {
    sqlx::query("DELETE FROM k_mentions m USING social_shared.actions a WHERE a.subject=$1 AND m.content_id=a.transaction_id").bind(subject).execute(&mut **tx).await?;
    sqlx::query("DELETE FROM k_tips t USING social_shared.actions a WHERE a.subject=$1 AND t.transaction_id=a.transaction_id").bind(subject).execute(&mut **tx).await?;
    Ok(())
}

/// Validate EACH current payment before signature deduplication. An invalid or
/// unavailable earlier copy must not prevent a later valid paying copy winning.
async fn prepare_payment(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    processor: &KProtocolProcessor,
    network: &str,
    current: &[u8],
    revision: i64,
    accepted: bool,
    body: &Value,
    tip: &VerifiedTip,
) -> Result<TipReconcile> {
    let post = hex::decode(&tip.tip().post_id)?;
    let mut valid = None;
    if accepted {
        let target=sqlx::query("SELECT accepting_block,source_transaction::text AS body FROM protocol_indexer.canonical WHERE transaction_id=$1 FOR SHARE SKIP LOCKED")
            .bind(&post).fetch_optional(&mut **tx).await?;
        let Some(target) = target else {
            return Ok(TipReconcile::Pending);
        };
        if target
            .get::<Option<Vec<u8>>, _>("accepting_block")
            .is_some()
        {
            let target_body: Value = serde_json::from_str(target.get::<&str, _>("body"))?;
            let transaction = Transaction {
                transaction_id: hex::encode(&post),
                payload: target_body["payload"]
                    .as_str()
                    .and_then(|v| v.strip_prefix("\\x"))
                    .map(str::to_owned),
                block_time: target_body["block_time"].as_i64(),
            };
            if let Some(parent) = processor.verify_transaction(&transaction) {
                let parent_fields = match parent.action() {
                    KActionType::Post(v) => Some((&v.sender_pubkey, &v.sender_signature)),
                    KActionType::Reply(v) => Some((&v.sender_pubkey, &v.sender_signature)),
                    KActionType::Quote(v) => Some((&v.sender_pubkey, &v.sender_signature)),
                    _ => None,
                };
                if let Some((author, signature)) = parent_fields {
                    if hex::decode(author)? == hex::decode(&tip.tip().author_pubkey)? {
                        let raw:Option<String>=sqlx::query_scalar("SELECT receipt::text FROM protocol_indexer.evidence WHERE transaction_id=$1")
                            .bind(current).fetch_optional(&mut **tx).await?;
                        let Some(raw) = raw else {
                            return Ok(TipReconcile::ProofPending);
                        };
                        let receipt: Value = serde_json::from_str(&raw)?;
                        let Ok(proof) =
                            TipOutputEvidence::checked(network, current, body, &receipt)
                        else {
                            return Ok(TipReconcile::ProofPending);
                        };
                        let amount = proof.payment_to_author(tip)?;
                        if let Ok(amount) = i64::try_from(amount) {
                            if amount > 0 {
                                valid = Some((amount, hex::decode(signature)?));
                            }
                        }
                    }
                }
            }
        }
    }
    if let Some((amount, signature)) = valid {
        sqlx::query("INSERT INTO social_shared.tip_payments(transaction_id,revision,amount,post_signature) VALUES($1,$2,$3,$4) ON CONFLICT(transaction_id) DO UPDATE SET revision=EXCLUDED.revision,amount=EXCLUDED.amount,post_signature=EXCLUDED.post_signature")
            .bind(current).bind(revision).bind(amount).bind(&signature).execute(&mut **tx).await?;
        sqlx::query(
            "UPDATE social_shared.tip_targets SET post_signature=$1 WHERE transaction_id=$2",
        )
        .bind(signature)
        .bind(current)
        .execute(&mut **tx)
        .await?;
    } else {
        sqlx::query("DELETE FROM social_shared.tip_payments WHERE transaction_id=$1")
            .bind(current)
            .execute(&mut **tx)
            .await?;
    }
    Ok(TipReconcile::Applied)
}

pub(crate) async fn reconcile_tip(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    processor: &KProtocolProcessor,
    network: &str,
    subject: &str,
    current: &[u8],
    revision: i64,
    accepted: bool,
    body: &Value,
    tip: &VerifiedTip,
) -> Result<TipReconcile> {
    let prepared = prepare_payment(
        tx, processor, network, current, revision, accepted, body, tip,
    )
    .await?;
    if prepared != TipReconcile::Applied {
        return Ok(prepared);
    }
    let selected=sqlx::query("SELECT a.transaction_id,a.original_time,a.source_payload,a.notification_consumed,a.snapshot::text AS snapshot,p.amount,p.post_signature,c.revision,d.post_id,target.revision AS target_revision FROM social_shared.actions a JOIN social_shared.tip_payments p USING(transaction_id) JOIN protocol_indexer.canonical c USING(transaction_id) JOIN social_shared.tip_targets d USING(transaction_id) JOIN protocol_indexer.canonical target ON target.transaction_id=d.post_id WHERE a.subject=$1 AND p.revision=c.revision AND c.accepting_block IS NOT NULL AND target.accepting_block IS NOT NULL ORDER BY EXISTS(SELECT 1 FROM k_tips t WHERE t.transaction_id=a.transaction_id) DESC,a.original_time,a.transaction_id LIMIT 1")
        .bind(subject).fetch_optional(&mut **tx).await?;
    let mut evaluated = vec![current.to_vec()];
    if let Some(selected) = &selected {
        for (id, expected) in [
            (
                selected.get::<Vec<u8>, _>("transaction_id"),
                selected.get::<i64, _>("revision"),
            ),
            (
                selected.get::<Vec<u8>, _>("post_id"),
                selected.get::<i64, _>("target_revision"),
            ),
        ] {
            let locked:Option<i64>=sqlx::query_scalar("SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=$1 FOR SHARE SKIP LOCKED").bind(&id).fetch_optional(&mut **tx).await?;
            if locked != Some(expected) {
                return Ok(TipReconcile::Busy);
            }
        }
    }
    clear_subject(tx, subject).await?;
    if let Some(selected) = selected {
        let id: Vec<u8> = selected.get("transaction_id");
        let transaction = Transaction {
            transaction_id: hex::encode(&id),
            payload: Some(selected.get("source_payload")),
            block_time: Some(selected.get("original_time")),
        };
        let verified = processor
            .verify_tip_transaction(&transaction)
            .context("stored native tip no longer verifies")?;
        let notify: bool = if !selected.get::<bool, _>("notification_consumed") {
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM protocol_indexer.events WHERE transaction_id=$1 AND revision<=$2 AND notify_allowed) AND $3 >= (SELECT since_ms FROM social_shared.decoder_epochs WHERE kind='kchat:1:tip')")
                .bind(&id).bind(selected.get::<i64,_>("revision")).bind(selected.get::<i64,_>("original_time")).fetch_one(&mut **tx).await?
        } else {
            false
        };
        sqlx::query("SELECT set_config('kasanova.social_notify_allowed',$1,true)")
            .bind(if notify { "true" } else { "false" })
            .execute(&mut **tx)
            .await?;
        // Upstream tip+mention CTE on the same caller connection, no direct push.
        sqlx::query("WITH tip_insert AS (INSERT INTO k_tips(transaction_id,block_time,sender_pubkey,sender_signature,post_id,author_pubkey,amount) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING RETURNING transaction_id,block_time,sender_pubkey,author_pubkey) INSERT INTO k_mentions(content_id,content_type,mentioned_pubkey,block_time,sender_pubkey) SELECT transaction_id,'tip',author_pubkey,block_time,sender_pubkey FROM tip_insert")
            .bind(&id).bind(selected.get::<i64,_>("original_time")).bind(hex::decode(&verified.tip().sender_pubkey)?).bind(hex::decode(&verified.tip().sender_signature)?)
            .bind(hex::decode(&verified.tip().post_id)?).bind(hex::decode(&verified.tip().author_pubkey)?).bind(selected.get::<i64,_>("amount")).execute(&mut **tx).await?;
        restore_snapshot(
            tx,
            "k_tips",
            &id,
            selected.get::<Option<String>, _>("snapshot").as_deref(),
        )
        .await?;
        capture_snapshot(tx, "k_tips", &id).await?;
        evaluated.push(id);
    }
    sqlx::query("UPDATE social_shared.actions a SET notification_consumed=true FROM protocol_indexer.canonical c WHERE a.transaction_id=ANY($1) AND NOT a.notification_consumed AND a.transaction_id=c.transaction_id AND c.accepting_block IS NOT NULL")
        .bind(evaluated).execute(&mut **tx).await?;
    sqlx::query("SELECT set_config('kasanova.social_notify_allowed','false',true)")
        .execute(&mut **tx)
        .await?;
    Ok(TipReconcile::Applied)
}

pub(crate) async fn content_reconciled(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    subject: &str,
) -> Result<()> {
    sqlx::query("WITH removed AS (DELETE FROM k_tips t USING social_shared.actions a WHERE a.subject=$1 AND a.transaction_id=t.post_id AND (NOT EXISTS(SELECT 1 FROM k_contents p JOIN social_shared.tip_targets d ON d.post_signature=p.sender_signature WHERE d.transaction_id=t.transaction_id) OR NOT EXISTS(SELECT 1 FROM protocol_indexer.canonical c WHERE c.transaction_id=t.post_id AND c.accepting_block IS NOT NULL)) RETURNING t.transaction_id) DELETE FROM k_mentions m USING removed r WHERE m.content_id=r.transaction_id AND m.content_type='tip'")
        .bind(subject).execute(&mut **tx).await?;
    sqlx::query("INSERT INTO protocol_indexer.consumer_jobs(consumer,transaction_id) SELECT 'native-social-v1',d.transaction_id FROM social_shared.tip_targets d JOIN social_shared.actions a ON a.transaction_id=d.post_id WHERE a.subject=$1 ON CONFLICT DO NOTHING")
        .bind(subject).execute(&mut **tx).await?;
    Ok(())
}
