//! Verified native tip metadata on the existing source-bound read transaction.
use crate::{
    database_postgres_impl::PostgresDbManager,
    database_trait::{DatabaseError, DatabaseResult},
    models::{ApiError, ContentRecord, KPostRecord, KReplyRecord},
    shared_reads::ReadSession,
    web_server::{AppState, check_rate_limit},
};
use axum::{
    Json,
    extract::{ConnectInfo, Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};

pub(crate) trait TipCount {
    fn tip_content_id(&self) -> Option<&str>;
    fn set_tip_count(&mut self, count: u64);
    fn set_protocol(&mut self, protocol: String);
    fn set_edited_at(&mut self, _edited_at: Option<u64>) {}
    fn set_legacy_k_compatible(&mut self, _compatible: bool) {}
    fn set_reference_legacy_k_compatible(&mut self, _compatible: bool) {}
    fn reference_content_id(&self) -> Option<&str> {
        None
    }
    fn set_reference_protocol(&mut self, _protocol: String) {}
    fn set_poll(&mut self, _poll: Option<crate::models::PollData>) {}
}
impl TipCount for KPostRecord {
    fn set_edited_at(&mut self, edited_at: Option<u64>) {
        self.edited_at = edited_at;
    }
    fn set_legacy_k_compatible(&mut self, compatible: bool) {
        self.legacy_k_compatible = Some(compatible);
    }
    fn set_reference_legacy_k_compatible(&mut self, compatible: bool) {
        self.referenced_legacy_k_compatible = Some(compatible);
    }
    fn reference_content_id(&self) -> Option<&str> {
        // Enrich only references whose content/author already passed the
        // existing read view. Hidden or absent references reveal no metadata.
        if self.referenced_message.is_some() && self.referenced_sender_pubkey.is_some() {
            self.referenced_content_id.as_deref()
        } else {
            None
        }
    }
    fn set_reference_protocol(&mut self, protocol: String) {
        self.referenced_protocol = Some(protocol);
    }
    fn set_poll(&mut self, poll: Option<crate::models::PollData>) {
        if poll.is_some() {
            self.content_type = Some("poll".into());
        }
        self.poll = poll;
    }
    fn set_protocol(&mut self, protocol: String) {
        self.protocol = Some(protocol);
    }
    fn tip_content_id(&self) -> Option<&str> {
        Some(&self.transaction_id)
    }
    fn set_tip_count(&mut self, count: u64) {
        self.tips_count = Some(count);
    }
}
impl TipCount for KReplyRecord {
    fn set_edited_at(&mut self, edited_at: Option<u64>) {
        self.edited_at = edited_at;
    }
    fn set_legacy_k_compatible(&mut self, compatible: bool) {
        self.legacy_k_compatible = Some(compatible);
    }
    fn set_protocol(&mut self, protocol: String) {
        self.protocol = Some(protocol);
    }
    fn tip_content_id(&self) -> Option<&str> {
        Some(&self.transaction_id)
    }
    fn set_tip_count(&mut self, count: u64) {
        self.tips_count = Some(count);
    }
}
// Votes have a protocol but do not advertise a post's tip/poll metadata.
impl TipCount for crate::models::KVoteRecord {
    fn set_legacy_k_compatible(&mut self, compatible: bool) {
        self.legacy_k_compatible = Some(compatible);
    }
    fn tip_content_id(&self) -> Option<&str> {
        Some(&self.transaction_id)
    }
    fn set_tip_count(&mut self, _count: u64) {}
    fn set_protocol(&mut self, protocol: String) {
        self.protocol = Some(protocol);
    }
}
impl TipCount for ContentRecord {
    fn set_edited_at(&mut self, edited_at: Option<u64>) {
        match self {
            Self::Post(p) => p.set_edited_at(edited_at),
            Self::Reply(p) => p.set_edited_at(edited_at),
            Self::Vote(_) => {}
        }
    }
    fn set_legacy_k_compatible(&mut self, compatible: bool) {
        match self {
            Self::Post(p) => p.set_legacy_k_compatible(compatible),
            Self::Reply(p) => p.set_legacy_k_compatible(compatible),
            Self::Vote(p) => p.set_legacy_k_compatible(compatible),
        }
    }
    fn set_reference_legacy_k_compatible(&mut self, compatible: bool) {
        if let Self::Post(p) = self {
            p.set_reference_legacy_k_compatible(compatible);
        }
    }
    fn reference_content_id(&self) -> Option<&str> {
        match self {
            Self::Post(p) => p.reference_content_id(),
            _ => None,
        }
    }
    fn set_reference_protocol(&mut self, protocol: String) {
        if let Self::Post(p) = self {
            p.set_reference_protocol(protocol);
        }
    }
    fn set_poll(&mut self, poll: Option<crate::models::PollData>) {
        if let Self::Post(p) = self {
            p.set_poll(poll);
        }
    }
    fn set_protocol(&mut self, protocol: String) {
        match self {
            Self::Post(p) => p.set_protocol(protocol),
            Self::Reply(p) => p.set_protocol(protocol),
            Self::Vote(p) => p.set_protocol(protocol),
        }
    }
    fn tip_content_id(&self) -> Option<&str> {
        match self {
            Self::Post(p) => p.tip_content_id(),
            Self::Reply(p) => p.tip_content_id(),
            Self::Vote(p) => p.tip_content_id(),
        }
    }
    fn set_tip_count(&mut self, count: u64) {
        match self {
            Self::Post(p) => p.set_tip_count(count),
            Self::Reply(p) => p.set_tip_count(count),
            _ => {}
        }
    }
}
impl TipCount for crate::models::NotificationContentRecord {
    fn set_edited_at(&mut self, edited_at: Option<u64>) {
        self.content.set_edited_at(edited_at);
    }
    fn set_legacy_k_compatible(&mut self, compatible: bool) {
        self.content.set_legacy_k_compatible(compatible);
    }
    fn set_reference_legacy_k_compatible(&mut self, compatible: bool) {
        self.content.set_reference_legacy_k_compatible(compatible);
    }
    fn reference_content_id(&self) -> Option<&str> {
        self.content.reference_content_id()
    }
    fn set_reference_protocol(&mut self, protocol: String) {
        self.content.set_reference_protocol(protocol);
    }
    fn tip_content_id(&self) -> Option<&str> {
        self.content.tip_content_id()
    }
    fn set_tip_count(&mut self, count: u64) {
        self.content.set_tip_count(count);
    }
    fn set_protocol(&mut self, protocol: String) {
        self.content.set_protocol(protocol);
    }
    fn set_poll(&mut self, poll: Option<crate::models::PollData>) {
        self.content.set_poll(poll);
    }
}
fn lookup_error(_: sqlx::Error) -> DatabaseError {
    DatabaseError::QueryError("Shared tip lookup unavailable".into())
}
fn content_id(value: &str) -> DatabaseResult<Vec<u8>> {
    let id = hex::decode(value)
        .map_err(|_| DatabaseError::QueryError("Invalid tip content identity".into()))?;
    if id.len() != 32 {
        return Err(DatabaseError::QueryError(
            "Invalid tip content identity".into(),
        ));
    }
    Ok(id)
}

fn action_protocol(
    payload_hex: &str,
    provenance: &str,
    retained_root: Option<&str>,
) -> DatabaseResult<String> {
    // Retained SDK fields were signed without the root. Their reconstructed
    // payload uses an import hint; it cannot prove the original wire namespace.
    if provenance == "retained_native" {
        return match retained_root {
            Some("k") => Ok("k".into()),
            Some("kchat") => Ok("kchat".into()),
            None => Ok("unknown".into()),
            _ => Err(DatabaseError::QueryError(
                "Invalid retained Social protocol".into(),
            )),
        };
    }
    let invalid = || DatabaseError::QueryError("Invalid Social protocol".into());
    let bytes = hex::decode(payload_hex).map_err(|_| invalid())?;
    let mut fields = bytes.splitn(3, |b| *b == b':');
    let root = fields.next().ok_or_else(invalid)?;
    if fields.next() != Some(b"1".as_slice()) || fields.next().is_none() {
        return Err(invalid());
    }
    match root {
        b"k" => Ok("k".into()),
        b"kchat" => Ok("kchat".into()),
        _ => Err(invalid()),
    }
}

impl PostgresDbManager {
    // One bounded page query, on the same repeatable-read snapshot as its posts.
    // Legacy readers omit the field, rather than advertising an unverified zero.
    pub(crate) async fn enrich_tip_counts<T: TipCount>(
        &self,
        read: &mut ReadSession<'_>,
        items: &mut [T],
    ) -> DatabaseResult<()> {
        if matches!(read, ReadSession::Legacy(_)) || items.is_empty() {
            return Ok(());
        }
        let ids = items
            .iter()
            .flat_map(|p| {
                p.tip_content_id()
                    .into_iter()
                    .chain(p.reference_content_id())
            })
            .map(content_id)
            .collect::<DatabaseResult<Vec<_>>>()?;
        if ids.is_empty() {
            return Ok(());
        }
        // Preserve the actual namespace of the visible representative. Markers
        // and author identities cannot establish a post's transaction root.
        let rows = sqlx::query("SELECT a.transaction_id,a.source_payload,a.provenance,proof.root AS retained_root,CASE WHEN a.provenance='retained_native' AND proof.root IS NULL THEN social_shared_legacy_api.is_k_member(a.transaction_id) ELSE false END AS legacy_k_member,post.content_type,post.edited_at,poll.options AS poll_options,poll.closes_at AS poll_closes_at,(SELECT COUNT(*)::bigint FROM social_shared.current_tips t WHERE t.content_transaction_id=a.transaction_id) AS tips_count FROM social_shared_api.current_actions a LEFT JOIN LATERAL (SELECT proof.root FROM social_shared.current_retained_wire_roots proof WHERE proof.transaction_id=a.transaction_id OFFSET 0) proof ON true LEFT JOIN social_shared_api.k_contents post USING(transaction_id) LEFT JOIN social_shared_api.current_polls poll ON poll.post_id=a.transaction_id WHERE a.transaction_id=ANY($1)")
            .bind(&ids).fetch_all(&mut **read).await.map_err(lookup_error)?;
        let mut counts = HashMap::new();
        for row in rows {
            let count = u64::try_from(row.get::<i64, _>("tips_count"))
                .map_err(|_| DatabaseError::QueryError("Invalid tip count".into()))?;
            let protocol = action_protocol(
                row.get("source_payload"),
                row.get("provenance"),
                row.get("retained_root"),
            )?;
            // Membership preserves the pre-existing K action contract only; it
            // never supplies missing original-wire evidence. Known roots win.
            let legacy_k_compatible =
                protocol == "unknown" && row.get::<bool, _>("legacy_k_member");
            let poll = if row.get::<Option<&str>, _>("content_type") == Some("poll") {
                let options: String = row
                    .get::<Option<String>, _>("poll_options")
                    .ok_or_else(|| DatabaseError::QueryError("Missing verified poll".into()))?;
                let options: Vec<String> = options.split(',').map(|s| s.trim().into()).collect();
                let closes_at = row
                    .get::<Option<i64>, _>("poll_closes_at")
                    .and_then(|v| u64::try_from(v).ok())
                    .filter(|v| *v > 0)
                    .ok_or_else(|| DatabaseError::QueryError("Invalid poll close time".into()))?;
                if !(2..=4).contains(&options.len()) {
                    return Err(DatabaseError::QueryError("Invalid poll options".into()));
                }
                Some(crate::models::PollData { options, closes_at })
            } else {
                None
            };
            let edited_at = row.get::<Option<i64>, _>("edited_at")
                .map(u64::try_from)
                .transpose()
                .map_err(|_| DatabaseError::QueryError("Invalid Social edit timestamp".into()))?;
            counts.insert(
                row.get::<Vec<u8>, _>("transaction_id"),
                (count, protocol, poll, legacy_k_compatible, edited_at),
            );
        }
        for item in items {
            if let Some(id) = item.tip_content_id() {
                let (count, protocol, poll, legacy_k_compatible, edited_at) = counts
                    .get(&content_id(id)?)
                    .ok_or_else(|| DatabaseError::QueryError("Missing Social protocol".into()))?;
                let count = *count;
                let protocol = protocol.clone();
                item.set_tip_count(count);
                if self
                    .shared
                    .as_ref()
                    .is_some_and(|b| b.read_contract == crate::shared_rooms::ReadContract::Unified)
                {
                    item.set_legacy_k_compatible(*legacy_k_compatible);
                    item.set_edited_at(*edited_at);
                    let reference_protocol = if let Some(reference) = item.reference_content_id() {
                        Some(
                            counts
                                .get(&content_id(reference)?)
                                .ok_or_else(|| {
                                    DatabaseError::QueryError(
                                        "Missing quoted Social protocol".into(),
                                    )
                                })?
                                .clone(),
                        )
                    } else {
                        None
                    };
                    item.set_protocol(protocol);
                    if let Some((_, reference_protocol, _, compatible, _)) = reference_protocol {
                        item.set_reference_protocol(reference_protocol);
                        item.set_reference_legacy_k_compatible(compatible);
                    }
                    item.set_poll(poll.clone());
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn tip_details(&self, id: &[u8]) -> DatabaseResult<Option<TipDetails>> {
        let mut read = self.read_session().await?;
        if matches!(read, ReadSession::Legacy(_)) {
            return Ok(None);
        }
        let row = sqlx::query("SELECT transaction_id,content_transaction_id,sender_pubkey,author_pubkey,amount FROM social_shared.current_tips WHERE transaction_id=$1")
            .bind(id).fetch_optional(&mut *read).await.map_err(lookup_error)?;
        row.map(|row| {
            let amount = u64::try_from(row.get::<i64, _>("amount"))
                .map_err(|_| DatabaseError::QueryError("Invalid tip amount".into()))?;
            Ok(TipDetails {
                id: hex::encode(row.get::<Vec<u8>, _>("transaction_id")),
                parent_post_id: hex::encode(row.get::<Vec<u8>, _>("content_transaction_id")),
                user_public_key: hex::encode(row.get::<Vec<u8>, _>("sender_pubkey")),
                author_public_key: hex::encode(row.get::<Vec<u8>, _>("author_pubkey")),
                amount_sompi: amount.to_string(),
            })
        })
        .transpose()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TipDetails {
    pub id: String,
    pub parent_post_id: String,
    pub user_public_key: String,
    pub author_public_key: String,
    pub amount_sompi: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TipDetailsResponse {
    pub tip: TipDetails,
}
#[derive(Deserialize)]
pub(crate) struct TipQuery {
    pub id: String,
}

pub(crate) async fn handle_tip_details(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<TipQuery>,
) -> Result<Json<TipDetailsResponse>, (StatusCode, Json<ApiError>)> {
    check_rate_limit(&state, addr).await?;
    let failure = |status, code: &str| {
        (
            status,
            Json(ApiError {
                error: "Tip is not available".into(),
                code: code.into(),
            }),
        )
    };
    let id =
        content_id(&query.id).map_err(|_| failure(StatusCode::BAD_REQUEST, "INVALID_TIP_ID"))?;
    let tip = state
        .db
        .get_tip_details(&id)
        .await
        .map_err(|_| failure(StatusCode::SERVICE_UNAVAILABLE, "READ_UNAVAILABLE"))?
        .ok_or_else(|| failure(StatusCode::NOT_FOUND, "NOT_FOUND"))?;
    Ok(Json(TipDetailsResponse { tip }))
}
