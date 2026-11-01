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
}
impl TipCount for KPostRecord {
    fn tip_content_id(&self) -> Option<&str> {
        Some(&self.transaction_id)
    }
    fn set_tip_count(&mut self, count: u64) {
        self.tips_count = Some(count);
    }
}
impl TipCount for KReplyRecord {
    fn tip_content_id(&self) -> Option<&str> {
        Some(&self.transaction_id)
    }
    fn set_tip_count(&mut self, count: u64) {
        self.tips_count = Some(count);
    }
}
impl TipCount for ContentRecord {
    fn tip_content_id(&self) -> Option<&str> {
        match self {
            Self::Post(p) => p.tip_content_id(),
            Self::Reply(p) => p.tip_content_id(),
            _ => None,
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
            .filter_map(|p| p.tip_content_id())
            .map(content_id)
            .collect::<DatabaseResult<Vec<_>>>()?;
        if ids.is_empty() {
            return Ok(());
        }
        let rows = sqlx::query("SELECT content_transaction_id,COUNT(*)::bigint AS tips_count FROM social_shared.current_tips WHERE content_transaction_id=ANY($1) GROUP BY content_transaction_id")
            .bind(&ids).fetch_all(&mut **read).await.map_err(lookup_error)?;
        let mut counts = HashMap::new();
        for row in rows {
            let count = u64::try_from(row.get::<i64, _>("tips_count"))
                .map_err(|_| DatabaseError::QueryError("Invalid tip count".into()))?;
            counts.insert(row.get::<Vec<u8>, _>("content_transaction_id"), count);
        }
        for item in items {
            if let Some(id) = item.tip_content_id() {
                let count = counts.get(&content_id(id)?).copied().unwrap_or(0);
                item.set_tip_count(count);
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
