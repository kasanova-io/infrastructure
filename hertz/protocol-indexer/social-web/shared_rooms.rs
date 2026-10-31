//! KaChat e2f462 public room DTO/query compatibility (ISC).
//! Existing K-webserver owns routing, rate limits, pool and response middleware.
use crate::{
    database_postgres_impl::PostgresDbManager,
    database_trait::{DatabaseError, DatabaseResult},
    models::ApiError,
    web_server::{AppState, check_rate_limit},
};
use axum::{
    extract::{ConnectInfo, Query, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Row, Transaction};
use std::{net::SocketAddr, sync::Arc};

#[derive(Debug, Clone)]
pub(crate) struct Binding {
    pub network: String,
    pub source_id: String,
    pub statement_timeout_ms: u64,
}
fn query_error(error: sqlx::Error) -> DatabaseError {
    DatabaseError::QueryError(error.to_string())
}
impl Binding {
    async fn check(&self, tx: &mut Transaction<'_, Postgres>) -> DatabaseResult<()> {
        let ready:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM protocol_indexer.config p JOIN social_shared.config s ON s.singleton=p.singleton JOIN public.k_vars k ON k.key='network' WHERE p.singleton AND p.network=$1 AND p.journal_id::text=$2 AND s.network=p.network AND s.source_id=p.journal_id::text AND k.value=p.network)")
            .bind(&self.network).bind(&self.source_id).fetch_one(&mut **tx).await.map_err(query_error)?;
        if !ready {
            return Err(DatabaseError::ConnectionError(
                "shared source/network ownership mismatch".into(),
            ));
        }
        // Refuse a partially provisioned projection, including on an empty page.
        sqlx::query("SELECT 1 FROM social_shared.current_broadcasts LIMIT 0")
            .execute(&mut **tx)
            .await
            .map_err(query_error)?;
        Ok(())
    }
    pub(crate) async fn begin<'a>(
        &self,
        db: &'a PostgresDbManager,
    ) -> DatabaseResult<Transaction<'a, Postgres>> {
        let mut tx = db.pool.begin().await.map_err(query_error)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(query_error)?;
        // Reuse the existing server request budget as a DB-side bound too.
        // The HTTP timeout alone cannot guarantee server-side query cancellation.
        sqlx::query("SELECT set_config('statement_timeout',$1,true)")
            .bind(
                self.statement_timeout_ms
                    .clamp(1, i32::MAX as u64)
                    .to_string(),
            )
            .execute(&mut *tx)
            .await
            .map_err(query_error)?;
        self.check(&mut tx).await?;
        Ok(tx)
    }
}

// Native upstream field names, with additive exclusive continuation metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BroadcastMessage {
    #[serde(rename = "txId")]
    pub tx_id: String,
    pub channel: String,
    #[serde(rename = "senderAddress")]
    pub sender_address: String,
    pub content: String,
    #[serde(rename = "blockTime")]
    pub block_time: i64,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BroadcastsResponse {
    pub messages: Vec<BroadcastMessage>,
    #[serde(rename = "hasMore")]
    pub has_more: bool,
    #[serde(rename = "nextCursor", skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}
#[derive(Debug, Serialize)]
pub(crate) struct BroadcastChannelsResponse {
    pub channels: Vec<String>,
    #[serde(rename = "hasMore")]
    pub has_more: bool,
    #[serde(rename = "nextCursor", skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ChannelQuery {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct RoomQuery {
    pub channel: Option<String>,
    pub limit: Option<u32>,
    pub before: Option<i64>,
    pub cursor: Option<String>,
}
// Opaque route/source/network/channel-bound key. It grants no ownership/auth.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    route: String,
    network: String,
    source: String,
    channel: String,
    time: i64,
    row: i64,
}
fn encode_cursor(binding: &Binding, channel: &str, time: i64, row: i64) -> String {
    // Reuse the product's exact hexadecimal byte representation; no new codec.
    hex::encode(
        serde_json::to_vec(&Cursor {
            version: 1,
            route: "get-broadcasts".into(),
            network: binding.network.clone(),
            source: binding.source_id.clone(),
            channel: channel.into(),
            time,
            row,
        })
        .expect("owned cursor serializes"),
    )
}
fn decode_cursor(binding: &Binding, channel: &str, raw: &str) -> DatabaseResult<(i64, i64)> {
    let invalid = || DatabaseError::InvalidInput("invalid room cursor".into());
    if raw.len() > 1024
        || raw.is_empty()
        || raw
            .bytes()
            .any(|b| !b.is_ascii_hexdigit() || b.is_ascii_uppercase())
    {
        return Err(invalid());
    }
    let cursor: Cursor =
        serde_json::from_slice(&hex::decode(raw).map_err(|_| invalid())?).map_err(|_| invalid())?;
    if cursor.version != 1
        || cursor.route != "get-broadcasts"
        || cursor.network != binding.network
        || cursor.source != binding.source_id
        || cursor.channel != channel
        || cursor.time < 0
        || cursor.row <= 0
    {
        return Err(invalid());
    }
    Ok((cursor.time, cursor.row))
}
impl PostgresDbManager {
    pub(crate) async fn validate_shared_binding(&self) -> DatabaseResult<()> {
        if let Some(binding) = &self.shared {
            binding
                .begin(self)
                .await?
                .commit()
                .await
                .map_err(query_error)?;
        }
        Ok(())
    }
    pub(crate) async fn read_rooms(&self, query: RoomQuery) -> DatabaseResult<BroadcastsResponse> {
        let binding = self.shared.as_ref().ok_or(DatabaseError::NotFound)?;
        let channel = query.channel.unwrap_or_default().trim().to_lowercase();
        if channel.chars().count() > 36
            || channel.contains('\0')
            || (query.before.is_some() && query.cursor.is_some())
        {
            return Err(DatabaseError::InvalidInput("invalid room query".into()));
        }
        let cursor = query
            .cursor
            .as_deref()
            .map(|raw| decode_cursor(binding, &channel, raw))
            .transpose()?;
        let mut tx = binding.begin(self).await?;
        let limit = query.limit.unwrap_or(200).clamp(1, 500) as usize;
        // Same upstream newest-first key and legacy strict timestamp boundary.
        // New clients can continue exclusive (time,id) without losing ties.
        let rows=sqlx::query("SELECT id,encode(transaction_id,'hex') AS tx,channel,sender_address,content,block_time FROM social_shared.current_broadcasts WHERE channel=$1 AND ($2::bigint IS NULL OR block_time<$2) AND ($3::bigint IS NULL OR (block_time,id)<($3,$4)) ORDER BY block_time DESC,id DESC LIMIT $5")
            .bind(&channel).bind(query.before).bind(cursor.map(|k|k.0)).bind(cursor.map(|k|k.1)).bind((limit+1) as i64).fetch_all(&mut *tx).await.map_err(query_error)?;
        let has_more = rows.len() > limit;
        let visible = &rows[..rows.len().min(limit)];
        let next_cursor = if has_more {
            visible
                .last()
                .map(|row| encode_cursor(binding, &channel, row.get("block_time"), row.get("id")))
        } else {
            None
        };
        let messages = visible
            .iter()
            .map(|row| BroadcastMessage {
                tx_id: row.get("tx"),
                channel: row.get("channel"),
                sender_address: row.get("sender_address"),
                content: row.get("content"),
                block_time: row.get("block_time"),
            })
            .collect();
        tx.commit().await.map_err(query_error)?;
        Ok(BroadcastsResponse {
            messages,
            has_more,
            next_cursor,
        })
    }
    pub(crate) async fn read_room_channels(
        &self,
        query: ChannelQuery,
    ) -> DatabaseResult<BroadcastChannelsResponse> {
        let binding = self.shared.as_ref().ok_or(DatabaseError::NotFound)?;
        let after = if let Some(raw) = query.cursor {
            if raw.len() > 1024 {
                return Err(DatabaseError::InvalidInput("invalid channel cursor".into()));
            }
            let c: Cursor = hex::decode(raw)
                .ok()
                .and_then(|v| serde_json::from_slice(&v).ok())
                .ok_or_else(|| DatabaseError::InvalidInput("invalid channel cursor".into()))?;
            if c.route != "broadcast-channels"
                || c.version != 1
                || c.source != binding.source_id
                || c.network != binding.network
                || c.channel.is_empty()
                || c.channel.chars().count() > 36
                || c.channel.contains('\0')
                || c.time != 0
                || c.row != 0
            {
                return Err(DatabaseError::InvalidInput("invalid channel cursor".into()));
            }
            Some(c.channel)
        } else {
            None
        };
        let mut tx = binding.begin(self).await?;
        let limit = query.limit.unwrap_or(200).clamp(1, 500) as usize;
        // Observed-current discovery, not an operator retention/push allowlist.
        let mut channels:Vec<String>=sqlx::query_scalar("SELECT DISTINCT channel FROM social_shared.current_broadcasts WHERE ($1::text IS NULL OR channel>$1) ORDER BY channel LIMIT $2").bind(after).bind((limit+1) as i64).fetch_all(&mut *tx).await.map_err(query_error)?;
        let has_more = channels.len() > limit;
        channels.truncate(limit);
        let next_cursor = if has_more {
            channels.last().map(|channel| {
                hex::encode(
                    serde_json::to_vec(&Cursor {
                        version: 1,
                        route: "broadcast-channels".into(),
                        network: binding.network.clone(),
                        source: binding.source_id.clone(),
                        channel: channel.clone(),
                        time: 0,
                        row: 0,
                    })
                    .expect("owned channel cursor"),
                )
            })
        } else {
            None
        };
        tx.commit().await.map_err(query_error)?;
        Ok(BroadcastChannelsResponse {
            channels,
            has_more,
            next_cursor,
        })
    }
}
fn api_error(error: DatabaseError) -> (StatusCode, Json<ApiError>) {
    let (status, code, message) = match error {
        DatabaseError::NotFound => (StatusCode::NOT_FOUND, "NOT_FOUND", "Room API unavailable"),
        DatabaseError::InvalidInput(_) => (
            StatusCode::BAD_REQUEST,
            "INVALID_QUERY",
            "Invalid room query",
        ),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            "UNAVAILABLE",
            "Room index unavailable",
        ),
    };
    (
        status,
        Json(ApiError {
            error: message.into(),
            code: code.into(),
        }),
    )
}
pub(crate) async fn broadcasts(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<RoomQuery>,
) -> Result<Json<BroadcastsResponse>, (StatusCode, Json<ApiError>)> {
    check_rate_limit(&state, addr).await?;
    state
        .db
        .get_room_broadcasts(query)
        .await
        .map(Json)
        .map_err(api_error)
}
pub(crate) async fn channels(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ChannelQuery>,
) -> Result<Json<BroadcastChannelsResponse>, (StatusCode, Json<ApiError>)> {
    check_rate_limit(&state, addr).await?;
    state
        .db
        .get_room_channels(query)
        .await
        .map(Json)
        .map_err(api_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pure_room_cursor_binds_route_channel_source_network_and_positive_key() {
        let binding = Binding {
            network: "testnet-10".into(),
            source_id: "a179f285-19a7-4646-9eef-cf1bf2c20a56".into(),
            statement_timeout_ms: 10000,
        };
        let cursor = encode_cursor(&binding, "en", 1234, 7);
        assert_eq!(decode_cursor(&binding, "en", &cursor).unwrap(), (1234, 7));
        assert!(decode_cursor(&binding, "jp", &cursor).is_err());
        for changed in [
            Binding {
                network: "mainnet".into(),
                ..binding.clone()
            },
            Binding {
                source_id: "other".into(),
                ..binding.clone()
            },
        ] {
            assert!(decode_cursor(&changed, "en", &cursor).is_err());
        }
        for bad in [
            "".to_string(),
            cursor.to_uppercase(),
            "00".into(),
            "x".repeat(1025),
            encode_cursor(&binding, "en", -1, 7),
            encode_cursor(&binding, "en", 1, 0),
        ] {
            assert!(decode_cursor(&binding, "en", &bad).is_err());
        }
        let mut value: serde_json::Value =
            serde_json::from_slice(&hex::decode(cursor).unwrap()).unwrap();
        value["route"] = "other".into();
        assert!(
            decode_cursor(
                &binding,
                "en",
                &hex::encode(serde_json::to_vec(&value).unwrap())
            )
            .is_err()
        );
    }
}
