//! Native inbox GET from KaChat e2f462171bee80ff26c6fe14ddafe06614f05efa,
//! contextual_messages.rs / contextual_message.rs (ISC, LICENSE.KaChat-Indexer).
//! Reuses existing Kasanova cursor/header contract. Tags are discovery hints;
//! they do not prove recipient ownership or authorize payments/notifications.
use super::super::pagination::{decode_cursor, encode_cursor, paginated_response};
use super::*;
use axum::response::Response;
use indexer_db::{
    IntoBytes,
    messages::contextual_message::{
        ContextualMessageByInboxKey, ContextualMessageByInboxPartition, INBOX_TAG_LEN,
    },
};

#[derive(Debug, Deserialize)]
pub(super) struct InboxPaginationParams {
    pub tag: String,
    pub limit: Option<usize>,
    pub block_time: Option<u64>,
    pub cursor: Option<String>,
}

pub(super) async fn get_contextual_messages_by_inbox(
    State(state): State<ContextualMessageApi>,
    Query(params): Query<InboxPaginationParams>,
) -> Response {
    let limit = params.limit.unwrap_or(100).min(500);
    let mut tag = [0; INBOX_TAG_LEN];
    if params.tag.len() != INBOX_TAG_LEN * 2
        || params.tag.bytes().any(|b| b.is_ascii_uppercase())
        || faster_hex::hex_decode(params.tag.as_bytes(), &mut tag).is_err()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "tag must be 32 lowercase hex characters".into(),
            }),
        )
            .into_response();
    }
    let cursor = match decode_cursor::<ContextualMessageByInboxKey>(
        params.cursor.as_deref(),
        params.block_time,
        "contextual-inbox",
        |key| key.inbox_tag == tag,
    ) {
        Ok(key) => key,
        Err(error) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error })).into_response();
        }
    };
    let from_time = params.block_time.unwrap_or(0);
    let metrics = state.metrics.clone();
    let started = std::time::Instant::now();
    let result = spawn_blocking(move || -> anyhow::Result<Vec<ContextualMessageResponse>> {
        let partition = ContextualMessageByInboxPartition::new(&state.tx_keyspace)?;
        let rtx = state.tx_keyspace.read_tx();
        let mut seen = std::collections::HashSet::with_capacity(limit);
        partition
            .iter_by_tag_from_block_time(&rtx, &tag, 0)
            .process_results(|iter| {
                iter.filter(|(key, _)| {
                    // Retain existing native pagination dedup before the exclusive
                    // cursor, so later block sightings cannot reappear next page.
                    seen.insert(key.tx_id)
                        && key.block_time.get() >= from_time
                        && cursor
                            .as_ref()
                            .is_none_or(|cursor| key.as_bytes() > cursor.as_bytes())
                })
                .take(if limit == 0 {
                    0
                } else {
                    limit.saturating_add(1)
                })
                .map(|(key, sender)| {
                    let sender = match to_rpc_address(&sender, state.context.network_type)? {
                        Some(value) => value.to_string(),
                        None => String::new(),
                    };
                    let acceptance = state
                        .tx_id_to_acceptance_partition
                        .acceptance_by_tx_id_rtx(&rtx, &key.tx_id)?;
                    let (accepting_block, accepting_daa_score) = acceptance
                        .map(|v| {
                            (
                                Some(faster_hex::hex_string(&v.header.accepting_block_hash)),
                                Some(v.header.accepting_daa.into()),
                            )
                        })
                        .unwrap_or((None, None));
                    let payload = state
                        .tx_id_to_contextual_message_partition
                        .get_rtx(&rtx, &key.tx_id)?
                        .ok_or_else(|| anyhow::anyhow!("Missing contextual message payload"))?;
                    Ok(ContextualMessageResponse {
                        cursor: encode_cursor("contextual-inbox", key.as_bytes()),
                        tx_id: faster_hex::hex_string(&key.tx_id),
                        sender,
                        alias: String::new(),
                        block_time: key.block_time.into(),
                        accepting_block,
                        accepting_daa_score,
                        message_payload: faster_hex::hex_string(payload.as_ref()),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()
            })
            .flatten()
    })
    .await;
    metrics.increment_db_read_ops_total(1);
    metrics.increment_db_read_time_ms_total(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
    if result.as_ref().is_err() || matches!(&result, Ok(Err(_))) {
        metrics.increment_db_errors_total();
    }
    match result {
        Ok(Ok(rows)) => paginated_response(rows, limit, |row| row.cursor.as_str()),
        Ok(Err(error)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: error.to_string(),
            }),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: error.to_string(),
            }),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer_db::{
        AddressPayload,
        messages::contextual_message::{
            ContextualMessageBySenderPartition, TxIdToContextualMessagePartition,
        },
        processing::tx_id_to_acceptance::TxIDToAcceptancePartition,
    };
    struct Fixture {
        _temp: tempfile::TempDir,
        db: fjall::TxKeyspace,
        api: ContextualMessageApi,
    }
    impl Fixture {
        fn new() -> anyhow::Result<Self> {
            let temp = tempfile::tempdir()?;
            let db = fjall::Config::new(temp.path().join("db")).open_transactional()?;
            let config = serde_json::from_value(
                serde_json::json!({"kasia_indexer_db_root":temp.path().join("context"),"kaspa_node_wborsh_url":"ws://127.0.0.1:1"}),
            )?;
            let context = crate::context::get_indexer_context(&config)?;
            let api = ContextualMessageApi::new(
                db.clone(),
                ContextualMessageBySenderPartition::new(&db)?,
                TxIDToAcceptancePartition::new(&db)?,
                TxIdToContextualMessagePartition::new(&db)?,
                std::sync::Arc::new(indexer_actors::metrics::IndexerMetrics::new()),
                context,
            );
            let part = ContextualMessageByInboxPartition::new(&db)?;
            let payload = TxIdToContextualMessagePartition::new(&db)?;
            let mut tx = db.write_tx()?;
            for n in 1..=523u32 {
                let mut id = [0; 32];
                id[..4].copy_from_slice(&n.to_be_bytes());
                let key = ContextualMessageByInboxKey {
                    inbox_tag: [0xab; 16],
                    block_time: 100.into(),
                    tx_id: id,
                };
                part.insert_wtx(&mut tx, &key, Some(AddressPayload::default()))?;
                payload.insert_wtx(&mut tx, &id, &[0xff, 0, b':']);
                // Duplicate later sighting must not create another page item.
                part.insert_wtx(
                    &mut tx,
                    &ContextualMessageByInboxKey {
                        block_time: 200.into(),
                        ..key
                    },
                    Some(AddressPayload::default()),
                )?;
            }
            tx.commit()?
                .map_err(|_| anyhow::anyhow!("fixture conflict"))?;
            Ok(Self {
                _temp: temp,
                db,
                api,
            })
        }
        async fn read(
            &self,
            tag: String,
            limit: usize,
            cursor: Option<String>,
            block_time: Option<u64>,
        ) -> Response {
            get_contextual_messages_by_inbox(
                State(self.api.clone()),
                Query(InboxPaginationParams {
                    tag,
                    limit: Some(limit),
                    cursor,
                    block_time,
                }),
            )
            .await
        }
    }
    async fn body(response: Response) -> serde_json::Value {
        serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap()
    }
    #[tokio::test]
    async fn inbox_tied_timestamp_pages_preserve_all_binary_records_and_headers()
    -> anyhow::Result<()> {
        let f = Fixture::new()?;
        let tag = "ab".repeat(16);
        let first = f.read(tag.clone(), 500, None, None).await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(first.headers()["x-kasia-has-more"], "true");
        let cursor = first.headers()["x-kasia-next-cursor"].to_str()?.to_owned();
        let rows = body(first).await;
        assert_eq!(rows.as_array().unwrap().len(), 500);
        assert_eq!(rows[0]["message_payload"], "ff003a");
        assert_eq!(rows[0]["alias"], "");
        assert_eq!(rows[0]["accepting_block"], serde_json::Value::Null);
        let second = f.read(tag.clone(), 500, Some(cursor), None).await;
        assert_eq!(second.headers()["x-kasia-has-more"], "false");
        assert!(!second.headers().contains_key("x-kasia-next-cursor"));
        let last = body(second).await;
        assert_eq!(last.as_array().unwrap().len(), 23);
        let ids = rows
            .as_array()
            .unwrap()
            .iter()
            .chain(last.as_array().unwrap())
            .map(|r| r["tx_id"].as_str().unwrap())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(ids.len(), 523);
        assert_eq!(
            body(f.read(tag.clone(), 500, None, Some(100)).await)
                .await
                .as_array()
                .unwrap()
                .len(),
            500
        );
        assert_eq!(
            body(f.read(tag, 500, None, Some(101)).await)
                .await
                .as_array()
                .unwrap()
                .len(),
            0,
            "later sightings cannot resurrect a timestamp-filtered tx"
        );
        Ok(())
    }
    #[tokio::test]
    async fn inbox_cursor_validation_unknown_tag_and_missing_payload_are_explicit()
    -> anyhow::Result<()> {
        let f = Fixture::new()?;
        let tag = "ab".repeat(16);
        let first = f.read(tag.clone(), 1, None, None).await;
        let cursor = first.headers()["x-kasia-next-cursor"].to_str()?.to_owned();
        for (tag, cursor, time) in [
            ("ab".repeat(15), None, None),
            ("AB".repeat(16), None, None),
            ("zz".repeat(16), None, None),
            ("ac".repeat(16), Some(cursor.clone()), None),
            (tag.clone(), Some(cursor.clone()), Some(0)),
            (
                tag.clone(),
                Some(cursor.replace("contextual-inbox", "contextual-sender")),
                None,
            ),
        ] {
            assert_eq!(
                f.read(tag, 1, cursor, time).await.status(),
                StatusCode::BAD_REQUEST
            );
        }
        let empty = f.read("ac".repeat(16), 1, None, None).await;
        assert_eq!(empty.status(), StatusCode::OK);
        assert_eq!(body(empty).await, serde_json::json!([]));
        let part = f.db.open_partition(
            "tx-id-to-contextual-message",
            fjall::PartitionCreateOptions::default(),
        )?;
        let mut tx = f.db.write_tx()?;
        let mut id = [0; 32];
        id[..4].copy_from_slice(&1u32.to_be_bytes());
        tx.remove(&part, id);
        tx.commit()?
            .map_err(|_| anyhow::anyhow!("fixture conflict"))?;
        assert_eq!(
            f.read(tag, 1, None, None).await.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        Ok(())
    }
}
