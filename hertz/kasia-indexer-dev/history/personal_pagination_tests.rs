use super::{contextual_messages::*, handshakes::*, payments::*, self_stash::*};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use indexer_db::{
    AddressPayload,
    messages::{contextual_message::*, handshake::*, payment::*, self_stash::*},
    processing::tx_id_to_acceptance::TxIDToAcceptancePartition,
};
use serde_json::Value;

const ROUTES: [&str; 6] = [
    "handshake-sender",
    "handshake-receiver",
    "contextual-sender",
    "payment-sender",
    "payment-receiver",
    "stash-owner",
];

struct Fixture {
    _temp: tempfile::TempDir,
    handshakes: HandshakeApi,
    contextual: ContextualMessageApi,
    payments: PaymentApi,
    stash: SelfStashApi,
    address: String,
}

impl Fixture {
    fn new() -> anyhow::Result<Self> {
        let temp = tempfile::tempdir()?;
        let db = fjall::Config::new(temp.path().join("db")).open_transactional()?;
        let hs = HandshakeBySenderPartition::new(&db)?;
        let hr = HandshakeByReceiverPartition::new(&db)?;
        let hp = TxIdToHandshakePartition::new(&db)?;
        let cm = ContextualMessageBySenderPartition::new(&db)?;
        let cp = TxIdToContextualMessagePartition::new(&db)?;
        let ps = PaymentBySenderPartition::new(&db)?;
        let pr = PaymentByReceiverPartition::new(&db)?;
        let pp = TxIdToPaymentPartition::new(&db)?;
        let ss = SelfStashByOwnerPartition::new(&db)?;
        let sp = TxIdToSelfStashPartition::new(&db)?;
        let accepted = TxIDToAcceptancePartition::new(&db)?;
        let rpc = kaspa_addresses::Address::new(
            kaspa_addresses::Prefix::Mainnet,
            kaspa_addresses::Version::PubKey,
            &[8; 32],
        );
        let owner = AddressPayload::try_from(&rpc)?;
        let mut wtx = db.write_tx()?;
        // 61 distinct transactions at one timestamp exceed the server's 50-row cap.
        // Later block sightings must not reappear on later pages or timestamp resumes.
        for (id, time, hash) in (1u8..=61).map(|id| (id, 100u64, id)).chain([
            (1, 100, 200),
            (2, 101, 201),
            (62, 102, 202),
        ]) {
            let tx_id = [id; 32];
            hs.insert_wtx(
                &mut wtx,
                &HandshakeKeyBySender {
                    sender: owner,
                    receiver: owner,
                    block_time: time.into(),
                    block_hash: [hash; 32],
                    version: [0, 2][id as usize % 2],
                    tx_id,
                },
            );
            hr.insert_wtx(
                &mut wtx,
                &HandshakeKeyByReceiver {
                    receiver: owner,
                    block_time: time.into(),
                    block_hash: [hash; 32],
                    version: [0, 2][id as usize % 2],
                    tx_id,
                },
                Some(owner),
            )?;
            hp.insert_wtx(&mut wtx, &tx_id, &[id, 0xff, b':']);
            cm.insert(
                &mut wtx,
                &ContextualMessageBySenderKey {
                    sender: owner,
                    receiver: owner,
                    alias: [9; 16],
                    block_time: time.into(),
                    block_hash: [hash; 32],
                    version: 1,
                    tx_id,
                },
            );
            cp.insert_wtx(&mut wtx, &tx_id, &[id, 0xff, b':']);
            ps.insert_wtx(
                &mut wtx,
                &PaymentKeyBySender {
                    sender: owner,
                    receiver: owner,
                    block_time: time.into(),
                    block_hash: [hash; 32],
                    version: 0,
                    tx_id,
                },
            );
            pr.insert_wtx(
                &mut wtx,
                &PaymentKeyByReceiver {
                    receiver: owner,
                    block_time: time.into(),
                    block_hash: [hash; 32],
                    version: 0,
                    tx_id,
                },
                Some(owner),
            )?;
            pp.insert_wtx(&mut wtx, &tx_id, 1000 + id as u64, &[id, 0xff, b':'])?;
            ss.insert_wtx(
                &mut wtx,
                &SelfStashKeyByOwner {
                    owner,
                    scope: [9u8; 255].as_slice().into(),
                    block_time: time.into(),
                    block_hash: [hash; 32],
                    version: 1,
                    tx_id,
                },
            );
            sp.insert_wtx(&mut wtx, &tx_id, &[id, 0xff, b':']);
        }
        // Missing stash payloads must not shorten pages and hide following valid records.
        ss.insert_wtx(
            &mut wtx,
            &SelfStashKeyByOwner {
                owner,
                scope: [9u8; 255].as_slice().into(),
                block_time: 99.into(),
                block_hash: [0; 32],
                version: 1,
                tx_id: [0; 32],
            },
        );
        wtx.commit()?
            .map_err(|_| anyhow::anyhow!("fixture conflict"))?;
        let config = serde_json::from_value(serde_json::json!({
            "kasia_indexer_db_root": temp.path().join("context"),
            "kaspa_node_wborsh_url": "ws://127.0.0.1:1"
        }))?;
        let context = crate::context::get_indexer_context(&config)?;
        let metrics = std::sync::Arc::new(indexer_actors::metrics::IndexerMetrics::new());
        Ok(Self {
            handshakes: HandshakeApi::new(
                db.clone(),
                hs,
                hr,
                accepted.clone(),
                hp,
                metrics.clone(),
                context.clone(),
            ),
            contextual: ContextualMessageApi::new(
                db.clone(),
                cm,
                accepted.clone(),
                cp,
                metrics.clone(),
                context.clone(),
            ),
            payments: PaymentApi::new(
                db.clone(),
                ps,
                pr,
                pp,
                accepted.clone(),
                metrics.clone(),
                context.clone(),
            ),
            stash: SelfStashApi::new(db, ss, accepted, sp, metrics, context),
            _temp: temp,
            address: rpc.to_string(),
        })
    }

    async fn read(
        &self,
        route: &str,
        limit: usize,
        cursor: Option<String>,
        block_time: Option<u64>,
    ) -> Response {
        self.query(route, limit, cursor, block_time, self.address.clone(), 9)
            .await
    }

    async fn query(
        &self,
        route: &str,
        limit: usize,
        cursor: Option<String>,
        block_time: Option<u64>,
        address: String,
        scope_byte: u8,
    ) -> Response {
        let limit = Some(limit);
        match route {
            "handshake-sender" => get_handshakes_by_sender(
                State(self.handshakes.clone()),
                Query(HandshakePaginationParams {
                    limit,
                    cursor,
                    block_time,
                    address,
                }),
            )
            .await
            .into_response(),
            "handshake-receiver" => get_handshakes_by_receiver(
                State(self.handshakes.clone()),
                Query(HandshakePaginationParams {
                    limit,
                    cursor,
                    block_time,
                    address,
                }),
            )
            .await
            .into_response(),
            "contextual-sender" => get_contextual_messages_by_sender(
                State(self.contextual.clone()),
                Query(ContextualMessagePaginationParams {
                    limit,
                    cursor,
                    block_time,
                    address,
                    alias: faster_hex::hex_string(&[scope_byte; 16]),
                }),
            )
            .await
            .into_response(),
            "payment-sender" => get_payments_by_sender(
                State(self.payments.clone()),
                Query(PaymentPaginationParams {
                    limit,
                    cursor,
                    block_time,
                    address,
                }),
            )
            .await
            .into_response(),
            "payment-receiver" => get_payments_by_receiver(
                State(self.payments.clone()),
                Query(PaymentPaginationParams {
                    limit,
                    cursor,
                    block_time,
                    address,
                }),
            )
            .await
            .into_response(),
            "stash-owner" => get_self_stash_by_owner(
                State(self.stash.clone()),
                Query(SelfStashPaginationParams {
                    limit,
                    cursor,
                    block_time,
                    owner: address,
                    scope: faster_hex::hex_string(&[scope_byte; 255]),
                }),
            )
            .await
            .into_response(),
            _ => unreachable!(),
        }
    }
}

async fn rows(response: Response) -> anyhow::Result<Vec<Value>> {
    assert_eq!(response.status(), StatusCode::OK);
    Ok(serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 1024 * 1024).await?,
    )?)
}

#[tokio::test]
async fn all_personal_routes_page_through_timestamp_ties_without_repeated_transactions()
-> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    for route in ROUTES {
        for limit in [1, 7, 50, 500] {
            let mut cursor = None;
            let mut actual = Vec::new();
            for _ in 0..64 {
                let page = rows(fixture.read(route, limit, cursor.clone(), None).await).await?;
                assert!(page.len() <= limit.min(50));
                if page.is_empty() {
                    break;
                }
                for row in &page {
                    actual.push(row["tx_id"].as_str().unwrap().to_owned());
                    let id = u8::from_str_radix(&row["tx_id"].as_str().unwrap()[..2], 16)?;
                    let field = if route.starts_with("payment") {
                        "message"
                    } else if route == "stash-owner" {
                        "stashed_data"
                    } else {
                        "message_payload"
                    };
                    assert_eq!(row[field], faster_hex::hex_string(&[id, 0xff, b':']));
                    if route.starts_with("payment") {
                        assert_eq!(row["amount"], 1000 + id as u64);
                    }
                }
                let next = page.last().unwrap()["cursor"].as_str().unwrap().to_owned();
                assert_ne!(cursor.as_ref(), Some(&next), "{route}");
                cursor = Some(next);
            }
            let expected: Vec<_> = (1u8..=62)
                .map(|id| faster_hex::hex_string(&[id; 32]))
                .collect();
            assert_eq!(actual, expected, "route={route}, limit={limit}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn timestamp_resume_excludes_later_sightings_and_zero_limit_is_empty() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    for route in ROUTES {
        assert!(
            rows(fixture.read(route, 0, None, None).await)
                .await?
                .is_empty()
        );
        let page = rows(fixture.read(route, 50, None, Some(101)).await).await?;
        assert_eq!(page.len(), 1, "{route}");
        assert_eq!(page[0]["tx_id"], "3e".repeat(32));
        assert!(
            rows(fixture.read(route, 50, None, Some(103)).await)
                .await?
                .is_empty()
        );
    }
    Ok(())
}

#[tokio::test]
async fn cursors_are_validated_before_reading_and_bound_to_route_address_and_scope()
-> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let other = kaspa_addresses::Address::new(
        kaspa_addresses::Prefix::Mainnet,
        kaspa_addresses::Version::PubKey,
        &[7; 32],
    )
    .to_string();
    for route in ROUTES {
        let page = rows(fixture.read(route, 1, None, None).await).await?;
        let cursor = page[0]["cursor"].as_str().unwrap();
        for bad in [
            String::new(),
            "junk".into(),
            format!("{cursor}00"),
            cursor.replacen(":1:", ":2:", 1),
            format!("{}z", &cursor[..cursor.len() - 1]),
        ] {
            assert_eq!(
                fixture.read(route, 50, Some(bad), None).await.status(),
                StatusCode::BAD_REQUEST,
                "{route}"
            );
        }
        assert_eq!(
            fixture
                .read(route, 50, Some(cursor.into()), Some(100))
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            fixture
                .query(route, 50, Some(cursor.into()), None, other.clone(), 9)
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
        if route == "contextual-sender" || route == "stash-owner" {
            assert_eq!(
                fixture
                    .query(
                        route,
                        50,
                        Some(cursor.into()),
                        None,
                        fixture.address.clone(),
                        8
                    )
                    .await
                    .status(),
                StatusCode::BAD_REQUEST
            );
        }
        for other_route in ROUTES.into_iter().filter(|other| *other != route) {
            assert_eq!(
                fixture
                    .read(other_route, 50, Some(cursor.into()), None)
                    .await
                    .status(),
                StatusCode::BAD_REQUEST
            );
        }
    }
    Ok(())
}
