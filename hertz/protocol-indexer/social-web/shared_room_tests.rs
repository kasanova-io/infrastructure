//! Actual existing Axum router + native view SQL in an owning disposable DB.
//! Room rows below are isolated materialization fixtures, not chain/SDK proof.
use crate::{
    config::ServerConfig,
    database_postgres_impl::PostgresDbManager,
    shared_rooms::{Binding, RoomQuery},
    web_server::WebServer,
};
use axum::{
    body::{Body, to_bytes},
    extract::ConnectInfo,
    http::{Request, StatusCode},
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{collections::HashSet, sync::Arc};
use tower::ServiceExt;
const SOURCE: &str = "a179f285-19a7-4646-9eef-cf1bf2c20a56";
async fn setup() -> PostgresDbManager {
    let dsn = std::env::var("SOCIAL_SHARED_WEB_TEST_DATABASE_URL")
        .expect("explicit isolated shared-web DSN");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&dsn)
        .await
        .unwrap();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        name.starts_with("protocol_test_social_web"),
        "only owning disposable DB"
    );
    sqlx::raw_sql("DROP SCHEMA IF EXISTS social_shared CASCADE; DROP SCHEMA IF EXISTS protocol_indexer CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; CREATE SCHEMA social_shared; CREATE SCHEMA protocol_indexer;
        CREATE TABLE public.k_vars(key text PRIMARY KEY,value text); INSERT INTO public.k_vars VALUES('network','testnet-10');
        CREATE TABLE protocol_indexer.config(singleton boolean PRIMARY KEY DEFAULT true,network text,journal_id uuid);
        CREATE TABLE social_shared.config(singleton boolean PRIMARY KEY DEFAULT true,network text,source_id text);
        CREATE TABLE protocol_indexer.canonical(transaction_id bytea PRIMARY KEY,revision bigint,accepting_block bytea);
        CREATE TABLE protocol_indexer.pending(transaction_id bytea PRIMARY KEY,acceptance_changed boolean,accepting_block bytea);
        CREATE TABLE social_shared.actions(transaction_id bytea PRIMARY KEY);
        CREATE TABLE social_shared.decoder_epochs(kind text PRIMARY KEY,since_ms bigint DEFAULT 0)").execute(&pool).await.unwrap();
    sqlx::raw_sql(include_str!("shared_room_schema.sql"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO protocol_indexer.config(network,journal_id) VALUES('testnet-10',$1::uuid)",
    )
    .bind(SOURCE)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO social_shared.config(network,source_id) VALUES('testnet-10',$1)")
        .bind(SOURCE)
        .execute(&pool)
        .await
        .unwrap();
    PostgresDbManager {
        pool,
        shared: Some(Binding {
            network: "testnet-10".into(),
            source_id: SOURCE.into(),
            statement_timeout_ms: 10000,
        }),
    }
}
async fn rows(pool: &PgPool, n: i32, time: i64, channel: &str, offset: i32) {
    sqlx::query("WITH source AS (SELECT decode(lpad(to_hex(g+$4),64,'0'),'hex') AS tx FROM generate_series(1,$1) g), actions AS (INSERT INTO social_shared.actions SELECT tx FROM source RETURNING transaction_id), canonical AS (INSERT INTO protocol_indexer.canonical SELECT tx,1,decode(repeat('ab',32),'hex') FROM source), native AS (INSERT INTO public.kachat_broadcasts(transaction_id,block_time,channel,sender_address,content) SELECT tx,$2,$3,'kaspatest:isolated-fixture','verbatim:内容' FROM source) INSERT INTO social_shared.room_facts SELECT transaction_id,1,'kaspatest:isolated-fixture' FROM actions")
        .bind(n).bind(time).bind(channel).bind(offset).execute(pool).await.unwrap();
}
async fn router(db: PostgresDbManager) -> axum::Router {
    WebServer::new(
        Arc::new(db),
        ServerConfig {
            bind_address: "127.0.0.1:0".into(),
            request_timeout: 10,
            rate_limit: 10000,
        },
    )
    .await
    .create_router()
}
async fn request(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    req.extensions_mut().insert(ConnectInfo(
        "127.0.0.1:12345".parse::<std::net::SocketAddr>().unwrap(),
    ));
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, value)
}
#[tokio::test]
async fn postgres_shared_room_router_preserves_legacy_shape_defaults_and_paginates_ties() {
    let db = setup().await;
    db.validate_shared_binding().await.unwrap();
    rows(&db.pool, 523, 2000, "en", 0).await;
    rows(&db.pool, 3, 1000, "en", 523).await;
    rows(&db.pool, 2, 2000, "jp", 526).await;
    let app = router(db).await;
    let (status, first) = request(&app, "/get-broadcasts?channel=%20EN%20").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["messages"].as_array().unwrap().len(), 200);
    assert_eq!(first["hasMore"], true);
    assert_eq!(first["messages"][0]["content"], "verbatim:内容");
    assert!(first["messages"][0]["txId"].as_str().is_some());
    assert!(first["messages"][0]["senderAddress"].as_str().is_some());
    assert_eq!(first["messages"][0]["blockTime"], 2000);
    let mut cursor = None;
    let mut seen = HashSet::new();
    loop {
        let uri = match &cursor {
            None => "/get-broadcasts?channel=en&limit=100".into(),
            Some(c) => format!("/get-broadcasts?channel=en&limit=100&cursor={c}"),
        };
        let (status, page) = request(&app, &uri).await;
        assert_eq!(status, StatusCode::OK);
        for row in page["messages"].as_array().unwrap() {
            assert!(seen.insert(row["txId"].as_str().unwrap().to_owned()));
        }
        if page["hasMore"] == false {
            assert!(page.get("nextCursor").is_none());
            break;
        }
        cursor = Some(page["nextCursor"].as_str().unwrap().to_owned());
        assert!(seen.len() <= 526);
    }
    assert_eq!(seen.len(), 526);
    let (_, old) = request(&app, "/get-broadcasts?channel=en&before=2000").await;
    assert_eq!(old["messages"].as_array().unwrap().len(), 3);
    for path in ["/get-broadcasts", "/get-broadcasts?channel=unknown"] {
        let (status, p) = request(&app, path).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(p["messages"], serde_json::json!([]));
        assert_eq!(p["hasMore"], false);
    }
    let (_, cap) = request(&app, "/get-broadcasts?channel=en&limit=999").await;
    assert_eq!(cap["messages"].as_array().unwrap().len(), 500);
    let (_, min) = request(&app, "/get-broadcasts?channel=en&limit=0").await;
    assert_eq!(min["messages"].as_array().unwrap().len(), 1);
    let c = first["nextCursor"].as_str().unwrap();
    for uri in [
        format!("/get-broadcasts?channel=jp&cursor={c}"),
        format!("/get-broadcasts?channel=en&before=1&cursor={c}"),
        "/get-broadcasts?channel=en&cursor=00".into(),
    ] {
        assert_eq!(request(&app, &uri).await.0, StatusCode::BAD_REQUEST);
    }
}
#[tokio::test]
async fn postgres_shared_rooms_hide_revoked_pending_and_stale_native_rows_in_all_reads() {
    let db = setup().await;
    rows(&db.pool, 4, 2000, "en", 0).await;
    sqlx::query("UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=decode(lpad('1',64,'0'),'hex')").execute(&db.pool).await.unwrap();
    sqlx::query("UPDATE protocol_indexer.canonical SET revision=2 WHERE transaction_id=decode(lpad('2',64,'0'),'hex')").execute(&db.pool).await.unwrap();
    sqlx::query(
        "INSERT INTO protocol_indexer.pending VALUES(decode(lpad('3',64,'0'),'hex'),true,NULL)",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let pool = db.pool.clone();
    let app = router(db).await;
    let (status, page) = request(&app, "/get-broadcasts?channel=en").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["messages"].as_array().unwrap().len(), 1);
    assert!(page["messages"][0]["txId"].as_str().unwrap().ends_with('4'));
    sqlx::query("UPDATE protocol_indexer.canonical SET accepting_block=NULL")
        .execute(&pool)
        .await
        .unwrap();
    let (status, list) = request(&app, "/broadcast-channels").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["channels"], serde_json::json!([]));
    let (_, page) = request(&app, "/get-broadcasts?channel=en").await;
    assert_eq!(page["messages"], serde_json::json!([]));
}
#[tokio::test]
async fn postgres_shared_room_binding_refuses_foreign_missing_and_partial_projection_without_leaks()
{
    let mut db = setup().await;
    rows(&db.pool, 1, 2000, "en", 0).await;
    db.shared.as_mut().unwrap().source_id = "another-journal".into();
    assert!(db.validate_shared_binding().await.is_err());
    db.shared.as_mut().unwrap().source_id = SOURCE.into();
    db.shared.as_mut().unwrap().network = "mainnet".into();
    assert!(db.validate_shared_binding().await.is_err());
    db.shared.as_mut().unwrap().network = "testnet-10".into();
    let pool = db.pool.clone();
    let app = router(db).await;
    sqlx::query("UPDATE social_shared.config SET source_id='foreign'")
        .execute(&pool)
        .await
        .unwrap();
    for uri in [
        "/get-broadcasts?channel=en",
        "/get-broadcasts",
        "/broadcast-channels",
    ] {
        let (status, body) = request(&app, uri).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "UNAVAILABLE");
        assert!(!body.to_string().contains("protocol_indexer"));
    }
    sqlx::query("UPDATE social_shared.config SET source_id=$1")
        .bind(SOURCE)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP VIEW social_shared.current_broadcasts")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        request(&app, "/get-broadcasts").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}
#[tokio::test]
async fn postgres_shared_rooms_channel_discovery_is_observed_bounded_and_cursor_separated() {
    let db = setup().await;
    for (offset, channel) in [(0, "en"), (1, "jp"), (2, "trading")] {
        rows(&db.pool, 1, 2000, channel, offset).await;
    }
    let app = router(db).await;
    let (status, first) = request(&app, "/broadcast-channels?limit=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["channels"], serde_json::json!(["en", "jp"]));
    assert_eq!(first["hasMore"], true);
    let cursor = first["nextCursor"].as_str().unwrap();
    let (status, next) = request(
        &app,
        &format!("/broadcast-channels?limit=2&cursor={cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(next["channels"], serde_json::json!(["trading"]));
    assert_eq!(next["hasMore"], false);
    assert_eq!(
        request(&app, &format!("/get-broadcasts?channel=en&cursor={cursor}"))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, "/broadcast-channels?cursor=00").await.0,
        StatusCode::BAD_REQUEST
    );
}
#[tokio::test]
async fn postgres_shared_room_snapshot_is_per_request_and_legacy_has_no_room_routes() {
    let mut db = setup().await;
    rows(&db.pool, 1, 2000, "en", 0).await;
    let binding = db.shared.clone().unwrap();
    let mut tx = binding.begin(&db).await.unwrap();
    // A real second connection changes ownership between guard and read.
    sqlx::query("UPDATE social_shared.config SET source_id='new-owner'")
        .execute(&db.pool)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM social_shared.current_broadcasts")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(n, 1);
    tx.commit().await.unwrap();
    assert!(db.read_rooms(RoomQuery::default()).await.is_err());
    db.shared = None;
    let app = router(db).await;
    for uri in [
        "/get-broadcasts",
        "/broadcast-channels",
        "/get-broadcasts?limit=not-a-number",
    ] {
        assert_eq!(request(&app, uri).await.0, StatusCode::NOT_FOUND);
    }
}
#[test]
fn pure_shared_cli_requires_both_binding_fields() {
    use clap::Parser;
    let args = [
        "K-webserver",
        "--db-host",
        "unused",
        "--db-name",
        "unused",
        "--db-user",
        "unused",
        "--db-password",
        "unused",
    ];
    assert!(crate::Args::try_parse_from(args).is_ok());
    for extra in [
        vec!["--shared-source-id", SOURCE],
        vec!["--shared-network", "testnet-10"],
    ] {
        assert!(crate::Args::try_parse_from(args.iter().copied().chain(extra)).is_err());
    }
    assert!(
        crate::Args::try_parse_from(args.iter().copied().chain([
            "--shared-source-id",
            SOURCE,
            "--shared-network",
            "testnet-10"
        ]))
        .is_ok()
    );
}

#[tokio::test]
async fn postgres_shared_room_query_budget_cancels_sql_and_does_not_leak_to_pool() {
    let mut db = setup().await;
    let before: String = sqlx::query_scalar("SELECT current_setting('statement_timeout')")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    db.shared.as_mut().unwrap().statement_timeout_ms = 200;
    let binding = db.shared.clone().unwrap();
    let mut tx = binding.begin(&db).await.unwrap();
    let error = sqlx::query("SELECT pg_sleep(5)")
        .execute(&mut *tx)
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("57014")
    );
    tx.rollback().await.unwrap();
    let after: String = sqlx::query_scalar("SELECT current_setting('statement_timeout')")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(before, after);
    db.shared.as_mut().unwrap().statement_timeout_ms = 10000;
    db.read_rooms(RoomQuery::default()).await.unwrap();
}

#[tokio::test]
async fn postgres_shared_room_full_network_binding_supports_mainnet_and_rejects_old_cursor() {
    let mut db = setup().await;
    rows(&db.pool, 2, 2000, "en", 0).await;
    let tn = db
        .read_rooms(RoomQuery {
            channel: Some("en".into()),
            limit: Some(1),
            ..RoomQuery::default()
        })
        .await
        .unwrap();
    sqlx::raw_sql("UPDATE public.k_vars SET value='mainnet' WHERE key='network'; UPDATE protocol_indexer.config SET network='mainnet'; UPDATE social_shared.config SET network='mainnet'; UPDATE public.kachat_broadcasts SET sender_address='kaspa:isolated-fixture'; UPDATE social_shared.room_facts SET sender_address='kaspa:isolated-fixture'").execute(&db.pool).await.unwrap();
    assert!(db.read_rooms(RoomQuery::default()).await.is_err());
    db.shared.as_mut().unwrap().network = "mainnet".into();
    db.validate_shared_binding().await.unwrap();
    assert!(
        db.read_rooms(RoomQuery {
            channel: Some("en".into()),
            cursor: tn.next_cursor,
            ..RoomQuery::default()
        })
        .await
        .is_err()
    );
    let page = db
        .read_rooms(RoomQuery {
            channel: Some("en".into()),
            ..RoomQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(page.messages.len(), 2);
    assert!(
        page.messages
            .iter()
            .all(|row| row.sender_address.starts_with("kaspa:"))
    );
}
