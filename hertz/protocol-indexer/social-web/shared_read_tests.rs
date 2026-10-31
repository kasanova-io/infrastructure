//! Existing native DDL + actual database methods in an owning disposable DB.
//! Rows are explicit materialization fixtures, never chain/signature evidence.
use crate::{
    api_handlers::ApiHandlers,
    database_postgres_impl::PostgresDbManager,
    database_trait::{DatabaseInterface, QueryOptions},
    shared_rooms::Binding,
};
use serde_json::json;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::sync::Arc;
const SOURCE: &str = "a179f285-19a7-4646-9eef-cf1bf2c20a56";
fn id(n: u8) -> Vec<u8> {
    vec![n; 32]
}
fn sig(n: u8) -> Vec<u8> {
    vec![n; 64]
}
fn author() -> Vec<u8> {
    [vec![2], vec![0x11; 32]].concat()
}
fn viewer() -> Vec<u8> {
    [vec![3], vec![0x22; 32]].concat()
}
fn options(limit: u64) -> QueryOptions {
    QueryOptions {
        limit: Some(limit),
        ..Default::default()
    }
}
fn schema() -> String {
    include_str!("shared_read_schema.sql")
        .replace(
            "__AUTHORIZED_EDIT__",
            include_str!("shared_edit_authorization.sql"),
        )
        .replace(
            "__UNQUOTE_ORDER__",
            include_str!("shared_unquote_order.sql"),
        )
}
async fn setup() -> PostgresDbManager {
    let dsn =
        std::env::var("SOCIAL_SHARED_WEB_TEST_DATABASE_URL").expect("explicit disposable DSN");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .unwrap();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        name.starts_with("protocol_test_social_web"),
        "owning disposable DB only"
    );
    sqlx::raw_sql("DROP SCHEMA IF EXISTS social_shared_api CASCADE; DROP SCHEMA IF EXISTS social_shared CASCADE; DROP SCHEMA IF EXISTS protocol_indexer CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; CREATE SCHEMA protocol_indexer;
      CREATE TABLE protocol_indexer.config(singleton boolean PRIMARY KEY DEFAULT true,network text,journal_id uuid);
      CREATE TABLE protocol_indexer.canonical(transaction_id bytea PRIMARY KEY,revision bigint,source_transaction jsonb,accepting_block bytea);
      CREATE TABLE protocol_indexer.pending(transaction_id bytea PRIMARY KEY,acceptance_changed boolean,accepting_block bytea)").execute(&pool).await.unwrap();
    sqlx::raw_sql(include_str!("native_schema.sql"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("shared_social_schema.sql"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("shared_kaposts_schema.sql"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("shared_room_schema.sql"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(&schema()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO public.k_vars(key,value) VALUES('network','testnet-10')")
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
async fn action(pool: &PgPool, n: u8, table: &str, subject: String, operation: &str, time: i64) {
    sqlx::query(
        "INSERT INTO protocol_indexer.canonical VALUES($1,1,'{}',decode(repeat('ab',32),'hex'))",
    )
    .bind(id(n))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO social_shared.actions(transaction_id,revision,subject,native_table,source_payload,original_time,status,state_action,snapshot) VALUES($1,1,$2,$3,'00',$4,'verified',$5,'{\"k_mentions\":[]}'::jsonb)")
        .bind(id(n)).bind(subject).bind(table).bind(time).bind(operation).execute(pool).await.unwrap();
}
async fn post(pool: &PgPool, n: u8, time: i64) {
    action(
        pool,
        n,
        "k_contents",
        format!("k_contents:{}", hex::encode(sig(n))),
        "signed",
        time,
    )
    .await;
    sqlx::query("INSERT INTO public.k_contents(transaction_id,block_time,sender_pubkey,sender_signature,base64_encoded_message,content_type) VALUES($1,$2,$3,$4,'aGk=','post')")
        .bind(id(n)).bind(time).bind(author()).bind(sig(n)).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO social_shared.content_keys VALUES($1,$2,$3,'post',NULL,$4)")
        .bind(id(n))
        .bind(sig(n))
        .bind(author())
        .bind(time)
        .execute(pool)
        .await
        .unwrap();
}
async fn mention(pool: &PgPool, n: u8, kind: &str, to: Vec<u8>, original: bool) {
    sqlx::query("INSERT INTO public.k_mentions(content_id,content_type,mentioned_pubkey,block_time,sender_pubkey) VALUES($1,$2,$3,2000,$4)")
        .bind(id(n)).bind(kind).bind(&to).bind(author()).execute(pool).await.unwrap();
    if original {
        sqlx::query("UPDATE social_shared.actions SET snapshot=jsonb_build_object('k_mentions',jsonb_build_array(jsonb_build_object('mentioned_pubkey',$2::text))) WHERE transaction_id=$1")
            .bind(id(n)).bind(format!("\\x{}",hex::encode(to))).execute(pool).await.unwrap();
    }
}
async fn extension(pool: &PgPool, n: u8, target: u8, kind: &str, time: i64, message: &str) {
    action(
        pool,
        n,
        "kaposts",
        format!("{kind}:{}", hex::encode(sig(n))),
        "signed",
        time,
    )
    .await;
    sqlx::query("INSERT INTO social_shared.extensions(transaction_id,revision,original_time,kind,sender,signature,target,message,mentions,shape_valid,target_signature,target_author,target_kind,target_time) VALUES($1,1,$2,$3,$4,$5,$6,$7,'[]',true,$8,$4,'post',1000)")
        .bind(id(n)).bind(time).bind(kind).bind(author()).bind(sig(n)).bind(id(target)).bind(message).bind(sig(target)).execute(pool).await.unwrap();
}
async fn show(pool: &PgPool, target: u8, time: Option<i64>, body: &str) {
    sqlx::query("UPDATE public.k_contents SET edited_at=$2,base64_encoded_message=$3 WHERE transaction_id=$1").bind(id(target)).bind(time).bind(body).execute(pool).await.unwrap();
}
async fn feed(db: &PostgresDbManager) -> Vec<crate::models::KPostRecord> {
    db.get_all_posts(&hex::encode(viewer()), options(20))
        .await
        .unwrap()
        .items
}

#[tokio::test]
async fn postgres_shared_reads_fence_revoked_pending_and_stale_rows_without_worker() {
    let db = setup().await;
    for n in 1..=4 {
        post(&db.pool, n, 2000).await;
    }
    sqlx::query(
        "UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=$1",
    )
    .bind(id(1))
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE protocol_indexer.canonical SET revision=2 WHERE transaction_id=$1")
        .bind(id(2))
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
        .bind(id(3))
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        feed(&db)
            .await
            .iter()
            .map(|r| r.transaction_id.clone())
            .collect::<Vec<_>>(),
        vec![hex::encode(id(4))]
    );
    assert_eq!(db.get_stats().await.unwrap().posts_count, 1);
    assert!(
        db.get_content_by_id(&hex::encode(id(1)), &hex::encode(viewer()))
            .await
            .unwrap()
            .is_none()
    );
    let legacy = PostgresDbManager {
        pool: db.pool.clone(),
        shared: None,
    };
    assert_eq!(feed(&legacy).await.len(), 4);
}

#[tokio::test]
async fn postgres_shared_reads_binding_and_each_missing_view_fail_even_when_empty() {
    let mut db = setup().await;
    db.validate_shared_reads().await.unwrap();
    for (network, source) in [
        ("mainnet", SOURCE),
        ("testnet-10", "a179f285-19a7-4646-9eef-cf1bf2c20a57"),
    ] {
        db.shared.as_mut().unwrap().network = network.into();
        db.shared.as_mut().unwrap().source_id = source.into();
        assert!(db.get_stats().await.is_err());
        assert!(db.get_network().await.is_err());
    }
    db.shared.as_mut().unwrap().network = "testnet-10".into();
    db.shared.as_mut().unwrap().source_id = SOURCE.into();
    for table in [
        "k_contents",
        "k_votes",
        "k_broadcasts",
        "k_follows",
        "k_blocks",
        "k_mentions",
        "k_hashtags",
        "k_vars",
    ] {
        sqlx::raw_sql(&format!("DROP VIEW social_shared_api.{table} CASCADE"))
            .execute(&db.pool)
            .await
            .unwrap();
        assert!(db.get_stats().await.is_err(), "{table}");
        assert!(db.get_network().await.is_err(), "{table}");
        sqlx::raw_sql(&schema()).execute(&db.pool).await.unwrap();
    }
    // A temp/public namesake cannot rescue an incomplete shared schema.
    sqlx::raw_sql(
        "CREATE TEMP TABLE k_contents(x int); DROP VIEW social_shared_api.k_contents CASCADE",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    assert!(db.get_stats().await.is_err());
}

#[tokio::test]
async fn postgres_shared_reads_profile_and_pair_reversals_use_native_selection() {
    let db = setup().await;
    let key = hex::encode(author());
    for n in [1, 2] {
        action(
            &db.pool,
            n,
            "k_broadcasts",
            format!("k_broadcasts:{key}"),
            "replace",
            1000 + n as i64,
        )
        .await;
    }
    sqlx::query("INSERT INTO public.k_broadcasts(transaction_id,block_time,sender_pubkey,sender_signature,base64_encoded_nickname,base64_encoded_message) VALUES($1,1001,$2,$3,'T2xk','')").bind(id(1)).bind(author()).bind(sig(1)).execute(&db.pool).await.unwrap();
    assert_eq!(
        db.get_users_count().await.unwrap(),
        0,
        "known newer profile not yet materialized"
    );
    sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
        .bind(id(2))
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(db.get_users_count().await.unwrap(), 1);
    for (table, kind, column, start) in [
        ("k_follows", "follow", "followed_user_pubkey", 10),
        ("k_blocks", "block", "blocked_user_pubkey", 20),
    ] {
        let subject = format!("{table}:{key}:{}", hex::encode(viewer()));
        action(&db.pool, start, table, subject.clone(), "activate", 2000).await;
        action(&db.pool, start + 1, table, subject, "deactivate", 3000).await;
        let verb = if kind == "follow" {
            "following_action"
        } else {
            "blocking_action"
        };
        sqlx::query(&format!("INSERT INTO public.{table}(transaction_id,block_time,sender_pubkey,sender_signature,{verb},{column}) VALUES($1,2000,$2,$3,$4,$5)"))
            .bind(id(start)).bind(author()).bind(sig(start)).bind(kind).bind(viewer()).execute(&db.pool).await.unwrap();
        let count: i64 = {
            let mut r = db.read_session().await.unwrap();
            sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&mut *r)
                .await
                .unwrap()
        };
        assert_eq!(count, 0);
        sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
            .bind(id(start + 1))
            .execute(&db.pool)
            .await
            .unwrap();
        let count: i64 = {
            let mut r = db.read_session().await.unwrap();
            sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&mut *r)
                .await
                .unwrap()
        };
        assert_eq!(count, 1);
    }
}

#[tokio::test]
async fn postgres_shared_reads_edits_bind_exact_latest_body_tuple_and_literal_target() {
    let db = setup().await;
    post(&db.pool, 1, 1000).await;
    extension(&db.pool, 10, 1, "edit", 1100, "b2xk").await;
    show(&db.pool, 1, Some(1100), "b2xk").await;
    assert_eq!(feed(&db).await[0].base64_encoded_message, "b2xk");
    extension(&db.pool, 20, 1, "edit", 1100, "bmV3").await;
    assert!(
        feed(&db).await.is_empty(),
        "same-time higher tx wins; stale display must not leak"
    );
    show(&db.pool, 1, Some(1100), "bmV3").await;
    assert_eq!(feed(&db).await[0].base64_encoded_message, "bmV3");
    extension(&db.pool, 30, 1, "edit", 1200, "Zm9yZ2Vk").await;
    sqlx::query("UPDATE social_shared.extensions SET sender=$2 WHERE transaction_id=$1")
        .bind(id(30))
        .bind(viewer())
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(feed(&db).await[0].base64_encoded_message, "bmV3");
    sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
        .bind(id(20))
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(feed(&db).await.is_empty());
    show(&db.pool, 1, Some(1100), "b2xk").await;
    assert_eq!(feed(&db).await[0].base64_encoded_message, "b2xk");
    // An accepted signature alias is not the explicit edit target's acceptance.
    action(
        &db.pool,
        2,
        "k_contents",
        format!("k_contents:{}", hex::encode(sig(1))),
        "signed",
        1000,
    )
    .await;
    sqlx::query(
        "UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=$1",
    )
    .bind(id(1))
    .execute(&db.pool)
    .await
    .unwrap();
    assert!(feed(&db).await.is_empty());
}

#[tokio::test]
async fn postgres_shared_reads_mentions_keep_any_current_supplier_and_original_recipient() {
    let db = setup().await;
    post(&db.pool, 1, 1000).await;
    extension(&db.pool, 10, 1, "edit", 1100, "b2xk").await;
    extension(&db.pool, 20, 1, "edit", 1200, "bmV3").await;
    show(&db.pool, 1, Some(1200), "bmV3").await;
    mention(&db.pool, 1, "post", viewer(), false).await;
    sqlx::query("INSERT INTO social_shared.edit_mentions VALUES($1,$2)")
        .bind(id(10))
        .bind(viewer())
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        db.get_notification_count(&hex::encode(viewer()), None)
            .await
            .unwrap(),
        1,
        "older still-current edit supplies recipient"
    );
    sqlx::query(
        "UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=$1",
    )
    .bind(id(10))
    .execute(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        db.get_notification_count(&hex::encode(viewer()), None)
            .await
            .unwrap(),
        0
    );
    sqlx::query("UPDATE social_shared.actions SET snapshot=jsonb_build_object('k_mentions',jsonb_build_array(jsonb_build_object('mentioned_pubkey',$2::text))) WHERE transaction_id=$1").bind(id(1)).bind(format!("\\x{}",hex::encode(viewer()))).execute(&db.pool).await.unwrap();
    assert_eq!(
        db.get_notification_count(&hex::encode(viewer()), None)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn postgres_shared_reads_unquote_ties_and_known_delete_hide_before_worker() {
    let db = setup().await;
    post(&db.pool, 20, 1000).await;
    sqlx::query("UPDATE public.k_contents SET content_type='quote',referenced_content_id=$2 WHERE transaction_id=$1").bind(id(20)).bind(id(99)).execute(&db.pool).await.unwrap();
    sqlx::query(
        "UPDATE social_shared.content_keys SET kind='quote',reference=$2 WHERE transaction_id=$1",
    )
    .bind(id(20))
    .bind(id(99))
    .execute(&db.pool)
    .await
    .unwrap();
    extension(&db.pool, 10, 99, "unquote", 1000, "").await;
    assert_eq!(
        feed(&db).await.len(),
        1,
        "earlier same-time unquote cannot hide later quote"
    );
    extension(&db.pool, 30, 99, "unquote", 1000, "").await;
    assert!(feed(&db).await.is_empty());
    sqlx::query(
        "UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=$1",
    )
    .bind(id(30))
    .execute(&db.pool)
    .await
    .unwrap();
    assert_eq!(feed(&db).await.len(), 1);
    extension(&db.pool, 40, 20, "delete", 1400, "").await;
    assert!(feed(&db).await.is_empty());
}

#[tokio::test]
async fn postgres_shared_reads_legacy_tip_mentions_do_not_strand_pagination_or_counts() {
    let db = setup().await;
    for n in 1..=3 {
        mention(&db.pool, n, "tip", viewer(), false).await;
    }
    assert_eq!(
        db.get_notification_count(&hex::encode(viewer()), None)
            .await
            .unwrap(),
        0
    );
    let empty = db
        .get_notifications(&hex::encode(viewer()), options(1))
        .await
        .unwrap();
    assert!(empty.items.is_empty());
    assert!(!empty.pagination.has_more);
    assert!(empty.pagination.next_cursor.is_none());
    post(&db.pool, 10, 1000).await;
    mention(&db.pool, 10, "post", viewer(), true).await;
    assert_eq!(
        db.get_notification_count(&hex::encode(viewer()), None)
            .await
            .unwrap(),
        1
    );
    let page = db
        .get_notifications(&hex::encode(viewer()), options(1))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert!(!page.pagination.has_more);
}

#[tokio::test]
async fn postgres_shared_reads_one_snapshot_timeout_and_pool_cleanup_preserve_legacy() {
    let mut db = setup().await;
    post(&db.pool, 1, 1000).await;
    let mut read = db.read_session().await.unwrap();
    let first: i64 = sqlx::query_scalar("SELECT count(*) FROM k_contents")
        .fetch_one(&mut *read)
        .await
        .unwrap();
    assert_eq!(first, 1);
    let dsn = std::env::var("SOCIAL_SHARED_WEB_TEST_DATABASE_URL").unwrap();
    let other = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .unwrap();
    sqlx::query("UPDATE protocol_indexer.canonical SET accepting_block=NULL")
        .execute(&other)
        .await
        .unwrap();
    let second: i64 = sqlx::query_scalar("SELECT count(*) FROM k_contents")
        .fetch_one(&mut *read)
        .await
        .unwrap();
    assert_eq!(second, 1, "same coherent snapshot");
    drop(read);
    assert!(feed(&db).await.is_empty());
    db.shared.as_mut().unwrap().statement_timeout_ms = 100;
    let mut read = db.read_session().await.unwrap();
    assert!(
        sqlx::query("SELECT pg_sleep(0.3)")
            .execute(&mut *read)
            .await
            .is_err()
    );
    drop(read);
    let legacy = PostgresDbManager {
        pool: db.pool.clone(),
        shared: None,
    };
    assert_eq!(feed(&legacy).await.len(), 1);
    let mut conn = db.pool.acquire().await.unwrap();
    let row=sqlx::query("SELECT current_setting('transaction_read_only') ro,current_setting('search_path') path,current_setting('statement_timeout') timeout").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(row.get::<String, _>("ro"), "off");
    assert!(!row.get::<String, _>("path").contains("social_shared_api"));
    assert_eq!(row.get::<String, _>("timeout"), "0");
    drop(conn);
    // Profile fallback executes its two reads without a second checkout at max1.
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        db.get_user_details(&hex::encode(author()), &hex::encode(viewer())),
    )
    .await
    .unwrap()
    .unwrap();
    other.close().await;
}

#[tokio::test]
async fn postgres_shared_reads_feed_and_search_keep_existing_json_and_tied_cursors() {
    let db = setup().await;
    for n in 1..=3 {
        post(&db.pool, n, 1000).await;
    }
    let shared = ApiHandlers::new(Arc::new(PostgresDbManager {
        pool: db.pool.clone(),
        shared: db.shared.clone(),
    }));
    let legacy = ApiHandlers::new(Arc::new(PostgresDbManager {
        pool: db.pool.clone(),
        shared: None,
    }));
    let requester = hex::encode(viewer());
    let a: serde_json::Value = serde_json::from_str(
        &shared
            .get_posts_watching_paginated(&requester, 2, None, None)
            .await
            .unwrap(),
    )
    .unwrap();
    let b: serde_json::Value = serde_json::from_str(
        &legacy
            .get_posts_watching_paginated(&requester, 2, None, None)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(a, b);
    let a = shared
        .search_posts_paginated(&requester, "hi", 2, None, None)
        .await
        .unwrap();
    let b = legacy
        .search_posts_paginated(&requester, "hi", 2, None, None)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&a).unwrap(),
        serde_json::from_str::<serde_json::Value>(&b).unwrap()
    );
    let page = db.get_all_posts(&requester, options(2)).await.unwrap();
    let cursor = page.pagination.next_cursor.unwrap();
    let next = db
        .get_all_posts(
            &requester,
            QueryOptions {
                before: Some(cursor),
                ..options(2)
            },
        )
        .await
        .unwrap();
    assert_eq!(next.items.len(), 1);
    assert!(
        page.items
            .iter()
            .all(|p| p.transaction_id != next.items[0].transaction_id)
    );
}

#[tokio::test]
async fn postgres_shared_reads_vote_lookup_preserves_unknown_but_fences_known_literal_target() {
    let db = setup().await;
    post(&db.pool, 1, 1000).await;
    action(
        &db.pool,
        10,
        "k_votes",
        format!("k_votes:{}", hex::encode(sig(10))),
        "signed",
        2000,
    )
    .await;
    sqlx::query("INSERT INTO public.k_votes(transaction_id,block_time,sender_pubkey,sender_signature,post_id,vote) VALUES($1,2000,$2,$3,$4,'upvote')")
        .bind(id(10)).bind(author()).bind(sig(10)).bind(id(99)).execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO social_shared.ordinary_vote_targets VALUES($1,$2)")
        .bind(id(10))
        .bind(id(99))
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        db.get_vote_by_id(&hex::encode(id(10)), &hex::encode(author()))
            .await
            .unwrap()
            .is_some(),
        "upstream permits lookup before target is indexed"
    );
    sqlx::query("INSERT INTO protocol_indexer.canonical VALUES($1,1,'{}',NULL)")
        .bind(id(99))
        .execute(&db.pool)
        .await
        .unwrap();
    // Same-signature target metadata cannot rescue its explicitly revoked ID.
    sqlx::query("INSERT INTO social_shared.actions(transaction_id,revision,subject,native_table,source_payload,original_time,status,state_action) VALUES($1,1,$2,'k_contents','00',1000,'verified','signed')")
        .bind(id(99)).bind(format!("k_contents:{}",hex::encode(sig(1)))).execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO social_shared.content_keys VALUES($1,$2,$3,'post',NULL,1000)")
        .bind(id(99))
        .bind(sig(1))
        .bind(author())
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        db.get_vote_by_id(&hex::encode(id(10)), &hex::encode(author()))
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("UPDATE protocol_indexer.canonical SET accepting_block=decode(repeat('ab',32),'hex') WHERE transaction_id=$1").bind(id(99)).execute(&db.pool).await.unwrap();
    assert!(
        db.get_vote_by_id(&hex::encode(id(10)), &hex::encode(author()))
            .await
            .unwrap()
            .is_some()
    );
    sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
        .bind(id(99))
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        db.get_vote_by_id(&hex::encode(id(10)), &hex::encode(author()))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn postgres_shared_reads_hashtags_and_both_network_configurations_keep_contract() {
    let mut db = setup().await;
    post(&db.pool, 1, 1000).await;
    post(&db.pool, 2, 2000).await;
    sqlx::query("INSERT INTO public.k_hashtags(content_id,hashtag,block_time,sender_pubkey) VALUES($1,'hello',1000,$3),($2,'hello',2000,$3)").bind(id(1)).bind(id(2)).bind(author()).execute(&db.pool).await.unwrap();
    sqlx::query(
        "UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=$1",
    )
    .bind(id(2))
    .execute(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        db.get_hashtag_content("hello", &hex::encode(viewer()), options(20))
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        db.get_trending_hashtags(0, 3000, 20).await.unwrap(),
        vec![("hello".into(), 1)]
    );
    assert_eq!(db.get_network().await.unwrap(), "testnet-10");
    sqlx::raw_sql("UPDATE protocol_indexer.config SET network='mainnet'; UPDATE social_shared.config SET network='mainnet'; UPDATE public.k_vars SET value='mainnet' WHERE key='network'").execute(&db.pool).await.unwrap();
    db.shared.as_mut().unwrap().network = "mainnet".into();
    assert_eq!(db.get_network().await.unwrap(), "mainnet");
    assert_eq!(feed(&db).await.len(), 1);
}

#[tokio::test]
async fn postgres_shared_reads_page_work_stays_bounded_with_unrelated_history() {
    let db = setup().await;
    sqlx::query("WITH source AS (SELECT g,decode(md5('post-a-'||g)||md5('post-b-'||g),'hex') tx,decode(repeat(md5('signature-'||g),4),'hex') signature FROM generate_series(1,10000) g), canonical AS (INSERT INTO protocol_indexer.canonical SELECT tx,1,'{}',decode(repeat('ab',32),'hex') FROM source), actions AS (INSERT INTO social_shared.actions(transaction_id,revision,subject,native_table,source_payload,original_time,status,state_action,snapshot) SELECT tx,1,'k_contents:'||encode(signature,'hex'),'k_contents','00',1700000000000+g,'verified','signed','{\"k_mentions\":[]}' FROM source), native AS (INSERT INTO public.k_contents(transaction_id,block_time,sender_pubkey,sender_signature,base64_encoded_message,content_type) SELECT tx,1700000000000+g,$1,signature,'aGk=','post' FROM source) INSERT INTO social_shared.content_keys SELECT tx,signature,$1,'post',NULL,1700000000000+g FROM source")
        .bind(author()).execute(&db.pool).await.unwrap();
    sqlx::raw_sql("ANALYZE public.k_contents; ANALYZE social_shared.actions; ANALYZE protocol_indexer.canonical; ANALYZE protocol_indexer.pending; ANALYZE social_shared.extensions").execute(&db.pool).await.unwrap();
    // Measure the actual existing feed DAO as well as the ordered view scan.
    // This extension is restricted to this owning disposable test database.
    sqlx::query("SELECT pg_stat_statements_reset()")
        .execute(&db.pool)
        .await
        .unwrap();
    let start = std::time::Instant::now();
    let page = feed(&db).await;
    assert_eq!(page.len(), 20);
    let dao = sqlx::query("SELECT calls,rows,shared_blks_hit+shared_blks_read AS buffers FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND query LIKE $1")
        .bind("%WITH all_posts AS (%").fetch_all(&db.pool).await.unwrap();
    assert_eq!(dao.len(), 1, "measure the real feed query exactly once");
    let dao_buffers: i64 = dao[0].get("buffers");
    assert_eq!(dao[0].get::<i64, _>("calls"), 1);
    assert_eq!(
        dao[0].get::<i64, _>("rows"),
        21,
        "legacy limit+1 pagination"
    );
    let mut read = db.read_session().await.unwrap();
    let plan:serde_json::Value=sqlx::query_scalar("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT * FROM k_contents ORDER BY block_time DESC,id DESC LIMIT 20").fetch_one(&mut *read).await.unwrap();
    let blocks = plan[0]["Plan"]["Shared Hit Blocks"].as_u64().unwrap_or(0)
        + plan[0]["Plan"]["Shared Read Blocks"].as_u64().unwrap_or(0);
    println!(
        "SHARED_READ_PLAN {}",
        json!({"history":10000,"returned":page.len(),"elapsed_ms":start.elapsed().as_millis(),"buffers":blocks,"dao_buffers":dao_buffers,"plan":plan})
    );
    assert!(
        blocks < 5000,
        "page read must not scan unrelated full history"
    );
    assert!(
        dao_buffers < 10000,
        "actual feed query must stay page bounded"
    );
}
