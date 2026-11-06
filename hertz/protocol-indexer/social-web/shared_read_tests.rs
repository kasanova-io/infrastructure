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
            read_contract: crate::shared_rooms::ReadContract::Unified,
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
    sqlx::query("INSERT INTO social_shared.actions(transaction_id,revision,subject,native_table,source_payload,original_time,status,state_action,snapshot) VALUES($1,1,$2,$3,encode(convert_to('k:1:fixture','UTF8'),'hex'),$4,'verified',$5,'{\"k_mentions\":[]}'::jsonb)")
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

fn k_contract(db: &PostgresDbManager) -> PostgresDbManager {
    let mut binding = db.shared.clone().unwrap();
    binding.read_contract = crate::shared_rooms::ReadContract::LegacyK;
    PostgresDbManager {
        pool: db.pool.clone(),
        shared: Some(binding),
    }
}

async fn poll_vote_fixture(
    pool: &PgPool,
    n: u8,
    target: u8,
    representative: u8,
    voter: Vec<u8>,
    option: i32,
    time: i64,
) {
    action(
        pool,
        n,
        "kaposts",
        format!("pollvote:{}", hex::encode(sig(n))),
        "signed",
        time,
    )
    .await;
    sqlx::query("INSERT INTO social_shared.extensions(transaction_id,revision,original_time,kind,sender,signature,target,mentions,option_index,shape_valid,target_signature,target_author,target_kind,target_time) VALUES($1,1,$2,'pollvote',$3,$4,$5,'[]',$6,true,$7,$8,'poll',1700000000000)")
        .bind(id(n)).bind(time).bind(&voter).bind(sig(n)).bind(id(target)).bind(option).bind(sig(representative)).bind(author()).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO social_shared.poll_vote_facts VALUES($1,1,$2)")
        .bind(id(n))
        .bind(sig(representative))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO public.k_poll_votes(transaction_id,poll_id,voter_pubkey,option_index,block_time,sender_signature) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(id(n)).bind(id(target)).bind(voter).bind(option).bind(time).bind(sig(n)).execute(pool).await.unwrap();
}

async fn poll_json(db: &PostgresDbManager, poll: u8, requester: Vec<u8>) -> serde_json::Value {
    let api = ApiHandlers::new(Arc::new(PostgresDbManager {
        pool: db.pool.clone(),
        shared: db.shared.clone(),
    }));
    let value: serde_json::Value = serde_json::from_str(
        &api.get_post_details(&hex::encode(id(poll)), &hex::encode(requester))
            .await
            .unwrap(),
    )
    .unwrap();
    value["post"]["poll"].clone()
}

#[tokio::test]
async fn postgres_shared_reads_poll_tallies_latest_voter_ties_requester_and_page_isolation() {
    let db = setup().await;
    poll_fixture(&db.pool, 70).await;
    poll_fixture(&db.pool, 75).await;
    poll_vote_fixture(&db.pool, 80, 70, 70, viewer(), 0, 1700000001000).await;
    poll_vote_fixture(&db.pool, 81, 70, 70, author(), 0, 1700000001000).await;
    poll_vote_fixture(&db.pool, 82, 70, 70, viewer(), 1, 1700000002000).await;
    poll_vote_fixture(&db.pool, 85, 75, 75, viewer(), 1, 1700000002000).await;
    let result = poll_json(&db, 70, viewer()).await;
    assert_eq!(result["counts"], json!([1, 1]));
    assert_eq!(result["total"], json!(2));
    assert_eq!(result["myVote"], json!(1));
    assert_eq!(poll_json(&db, 70, author()).await["myVote"], json!(0));
    assert_eq!(
        poll_json(&db, 70, [vec![2], vec![0x44; 32]].concat()).await["myVote"],
        json!(null)
    );
    assert_eq!(poll_json(&db, 75, viewer()).await["counts"], json!([0, 1]));
    // A later transaction ID wins a chain-time tie, regardless of arrival.
    poll_vote_fixture(&db.pool, 83, 70, 70, viewer(), 0, 1700000002000).await;
    poll_vote_fixture(&db.pool, 84, 70, 70, viewer(), 1, 1700000001500).await;
    let result = poll_json(&db, 70, viewer()).await;
    assert_eq!(result["counts"], json!([2, 0]));
    assert_eq!(result["total"], json!(2));
    assert_eq!(result["myVote"], json!(0));
    let posts = feed(&db).await;
    let poll = posts
        .iter()
        .find(|p| p.transaction_id == hex::encode(id(70)))
        .unwrap()
        .poll
        .as_ref()
        .unwrap();
    assert_eq!(poll.counts, vec![2, 0]);
    assert_eq!(poll.my_vote, Some(0));
}

#[tokio::test]
async fn postgres_shared_reads_poll_tallies_reorg_pending_revision_and_same_read_snapshot() {
    let db = setup().await;
    poll_fixture(&db.pool, 70).await;
    poll_vote_fixture(&db.pool, 80, 70, 70, viewer(), 0, 1700000001000).await;
    poll_vote_fixture(&db.pool, 81, 70, 70, author(), 0, 1700000001000).await;
    poll_vote_fixture(&db.pool, 82, 70, 70, viewer(), 1, 1700000002000).await;
    sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
        .bind(id(82))
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(poll_json(&db, 70, viewer()).await["counts"], json!([2, 0]));
    sqlx::query("DELETE FROM protocol_indexer.pending WHERE transaction_id=$1")
        .bind(id(82))
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE protocol_indexer.canonical SET revision=2 WHERE transaction_id=$1")
        .bind(id(82))
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(poll_json(&db, 70, viewer()).await["counts"], json!([2, 0]));
    sqlx::query("UPDATE protocol_indexer.canonical SET revision=1 WHERE transaction_id=$1")
        .bind(id(82))
        .execute(&db.pool)
        .await
        .unwrap();
    let mut posts = feed(&db).await;
    let mut read = db.read_session().await.unwrap();
    let dsn = std::env::var("SOCIAL_SHARED_WEB_TEST_DATABASE_URL").unwrap();
    let mut writer = <sqlx::PgConnection as sqlx::Connection>::connect(&dsn)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=$1",
    )
    .bind(id(82))
    .execute(&mut writer)
    .await
    .unwrap();
    db.enrich_tip_counts(&mut read, &mut posts, &hex::encode(viewer()))
        .await
        .unwrap();
    assert_eq!(posts[0].poll.as_ref().unwrap().counts, vec![1, 1]);
    drop(read);
    assert_eq!(poll_json(&db, 70, viewer()).await["counts"], json!([2, 0]));
    sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
        .bind(id(70))
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(feed(&db).await.is_empty());
}

#[tokio::test]
async fn postgres_shared_reads_poll_tallies_alias_target_and_restricted_reader_grants() {
    let db = setup().await;
    poll_fixture(&db.pool, 70).await;
    action(
        &db.pool,
        71,
        "k_contents",
        format!("k_contents:{}", hex::encode(sig(70))),
        "signed",
        1700000000000,
    )
    .await;
    poll_vote_fixture(&db.pool, 80, 71, 70, viewer(), 1, 1700000001000).await;
    assert_eq!(poll_json(&db, 70, viewer()).await["counts"], json!([0, 1]));
    // A real NOLOGIN role in this explicitly guarded disposable cluster proves
    // the actual invoker-view dependencies, instead of testing only as owner.
    let role = format!("social_web_poll_reader_{}", std::process::id());
    sqlx::raw_sql(&format!("CREATE ROLE {role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS; GRANT USAGE ON SCHEMA public,protocol_indexer,social_shared,social_shared_api,social_shared_legacy_api TO {role}; GRANT SELECT ON ALL TABLES IN SCHEMA social_shared_api,social_shared_legacy_api TO {role}; GRANT SELECT ON protocol_indexer.config,protocol_indexer.pending,public.k_vars,public.k_contents,public.k_votes,social_shared.config,social_shared.actions,social_shared.source_facts,social_shared.extensions,social_shared.retained_imports,social_shared.retained_wire_roots,social_shared.current_retained_wire_roots,social_shared.current_tips,social_shared.current_broadcasts,social_shared.ordinary_vote_targets,social_shared.current_vote_withdrawals TO {role};"))
        .execute(&db.pool).await.unwrap();
    let set_role = format!("SET ROLE {role}");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |conn, _| {
            let command = set_role.clone();
            Box::pin(async move {
                sqlx::query(&command).execute(conn).await?;
                Ok(())
            })
        })
        .connect(&std::env::var("SOCIAL_SHARED_WEB_TEST_DATABASE_URL").unwrap())
        .await
        .unwrap();
    let restricted = PostgresDbManager {
        pool,
        shared: db.shared.clone(),
    };
    assert!(
        restricted
            .get_all_posts(&hex::encode(viewer()), options(20))
            .await
            .is_err()
    );
    sqlx::raw_sql(&format!(
        "GRANT SELECT ON social_shared.current_poll_votes TO {role}"
    ))
    .execute(&db.pool)
    .await
    .unwrap();
    let posts = feed(&restricted).await;
    assert_eq!(posts[0].poll.as_ref().unwrap().counts, vec![0, 1]);
    assert_eq!(posts[0].poll.as_ref().unwrap().my_vote, Some(1));
    let writes: bool = sqlx::query_scalar("SELECT has_table_privilege(current_user,'public.k_contents','INSERT,UPDATE,DELETE,TRUNCATE') OR has_table_privilege(current_user,'social_shared.ordinary_vote_targets','INSERT,UPDATE,DELETE,TRUNCATE')")
        .fetch_one(&restricted.pool).await.unwrap();
    assert!(!writes);
    restricted.pool.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .execute(&db.pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_shared_reads_k_contract_filters_before_limits_across_existing_routes() {
    let db = setup().await;
    for n in 1..=6 {
        post(&db.pool, n, 1000 + n as i64).await;
    }
    sqlx::query("UPDATE social_shared.actions SET source_payload=encode(convert_to('kchat:1:fixture','UTF8'),'hex') WHERE transaction_id=ANY($1)")
        .bind(vec![id(2),id(4),id(6)]).execute(&db.pool).await.unwrap();
    for n in [5, 6] {
        sqlx::query("UPDATE public.k_contents SET content_type='reply',referenced_content_id=$2 WHERE transaction_id=$1")
            .bind(id(n)).bind(id(1)).execute(&db.pool).await.unwrap();
        sqlx::query("UPDATE social_shared.content_keys SET kind='reply',reference=$2 WHERE transaction_id=$1")
            .bind(id(n)).bind(id(1)).execute(&db.pool).await.unwrap();
    }
    for n in [1, 2] {
        mention(&db.pool, n, "post", viewer(), true).await;
        sqlx::query("INSERT INTO public.k_hashtags(content_id,block_time,sender_pubkey,hashtag) VALUES($1,1000,$2,'same')")
            .bind(id(n)).bind(author()).execute(&db.pool).await.unwrap();
    }
    let legacy = k_contract(&db);
    let requester = hex::encode(viewer());
    let author_key = hex::encode(author());
    let first = legacy.get_all_posts(&requester, options(1)).await.unwrap();
    assert_eq!(first.items[0].transaction_id, hex::encode(id(3)));
    assert!(first.items[0].protocol.is_none());
    let next = legacy
        .get_all_posts(
            &requester,
            QueryOptions {
                before: first.pagination.next_cursor,
                ..options(1)
            },
        )
        .await
        .unwrap();
    assert_eq!(next.items[0].transaction_id, hex::encode(id(1)));
    assert!(!next.pagination.has_more);
    assert_eq!(
        legacy
            .get_posts_by_user(&author_key, &requester, options(1))
            .await
            .unwrap()
            .items[0]
            .transaction_id,
        hex::encode(id(3))
    );
    assert!(
        legacy
            .get_content_by_id(&hex::encode(id(4)), &requester)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.get_content_by_id(&hex::encode(id(4)), &requester)
            .await
            .unwrap()
            .is_some()
    );
    let replies = legacy
        .get_replies_by_post_id(&hex::encode(id(1)), &requester, options(1))
        .await
        .unwrap();
    assert_eq!(replies.items[0].transaction_id, hex::encode(id(5)));
    assert!(!replies.pagination.has_more);
    assert_eq!(
        legacy
            .get_hashtag_content("same", &requester, options(1))
            .await
            .unwrap()
            .items[0]
            .transaction_id,
        hex::encode(id(1))
    );
    assert_eq!(
        legacy
            .get_contents_mentioning_user(&requester, &requester, options(1))
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        legacy
            .get_notification_count(&requester, None)
            .await
            .unwrap(),
        1
    );
    let api = ApiHandlers::new(Arc::new(k_contract(&db)));
    let search: serde_json::Value = serde_json::from_str(
        &api.search_posts_paginated(&requester, "hi", 1, None, None)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(search["posts"][0]["id"], json!(hex::encode(id(3))));
    assert!(search["posts"][0].get("protocol").is_none());
    // Author-wide follows/blocks keep working even when sent through KaChat.
    for (n, table, verb, column) in [
        (10, "k_follows", "following_action", "followed_user_pubkey"),
        (20, "k_blocks", "blocking_action", "blocked_user_pubkey"),
    ] {
        action(
            &db.pool,
            n,
            table,
            format!("{table}:{requester}:{author_key}"),
            "activate",
            2000,
        )
        .await;
        sqlx::query("UPDATE social_shared.actions SET source_payload=encode(convert_to('kchat:1:fixture','UTF8'),'hex') WHERE transaction_id=$1")
            .bind(id(n)).execute(&db.pool).await.unwrap();
        let value = if n == 10 { "follow" } else { "block" };
        sqlx::query(&format!("INSERT INTO public.{table}(transaction_id,block_time,sender_pubkey,sender_signature,{verb},{column}) VALUES($1,2000,$2,$3,$4,$5)"))
            .bind(id(n)).bind(viewer()).bind(sig(n)).bind(value).bind(author()).execute(&db.pool).await.unwrap();
        if n == 10 {
            assert_eq!(
                legacy
                    .get_content_following(&requester, options(1))
                    .await
                    .unwrap()
                    .items[0]
                    .transaction_id,
                hex::encode(id(5))
            );
        }
    }
    assert!(
        legacy
            .get_all_posts(&requester, options(20))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        db.get_all_posts(&requester, options(20))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    db.pool.close().await;
}

#[tokio::test]
async fn postgres_shared_reads_legacy_import_membership_never_claims_actual_wire_root() {
    let db = setup().await;
    post(&db.pool, 1, 1000).await;
    let import = "a".repeat(64);
    sqlx::query("INSERT INTO social_shared.retained_imports(import_id,network,source_id,protocol_hint,source_backup_sha256,inventory,native_records) VALUES($1,'testnet-10',$2,'k',$3,'{}',1)")
        .bind(&import).bind(SOURCE).bind("b".repeat(64)).execute(&db.pool).await.unwrap();
    sqlx::query("UPDATE social_shared.actions SET provenance='retained_native',retained_import_id=$2 WHERE transaction_id=$1")
        .bind(id(1)).bind(&import).execute(&db.pool).await.unwrap();
    sqlx::query("DELETE FROM protocol_indexer.canonical WHERE transaction_id=$1")
        .bind(id(1))
        .execute(&db.pool)
        .await
        .unwrap();
    let legacy = k_contract(&db);
    let requester = hex::encode(viewer());
    let old = legacy.get_all_posts(&requester, options(20)).await.unwrap();
    assert_eq!(old.items.len(), 1);
    assert!(old.items[0].protocol.is_none());
    assert!(old.items[0].legacy_k_compatible.is_none());
    let mixed = feed(&db).await;
    assert_eq!(mixed[0].legacy_k_compatible, Some(true));
    let api = ApiHandlers::new(Arc::new(PostgresDbManager {
        pool: db.pool.clone(),
        shared: db.shared.clone(),
    }));
    let dto: serde_json::Value = serde_json::from_str(
        &api.get_post_details(&hex::encode(id(1)), &requester)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(dto["post"]["protocol"], json!("unknown"));
    assert_eq!(dto["post"]["legacyKCompatible"], json!(true));
    assert_eq!(feed(&db).await[0].protocol.as_deref(), Some("unknown"));
    sqlx::query(
        "UPDATE social_shared.retained_imports SET protocol_hint='kchat' WHERE import_id=$1",
    )
    .bind(&import)
    .execute(&db.pool)
    .await
    .unwrap();
    assert!(
        legacy
            .get_all_posts(&requester, options(20))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(feed(&db).await[0].protocol.as_deref(), Some("unknown"));
    assert_eq!(feed(&db).await[0].legacy_k_compatible, Some(false));
    sqlx::query("UPDATE social_shared.retained_imports SET protocol_hint='k' WHERE import_id=$1")
        .bind(&import)
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(feed(&db).await[0].legacy_k_compatible, Some(true));
    sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
        .bind(id(1))
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(feed(&db).await.is_empty());
    sqlx::query("DELETE FROM protocol_indexer.pending WHERE transaction_id=$1")
        .bind(id(1))
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(feed(&db).await[0].legacy_k_compatible, Some(true));
    sqlx::query("INSERT INTO protocol_indexer.canonical VALUES($1,1,'{}',NULL)")
        .bind(id(1))
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        legacy
            .get_all_posts(&requester, options(20))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        db.get_all_posts(&requester, options(20))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    sqlx::query("DROP VIEW social_shared_legacy_api.k_contents CASCADE")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(legacy.get_all_posts(&requester, options(20)).await.is_err());
    assert!(db.get_all_posts(&requester, options(20)).await.is_err());
    db.pool.close().await;
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
    assert_eq!(feed(&db).await[0].edited_at, Some(1100));
    let api = ApiHandlers::new(Arc::new(PostgresDbManager {
        pool: db.pool.clone(),
        shared: db.shared.clone(),
    }));
    let detail: serde_json::Value = serde_json::from_str(
        &api.get_post_details(&hex::encode(id(1)), &hex::encode(viewer()))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(detail["post"]["editedAt"], json!(1100));
    let record = &feed(&db).await[0];
    let blocked =
        crate::models::ServerPost::from_enriched_k_post_record_with_block_status(record, true);
    assert!(
        serde_json::to_value(blocked)
            .unwrap()
            .get("editedAt")
            .is_none()
    );
    let legacy = PostgresDbManager {
        pool: db.pool.clone(),
        shared: None,
    };
    let record = &feed(&legacy).await[0];
    let legacy_post =
        crate::models::ServerPost::from_enriched_k_post_record_with_block_status(record, false);
    assert!(
        serde_json::to_value(legacy_post)
            .unwrap()
            .get("editedAt")
            .is_none()
    );
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
    let original_jit: String = sqlx::query_scalar("SHOW jit")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    post(&db.pool, 1, 1000).await;
    let mut read = db.read_session().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SHOW jit")
            .fetch_one(&mut *read)
            .await
            .unwrap(),
        "off"
    );
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
    let row=sqlx::query("SELECT current_setting('transaction_read_only') ro,current_setting('search_path') path,current_setting('statement_timeout') timeout,current_setting('jit') jit").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(row.get::<String, _>("ro"), "off");
    assert!(!row.get::<String, _>("path").contains("social_shared_api"));
    assert_eq!(row.get::<String, _>("timeout"), "0");
    assert_eq!(row.get::<String, _>("jit"), original_jit);
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
    let mut a: serde_json::Value = serde_json::from_str(
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
    // The new field is additive; assert its verified zero and preserve every
    // original DTO field and exclusive cursor comparison against legacy.
    for post in a["posts"].as_array_mut().unwrap() {
        assert_eq!(
            post.as_object_mut().unwrap().remove("legacyKCompatible"),
            Some(json!(false))
        );
        assert_eq!(
            post.as_object_mut().unwrap().remove("protocol"),
            Some(json!("k"))
        );
        assert_eq!(
            post.as_object_mut().unwrap().remove("tipsCount"),
            Some(json!(0))
        );
    }
    assert_eq!(a, b);
    let a = shared
        .search_posts_paginated(&requester, "hi", 2, None, None)
        .await
        .unwrap();
    let b = legacy
        .search_posts_paginated(&requester, "hi", 2, None, None)
        .await
        .unwrap();
    let mut a: serde_json::Value = serde_json::from_str(&a).unwrap();
    for post in a["posts"].as_array_mut().unwrap() {
        assert_eq!(
            post.as_object_mut().unwrap().remove("legacyKCompatible"),
            Some(json!(false))
        );
        assert_eq!(
            post.as_object_mut().unwrap().remove("protocol"),
            Some(json!("k"))
        );
        assert_eq!(
            post.as_object_mut().unwrap().remove("tipsCount"),
            Some(json!(0))
        );
    }
    assert_eq!(a, serde_json::from_str::<serde_json::Value>(&b).unwrap());
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
    sqlx::query("INSERT INTO social_shared.actions(transaction_id,revision,subject,native_table,source_payload,original_time,status,state_action) VALUES($1,1,$2,'k_contents',encode(convert_to('k:1:fixture','UTF8'),'hex'),1000,'verified','signed')")
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
    sqlx::query("WITH source AS (SELECT g,decode(md5('post-a-'||g)||md5('post-b-'||g),'hex') tx,decode(repeat(md5('signature-'||g),4),'hex') signature FROM generate_series(1,10000) g), canonical AS (INSERT INTO protocol_indexer.canonical SELECT tx,1,'{}',decode(repeat('ab',32),'hex') FROM source), actions AS (INSERT INTO social_shared.actions(transaction_id,revision,subject,native_table,source_payload,original_time,status,state_action,snapshot) SELECT tx,1,'k_contents:'||encode(signature,'hex'),'k_contents',encode(convert_to('k:1:fixture','UTF8'),'hex'),1700000000000+g,'verified','signed','{\"k_mentions\":[]}' FROM source), native AS (INSERT INTO public.k_contents(transaction_id,block_time,sender_pubkey,sender_signature,base64_encoded_message,content_type) SELECT tx,1700000000000+g,$1,signature,'aGk=','post' FROM source) INSERT INTO social_shared.content_keys SELECT tx,signature,$1,'post',NULL,1700000000000+g FROM source")
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

// Explicit already-verified native facts; SDK/chain acceptance stays separate.
async fn tip_fixture(db: &PostgresDbManager) {
    post(&db.pool, 1, 1000).await;
    post(&db.pool, 2, 1001).await;
    action(
        &db.pool,
        9,
        "k_contents",
        format!("k_contents:{}", hex::encode(sig(1))),
        "signed",
        1000,
    )
    .await;
    action(
        &db.pool,
        3,
        "k_tips",
        format!("k_tips:{}", hex::encode(sig(3))),
        "signed",
        2000,
    )
    .await;
    sqlx::query("INSERT INTO public.k_tips(transaction_id,block_time,sender_pubkey,sender_signature,post_id,author_pubkey,amount) VALUES($1,2000,$2,$3,$4,$5,10000000)")
        .bind(id(3)).bind(viewer()).bind(sig(3)).bind(id(9)).bind(author()).execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO social_shared.tip_targets VALUES($1,$2,$3)")
        .bind(id(3))
        .bind(id(9))
        .bind(sig(1))
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO social_shared.tip_payments VALUES($1,1,10000000,$2)")
        .bind(id(3))
        .bind(sig(1))
        .execute(&db.pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_shared_reads_protocol_is_actual_representative_root_on_same_snapshot() {
    let db = setup().await;
    tip_fixture(&db).await;
    // The signature alias is KaChat but the visible retained representative is K.
    sqlx::query("UPDATE social_shared.actions SET source_payload=encode(convert_to('kchat:1:post:key:sig:4oGgaGk=:[]','UTF8'),'hex') WHERE transaction_id=ANY($1)")
        .bind(vec![id(2), id(9)]).execute(&db.pool).await.unwrap();
    // A marker on K content must not change the transaction's actual namespace.
    sqlx::query(
        "UPDATE public.k_contents SET base64_encoded_message='4oGgaGk=' WHERE transaction_id=$1",
    )
    .bind(id(1))
    .execute(&db.pool)
    .await
    .unwrap();
    let mut posts = feed(&db).await;
    let root = |posts: &[crate::models::KPostRecord], n| {
        posts
            .iter()
            .find(|p| p.transaction_id == hex::encode(id(n)))
            .unwrap()
            .protocol
            .clone()
    };
    assert_eq!(root(&posts, 1).as_deref(), Some("k"));
    assert_eq!(root(&posts, 2).as_deref(), Some("kchat"));
    let legacy = PostgresDbManager {
        pool: db.pool.clone(),
        shared: None,
    };
    assert!(feed(&legacy).await.iter().all(|p| p.protocol.is_none()));
    let mut read = db.read_session().await.unwrap();
    let dsn = std::env::var("SOCIAL_SHARED_WEB_TEST_DATABASE_URL").unwrap();
    let mut writer = <sqlx::PgConnection as sqlx::Connection>::connect(&dsn)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE social_shared.actions SET source_payload=encode(convert_to('k:1:fixture','UTF8'),'hex') WHERE transaction_id=$1",
    )
    .bind(id(2))
    .execute(&mut writer)
    .await
    .unwrap();
    db.enrich_tip_counts(&mut read, &mut posts, &hex::encode(viewer()))
        .await
        .unwrap();
    assert_eq!(root(&posts, 2).as_deref(), Some("kchat"));
    drop(read);
    assert_eq!(root(&feed(&db).await, 2).as_deref(), Some("k"));
    sqlx::query(
        "UPDATE social_shared.actions SET provenance='retained_native' WHERE transaction_id=$1",
    )
    .bind(id(1))
    .execute(&db.pool)
    .await
    .unwrap();
    let retained = feed(&db).await;
    assert_eq!(root(&retained, 1).as_deref(), Some("unknown"));
    assert_eq!(
        retained
            .iter()
            .find(|p| p.transaction_id == hex::encode(id(1)))
            .unwrap()
            .tips_count,
        Some(1)
    );
    sqlx::query(
        "UPDATE social_shared.actions SET source_payload=encode(convert_to('other:1:post','UTF8'),'hex') WHERE transaction_id=$1",
    )
    .bind(id(2))
    .execute(&db.pool)
    .await
    .unwrap();
    assert!(
        db.get_all_posts(&hex::encode(viewer()), options(20))
            .await
            .is_err()
    );
    db.pool.close().await;
}

#[tokio::test]
async fn postgres_shared_reads_tip_counts_receipts_aliases_and_snapshot_follow_native_facts() {
    let db = setup().await;
    tip_fixture(&db).await;
    let mut posts = feed(&db).await;
    assert_eq!(
        posts
            .iter()
            .find(|p| p.transaction_id == hex::encode(id(1)))
            .unwrap()
            .tips_count,
        Some(1)
    );
    assert_eq!(
        posts
            .iter()
            .find(|p| p.transaction_id == hex::encode(id(2)))
            .unwrap()
            .tips_count,
        Some(0)
    );
    let receipt = db.tip_details(&id(3)).await.unwrap().unwrap();
    assert_eq!(receipt.parent_post_id, hex::encode(id(1)));
    assert_eq!(receipt.user_public_key, hex::encode(viewer()));
    assert_eq!(receipt.author_public_key, hex::encode(author()));
    assert_eq!(receipt.amount_sompi, "10000000");
    assert!(db.tip_details(&id(99)).await.unwrap().is_none());
    for n in [3, 9, 1] {
        sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
            .bind(id(n))
            .execute(&db.pool)
            .await
            .unwrap();
        assert!(db.tip_details(&id(3)).await.unwrap().is_none());
        let current = feed(&db).await;
        if n == 1 {
            assert!(
                !current
                    .iter()
                    .any(|p| p.transaction_id == hex::encode(id(1)))
            );
        } else {
            assert_eq!(
                current
                    .iter()
                    .find(|p| p.transaction_id == hex::encode(id(1)))
                    .unwrap()
                    .tips_count,
                Some(0)
            );
        }
        sqlx::query("DELETE FROM protocol_indexer.pending")
            .execute(&db.pool)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE protocol_indexer.canonical SET revision=2 WHERE transaction_id=$1")
        .bind(id(3))
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(db.tip_details(&id(3)).await.unwrap().is_none());
    sqlx::query("UPDATE protocol_indexer.canonical SET revision=1 WHERE transaction_id=$1")
        .bind(id(3))
        .execute(&db.pool)
        .await
        .unwrap();
    // The helper must use the supplied read snapshot, not another checkout.
    let mut read = db.read_session().await.unwrap();
    let dsn = std::env::var("SOCIAL_SHARED_WEB_TEST_DATABASE_URL").unwrap();
    let mut writer = <sqlx::PgConnection as sqlx::Connection>::connect(&dsn)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=$1",
    )
    .bind(id(3))
    .execute(&mut writer)
    .await
    .unwrap();
    db.enrich_tip_counts(&mut read, &mut posts, &hex::encode(viewer()))
        .await
        .unwrap();
    assert_eq!(
        posts
            .iter()
            .find(|p| p.transaction_id == hex::encode(id(1)))
            .unwrap()
            .tips_count,
        Some(1)
    );
    drop(read);
    assert_eq!(
        feed(&db)
            .await
            .iter()
            .find(|p| p.transaction_id == hex::encode(id(1)))
            .unwrap()
            .tips_count,
        Some(0)
    );
    let legacy = PostgresDbManager {
        pool: db.pool.clone(),
        shared: None,
    };
    assert!(feed(&legacy).await.iter().all(|p| p.tips_count.is_none()));
    sqlx::query("DROP VIEW social_shared.current_tips")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        db.get_all_posts(&hex::encode(viewer()), options(20))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn postgres_shared_reads_tip_details_router_checks_receipt_source_and_query() {
    use axum::{
        body::{Body, to_bytes},
        extract::ConnectInfo,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let db = setup().await;
    tip_fixture(&db).await;
    let pool = db.pool.clone();
    let config = crate::config::ServerConfig {
        bind_address: "127.0.0.1:0".into(),
        request_timeout: 10,
        rate_limit: 200,
    };
    let server = crate::web_server::WebServer::new(Arc::new(db), config).await;
    let app = server.create_router();
    async fn request(app: &axum::Router, uri: String) -> (StatusCode, serde_json::Value) {
        let mut request = Request::builder().uri(uri).body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(
            "127.0.0.1:12345".parse::<std::net::SocketAddr>().unwrap(),
        ));
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 100000).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }
    let uri = format!("/get-tip-details?id={}", hex::encode(id(3)));
    let (status, body) = request(&app, uri.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({"tip":{"id":hex::encode(id(3)),"parentPostId":hex::encode(id(1)),"userPublicKey":hex::encode(viewer()),"authorPublicKey":hex::encode(author()),"amountSompi":"10000000"}})
    );
    assert_eq!(
        request(&app, "/get-tip-details?id=bad".into()).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, format!("/get-tip-details?id={}", hex::encode(id(99))))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
        .bind(id(3))
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(request(&app, uri.clone()).await.0, StatusCode::NOT_FOUND);
    sqlx::query("UPDATE social_shared.config SET source_id='new-owner'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(request(&app, uri).await.0, StatusCode::SERVICE_UNAVAILABLE);
}

async fn poll_fixture(pool: &PgPool, n: u8) {
    post(pool, n, 1700000000000).await;
    sqlx::query("UPDATE public.k_contents SET content_type='poll' WHERE transaction_id=$1")
        .bind(id(n))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE social_shared.content_keys SET kind='poll' WHERE transaction_id=$1")
        .bind(id(n))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE social_shared.actions SET source_payload=encode(convert_to('kchat:1:poll:fixture','UTF8'),'hex') WHERE transaction_id=$1")
        .bind(id(n)).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO social_shared.extensions(transaction_id,revision,original_time,kind,sender,signature,message,mentions,options,closes_at,shape_valid) VALUES($1,1,1700000000000,'poll',$2,$3,'aGk=','[]','Tm9ydGg=,U291dGg=',1700003600000,true)")
        .bind(id(n)).bind(author()).bind(sig(n)).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO public.k_polls VALUES($1,'Tm9ydGg=,U291dGg=',1700003600000,1700000000000)",
    )
    .bind(id(n))
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn postgres_shared_reads_poll_metadata_is_visible_in_feed_detail_search_and_current_revision_only()
 {
    let db = setup().await;
    poll_fixture(&db.pool, 70).await;
    mention(&db.pool, 70, "poll", viewer(), true).await;
    sqlx::query("INSERT INTO public.k_hashtags(content_id,block_time,sender_pubkey,hashtag) VALUES($1,1700000000000,$2,'poll')")
        .bind(id(70)).bind(author()).execute(&db.pool).await.unwrap();
    let requester = hex::encode(viewer());
    let rows = feed(&db).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].content_type.as_deref(), Some("poll"));
    assert_eq!(rows[0].protocol.as_deref(), Some("kchat"));
    assert_eq!(
        rows[0].poll.as_ref().unwrap().options,
        vec!["Tm9ydGg=", "U291dGg="]
    );
    let api = ApiHandlers::new(Arc::new(PostgresDbManager {
        pool: db.pool.clone(),
        shared: db.shared.clone(),
    }));
    let detail: serde_json::Value = serde_json::from_str(
        &api.get_post_details(&hex::encode(id(70)), &requester)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(detail["post"]["contentType"], json!("poll"));
    assert_eq!(detail["post"]["poll"]["closesAt"], json!(1700003600000u64));
    let search: serde_json::Value = serde_json::from_str(
        &api.search_posts_paginated(&requester, "hi", 1, None, None)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(search["posts"][0]["poll"], detail["post"]["poll"]);
    let mentions = db
        .get_contents_mentioning_user(&requester, &requester, options(1))
        .await
        .unwrap();
    assert_eq!(mentions.items.len(), 1);
    let notifications: serde_json::Value = serde_json::from_str(
        &api.get_notifications_paginated(&requester, 1, None, None)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        notifications["notifications"][0]["contentType"],
        json!("poll")
    );
    assert_eq!(
        notifications["notifications"][0]["protocol"],
        json!("kchat")
    );
    assert_eq!(
        notifications["notifications"][0]["poll"],
        detail["post"]["poll"]
    );

    let hashtag = db
        .get_hashtag_content("poll", &requester, options(1))
        .await
        .unwrap();
    assert_eq!(
        hashtag.items[0].poll.as_ref().unwrap().closes_at,
        1700003600000
    );
    assert!(feed(&k_contract(&db)).await.is_empty());
    // Corrupt or stale options cannot become an apparently normal post.
    sqlx::query("UPDATE public.k_polls SET options='YQ==,Yg==' WHERE post_id=$1")
        .bind(id(70))
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(db.get_all_posts(&requester, options(1)).await.is_err());
    sqlx::query("UPDATE public.k_polls SET options='Tm9ydGg=,U291dGg=' WHERE post_id=$1")
        .bind(id(70))
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE social_shared.extensions SET shape_valid=false WHERE transaction_id=$1")
        .bind(id(70))
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(feed(&db).await.is_empty());
}

#[tokio::test]
async fn postgres_shared_reads_verified_retained_root_overrides_hint_and_stale_evidence_fails_closed()
 {
    let db = setup().await;
    post(&db.pool, 1, 1000).await;
    let import = "ab".repeat(32);
    sqlx::query("INSERT INTO social_shared.retained_imports(import_id,network,source_id,protocol_hint,source_backup_sha256,inventory,native_records) VALUES($1,'testnet-10',$2,'k',$3,'{}',1)")
        .bind(&import).bind(SOURCE).bind("cd".repeat(32)).execute(&db.pool).await.unwrap();
    sqlx::query("UPDATE social_shared.actions SET provenance='retained_native',retained_import_id=$2 WHERE transaction_id=$1")
        .bind(id(1)).bind(&import).execute(&db.pool).await.unwrap();
    sqlx::query("DELETE FROM protocol_indexer.canonical WHERE transaction_id=$1")
        .bind(id(1))
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(feed(&db).await[0].protocol.as_deref(), Some("unknown"));
    assert_eq!(feed(&db).await[0].legacy_k_compatible, Some(true));
    assert_eq!(feed(&k_contract(&db)).await.len(), 1);
    // Reader materialization fixture only. The owning native tests prove that
    // only an SDK body/ID + retained-fields match is accepted by the writer.
    sqlx::query("INSERT INTO social_shared.retained_wire_roots SELECT transaction_id,retained_import_id,revision,original_time,sha256(convert_to(source_payload,'UTF8')),'kchat',decode(repeat('ab',32),'hex'),'01b532e8b553523216471682649693af92f0fd16' FROM social_shared.actions WHERE transaction_id=$1")
        .bind(id(1)).execute(&db.pool).await.unwrap();
    assert_eq!(feed(&db).await[0].protocol.as_deref(), Some("kchat"));
    assert_eq!(feed(&db).await[0].legacy_k_compatible, Some(false));
    assert!(
        feed(&k_contract(&db)).await.is_empty(),
        "a proven KaChat root overrides the legacy import hint before pagination"
    );
    sqlx::query("UPDATE social_shared.actions SET source_payload=source_payload||'00' WHERE transaction_id=$1")
        .bind(id(1)).execute(&db.pool).await.unwrap();
    assert_eq!(feed(&db).await[0].protocol.as_deref(), Some("unknown"));
    assert_eq!(feed(&db).await[0].legacy_k_compatible, Some(false));
    assert!(
        feed(&k_contract(&db)).await.is_empty(),
        "a stale proof must not silently restore a K-only classification"
    );
    sqlx::query(
        "UPDATE social_shared.actions SET source_payload=left(source_payload,length(source_payload)-2) WHERE transaction_id=$1",
    )
    .bind(id(1))
    .execute(&db.pool)
    .await
    .unwrap();
    assert_eq!(feed(&db).await[0].protocol.as_deref(), Some("kchat"));
    assert_eq!(feed(&db).await[0].legacy_k_compatible, Some(false));
    sqlx::query("INSERT INTO protocol_indexer.canonical VALUES($1,1,'{}',NULL)")
        .bind(id(1))
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        feed(&db).await.is_empty(),
        "root proof never grants acceptance"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM social_shared.retained_wire_roots")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        1
    );
    sqlx::query("DROP VIEW social_shared.current_retained_wire_roots CASCADE")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        db.get_network().await.is_err(),
        "missing proof view cannot fall back to unbound tables, even with no visible posts"
    );
    assert!(db.get_stats().await.is_err());
    db.pool.close().await;
}

#[tokio::test]
async fn postgres_shared_reads_vote_notifications_and_exact_receipts_preserve_actual_root_and_target()
 {
    let db = setup().await;
    for (n, parent, root) in [(10, 1, "k"), (11, 2, "kchat")] {
        post(&db.pool, parent, 1000 + parent as i64).await;
        sqlx::query("UPDATE public.k_contents SET sender_pubkey=$2 WHERE transaction_id=$1")
            .bind(id(parent))
            .bind(viewer())
            .execute(&db.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE social_shared.content_keys SET author=$2 WHERE transaction_id=$1")
            .bind(id(parent))
            .bind(viewer())
            .execute(&db.pool)
            .await
            .unwrap();
        action(
            &db.pool,
            n,
            "k_votes",
            format!("k_votes:{}", hex::encode(sig(n))),
            "signed",
            2000,
        )
        .await;
        sqlx::query("UPDATE social_shared.actions SET source_payload=encode(convert_to($2,'UTF8'),'hex') WHERE transaction_id=$1")
            .bind(id(n)).bind(format!("{root}:1:vote:fixture")).execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO public.k_votes(transaction_id,block_time,sender_pubkey,sender_signature,post_id,vote) VALUES($1,2000,$2,$3,$4,'upvote')")
            .bind(id(n)).bind(author()).bind(sig(n)).bind(id(parent)).execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO social_shared.ordinary_vote_targets VALUES($1,$2)")
            .bind(id(n))
            .bind(id(parent))
            .execute(&db.pool)
            .await
            .unwrap();
        mention(&db.pool, n, "vote", viewer(), false).await;
    }
    let api = ApiHandlers::new(Arc::new(PostgresDbManager {
        pool: db.pool.clone(),
        shared: db.shared.clone(),
    }));
    let notify: serde_json::Value = serde_json::from_str(
        &api.get_notifications_paginated(&hex::encode(viewer()), 20, None, None)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(notify["notifications"].as_array().unwrap().len(), 2);
    for (n, parent, root) in [(10, 1, "k"), (11, 2, "kchat")] {
        let notification = notify["notifications"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["id"] == json!(hex::encode(id(n))))
            .unwrap();
        assert_eq!(notification["protocol"], json!(root));
        assert_eq!(notification["contentId"], json!(hex::encode(id(parent))));
        assert!(notification.get("poll").is_none());
        let exact: serde_json::Value = serde_json::from_str(
            &api.get_vote_details(&hex::encode(id(n)), &hex::encode(author()))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(exact["vote"]["protocol"], json!(root));
        assert_eq!(
            exact["vote"]["parentPostId"],
            json!(hex::encode(id(parent)))
        );
        assert!(
            api.get_vote_details(&hex::encode(id(n)), &hex::encode(viewer()))
                .await
                .is_err(),
            "a notification recipient cannot claim the sender's exact receipt"
        );
    }
    let legacy = ApiHandlers::new(Arc::new(k_contract(&db)));
    let old: serde_json::Value = serde_json::from_str(
        &legacy
            .get_notifications_paginated(&hex::encode(viewer()), 20, None, None)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(old["notifications"].as_array().unwrap().len(), 1);
    assert!(old["notifications"][0].get("protocol").is_none());
    let old_exact: serde_json::Value = serde_json::from_str(
        &legacy
            .get_vote_details(&hex::encode(id(10)), &hex::encode(author()))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        old_exact["vote"].as_object().unwrap().len(),
        4,
        "legacy receipt keeps its original shape"
    );
    let import = "dd".repeat(32);
    sqlx::query("INSERT INTO social_shared.retained_imports(import_id,network,source_id,protocol_hint,source_backup_sha256,inventory,native_records) VALUES($1,'testnet-10',$2,'k',$3,'{}',1)")
        .bind(&import).bind(SOURCE).bind("ee".repeat(32)).execute(&db.pool).await.unwrap();
    sqlx::query("UPDATE social_shared.actions SET provenance='retained_native',retained_import_id=$2 WHERE transaction_id=$1")
        .bind(id(11)).bind(&import).execute(&db.pool).await.unwrap();
    sqlx::query("DELETE FROM protocol_indexer.canonical WHERE transaction_id=$1")
        .bind(id(11))
        .execute(&db.pool)
        .await
        .unwrap();
    let unknown: serde_json::Value = serde_json::from_str(
        &api.get_vote_details(&hex::encode(id(11)), &hex::encode(author()))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(unknown["vote"]["protocol"], json!("unknown"));
    assert_eq!(unknown["vote"]["legacyKCompatible"], json!(true));
    let notices: serde_json::Value = serde_json::from_str(
        &api.get_notifications_paginated(&hex::encode(viewer()), 20, None, None)
            .await
            .unwrap(),
    )
    .unwrap();
    let notice = notices["notifications"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == json!(hex::encode(id(11))))
        .unwrap();
    assert_eq!(notice["protocol"], json!("unknown"));
    assert_eq!(notice["legacyKCompatible"], json!(true));
    // Explicit reader proof fixture; native writer's SDK checks are separate.
    sqlx::query("INSERT INTO social_shared.retained_wire_roots SELECT transaction_id,retained_import_id,revision,original_time,sha256(convert_to(source_payload,'UTF8')),'kchat',decode(repeat('ab',32),'hex'),'01b532e8b553523216471682649693af92f0fd16' FROM social_shared.actions WHERE transaction_id=$1")
        .bind(id(11)).execute(&db.pool).await.unwrap();
    let recovered: serde_json::Value = serde_json::from_str(
        &api.get_vote_details(&hex::encode(id(11)), &hex::encode(author()))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(recovered["vote"]["protocol"], json!("kchat"));
    assert_eq!(recovered["vote"]["legacyKCompatible"], json!(false));
    sqlx::query(
        "UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=$1",
    )
    .bind(id(10))
    .execute(&db.pool)
    .await
    .unwrap();
    assert!(
        api.get_vote_details(&hex::encode(id(10)), &hex::encode(author()))
            .await
            .is_err(),
        "root metadata never rescues a revoked vote"
    );
    db.pool.close().await;
}

#[tokio::test]
async fn postgres_shared_reads_quotes_keep_reference_protocol_separate_from_quote_protocol() {
    let db = setup().await;
    for n in [1, 2] {
        post(&db.pool, n, 1000 + n as i64).await;
    }
    sqlx::query("UPDATE social_shared.actions SET source_payload=encode(convert_to('kchat:1:post:fixture','UTF8'),'hex') WHERE transaction_id=$1")
        .bind(id(2)).execute(&db.pool).await.unwrap();
    for (n, target, root) in [(10, 1, "kchat"), (11, 2, "k"), (12, 1, "k")] {
        post(&db.pool, n, 2000 + n as i64).await;
        sqlx::query("UPDATE public.k_contents SET content_type='quote',referenced_content_id=$2 WHERE transaction_id=$1")
            .bind(id(n)).bind(id(target)).execute(&db.pool).await.unwrap();
        sqlx::query("UPDATE social_shared.content_keys SET kind='quote',reference=$2 WHERE transaction_id=$1")
            .bind(id(n)).bind(id(target)).execute(&db.pool).await.unwrap();
        sqlx::query("UPDATE social_shared.actions SET source_payload=encode(convert_to($2,'UTF8'),'hex') WHERE transaction_id=$1")
            .bind(id(n)).bind(format!("{root}:1:quote:fixture")).execute(&db.pool).await.unwrap();
    }
    let api = ApiHandlers::new(Arc::new(PostgresDbManager {
        pool: db.pool.clone(),
        shared: db.shared.clone(),
    }));
    let requester = hex::encode(viewer());
    for (n, target, own_root, reference_root) in [(10, 1, "kchat", "k"), (11, 2, "k", "kchat")] {
        let result: serde_json::Value = serde_json::from_str(
            &api.get_post_details(&hex::encode(id(n)), &requester)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result["post"]["protocol"], json!(own_root));
        assert_eq!(
            result["post"]["quote"]["referencedProtocol"],
            json!(reference_root)
        );
        assert_eq!(
            result["post"]["quote"]["referencedContentId"],
            json!(hex::encode(id(target)))
        );
    }
    let legacy = ApiHandlers::new(Arc::new(k_contract(&db)));
    let old: serde_json::Value = serde_json::from_str(
        &legacy
            .get_post_details(&hex::encode(id(12)), &requester)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(old["post"].get("protocol").is_none());
    assert!(old["post"]["quote"].get("referencedProtocol").is_none());
    let import = "ac".repeat(32);
    sqlx::query("INSERT INTO social_shared.retained_imports(import_id,network,source_id,protocol_hint,source_backup_sha256,inventory,native_records) VALUES($1,'testnet-10',$2,'k',$3,'{}',1)")
        .bind(&import).bind(SOURCE).bind("bc".repeat(32)).execute(&db.pool).await.unwrap();
    sqlx::query("UPDATE social_shared.actions SET provenance='retained_native',retained_import_id=$2 WHERE transaction_id=$1")
        .bind(id(2)).bind(&import).execute(&db.pool).await.unwrap();
    sqlx::query("DELETE FROM protocol_indexer.canonical WHERE transaction_id=$1")
        .bind(id(2))
        .execute(&db.pool)
        .await
        .unwrap();
    let unknown: serde_json::Value = serde_json::from_str(
        &api.get_post_details(&hex::encode(id(11)), &requester)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(unknown["post"]["protocol"], json!("k"));
    assert_eq!(
        unknown["post"]["quote"]["referencedProtocol"],
        json!("unknown"),
        "the quoting post's K root must not be inferred for its reference"
    );
    assert_eq!(
        unknown["post"]["quote"]["referencedLegacyKCompatible"],
        json!(true)
    );
    // Explicit reader fixture, not a chain or SDK proof.
    sqlx::query("INSERT INTO social_shared.retained_wire_roots SELECT transaction_id,retained_import_id,revision,original_time,sha256(convert_to(source_payload,'UTF8')),'kchat',decode(repeat('ab',32),'hex'),'01b532e8b553523216471682649693af92f0fd16' FROM social_shared.actions WHERE transaction_id=$1")
        .bind(id(2)).execute(&db.pool).await.unwrap();
    let recovered: serde_json::Value = serde_json::from_str(
        &api.get_post_details(&hex::encode(id(11)), &requester)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        recovered["post"]["quote"]["referencedProtocol"],
        json!("kchat")
    );
    assert_eq!(
        recovered["post"]["quote"]["referencedLegacyKCompatible"],
        json!(false)
    );
    sqlx::query("INSERT INTO protocol_indexer.canonical VALUES($1,1,'{}',NULL)")
        .bind(id(2))
        .execute(&db.pool)
        .await
        .unwrap();
    let hidden: serde_json::Value = serde_json::from_str(
        &api.get_post_details(&hex::encode(id(11)), &requester)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        hidden["post"].get("quote").is_none(),
        "a revoked reference must not reveal content, author or protocol"
    );
    db.pool.close().await;
}
