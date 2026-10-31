//! Real SDK cryptography + native SQL, not a replacement protocol parser.
use crate::database::Transaction;
use crate::k_protocol::{KActionType, KProtocolProcessor};
use kaspa_wallet_core::message::{PersonalMessage, SignMessageOptions, sign_message};
use secp256k1::{Keypair, Secp256k1};
use sqlx::postgres::PgPoolOptions;

fn processor() -> (KProtocolProcessor, sqlx::PgPool) {
    // Pure tests never acquire this lazy pool.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://unused@127.0.0.1/unused")
        .unwrap();
    (KProtocolProcessor::new(pool.clone()), pool)
}

fn public_key() -> String {
    let pair = Keypair::from_seckey_slice(&Secp256k1::new(), &[3; 32]).unwrap();
    pair.x_only_public_key().0.to_string()
}

fn signed(root: &str, action: &str, tail: &str, message: &str, id: u8) -> Transaction {
    // Public, deterministic test key. Never a wallet fixture or broadcaster.
    let signature = sign_message(
        &PersonalMessage(message),
        &[3; 32],
        &SignMessageOptions { no_aux_rand: true },
    )
    .unwrap();
    Transaction {
        transaction_id: format!("{id:02x}").repeat(32),
        block_time: Some(12345),
        payload: Some(hex::encode(format!(
            "{root}:1:{action}:{}:{}:{tail}",
            public_key(),
            hex::encode(signature)
        ))),
    }
}

#[tokio::test]
async fn pure_both_protocol_roots_preserve_all_existing_signed_actions_without_filters() {
    let (processor, pool) = processor();
    let target = format!("02{}", public_key());
    let parent = "ab".repeat(32);
    let message = "4oGgaGk="; // Exact signed marker+hi; no filter or normalization.
    let mentions = format!(r#"["{target}"]"#);
    let cases = vec![
        (
            "post",
            format!("{message}:{mentions}"),
            format!("{message}:{mentions}"),
        ),
        (
            "reply",
            format!("{parent}:{message}:{mentions}"),
            format!("{parent}:{message}:{mentions}"),
        ),
        (
            "quote",
            format!("{parent}:{message}:{target}"),
            format!("{parent}:{message}:{target}"),
        ),
        (
            "broadcast",
            format!("YWxpY2U=::{message}"),
            format!("YWxpY2U=::{message}"),
        ),
        (
            "vote",
            format!("{parent}:upvote:{target}"),
            format!("{parent}:upvote:{target}"),
        ),
        (
            "follow",
            format!("follow:{target}"),
            format!("follow:{target}"),
        ),
        (
            "follow",
            format!("unfollow:{target}"),
            format!("unfollow:{target}"),
        ),
        (
            "block",
            format!("block:{target}"),
            format!("block:{target}"),
        ),
        (
            "block",
            format!("unblock:{target}"),
            format!("unblock:{target}"),
        ),
    ];
    for root in ["k", "kchat"] {
        for (index, (kind, tail, signature_message)) in cases.iter().enumerate() {
            let transaction = signed(root, kind, tail, signature_message, (index + 1) as u8);
            let verified = processor
                .verify_transaction(&transaction)
                .expect("valid SDK signature");
            assert_eq!(verified.protocol(), root);
            assert_eq!(verified.transaction().payload, transaction.payload);
            if let KActionType::Post(post) = verified.action() {
                assert_eq!(post.base64_encoded_message, message);
            }
        }
    }
    // Ordinary non-marked KaChat bodies also coexist; exclusivity is not part of Kasanova.
    let transaction = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 20);
    assert!(processor.verify_transaction(&transaction).is_some());
    assert_eq!(pool.size(), 0);
}

#[tokio::test]
async fn pure_genuine_k_post_signature_and_synthetic_kachat_header_keep_signed_bytes() {
    let (processor, _) = processor();
    let row: serde_json::Value = serde_json::from_str(include_str!("real_kpost.json")).unwrap();
    let transaction = Transaction {
        transaction_id: row["transaction_id"].as_str().unwrap().to_owned(),
        payload: Some(row["payload"].as_str().unwrap().to_owned()),
        block_time: row["block_time"].as_i64(),
    };
    assert_eq!(
        processor
            .verify_transaction(&transaction)
            .unwrap()
            .protocol(),
        "k"
    );
    // This is a synthetic compatibility vector, NOT an additional chain transaction.
    let raw =
        String::from_utf8(hex::decode(transaction.payload.as_ref().unwrap()).unwrap()).unwrap();
    let mut counterpart = transaction.clone();
    counterpart.payload = Some(hex::encode(raw.replacen("k:1:", "kchat:1:", 1)));
    assert_eq!(
        processor
            .verify_transaction(&counterpart)
            .unwrap()
            .protocol(),
        "kchat"
    );
}

#[tokio::test]
async fn pure_invalid_versions_signatures_and_key_forms_never_become_verified() {
    let (processor, pool) = processor();
    let transaction = signed("k", "post", "aGk=:[]", "aGk=:[]", 1);
    let raw =
        String::from_utf8(hex::decode(transaction.payload.as_ref().unwrap()).unwrap()).unwrap();
    for root in ["k:2:", "kchat:2:", "ciph_msg:1:", "unknown:1:"] {
        let mut bad = transaction.clone();
        bad.payload = Some(hex::encode(raw.replacen("k:1:", root, 1)));
        assert!(processor.verify_transaction(&bad).is_none());
    }
    let fields: Vec<_> = raw.split(':').collect();
    for replacement in [format!("04{}", public_key()), "00".repeat(32)] {
        let mut bad = transaction.clone();
        bad.payload = Some(hex::encode(raw.replacen(fields[3], &replacement, 1)));
        assert!(processor.verify_transaction(&bad).is_none());
    }
    for replacement in ["00".repeat(64), format!("{}00", fields[4])] {
        let mut bad = transaction.clone();
        bad.payload = Some(hex::encode(raw.replacen(fields[4], &replacement, 1)));
        assert!(processor.verify_transaction(&bad).is_none());
    }
    for prefix in ["02", "03"] {
        let mut compressed = transaction.clone();
        compressed.payload = Some(hex::encode(raw.replacen(
            fields[3],
            &format!("{prefix}{}", public_key()),
            1,
        )));
        assert!(processor.verify_transaction(&compressed).is_some());
    }
    assert_eq!(pool.size(), 0);
}

#[tokio::test]
async fn pure_verified_envelope_owns_reviewed_transaction_not_latest_mutable_input() {
    let (processor, _) = processor();
    let mut transaction = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 1);
    let verified = processor.verify_transaction(&transaction).unwrap();
    transaction.payload = Some("00".into());
    transaction.transaction_id = "ff".repeat(32);
    transaction.block_time = Some(99999);
    assert_ne!(verified.transaction().payload, transaction.payload);
    assert_eq!(verified.transaction().transaction_id, "01".repeat(32));
    assert_eq!(verified.transaction().block_time, Some(12345));
}

#[cfg(feature = "shared-postgres-contract")]
async fn database() -> sqlx::PgPool {
    let dsn = std::env::var("SOCIAL_TEST_DATABASE_URL")
        .expect("explicit disposable PostgreSQL DSN required");
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .connect(&dsn)
        .await
        .unwrap();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        name.starts_with("protocol_test_social"),
        "only owning disposable database allowed"
    );
    sqlx::raw_sql("DROP SCHEMA public CASCADE; CREATE SCHEMA public; CREATE TABLE transactions(transaction_id bytea PRIMARY KEY,payload bytea,block_time bigint)").execute(&pool).await.unwrap();
    crate::database::KDbClient::new(pool.clone())
        .create_schema(true)
        .await
        .unwrap();
    pool
}

#[cfg(feature = "shared-postgres-contract")]
#[tokio::test]
async fn postgres_all_native_effects_use_the_callers_transaction_and_rollback_together() {
    let pool = database().await;
    let processor = KProtocolProcessor::new(pool.clone());
    let target = format!("02{}", public_key());
    let mentions = format!(r#"["{target}"]"#);
    // #kaspa plus a mention exercises the original compound native projection.
    let tail = format!("aGkgI2thc3Bh:{mentions}");
    let transaction = signed("kchat", "post", &tail, &tail, 1);
    let verified = processor.verify_transaction(&transaction).unwrap();
    let mut owner = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('kasanova.social_notify_allowed','false',true)")
        .execute(&mut *owner)
        .await
        .unwrap();
    processor
        .apply_verified_in(&mut owner, &verified)
        .await
        .unwrap();
    let inside: i64 = sqlx::query_scalar("SELECT count(*) FROM k_contents")
        .fetch_one(&mut *owner)
        .await
        .unwrap();
    let outside: i64 = sqlx::query_scalar("SELECT count(*) FROM k_contents")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!((inside, outside), (1, 0));
    owner.rollback().await.unwrap();
    for table in ["k_contents", "k_mentions", "k_hashtags"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "{table} rolled back with owning transaction");
    }
    processor.process_k_transaction(&transaction).await.unwrap();
    for table in ["k_contents", "k_mentions", "k_hashtags"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            count, 1,
            "legacy entrypoint keeps native {table} projection"
        );
    }
    // Same signed action through the other root retains native signature dedup.
    let counterpart = signed("k", "post", &tail, &tail, 2);
    processor.process_k_transaction(&counterpart).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM k_contents")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[cfg(feature = "shared-postgres-contract")]
mod shared_consumer {
    use super::*;
    use crate::shared_social::SharedSocial;
    const SOURCE: &str = "a179f285-19a7-4646-9eef-cf1bf2c20a56";

    async fn setup() -> sqlx::PgPool {
        let pool = database().await;
        sqlx::raw_sql("DROP SCHEMA IF EXISTS social_shared CASCADE; DROP SCHEMA IF EXISTS social_notifications CASCADE; DROP SCHEMA IF EXISTS protocol_indexer CASCADE;
            INSERT INTO k_vars(key,value) VALUES('network','testnet-10');
            CREATE SCHEMA protocol_indexer;
            CREATE TABLE protocol_indexer.config(singleton boolean PRIMARY KEY DEFAULT true, network text, journal_id uuid);
            CREATE TABLE protocol_indexer.canonical(transaction_id bytea PRIMARY KEY,revision bigint,source_transaction jsonb,accepting_block bytea);
            CREATE TABLE protocol_indexer.consumer_jobs(consumer text,transaction_id bytea REFERENCES protocol_indexer.canonical,available_at timestamptz DEFAULT clock_timestamp(),last_error text,PRIMARY KEY(consumer,transaction_id));
            CREATE TABLE protocol_indexer.events(transaction_id bytea,revision bigint,notify_allowed boolean,PRIMARY KEY(transaction_id,revision));
            CREATE TABLE notification_probe(id bigserial PRIMARY KEY,transaction_id bytea NOT NULL);
            CREATE FUNCTION notification_probe_capture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                IF current_setting('kasanova.social_notify_allowed',true)='true' THEN
                    INSERT INTO notification_probe(transaction_id) VALUES(NEW.transaction_id);
                END IF;
                RETURN NEW; END $$;
            CREATE TRIGGER probe_content AFTER INSERT ON k_contents FOR EACH ROW EXECUTE FUNCTION notification_probe_capture();
            CREATE TRIGGER probe_follow AFTER INSERT ON k_follows FOR EACH ROW EXECUTE FUNCTION notification_probe_capture();")
            .execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO protocol_indexer.config(network,journal_id) VALUES('testnet-10',$1::uuid)",
        )
        .bind(SOURCE)
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    async fn capture(
        pool: &sqlx::PgPool,
        value: &Transaction,
        revision: i64,
        accepted: bool,
        notify: bool,
    ) {
        let mut tx = pool.begin().await.unwrap();
        let id = hex::decode(&value.transaction_id).unwrap();
        let body = serde_json::json!({"payload":format!("\\x{}",value.payload.as_ref().unwrap()),"block_time":value.block_time});
        sqlx::query("INSERT INTO protocol_indexer.canonical VALUES($1,$2,$3::jsonb,$4) ON CONFLICT(transaction_id) DO UPDATE SET revision=EXCLUDED.revision,source_transaction=EXCLUDED.source_transaction,accepting_block=EXCLUDED.accepting_block")
            .bind(&id).bind(revision).bind(body.to_string()).bind(accepted.then(||vec![9u8;32]))
            .execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO protocol_indexer.events VALUES($1,$2,$3)")
            .bind(&id)
            .bind(revision)
            .bind(notify)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("INSERT INTO protocol_indexer.consumer_jobs(consumer,transaction_id) VALUES('native-social-v1',$1) ON CONFLICT DO NOTHING")
            .bind(&id).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
    }

    async fn count(pool: &sqlx::PgPool, table: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn consume(consumer: &SharedSocial) {
        assert!(consumer.run_one().await.unwrap());
    }

    #[tokio::test]
    async fn postgres_shared_binding_rejects_foreign_source_before_schema_or_history_writes() {
        let pool = setup().await;
        assert!(
            SharedSocial::bind(pool.clone(), "mainnet", SOURCE)
                .await
                .is_err()
        );
        assert!(
            SharedSocial::bind(
                pool.clone(),
                "testnet-10",
                "00000000-0000-0000-0000-000000000000"
            )
            .await
            .is_err()
        );
        let absent: bool = sqlx::query_scalar("SELECT to_regclass('social_shared.config') IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(absent);
        let processor = KProtocolProcessor::new(pool.clone());
        processor
            .process_k_transaction(&signed("k", "post", "aGk=:[]", "aGk=:[]", 1))
            .await
            .unwrap();
        assert!(
            SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
                .await
                .is_err()
        );
        assert_eq!(
            count(&pool, "k_contents").await,
            1,
            "legacy history preserved, never implicitly adopted/reset"
        );
        sqlx::raw_sql("CREATE SCHEMA social_shared; CREATE TABLE social_shared.config(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),network text NOT NULL,source_id text NOT NULL)")
            .execute(&pool).await.unwrap();
        assert!(
            SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
                .await
                .is_err()
        );
        assert_eq!(
            count(&pool, "social_shared.config").await,
            0,
            "empty metadata table does not establish ownership"
        );
        assert_eq!(count(&pool, "k_contents").await, 1);
    }

    #[tokio::test]
    async fn postgres_shared_configured_mainnet_works_and_newer_native_schema_fails_closed() {
        let pool = setup().await;
        sqlx::query("UPDATE protocol_indexer.config SET network='mainnet'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE k_vars SET value='mainnet' WHERE key='network'")
            .execute(&pool)
            .await
            .unwrap();
        let consumer = SharedSocial::bind(pool.clone(), "mainnet", SOURCE)
            .await
            .unwrap();
        let value = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 1);
        capture(&pool, &value, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_contents").await, 1);
        let other = signed("k", "post", "bGl2ZQ==:[]", "bGl2ZQ==:[]", 2);
        capture(&pool, &other, 1, true, false).await;
        sqlx::query("UPDATE k_vars SET value='3' WHERE key='schema_version'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(consumer.run_one().await.is_err());
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 1);
        assert_eq!(count(&pool, "k_contents").await, 1);
    }

    #[tokio::test]
    async fn postgres_shared_rejects_old_notification_body_including_comment_preserving_rollback() {
        let pool = setup().await;
        sqlx::raw_sql("CREATE SCHEMA social_notifications; CREATE FUNCTION social_notifications.capture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;")
            .execute(&pool).await.unwrap();
        assert!(
            SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
                .await
                .is_err()
        );
        let absent: bool = sqlx::query_scalar("SELECT to_regclass('social_shared.config') IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(absent);
        sqlx::raw_sql(include_str!("notification_capture.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE social_notifications.activation SET since_ms=0")
            .execute(&pool)
            .await
            .unwrap();
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let first = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 1);
        capture(&pool, &first, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(
            count(&pool, "social_notifications.outbox").await,
            0,
            "actual outbox respects quiet source history after older notification epoch"
        );
        let value = signed("k", "post", "bGl2ZQ==:[]", "bGl2ZQ==:[]", 2);
        capture(&pool, &value, 1, true, true).await;
        // PostgreSQL preserves this stale marker on old CREATE OR REPLACE.
        sqlx::query("COMMENT ON FUNCTION social_notifications.capture() IS 'kasanova.social-notify-context-v1'").execute(&pool).await.unwrap();
        let approved = include_str!("notification_capture.sql");
        let old=approved.replace("    IF current_setting('kasanova.social_notify_allowed', true) = 'false' THEN\n        RETURN NEW;\n    END IF;\n","");
        assert_ne!(old, approved);
        sqlx::raw_sql(&old).execute(&pool).await.unwrap();
        assert!(consumer.run_one().await.is_err());
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 1);
        assert_eq!(count(&pool, "social_shared.actions").await, 1);
        sqlx::raw_sql(approved).execute(&pool).await.unwrap();
        consume(&consumer).await;
        assert_eq!(
            count(&pool, "social_notifications.outbox").await,
            1,
            "actual live native capture preserved"
        );
    }

    #[tokio::test]
    async fn postgres_shared_notification_installer_cannot_replace_body_after_preflight() {
        let pool = setup().await;
        sqlx::raw_sql(include_str!("notification_capture.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE social_notifications.activation SET since_ms=0")
            .execute(&pool)
            .await
            .unwrap();
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let value = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 1);
        capture(&pool, &value, 1, true, false).await;
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query(
            "SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=$1 FOR UPDATE",
        )
        .bind(hex::decode(&value.transaction_id).unwrap())
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
        let running = consumer.clone();
        let task = tokio::spawn(async move { running.run_one().await });
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let held:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='k_contents'::regclass AND mode='RowExclusiveLock' AND granted)").fetch_one(&pool).await.unwrap();
            if held {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "preflight acquired actual DML table lease"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let installer_pool = pool.clone();
        let installer = async move {
            let mut tx = installer_pool.begin().await.unwrap();
            // Exact lock contract of owning app/social_worker.install_outbox.
            sqlx::query("LOCK TABLE public.k_contents,public.k_votes,public.k_follows IN SHARE ROW EXCLUSIVE MODE").execute(&mut *tx).await.unwrap();
            let old=include_str!("notification_capture.sql").replace("    IF current_setting('kasanova.social_notify_allowed', true) = 'false' THEN\n        RETURN NEW;\n    END IF;\n","");
            sqlx::raw_sql(&old).execute(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
        };
        tokio::pin!(installer);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut installer)
                .await
                .is_err(),
            "owning installer must wait across capability check and actual projection"
        );
        blocker.rollback().await.unwrap();
        assert!(task.await.unwrap().unwrap());
        installer.await;
        assert_eq!(count(&pool, "social_notifications.outbox").await, 0);
        let other = signed("k", "post", "bGl2ZQ==:[]", "bGl2ZQ==:[]", 2);
        capture(&pool, &other, 1, true, true).await;
        assert!(consumer.run_one().await.is_err());
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 1);
    }

    #[tokio::test]
    async fn postgres_shared_posts_revoke_reaccept_keep_all_native_cursors_and_quiet_history() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let tail = format!("aGkgI2thc3Bh:[\"{}\"]", public_key());
        let mut value = signed("kchat", "post", &tail, &tail, 1);
        capture(&pool, &value, 1, true, false).await;
        consume(&consumer).await;
        async fn cursors(pool: &sqlx::PgPool) -> Vec<i64> {
            let mut result = Vec::new();
            for table in ["k_contents", "k_mentions", "k_hashtags"] {
                result.push(
                    sqlx::query_scalar(&format!("SELECT id FROM {table}"))
                        .fetch_one(pool)
                        .await
                        .unwrap(),
                );
            }
            result
        }
        let original = cursors(&pool).await;
        value.block_time = Some(999999);
        capture(&pool, &value, 2, true, false).await;
        consume(&consumer).await;
        assert_eq!(cursors(&pool).await, original);
        let time: i64 = sqlx::query_scalar("SELECT block_time FROM k_contents")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(time, 12345);
        capture(&pool, &value, 3, false, false).await;
        consume(&consumer).await;
        for table in ["k_contents", "k_mentions", "k_hashtags"] {
            assert_eq!(count(&pool, table).await, 0);
        }
        capture(&pool, &value, 4, true, true).await;
        consume(&consumer).await;
        assert_eq!(cursors(&pool).await, original);
        assert_eq!(
            count(&pool, "notification_probe").await,
            0,
            "history/reaccept cannot become new pushes"
        );
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 0);
    }

    #[tokio::test]
    async fn postgres_shared_signature_dedup_keeps_active_representative_then_falls_back() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let mut a = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 2);
        a.block_time = Some(200);
        let mut b = signed("k", "post", "aGk=:[]", "aGk=:[]", 1);
        b.block_time = Some(100);
        capture(&pool, &a, 1, true, true).await;
        consume(&consumer).await;
        let original: i64 = sqlx::query_scalar("SELECT id FROM k_contents")
            .fetch_one(&pool)
            .await
            .unwrap();
        capture(&pool, &b, 1, true, false).await;
        consume(&consumer).await;
        let representative: String =
            sqlx::query_scalar("SELECT encode(transaction_id,'hex') FROM k_contents")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            representative, a.transaction_id,
            "late older duplicate cannot replace existing accepted identity"
        );
        capture(&pool, &a, 2, false, false).await;
        consume(&consumer).await;
        let representative: String =
            sqlx::query_scalar("SELECT encode(transaction_id,'hex') FROM k_contents")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(representative, b.transaction_id);
        capture(&pool, &b, 2, false, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_contents").await, 0);
        capture(&pool, &a, 3, true, false).await;
        consume(&consumer).await;
        let restored: i64 = sqlx::query_scalar("SELECT id FROM k_contents")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(restored, original);
        assert_eq!(count(&pool, "notification_probe").await, 1);
    }

    #[tokio::test]
    async fn postgres_shared_relationships_and_profile_rebuild_chronologically_on_reorg() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let target = public_key();
        for (action, positive, negative, table) in [
            ("follow", "follow", "unfollow", "k_follows"),
            ("block", "block", "unblock", "k_blocks"),
        ] {
            let tail = format!("{positive}:{target}");
            let mut a = signed(
                "kchat",
                action,
                &tail,
                &tail,
                if action == "follow" { 1 } else { 3 },
            );
            a.block_time = Some(100);
            let tail = format!("{negative}:{target}");
            let mut b = signed(
                "k",
                action,
                &tail,
                &tail,
                if action == "follow" { 2 } else { 4 },
            );
            b.block_time = Some(200);
            capture(&pool, &b, 1, true, false).await;
            consume(&consumer).await;
            capture(&pool, &a, 1, true, false).await;
            consume(&consumer).await;
            assert_eq!(
                count(&pool, table).await,
                0,
                "late older action cannot undo newer reversal"
            );
            capture(&pool, &b, 2, false, false).await;
            consume(&consumer).await;
            assert_eq!(
                count(&pool, table).await,
                1,
                "revoking reversal restores accepted previous state"
            );
            let original: i64 = sqlx::query_scalar(&format!("SELECT id FROM {table}"))
                .fetch_one(&pool)
                .await
                .unwrap();
            capture(&pool, &b, 3, true, false).await;
            consume(&consumer).await;
            assert_eq!(count(&pool, table).await, 0);
            capture(&pool, &b, 4, false, false).await;
            consume(&consumer).await;
            let restored: i64 = sqlx::query_scalar(&format!("SELECT id FROM {table}"))
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(original, restored);
        }
        let mut a = signed("k", "broadcast", "QQ==::YQ==", "QQ==::YQ==", 5);
        a.block_time = Some(100);
        let mut b = signed("kchat", "broadcast", "Qg==::Yg==", "Qg==::Yg==", 6);
        b.block_time = Some(200);
        capture(&pool, &b, 1, true, false).await;
        consume(&consumer).await;
        capture(&pool, &a, 1, true, false).await;
        consume(&consumer).await;
        let latest: String = sqlx::query_scalar("SELECT base64_encoded_nickname FROM k_broadcasts")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(latest, "Qg==");
        capture(&pool, &b, 2, false, false).await;
        consume(&consumer).await;
        let latest: String = sqlx::query_scalar("SELECT base64_encoded_nickname FROM k_broadcasts")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(latest, "QQ==");
        assert_eq!(count(&pool, "notification_probe").await, 0);
    }

    #[tokio::test]
    async fn postgres_shared_selected_row_contention_defers_without_lock_inversion() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let tail = format!("follow:{}", public_key());
        let mut b = signed("kchat", "follow", &tail, &tail, 2);
        b.block_time = Some(100);
        capture(&pool, &b, 1, false, false).await;
        consume(&consumer).await;
        let tail = format!("unfollow:{}", public_key());
        let mut a = signed("k", "follow", &tail, &tail, 1);
        a.block_time = Some(50);
        capture(&pool, &a, 1, true, false).await;
        capture(&pool, &b, 2, true, true).await;
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query(
            "SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=$1 FOR UPDATE",
        )
        .bind(hex::decode(&b.transaction_id).unwrap())
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), consumer.run_one())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            count(&pool, "protocol_indexer.consumer_jobs").await,
            2,
            "contended subject has no ack/effects"
        );
        assert_eq!(count(&pool, "k_follows").await, 0);
        blocker.rollback().await.unwrap();
        sqlx::query("UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp()")
            .execute(&pool)
            .await
            .unwrap();
        consume(&consumer).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_follows").await, 1);
        assert_eq!(count(&pool, "notification_probe").await, 1);
    }

    #[tokio::test]
    async fn postgres_shared_selected_canonical_fence_blocks_revoke_until_projection_commit() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let tail = format!("follow:{}", public_key());
        let mut b = signed("kchat", "follow", &tail, &tail, 2);
        b.block_time = Some(100);
        capture(&pool, &b, 1, false, false).await;
        consume(&consumer).await;
        let tail = format!("unfollow:{}", public_key());
        let mut a = signed("k", "follow", &tail, &tail, 1);
        a.block_time = Some(50);
        capture(&pool, &a, 1, true, false).await;
        capture(&pool, &b, 2, true, true).await;
        let mut barrier = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(736124,99)")
            .execute(&mut *barrier)
            .await
            .unwrap();
        sqlx::raw_sql("CREATE FUNCTION projection_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(736124,99); RETURN NEW; END $$; CREATE TRIGGER projection_barrier BEFORE INSERT ON k_follows FOR EACH ROW EXECUTE FUNCTION projection_barrier();")
            .execute(&pool).await.unwrap();
        let running = consumer.clone();
        let task = tokio::spawn(async move { running.run_one().await });
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=736124 AND objid=99 AND NOT granted)").fetch_one(&pool).await.unwrap();
            if waiting {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "consumer reached actual native SQL barrier"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let producer_pool = pool.clone();
        let changed = b.clone();
        let mut writer = tokio::spawn(async move {
            capture(&producer_pool, &changed, 3, false, false).await;
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut writer)
                .await
                .is_err(),
            "real concurrent revoke must wait on selected canonical row fence"
        );
        barrier.commit().await.unwrap();
        assert!(task.await.unwrap().unwrap());
        writer.await.unwrap();
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_follows").await, 0);
        capture(&pool, &b, 4, true, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_follows").await, 1);
        assert_eq!(
            count(&pool, "notification_probe").await,
            1,
            "revoke/reaccept never repeats an eligible trigger attempt"
        );
    }

    #[tokio::test]
    async fn postgres_shared_signature_selection_uses_index_among_unrelated_native_history() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let value = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 1);
        capture(&pool, &value, 1, true, false).await;
        consume(&consumer).await;
        // Query-plan fixture only; this is not signed projection acceptance.
        sqlx::query("INSERT INTO k_contents(transaction_id,block_time,sender_pubkey,sender_signature,base64_encoded_message,content_type) SELECT decode(lpad(to_hex(i),64,'0'),'hex'),i,decode('01','hex'),decode(lpad(to_hex(i),128,'0'),'hex'),'','post' FROM generate_series(1000,10999) i").execute(&pool).await.unwrap();
        sqlx::query("ANALYZE k_contents")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("ANALYZE social_shared.actions")
            .execute(&pool)
            .await
            .unwrap();
        let subject: String = sqlx::query_scalar("SELECT subject FROM social_shared.actions")
            .fetch_one(&pool)
            .await
            .unwrap();
        let query = format!(
            "EXPLAIN (ANALYZE,FORMAT JSON) {}",
            crate::shared_social::selection_sql("k_contents")
        );
        // Cast the JSON result through a wrapper is not valid for EXPLAIN;
        // SQLx JSON support is not enabled, so use text-format plan rows.
        let query = query.replace("(ANALYZE,FORMAT JSON)", "(ANALYZE)");
        let rows = sqlx::query(&query)
            .bind(subject)
            .fetch_all(&pool)
            .await
            .unwrap();
        use sqlx::Row;
        let plan = rows
            .iter()
            .map(|r| r.get::<String, _>(0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!plan.contains("Seq Scan on k_contents"), "{plan}");
        assert!(
            plan.contains("Index Only Scan") || plan.contains("Index Scan"),
            "{plan}"
        );
        assert_eq!(
            count(&pool, "k_contents").await,
            10001,
            "unrelated history preserved"
        );
    }

    #[tokio::test]
    async fn postgres_shared_long_profile_history_applies_only_effective_native_state() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        sqlx::raw_sql("CREATE TABLE native_attempts(operation text); CREATE FUNCTION native_attempt_capture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO native_attempts VALUES(TG_OP); RETURN COALESCE(NEW,OLD); END $$; CREATE TRIGGER native_attempts AFTER INSERT OR UPDATE OR DELETE ON k_broadcasts FOR EACH ROW EXECUTE FUNCTION native_attempt_capture();")
            .execute(&pool).await.unwrap();
        sqlx::raw_sql("CREATE FUNCTION ledger_attempt_capture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO native_attempts VALUES('LEDGER_UPDATE'); RETURN NEW; END $$; CREATE TRIGGER ledger_attempts AFTER UPDATE ON social_shared.actions FOR EACH ROW EXECUTE FUNCTION ledger_attempt_capture();")
            .execute(&pool).await.unwrap();
        for id in 1..=80u8 {
            let mut value = signed("kchat", "broadcast", "QQ==::YQ==", "QQ==::YQ==", id);
            value.block_time = Some(i64::from(id));
            capture(&pool, &value, 1, true, false).await;
            consume(&consumer).await;
        }
        let inserts: i64 =
            sqlx::query_scalar("SELECT count(*) FROM native_attempts WHERE operation='INSERT'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let deletes: i64 =
            sqlx::query_scalar("SELECT count(*) FROM native_attempts WHERE operation='DELETE'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            inserts, 80,
            "one effective original native apply per job, not triangular history replay"
        );
        assert_eq!(deletes, 79);
        let updates: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM native_attempts WHERE operation='LEDGER_UPDATE'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            updates, 160,
            "snapshot+consumption only for selected/current, not triangular ledger churn"
        );
        let current: String =
            sqlx::query_scalar("SELECT encode(transaction_id,'hex') FROM k_broadcasts")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(current, "50".repeat(32));
    }

    #[tokio::test]
    async fn postgres_shared_signed_poison_and_missing_metadata_do_not_starve_ready_work() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let tail = "aGk=:[\"not-hex\"]";
        let poison = signed("kchat", "post", tail, tail, 1);
        let mut pending = signed("k", "post", "cGVuZGluZw==:[]", "cGVuZGluZw==:[]", 2);
        pending.block_time = None;
        let good = signed("kchat", "post", "Z29vZA==:[]", "Z29vZA==:[]", 3);
        capture(&pool, &poison, 1, true, true).await;
        capture(&pool, &pending, 1, true, false).await;
        capture(&pool, &good, 1, true, true).await;
        consume(&consumer).await;
        consume(&consumer).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_contents").await, 1);
        let status: String =
            sqlx::query_scalar("SELECT status FROM social_shared.actions WHERE transaction_id=$1")
                .bind(hex::decode(&poison.transaction_id).unwrap())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "unsupported_or_invalid");
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 1);
        assert!(
            !consumer.run_one().await.unwrap(),
            "deferred metadata does not busy-loop"
        );
        pending.block_time = Some(12345);
        capture(&pool, &pending, 2, true, false).await;
        sqlx::query("UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp()")
            .execute(&pool)
            .await
            .unwrap();
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_contents").await, 2);
        assert_eq!(count(&pool, "notification_probe").await, 1);
    }

    #[tokio::test]
    async fn postgres_shared_other_subject_job_cannot_consume_pending_acceptance_silently() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let tail = format!("follow:{}", public_key());
        let mut first = signed("kchat", "follow", &tail, &tail, 1);
        first.block_time = Some(100);
        capture(&pool, &first, 1, false, false).await;
        consume(&consumer).await;
        let tail = format!("unfollow:{}", public_key());
        let mut second = signed("k", "follow", &tail, &tail, 2);
        second.block_time = Some(50);
        // Earlier unfollow is queued FIRST. First follow is already in the
        // ledger observed, then becomes accepted before second's job runs.
        capture(&pool, &second, 1, true, false).await;
        capture(&pool, &first, 2, true, true).await;
        consume(&consumer).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_follows").await, 1);
        assert_eq!(count(&pool, "notification_probe").await, 1);
    }

    #[tokio::test]
    async fn postgres_shared_coalesced_metadata_keeps_first_acceptance_notification() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let mut value = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 1);
        capture(&pool, &value, 1, false, false).await;
        capture(&pool, &value, 2, true, true).await;
        value.block_time = Some(22222);
        capture(&pool, &value, 3, true, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "notification_probe").await, 1);
        capture(&pool, &value, 4, true, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "notification_probe").await, 1);
    }

    #[tokio::test]
    async fn postgres_shared_observed_then_first_acceptance_notifies_once() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let value = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 1);
        capture(&pool, &value, 1, false, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_contents").await, 0);
        let consumed: bool =
            sqlx::query_scalar("SELECT notification_consumed FROM social_shared.actions")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(!consumed);
        capture(&pool, &value, 2, true, true).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "notification_probe").await, 1);
        capture(&pool, &value, 3, false, false).await;
        consume(&consumer).await;
        capture(&pool, &value, 4, true, true).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "notification_probe").await, 1);
    }

    #[tokio::test]
    async fn postgres_shared_native_failure_rolls_back_effects_ledger_and_ack_then_retries() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        sqlx::raw_sql("CREATE FUNCTION fail_native() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owning regression'; END $$; CREATE TRIGGER fail_native BEFORE INSERT ON k_contents FOR EACH ROW EXECUTE FUNCTION fail_native();")
            .execute(&pool).await.unwrap();
        let value = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 1);
        capture(&pool, &value, 1, true, true).await;
        assert!(consumer.run_one().await.is_err());
        for table in ["k_contents", "social_shared.actions", "notification_probe"] {
            assert_eq!(count(&pool, table).await, 0);
        }
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 1);
        sqlx::query("DROP TRIGGER fail_native ON k_contents")
            .execute(&pool)
            .await
            .unwrap();
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_contents").await, 1);
        assert_eq!(count(&pool, "notification_probe").await, 1);
        assert!(!consumer.run_one().await.unwrap());
    }

    #[tokio::test]
    async fn postgres_shared_unsupported_raw_is_retained_and_foreign_rebind_cannot_ack() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let mut value = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 1);
        value.payload = Some(hex::encode("kchat:1:tip:unsupported"));
        capture(&pool, &value, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_contents").await, 0);
        let status: String = sqlx::query_scalar("SELECT status FROM social_shared.actions")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "unsupported_or_invalid");
        assert_eq!(count(&pool, "protocol_indexer.canonical").await, 1);
        sqlx::query("INSERT INTO protocol_indexer.consumer_jobs(consumer,transaction_id) SELECT 'native-social-v1',transaction_id FROM protocol_indexer.canonical").execute(&pool).await.unwrap();
        sqlx::query("UPDATE protocol_indexer.config SET network='mainnet'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(consumer.run_one().await.is_err());
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 1);
    }
}
