//! Real SDK cryptography + native SQL, not a replacement protocol parser.
use crate::database::Transaction;
use crate::k_protocol::{KActionType, KProtocolProcessor};
use kaspa_wallet_core::message::{PersonalMessage, SignMessageOptions, sign_message};
use secp256k1::{Keypair, Secp256k1};
use sqlx::postgres::PgPoolOptions;

fn processor() -> (KProtocolProcessor, sqlx::PgPool) {
    // Pure tests never acquire this lazy pool.
    let pool = PgPoolOptions::new().max_connections(1)
        .connect_lazy("postgres://unused@127.0.0.1/unused").unwrap();
    (KProtocolProcessor::new(pool.clone()), pool)
}

fn public_key() -> String {
    let pair = Keypair::from_seckey_slice(&Secp256k1::new(), &[3; 32]).unwrap();
    pair.x_only_public_key().0.to_string()
}

fn signed(root: &str, action: &str, tail: &str, message: &str, id: u8) -> Transaction {
    // Public, deterministic test key. Never a wallet fixture or broadcaster.
    let signature = sign_message(&PersonalMessage(message), &[3; 32],
        &SignMessageOptions { no_aux_rand: true }).unwrap();
    Transaction { transaction_id: format!("{id:02x}").repeat(32), block_time: Some(12345),
        payload: Some(hex::encode(format!("{root}:1:{action}:{}:{}:{tail}", public_key(), hex::encode(signature)))) }
}

#[tokio::test]
async fn pure_both_protocol_roots_preserve_all_existing_signed_actions_without_filters() {
    let (processor, pool) = processor();
    let target = format!("02{}", public_key());
    let parent = "ab".repeat(32);
    let message = "4oGgaGk="; // Exact signed marker+hi; no filter or normalization.
    let mentions = format!(r#"["{target}"]"#);
    let cases = vec![
        ("post", format!("{message}:{mentions}"), format!("{message}:{mentions}")),
        ("reply", format!("{parent}:{message}:{mentions}"), format!("{parent}:{message}:{mentions}")),
        ("quote", format!("{parent}:{message}:{target}"), format!("{parent}:{message}:{target}")),
        ("broadcast", format!("YWxpY2U=::{message}"), format!("YWxpY2U=::{message}")),
        ("vote", format!("{parent}:upvote:{target}"), format!("{parent}:upvote:{target}")),
        ("follow", format!("follow:{target}"), format!("follow:{target}")),
        ("follow", format!("unfollow:{target}"), format!("unfollow:{target}")),
        ("block", format!("block:{target}"), format!("block:{target}")),
        ("block", format!("unblock:{target}"), format!("unblock:{target}")),
    ];
    for root in ["k", "kchat"] {
        for (index, (kind, tail, signature_message)) in cases.iter().enumerate() {
            let transaction = signed(root, kind, tail, signature_message, (index + 1) as u8);
            let verified = processor.verify_transaction(&transaction).expect("valid SDK signature");
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
    let transaction = Transaction { transaction_id: row["transaction_id"].as_str().unwrap().to_owned(),
        payload: Some(row["payload"].as_str().unwrap().to_owned()), block_time: row["block_time"].as_i64() };
    assert_eq!(processor.verify_transaction(&transaction).unwrap().protocol(), "k");
    // This is a synthetic compatibility vector, NOT an additional chain transaction.
    let raw = String::from_utf8(hex::decode(transaction.payload.as_ref().unwrap()).unwrap()).unwrap();
    let mut counterpart = transaction.clone();
    counterpart.payload = Some(hex::encode(raw.replacen("k:1:", "kchat:1:", 1)));
    assert_eq!(processor.verify_transaction(&counterpart).unwrap().protocol(), "kchat");
}

#[tokio::test]
async fn pure_invalid_versions_signatures_and_key_forms_never_become_verified() {
    let (processor, pool) = processor();
    let transaction = signed("k", "post", "aGk=:[]", "aGk=:[]", 1);
    let raw = String::from_utf8(hex::decode(transaction.payload.as_ref().unwrap()).unwrap()).unwrap();
    for root in ["k:2:", "kchat:2:", "ciph_msg:1:", "unknown:1:"] {
        let mut bad = transaction.clone(); bad.payload = Some(hex::encode(raw.replacen("k:1:", root, 1)));
        assert!(processor.verify_transaction(&bad).is_none());
    }
    let fields: Vec<_> = raw.split(':').collect();
    for replacement in [format!("04{}", public_key()), "00".repeat(32)] {
        let mut bad = transaction.clone(); bad.payload = Some(hex::encode(raw.replacen(fields[3], &replacement, 1)));
        assert!(processor.verify_transaction(&bad).is_none());
    }
    for replacement in ["00".repeat(64), format!("{}00", fields[4])] {
        let mut bad = transaction.clone(); bad.payload = Some(hex::encode(raw.replacen(fields[4], &replacement, 1)));
        assert!(processor.verify_transaction(&bad).is_none());
    }
    for prefix in ["02", "03"] {
        let mut compressed = transaction.clone();
        compressed.payload = Some(hex::encode(raw.replacen(fields[3], &format!("{prefix}{}", public_key()), 1)));
        assert!(processor.verify_transaction(&compressed).is_some());
    }
    assert_eq!(pool.size(), 0);
}

#[tokio::test]
async fn pure_verified_envelope_owns_reviewed_transaction_not_latest_mutable_input() {
    let (processor, _) = processor();
    let mut transaction = signed("kchat", "post", "aGk=:[]", "aGk=:[]", 1);
    let verified = processor.verify_transaction(&transaction).unwrap();
    transaction.payload = Some("00".into()); transaction.transaction_id = "ff".repeat(32); transaction.block_time = Some(99999);
    assert_ne!(verified.transaction().payload, transaction.payload);
    assert_eq!(verified.transaction().transaction_id, "01".repeat(32));
    assert_eq!(verified.transaction().block_time, Some(12345));
}

#[cfg(feature = "shared-postgres-contract")]
async fn database() -> sqlx::PgPool {
    let dsn = std::env::var("SOCIAL_TEST_DATABASE_URL").expect("explicit disposable PostgreSQL DSN required");
    let pool = PgPoolOptions::new().max_connections(3).connect(&dsn).await.unwrap();
    let name: String = sqlx::query_scalar("SELECT current_database()").fetch_one(&pool).await.unwrap();
    assert!(name.starts_with("protocol_test_social"), "only owning disposable database allowed");
    sqlx::raw_sql("DROP SCHEMA public CASCADE; CREATE SCHEMA public; CREATE TABLE transactions(transaction_id bytea PRIMARY KEY,payload bytea,block_time bigint)").execute(&pool).await.unwrap();
    crate::database::KDbClient::new(pool.clone()).create_schema(true).await.unwrap();
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
    sqlx::query("SELECT set_config('kasanova.social_notify_allowed','false',true)").execute(&mut *owner).await.unwrap();
    processor.apply_verified_in(&mut owner, &verified).await.unwrap();
    let inside: i64 = sqlx::query_scalar("SELECT count(*) FROM k_contents").fetch_one(&mut *owner).await.unwrap();
    let outside: i64 = sqlx::query_scalar("SELECT count(*) FROM k_contents").fetch_one(&pool).await.unwrap();
    assert_eq!((inside, outside), (1, 0));
    owner.rollback().await.unwrap();
    for table in ["k_contents", "k_mentions", "k_hashtags"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}")).fetch_one(&pool).await.unwrap();
        assert_eq!(count, 0, "{table} rolled back with owning transaction");
    }
    processor.process_k_transaction(&transaction).await.unwrap();
    for table in ["k_contents", "k_mentions", "k_hashtags"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}")).fetch_one(&pool).await.unwrap();
        assert_eq!(count, 1, "legacy entrypoint keeps native {table} projection");
    }
    // Same signed action through the other root retains native signature dedup.
    let counterpart = signed("k", "post", &tail, &tail, 2);
    processor.process_k_transaction(&counterpart).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM k_contents").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 1);
}
