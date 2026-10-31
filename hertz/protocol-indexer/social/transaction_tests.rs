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

fn fixture_transaction(value: &serde_json::Value) -> Transaction {
    Transaction {
        transaction_id: value["transaction_id"].as_str().unwrap().into(),
        payload: value["payload"].as_str().map(str::to_owned),
        block_time: value["block_time"].as_i64(),
    }
}

fn synthetic_rpc(
    payload: &str,
    outputs: serde_json::Value,
    nonce: u8,
) -> (String, serde_json::Value) {
    let fixture: serde_json::Value = serde_json::from_str(include_str!("real_tip.json")).unwrap();
    let mut rpc = fixture["receipt"]["rpc_transaction"].clone();
    rpc["payload"] = payload.into();
    rpc["outputs"] = outputs;
    rpc["lockTime"] = u64::from(nonce).into();
    let id = crate::transaction_evidence::fixture_transaction_id(&rpc).unwrap();
    rpc["verboseData"]["transactionId"] = id.clone().into();
    (id, rpc)
}


// SDK-coherent isolated transaction bodies, not signatures/UTXO/chain proof.
fn room_vector(root: &str, channel: &str, content: &str, nonce: u8, forged: bool) -> serde_json::Value {
    use serde_json::json;
    let input_script = hex::encode(crate::tips::p2pk_script(&crate::tips::xonly_bytes(&public_key()).unwrap()));
    let output_script = if forged { hex::encode(crate::tips::p2pk_script(&[9u8;32])) } else { input_script.clone() };
    let (previous_id, previous_rpc) = synthetic_rpc("", json!([{"amount":50000000,"scriptPublicKey":{"version":0,"scriptPublicKey":input_script}}]), nonce);
    let payload = hex::encode(format!("{root}:1:bcast:{channel}:{content}"));
    let (_, mut rpc) = synthetic_rpc(&payload,json!([{"amount":40000000,"scriptPublicKey":{"version":0,"scriptPublicKey":output_script}}]), nonce);
    let mut input = rpc["inputs"][0].clone();
    input["previousOutpoint"] = json!({"transactionId":previous_id,"index":0});
    rpc["inputs"] = json!([input]);
    let id = crate::transaction_evidence::fixture_transaction_id(&rpc).unwrap();
    rpc["verboseData"]["transactionId"] = json!(id);
    let signature = rpc["inputs"][0]["signatureScript"].as_str().unwrap();
    json!({"transaction":{"transaction_id":id,"payload":payload,"block_time":1700000000000i64},
      "body":{"transaction_id":format!("\\x{id}"),"payload":format!("\\x{payload}"),"block_time":1700000000000i64,
        "inputs":[{"index":0,"previous_outpoint_hash":format!("\\x{previous_id}"),"previous_outpoint_index":0,"signature_script":format!("\\x{signature}")}],
        "outputs":[{"index":0,"amount":40000000,"script_public_key":format!("\\x{output_script}")}]},
      "receipt":{"schema_version":1,"network":"testnet-10","transaction_id":id,"payload":payload,
        "archive_transaction":{"transaction_id":id,"payload":payload,"outputs":[{"index":0,"amount":40000000,"script_public_key":output_script}]},
        "rpc_transaction":rpc,"input0":{"script":input_script,"amount":50000000,
          "previous_transaction":{"schema_version":1,"network":"testnet-10","transaction_id":previous_id,"rpc_transaction":previous_rpc}}}})
}

#[tokio::test]
async fn pure_rooms_keep_original_wire_content_and_normalize_only_channel() {
    use crate::rooms::{RoomEnvelope, Verdict, judge};
    for root in ["kchat","ciph_msg"] {
        let v=room_vector(root,"  TrADING  ","a:b:日本語",91,false);
        assert!(RoomEnvelope::parse(&fixture_transaction(&v["transaction"])).is_some());
    }
    for (channel,content) in [(" ".to_string(),"x"),("x".repeat(37),"x"),("İ".repeat(19),"x"),("good".to_string(),"x\0y")] {
        let v=room_vector("kchat",&channel,content,92,false);
        assert!(RoomEnvelope::parse(&fixture_transaction(&v["transaction"])).is_none());
    }
    let mut v=fixture_transaction(&room_vector("kchat","x","y",93,false)["transaction"]);
    v.payload=Some(hex::encode(b"kchat:1:bcast:x:\xff"));
    assert!(RoomEnvelope::parse(&v).is_none());
    assert_eq!(judge(Some(" A "),Some("a")),Verdict::Verified("a".into()));
    assert!(matches!(judge(Some("a"),Some("b")),Verdict::Forged{..}));
    assert_eq!(judge(None,Some("a")),Verdict::Unknown);
}

#[tokio::test]
async fn pure_room_sender_requires_sdk_current_prior_id_and_exact_canonical_outpoint() {
    use crate::rooms::{sender,Verdict};
    use serde_json::json;
    let v=room_vector("kchat","all","body:verbatim",94,false);
    let id=hex::decode(v["transaction"]["transaction_id"].as_str().unwrap()).unwrap();
    let Verdict::Verified(address)=sender("testnet-10",&id,&v["body"],&v["receipt"]).unwrap() else {panic!("self send")};
    assert!(address.starts_with("kaspatest:"));
    // Complete proof intentionally has no verbose/address metadata anywhere.
    assert!(v["receipt"]["input0"]["address"].is_null());
    for field in ["current-version","previous-version","previous-id","proof-amount","claim","canonical-outpoint","canonical-signature","canonical-amount"] {
        let mut changed=v.clone();
        match field {
            "current-version"=>changed["receipt"]["rpc_transaction"]["outputs"][0]["scriptPublicKey"]["version"]=json!(1),
            "previous-version"=>changed["receipt"]["input0"]["previous_transaction"]["rpc_transaction"]["outputs"][0]["scriptPublicKey"]["version"]=json!(1),
            "previous-id"=>changed["receipt"]["input0"]["previous_transaction"]["transaction_id"]=json!("ab".repeat(32)),
            "proof-amount"=>changed["receipt"]["input0"]["amount"]=json!(1),
            "claim"=>changed["receipt"]["input0"]["address"]=json!("kaspatest:spoofed"),
            "canonical-outpoint"=>changed["body"]["inputs"][0]["previous_outpoint_index"]=json!(1),
            "canonical-signature"=>changed["body"]["inputs"][0]["signature_script"]=json!("\\x00"),
            _=>changed["body"]["inputs"][0]["previous_outpoint_amount"]=json!(1),
        }
        assert!(sender("testnet-10",&id,&changed["body"],&changed["receipt"]).is_err(),"{field}");
    }
    let forged=room_vector("kchat","all","forged",95,true);
    let id=hex::decode(forged["transaction"]["transaction_id"].as_str().unwrap()).unwrap();
    assert!(matches!(sender("testnet-10",&id,&forged["body"],&forged["receipt"]).unwrap(),Verdict::Forged{..}));
    assert!(sender("mainnet",&id,&forged["body"],&forged["receipt"]).is_err());
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

#[test]
fn pure_kaposts_poll_shape_unicode_and_chain_time_boundaries() {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let make = |options: &[&str], closes_at_ms| crate::kaposts::KPoll {
        sender_pubkey: public_key(),
        sender_signature: String::new(),
        base64_encoded_question: STANDARD.encode("question"),
        options_b64_csv: options
            .iter()
            .map(|s| STANDARD.encode(s))
            .collect::<Vec<_>>()
            .join(","),
        closes_at_ms,
        mentioned_pubkeys: vec![],
        is_kchat: true,
    };
    let valid = crate::kaposts_projection::valid_poll;
    assert!(valid(&make(&["yes", "no"], 1001), 1000));
    assert!(valid(&make(&["a", "b", "c", "d"], 604801000), 1000));
    assert!(!valid(&make(&["yes", "no"], 1000), 1000));
    assert!(!valid(&make(&["yes", "no"], 604801001), 1000));
    for options in [
        &["a"][..],
        &["a", "b", "c", "d", "e"],
        &["same", "same"],
        &["", "no"],
    ] {
        assert!(!valid(&make(options, 2000), 1000));
    }
    assert!(valid(&make(&[&"é".repeat(40), "no"], 2000), 1000));
    assert!(!valid(&make(&[&"é".repeat(41), "no"], 2000), 1000));
    assert!(!valid(&make(&["yes", "no"], i64::MAX), i64::MAX - 1));
    let mut malformed = make(&["yes", "no"], 2000);
    malformed.base64_encoded_question = "/w==".into();
    assert!(!valid(&malformed, 1000));
    malformed = make(&["yes", "no"], 2000);
    malformed.options_b64_csv = "eWVz,/w==".into();
    assert!(!valid(&malformed, 1000));
}

#[tokio::test]
async fn pure_kaposts_all_five_upstream_forms_use_original_sdk_and_own_reviewed_input() {
    use crate::kaposts::KaPostsAction;
    let (processor, pool) = processor();
    let target = "ab".repeat(32);
    let cases = [
        ("unquote", target.clone(), target.clone()),
        ("delete", target.clone(), format!("delete:{target}")),
        (
            "edit",
            format!("{target}:aGk=:[]"),
            format!("edit:{target}:aGk=:[]"),
        ),
        (
            "poll",
            "aGk=:eWVz,bm8=:604800000:[]".into(),
            "poll:aGk=:eWVz,bm8=:604800000:[]".into(),
        ),
        (
            "pollvote",
            format!("{target}:1"),
            format!("pollvote:{target}:1"),
        ),
    ];
    for (kind, tail, message) in cases {
        let mut input = signed("kchat", kind, &tail, &message, 11);
        let original = input.payload.clone();
        let verified = processor
            .verify_kaposts_transaction(&input)
            .expect("upstream signing form verifies with original Kaspa SDK");
        assert_eq!(verified.action().signing_string().unwrap(), message);
        assert!(matches!(
            (kind, verified.action()),
            ("unquote", KaPostsAction::Unquote(_))
                | ("delete", KaPostsAction::Delete(_))
                | ("edit", KaPostsAction::Edit(_))
                | ("poll", KaPostsAction::Poll(_))
                | ("pollvote", KaPostsAction::PollVote(_))
        ));
        input.payload = Some("00".into());
        input.transaction_id = "ee".repeat(32);
        input.block_time = Some(999);
        assert_eq!(verified.transaction().payload, original);
        assert_eq!(verified.transaction().transaction_id, "0b".repeat(32));
        assert_eq!(verified.transaction().block_time, Some(12345));
        let mut compressed = verified.transaction().clone();
        let wire =
            String::from_utf8(hex::decode(compressed.payload.as_ref().unwrap()).unwrap()).unwrap();
        let mut fields: Vec<String> = wire.split(':').map(str::to_owned).collect();
        fields[3] = format!("02{}", public_key());
        compressed.payload = Some(hex::encode(fields.join(":")));
        assert!(
            processor.verify_kaposts_transaction(&compressed).is_some(),
            "compressed signer {kind}"
        );
    }
    pool.close().await;
}

#[tokio::test]
async fn pure_kaposts_cross_action_changed_content_and_foreign_root_never_verify() {
    let (processor, pool) = processor();
    let id = "ab".repeat(32);
    let cases = [
        ("unquote", id.clone(), format!("unquote:{id}")),
        ("delete", id.clone(), id.clone()),
        ("edit", format!("{id}:aGk=:[]"), format!("{id}:aGk=:[]")),
        (
            "poll",
            "aGk=:eWVz,bm8=:100000:[]".into(),
            "aGk=:eWVz,bm8=:100000:[]".into(),
        ),
        ("pollvote", format!("{id}:1"), format!("pollvote:{id}:0")),
    ];
    for (kind, tail, wrong_message) in cases {
        assert!(
            processor
                .verify_kaposts_transaction(&signed("kchat", kind, &tail, &wrong_message, 12))
                .is_none(),
            "wrong signed form {kind}"
        );
    }
    for prefix in ["k:1:", "kchat:2:", "ciph_msg:1:"] {
        let mut value = signed("kchat", "delete", &id, &format!("delete:{id}"), 12);
        let wire =
            String::from_utf8(hex::decode(value.payload.as_ref().unwrap()).unwrap()).unwrap();
        value.payload = Some(hex::encode(wire.replacen("kchat:1:", prefix, 1)));
        assert!(processor.verify_kaposts_transaction(&value).is_none());
    }
    let mut value = signed("kchat", "delete", &id, &format!("delete:{id}"), 12);
    let wire = String::from_utf8(hex::decode(value.payload.as_ref().unwrap()).unwrap()).unwrap();
    value.payload = Some(hex::encode(wire.replacen(&id, &"cd".repeat(32), 1)));
    assert!(processor.verify_kaposts_transaction(&value).is_none());
    pool.close().await;
}

#[test]
fn pure_kaposts_optional_mentions_and_numeric_fields_match_upstream_parser() {
    use crate::kaposts::parse_parts;
    for (wire, expected) in [
        ("edit:key:sig:abcd:aGk=", "edit:abcd:aGk=:[]"),
        ("edit:key:sig:abcd:aGk=:not-json", "edit:abcd:aGk=:[]"),
        (
            "edit:key:sig:abcd:aGk=:[\"02aa\"]",
            "edit:abcd:aGk=:[\"02aa\"]",
        ),
        (
            "poll:key:sig:aGk=:eWVz,bm8=:001000",
            "poll:aGk=:eWVz,bm8=:1000:[]",
        ),
        ("pollvote:key:sig:abcd:-1", "pollvote:abcd:-1"),
        ("unquote:key:sig:abcd:extra", "abcd"),
    ] {
        let parsed = parse_parts(&wire.split(':').collect::<Vec<_>>()).unwrap();
        assert_eq!(parsed.signing_string().unwrap(), expected);
    }
    for wire in [
        "",
        "delete:key:sig",
        "edit:key:sig:id",
        "poll:key:sig:question:options:9223372036854775808",
        "pollvote:key:sig:id:2147483648",
        "tip:key:sig:id:author:00",
    ] {
        assert!(
            parse_parts(&wire.split(':').collect::<Vec<_>>()).is_err(),
            "{wire}"
        );
    }
}

#[tokio::test]
async fn pure_kaposts_signed_bad_storage_hex_is_rejected_before_materialization() {
    let (processor, pool) = processor();
    for (kind, tail, message) in [
        ("delete", "not-hex", "delete:not-hex"),
        ("unquote", "not-hex", "not-hex"),
        ("edit", "not-hex:aGk=:[]", "edit:not-hex:aGk=:[]"),
        ("pollvote", "not-hex:0", "pollvote:not-hex:0"),
        (
            "poll",
            "aGk=:eWVz,bm8=:10000:[\"not-hex\"]",
            "poll:aGk=:eWVz,bm8=:10000:[\"not-hex\"]",
        ),
    ] {
        assert!(
            processor
                .verify_kaposts_transaction(&signed("kchat", kind, tail, message, 13))
                .is_none()
        );
    }
    pool.close().await;
}

#[tokio::test]
async fn pure_kaposts_malformed_transaction_and_signer_cannot_create_verified_envelope() {
    let (processor, pool) = processor();
    let target = "ab".repeat(32);
    let base = signed("kchat", "delete", &target, &format!("delete:{target}"), 14);
    for bad_id in ["not-hex".to_string(), "ab".repeat(31)] {
        let mut value = base.clone();
        value.transaction_id = bad_id;
        assert!(processor.verify_kaposts_transaction(&value).is_none());
    }
    for payload in [None, Some("ff".into()), Some("not-hex".into())] {
        let mut value = base.clone();
        value.payload = payload;
        assert!(processor.verify_kaposts_transaction(&value).is_none());
    }
    let wire = String::from_utf8(hex::decode(base.payload.as_ref().unwrap()).unwrap()).unwrap();
    let mut fields: Vec<String> = wire.split(':').map(str::to_owned).collect();
    fields[3] = format!("04{}", public_key());
    let mut value = base;
    value.payload = Some(hex::encode(fields.join(":")));
    assert!(processor.verify_kaposts_transaction(&value).is_none());
    pool.close().await;
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
            CREATE TABLE protocol_indexer.pending(transaction_id bytea PRIMARY KEY,acceptance_changed boolean NOT NULL DEFAULT false,accepting_block bytea);
            CREATE TABLE protocol_indexer.evidence(transaction_id bytea PRIMARY KEY REFERENCES protocol_indexer.canonical,receipt jsonb);
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
    #[tokio::test]
    async fn postgres_tip_upgrade_refuses_unowned_nonempty_tip_history_without_writes() {
        let pool = setup().await;
        sqlx::raw_sql("CREATE TABLE k_tips(legacy_marker bigint); INSERT INTO k_tips VALUES(42)")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
                .await
                .is_err()
        );
        let marker: i64 = sqlx::query_scalar("SELECT legacy_marker FROM k_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(marker, 42);
        let untouched: bool = sqlx::query_scalar("SELECT to_regnamespace('social_shared') IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            untouched,
            "failed adoption rolls back new metadata/schema, retaining legacy rows"
        );
    }

    fn at(mut value: Transaction, time: i64) -> Transaction {
        value.block_time = Some(time);
        value
    }

    fn signed_as(key: u8, action: &str, tail: &str, message: &str, id: u8) -> Transaction {
        let bytes = [key; 32];
        let pair = Keypair::from_seckey_slice(&Secp256k1::new(), &bytes).unwrap();
        let signature = sign_message(
            &PersonalMessage(message),
            &bytes,
            &SignMessageOptions { no_aux_rand: true },
        )
        .unwrap();
        Transaction {
            transaction_id: format!("{id:02x}").repeat(32),
            payload: Some(hex::encode(format!(
                "kchat:1:{action}:{}:{}:{tail}",
                pair.x_only_public_key().0,
                hex::encode(signature)
            ))),
            block_time: Some(1000),
        }
    }

    #[tokio::test]
    async fn postgres_kaposts_third_edit_supplier_is_quiet_after_base_rebuild() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE social_shared.decoder_epochs SET since_ms=0 WHERE kind='kchat:1:extensions'",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::raw_sql("CREATE FUNCTION mention_probe_capture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('kasanova.social_notify_allowed',true)='true' THEN INSERT INTO notification_probe(transaction_id) VALUES(NEW.content_id); END IF; RETURN NEW; END $$; CREATE TRIGGER probe_edit_mentions AFTER INSERT ON k_mentions FOR EACH ROW EXECUTE FUNCTION mention_probe_capture()")
            .execute(&pool).await.unwrap();
        let base = at(signed("k", "post", "aGk=:[]", "aGk=:[]", 71), 1000);
        capture(&pool, &base, 1, true, false).await;
        consume(&consumer).await;
        let alias = at(signed("kchat", "post", "aGk=:[]", "aGk=:[]", 72), 1000);
        capture(&pool, &alias, 1, true, false).await;
        let mut hold = pool.begin().await.unwrap();
        sqlx::query("SELECT transaction_id FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1' AND transaction_id=$1 FOR UPDATE").bind(hex::decode(&alias.transaction_id).unwrap()).fetch_one(&mut *hold).await.unwrap();
        let alice = format!("02{}", "44".repeat(32));
        let bob = format!("02{}", "55".repeat(32));
        let carol = format!("02{}", "66".repeat(32));
        let early_tail = format!("{}:b25l:[\"{alice}\",\"{bob}\"]", alias.transaction_id);
        let early = at(
            signed(
                "kchat",
                "edit",
                &early_tail,
                &format!("edit:{early_tail}"),
                73,
            ),
            1100,
        );
        let head_tail = format!("{}:dHdv:[\"{carol}\"]", alias.transaction_id);
        let head = at(
            signed(
                "kchat",
                "edit",
                &head_tail,
                &format!("edit:{head_tail}"),
                74,
            ),
            1400,
        );
        for value in [&early, &head] {
            capture(&pool, value, 1, true, true).await;
            consume(&consumer).await;
        }
        let current_tail = format!("{}:dGhyZWU=:[\"{alice}\"]", base.transaction_id);
        let current = at(
            signed(
                "kchat",
                "edit",
                &current_tail,
                &format!("edit:{current_tail}"),
                75,
            ),
            1300,
        );
        capture(&pool, &current, 1, true, true).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "notification_probe").await, 2);
        let consumed: bool = sqlx::query_scalar(
            "SELECT notification_consumed FROM social_shared.actions WHERE transaction_id=$1",
        )
        .bind(hex::decode(&early.transaction_id).unwrap())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            !consumed,
            "early supplier's other recipient has not been evaluated"
        );
        for revision in [2, 3] {
            capture(&pool, &base, revision, true, false).await;
            consume(&consumer).await;
            assert_eq!(
                count(&pool, "notification_probe").await,
                3,
                "Alice and Carol restore quietly; previously unevaluated Bob notifies once"
            );
        }
        hold.rollback().await.unwrap();
        ready_all(&pool).await;
        drain(&consumer).await;
        assert_eq!(count(&pool, "notification_probe").await, 3);
        assert_eq!(count(&pool, "k_mentions").await, 3);
    }

    #[tokio::test]
    async fn postgres_kaposts_vote_delete_revoke_serializes_before_vote_acknowledgement() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let post = at(signed("k", "post", "aGk=:[]", "aGk=:[]", 81), 1000);
        capture(&pool, &post, 1, true, false).await;
        consume(&consumer).await;
        let deletion = at(
            signed(
                "kchat",
                "delete",
                &post.transaction_id,
                &format!("delete:{}", post.transaction_id),
                82,
            ),
            1200,
        );
        capture(&pool, &deletion, 1, true, false).await;
        consume(&consumer).await;
        let tail = format!("{}:upvote:{}", post.transaction_id, public_key());
        let vote = at(signed("k", "vote", &tail, &tail, 83), 1300);
        capture(&pool, &vote, 1, true, false).await;
        let mut barrier = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(736124,98)")
            .execute(&mut *barrier)
            .await
            .unwrap();
        sqlx::raw_sql(&format!("CREATE FUNCTION vote_ack_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.transaction_id=decode('{}','hex') AND NEW.notification_consumed AND NOT OLD.notification_consumed THEN PERFORM pg_advisory_xact_lock(736124,98); END IF; RETURN NEW; END $$; CREATE TRIGGER vote_ack_barrier BEFORE UPDATE ON social_shared.actions FOR EACH ROW EXECUTE FUNCTION vote_ack_barrier()", vote.transaction_id)).execute(&pool).await.unwrap();
        let running = consumer.clone();
        let task = tokio::spawn(async move { running.run_one().await });
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=736124 AND objid=98 AND NOT granted)").fetch_one(&pool).await.unwrap();
            if waiting {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "vote reached actual post-decision acknowledgement barrier"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        capture(&pool, &deletion, 2, false, false).await;
        tokio::time::timeout(std::time::Duration::from_secs(2), consumer.run_one())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            count(&pool, "k_contents").await,
            0,
            "revoke must defer behind vote's content decision lock"
        );
        let retained: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM protocol_indexer.consumer_jobs WHERE transaction_id=$1 AND last_error='subject_revision_busy')").bind(hex::decode(&deletion.transaction_id).unwrap()).fetch_one(&pool).await.unwrap();
        assert!(
            retained,
            "revoke retry survives until old vote acknowledges"
        );
        let unrelated = at(signed("k", "post", "b3RoZXI=:[]", "b3RoZXI=:[]", 84), 1500);
        capture(&pool, &unrelated, 1, true, false).await;
        consume(&consumer).await;
        assert!(
            native_content(&pool, &unrelated.transaction_id)
                .await
                .is_some()
        );
        barrier.commit().await.unwrap();
        assert!(task.await.unwrap().unwrap());
        ready_all(&pool).await;
        drain(&consumer).await;
        assert!(native_content(&pool, &post.transaction_id).await.is_some());
        assert_eq!(
            count(&pool, "k_votes").await,
            1,
            "revocation wakes the vote after its old job is gone"
        );
    }

    #[tokio::test]
    async fn postgres_kaposts_long_edit_history_does_not_rewrite_prior_native_or_ledger_rows() {
        use base64::Engine as _;
        use sqlx::Row;
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let post = at(signed("k", "post", "aGk=:[]", "aGk=:[]", 90), 1000);
        capture(&pool, &post, 1, true, false).await;
        consume(&consumer).await;
        let original = native_content(&pool, &post.transaction_id).await.unwrap();
        sqlx::raw_sql("CREATE TABLE write_probe(relation text,operation text); CREATE FUNCTION write_probe_capture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO write_probe VALUES(TG_TABLE_NAME,TG_OP); RETURN COALESCE(NEW,OLD); END $$; CREATE TRIGGER content_write_probe AFTER INSERT OR UPDATE OR DELETE ON k_contents FOR EACH ROW EXECUTE FUNCTION write_probe_capture(); CREATE TRIGGER edit_write_probe AFTER INSERT OR UPDATE OR DELETE ON k_edits FOR EACH ROW EXECUTE FUNCTION write_probe_capture(); CREATE TRIGGER ledger_write_probe AFTER UPDATE ON social_shared.actions FOR EACH ROW EXECUTE FUNCTION write_probe_capture()")
            .execute(&pool).await.unwrap();
        for id in 100..180u8 {
            let message = base64::engine::general_purpose::STANDARD.encode(format!("edit {id}"));
            let tail = format!("{}:{message}:[]", post.transaction_id);
            let edit = at(
                signed("kchat", "edit", &tail, &format!("edit:{tail}"), id),
                1000 + i64::from(id),
            );
            capture(&pool, &edit, 1, true, false).await;
            consume(&consumer).await;
        }
        for (relation, operation, expected) in [
            ("k_contents", "UPDATE", 80),
            ("k_edits", "INSERT", 80),
            ("actions", "UPDATE", 160),
        ] {
            let actual: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM write_probe WHERE relation=$1 AND operation=$2",
            )
            .bind(relation)
            .bind(operation)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                actual, expected,
                "{relation} {operation}: linear effective-state writes, no historical replay"
            );
        }
        assert_eq!(count(&pool, "write_probe").await, 320);
        assert_eq!(
            native_content(&pool, &post.transaction_id).await.unwrap().0,
            original.0
        );
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 0);
        // Plan-only synthetic history: mixed unrelated, invalid and revoked
        // rows. These are never submitted to a decoder or claimed chain facts.
        sqlx::query("WITH ids AS MATERIALIZED (SELECT n,decode(md5('plan-a-'||n)||md5('plan-b-'||n),'hex') AS id FROM generate_series(1,10000) n), canonical AS (INSERT INTO protocol_indexer.canonical SELECT i.id,1,c.source_transaction,CASE WHEN i.n%3=0 THEN NULL ELSE c.accepting_block END FROM ids i CROSS JOIN protocol_indexer.canonical c WHERE c.transaction_id=$1 RETURNING transaction_id), actions AS (INSERT INTO social_shared.actions(transaction_id,revision,subject,native_table,state_action,source_payload,original_time,status,notification_consumed) SELECT c.transaction_id,1,'plan-only','kaposts','signed','',1100,'verified',true FROM canonical c RETURNING transaction_id), template AS (SELECT * FROM social_shared.extensions WHERE kind='edit' LIMIT 1) INSERT INTO social_shared.extensions SELECT i.id,1,1100,e.kind,e.sender,e.signature,e.target,e.message,e.mentions,e.options,e.closes_at,e.option_index,(i.n%3<>0),CASE WHEN i.n%5=0 THEN e.target_signature ELSE i.id END,e.target_author,e.target_kind,e.target_time FROM ids i JOIN actions a ON a.transaction_id=i.id CROSS JOIN template e")
            .bind(hex::decode(&post.transaction_id).unwrap()).execute(&pool).await.unwrap();
        sqlx::raw_sql("ANALYZE social_shared.extensions; ANALYZE social_shared.actions; ANALYZE protocol_indexer.canonical")
            .execute(&pool)
            .await
            .unwrap();
        let signature: Vec<u8> = sqlx::query_scalar(
            "SELECT signature FROM social_shared.content_keys WHERE transaction_id=$1",
        )
        .bind(hex::decode(&post.transaction_id).unwrap())
        .fetch_one(&pool)
        .await
        .unwrap();
        let rows = sqlx::query(&format!(
            "EXPLAIN (ANALYZE,BUFFERS) {}",
            crate::kaposts_projection::edit_selection_sql()
        ))
        .bind(signature)
        .fetch_all(&pool)
        .await
        .unwrap();
        let plan = rows
            .iter()
            .map(|row| row.get::<String, _>(0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(plan.contains("social_shared_edit_effective"), "{plan}");
        println!("KaPosts effective edit production query plan:\n{plan}");
        assert!(
            !plan.contains("Sort"),
            "effective-head lookup must use ordered index: {plan}"
        );
    }

    #[tokio::test]
    async fn postgres_kaposts_two_pending_edits_evaluate_effective_recipients_before_consumption() {
        for newer_pending in [true, false] {
            let pool = setup().await;
            let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
                .await
                .unwrap();
            sqlx::query("UPDATE social_shared.decoder_epochs SET since_ms=0 WHERE kind='kchat:1:extensions'").execute(&pool).await.unwrap();
            sqlx::raw_sql("CREATE FUNCTION mention_probe_capture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('kasanova.social_notify_allowed',true)='true' THEN INSERT INTO notification_probe(transaction_id) VALUES(NEW.content_id); END IF; RETURN NEW; END $$; CREATE TRIGGER probe_edit_mentions AFTER INSERT ON k_mentions FOR EACH ROW EXECUTE FUNCTION mention_probe_capture()")
                .execute(&pool).await.unwrap();
            let base = at(signed("k", "post", "aGk=:[]", "aGk=:[]", 61), 1000);
            capture(&pool, &base, 1, true, false).await;
            consume(&consumer).await;
            let alias = at(signed("kchat", "post", "aGk=:[]", "aGk=:[]", 62), 1000);
            capture(&pool, &alias, 1, true, false).await;
            let mut hold = pool.begin().await.unwrap();
            sqlx::query("SELECT transaction_id FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1' AND transaction_id=$1 FOR UPDATE").bind(hex::decode(&alias.transaction_id).unwrap()).fetch_one(&mut *hold).await.unwrap();
            let alice = format!("02{}", "44".repeat(32));
            let bob = format!("02{}", "55".repeat(32));
            let pending_time = if newer_pending { 1300 } else { 1200 };
            let current_time = if newer_pending { 1200 } else { 1300 };
            let pending_tail = format!("{}:dHdv:[\"{bob}\"]", alias.transaction_id);
            let pending = at(
                signed(
                    "kchat",
                    "edit",
                    &pending_tail,
                    &format!("edit:{pending_tail}"),
                    63,
                ),
                pending_time,
            );
            capture(&pool, &pending, 1, true, true).await;
            consume(&consumer).await;
            let consumed: bool = sqlx::query_scalar(
                "SELECT notification_consumed FROM social_shared.actions WHERE transaction_id=$1",
            )
            .bind(hex::decode(&pending.transaction_id).unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(!consumed);
            let current_tail = format!("{}:b25l:[\"{alice}\"]", base.transaction_id);
            let current = at(
                signed(
                    "kchat",
                    "edit",
                    &current_tail,
                    &format!("edit:{current_tail}"),
                    64,
                ),
                current_time,
            );
            capture(&pool, &current, 1, true, true).await;
            consume(&consumer).await;
            if newer_pending {
                assert_eq!(
                    count(&pool, "notification_probe").await,
                    2,
                    "effective pending edit's recipient is evaluated with its displayed body"
                );
            }
            hold.rollback().await.unwrap();
            ready_all(&pool).await;
            drain(&consumer).await;
            assert_eq!(
                count(&pool, "notification_probe").await,
                2,
                "both delivery orders notify each added recipient once"
            );
            assert_eq!(count(&pool, "k_mentions").await, 2);
        }
    }

    #[tokio::test]
    async fn postgres_kaposts_unknown_parent_intent_and_busy_rollback_are_narrow() {
        {
            let pool = setup().await;
            let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
                .await
                .unwrap();
            let parent = at(signed("k", "post", "aGk=:[]", "aGk=:[]", 71), 1000);
            let deletion = at(
                signed(
                    "kchat",
                    "delete",
                    &parent.transaction_id,
                    &format!("delete:{}", parent.transaction_id),
                    72,
                ),
                1500,
            );
            capture(&pool, &deletion, 1, true, false).await;
            consume(&consumer).await;
            assert_eq!(
                count(&pool, "kachat_delete_tombstones").await,
                1,
                "signed intent may persist before ownership can be checked"
            );
            for table in [
                "k_contents",
                "k_edits",
                "k_deletes",
                "k_votes",
                "k_mentions",
                "k_hashtags",
                "k_polls",
                "k_poll_votes",
                "k_tips",
            ] {
                assert_eq!(count(&pool, table).await, 0, "{table}");
            }
            let other = at(signed("k", "post", "b3RoZXI=:[]", "b3RoZXI=:[]", 73), 1000);
            capture(&pool, &other, 1, true, false).await;
            consume(&consumer).await;
            capture(&pool, &parent, 1, true, true).await;
            consume(&consumer).await;
            assert!(
                native_content(&pool, &parent.transaction_id)
                    .await
                    .is_none()
            );
            assert!(native_content(&pool, &other.transaction_id).await.is_some());
            assert_eq!(
                count(&pool, "notification_probe").await,
                0,
                "deleted-before-post never inserts an eligible hidden post"
            );
        }
        {
            let pool = setup().await;
            let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
                .await
                .unwrap();
            let parent = at(signed("k", "post", "aGk=:[]", "aGk=:[]", 74), 1000);
            capture(&pool, &parent, 1, true, false).await;
            consume(&consumer).await;
            let before = native_content(&pool, &parent.transaction_id).await;
            let mut hold = pool.begin().await.unwrap();
            sqlx::query("SELECT transaction_id FROM protocol_indexer.canonical WHERE transaction_id=$1 FOR UPDATE").bind(hex::decode(&parent.transaction_id).unwrap()).fetch_one(&mut *hold).await.unwrap();
            let deletion = at(
                signed(
                    "kchat",
                    "delete",
                    &parent.transaction_id,
                    &format!("delete:{}", parent.transaction_id),
                    75,
                ),
                1500,
            );
            capture(&pool, &deletion, 1, true, false).await;
            consume(&consumer).await;
            assert_eq!(native_content(&pool, &parent.transaction_id).await, before);
            for table in [
                "k_edits",
                "k_deletes",
                "kachat_delete_tombstones",
                "k_votes",
                "k_mentions",
                "k_hashtags",
                "k_polls",
                "k_poll_votes",
                "k_tips",
            ] {
                assert_eq!(count(&pool, table).await, 0, "busy rolls back {table}");
            }
            let captured: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM social_shared.extensions WHERE transaction_id=$1",
            )
            .bind(hex::decode(&deletion.transaction_id).unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(captured, 0);
            hold.rollback().await.unwrap();
            ready_all(&pool).await;
            drain(&consumer).await;
            assert!(
                native_content(&pool, &parent.transaction_id)
                    .await
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn postgres_kaposts_author_window_poll_immutability_and_tip_target() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let post = signed_as(4, "post", "aGk=:[]", "aGk=:[]", 81);
        capture(&pool, &post, 1, true, false).await;
        consume(&consumer).await;
        let original = native_content(&pool, &post.transaction_id).await;
        let tail = format!("{}:bmV3:[]", post.transaction_id);
        let forged = at(
            signed("kchat", "edit", &tail, &format!("edit:{tail}"), 82),
            1500,
        );
        capture(&pool, &forged, 1, true, false).await;
        consume(&consumer).await;
        let delete_forged = at(
            signed(
                "kchat",
                "delete",
                &post.transaction_id,
                &format!("delete:{}", post.transaction_id),
                83,
            ),
            1600,
        );
        capture(&pool, &delete_forged, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(native_content(&pool, &post.transaction_id).await, original);
        assert_eq!(count(&pool, "k_edits").await, 0);
        assert_eq!(count(&pool, "k_deletes").await, 0);
        let valid = at(
            signed_as(4, "edit", &tail, &format!("edit:{tail}"), 84),
            1000 + 7_200_000,
        );
        capture(&pool, &valid, 1, true, false).await;
        consume(&consumer).await;
        let last = native_content(&pool, &post.transaction_id).await;
        let late_tail = format!("{}:bGF0ZQ==:[]", post.transaction_id);
        let late = at(
            signed_as(4, "edit", &late_tail, &format!("edit:{late_tail}"), 85),
            1000 + 7_200_001,
        );
        capture(&pool, &late, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(native_content(&pool, &post.transaction_id).await, last);
        let poll_tail = "aGk=:eWVz,bm8=:2000:[]";
        let poll = signed_as(4, "poll", poll_tail, &format!("poll:{poll_tail}"), 86);
        capture(&pool, &poll, 1, true, false).await;
        consume(&consumer).await;
        let poll_before = native_content(&pool, &poll.transaction_id).await;
        let edit_poll_tail = format!("{}:bmV3:[]", poll.transaction_id);
        let edit_poll = at(
            signed_as(
                4,
                "edit",
                &edit_poll_tail,
                &format!("edit:{edit_poll_tail}"),
                87,
            ),
            1500,
        );
        capture(&pool, &edit_poll, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(
            native_content(&pool, &poll.transaction_id).await,
            poll_before
        );
        let tip = synthetic_tip_vector(88, &poll, 10_000_000, 0, None);
        capture_vector(&pool, &tip, 1, true, false, true).await;
        consume(&consumer).await;
        assert_eq!(
            count(&pool, "social_shared.current_tips").await,
            1,
            "poll author uses the same native SDK identity and real output rule"
        );
    }

    async fn native_content(pool: &sqlx::PgPool, id: &str) -> Option<(i64, String, Option<i64>)> {
        sqlx::query_as(
            "SELECT id,base64_encoded_message,edited_at FROM k_contents WHERE transaction_id=$1",
        )
        .bind(hex::decode(id).unwrap())
        .fetch_optional(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn postgres_kaposts_edits_delete_withdraw_and_reorg_preserve_existing_content() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let post = at(signed("k", "post", "aGk=:[]", "aGk=:[]", 31), 1000);
        capture(&pool, &post, 1, true, false).await;
        consume(&consumer).await;
        let original = native_content(&pool, &post.transaction_id).await.unwrap();
        let recipient = format!("02{}", "44".repeat(32));
        let tail = format!("{}:bmV3:[\"{recipient}\"]", post.transaction_id);
        let edit = at(
            signed("kchat", "edit", &tail, &format!("edit:{tail}"), 32),
            1500,
        );
        capture(&pool, &edit, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(
            native_content(&pool, &post.transaction_id).await.unwrap(),
            (original.0, "bmV3".into(), Some(1500))
        );
        assert_eq!(count(&pool, "k_edits").await, 1);
        let mention: i64 = sqlx::query_scalar(
            "SELECT id FROM k_mentions WHERE content_id=$1 AND mentioned_pubkey=$2",
        )
        .bind(hex::decode(&post.transaction_id).unwrap())
        .bind(hex::decode(&recipient).unwrap())
        .fetch_one(&pool)
        .await
        .unwrap();
        capture(&pool, &edit, 2, false, false).await;
        consume(&consumer).await;
        assert_eq!(
            native_content(&pool, &post.transaction_id).await.unwrap(),
            original
        );
        assert_eq!(count(&pool, "k_edits").await, 0);
        assert_eq!(count(&pool, "k_mentions").await, 0);
        capture(&pool, &edit, 3, true, false).await;
        consume(&consumer).await;
        let restored: i64 = sqlx::query_scalar("SELECT id FROM k_mentions WHERE content_id=$1")
            .bind(hex::decode(&post.transaction_id).unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(restored, mention);
        let quote_tail = format!("{}:aGk=:{}", post.transaction_id, public_key());
        let quote = at(signed("k", "quote", &quote_tail, &quote_tail, 33), 1600);
        capture(&pool, &quote, 1, true, false).await;
        consume(&consumer).await;
        let quote_original = native_content(&pool, &quote.transaction_id).await.unwrap();
        let withdraw = at(
            signed(
                "kchat",
                "unquote",
                &post.transaction_id,
                &post.transaction_id,
                34,
            ),
            1700,
        );
        capture(&pool, &withdraw, 1, true, false).await;
        consume(&consumer).await;
        assert!(native_content(&pool, &quote.transaction_id).await.is_none());
        capture(&pool, &withdraw, 2, false, false).await;
        consume(&consumer).await;
        assert_eq!(
            native_content(&pool, &quote.transaction_id).await.unwrap(),
            quote_original
        );
        let deletion = at(
            signed(
                "kchat",
                "delete",
                &post.transaction_id,
                &format!("delete:{}", post.transaction_id),
                35,
            ),
            1800,
        );
        capture(&pool, &deletion, 1, true, false).await;
        consume(&consumer).await;
        assert!(native_content(&pool, &post.transaction_id).await.is_none());
        assert!(
            native_content(&pool, &quote.transaction_id).await.is_some(),
            "other authors' references survive delete"
        );
        assert_eq!(count(&pool, "k_deletes").await, 1);
        capture(&pool, &deletion, 2, false, false).await;
        ready_all(&pool).await;
        drain(&consumer).await;
        assert_eq!(
            native_content(&pool, &post.transaction_id).await.unwrap(),
            (original.0, "bmV3".into(), Some(1500))
        );
        let again: i64 = sqlx::query_scalar(
            "SELECT id FROM k_mentions WHERE content_id=$1 AND mentioned_pubkey=$2",
        )
        .bind(hex::decode(&post.transaction_id).unwrap())
        .bind(hex::decode(recipient).unwrap())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(again, mention);
        assert_eq!(count(&pool, "k_deletes").await, 0);
        assert_eq!(count(&pool, "kachat_delete_tombstones").await, 0);
    }

    #[tokio::test]
    async fn postgres_kaposts_poll_votes_validate_each_copy_and_recover_parent_reorg() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let tail = "aGk=:eWVz,bm8=:2000:[]";
        let poll = at(
            signed("kchat", "poll", tail, &format!("poll:{tail}"), 41),
            1000,
        );
        capture(&pool, &poll, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_polls").await, 1);
        let original = native_content(&pool, &poll.transaction_id).await.unwrap();
        let vote_tail = format!("{}:0", poll.transaction_id);
        let late = at(
            signed(
                "kchat",
                "pollvote",
                &vote_tail,
                &format!("pollvote:{vote_tail}"),
                42,
            ),
            2000,
        );
        capture(&pool, &late, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_poll_votes").await, 0);
        let timely = at(
            signed(
                "kchat",
                "pollvote",
                &vote_tail,
                &format!("pollvote:{vote_tail}"),
                43,
            ),
            1500,
        );
        capture(&pool, &timely, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "social_shared.current_poll_votes").await, 1);
        capture(&pool, &late, 2, true, false).await;
        consume(&consumer).await;
        let id: String =
            sqlx::query_scalar("SELECT encode(transaction_id,'hex') FROM k_poll_votes")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            id, timely.transaction_id,
            "late same-signature copy cannot displace valid vote"
        );
        capture(&pool, &poll, 2, false, false).await;
        assert_eq!(
            count(&pool, "social_shared.current_poll_votes").await,
            0,
            "read view hides parent revoke before consumer"
        );
        ready_all(&pool).await;
        drain(&consumer).await;
        assert_eq!(count(&pool, "k_poll_votes").await, 0);
        capture(&pool, &poll, 3, true, false).await;
        ready_all(&pool).await;
        drain(&consumer).await;
        assert_eq!(
            native_content(&pool, &poll.transaction_id).await.unwrap(),
            original
        );
        assert_eq!(count(&pool, "social_shared.current_poll_votes").await, 1);
    }

    #[tokio::test]
    async fn postgres_kaposts_invalid_poll_progresses_old_poll_quiet_live_poll_positive() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let bad_tail = "aGk=:eWVz:2000:[]";
        let bad = at(
            signed("kchat", "poll", bad_tail, &format!("poll:{bad_tail}"), 51),
            1000,
        );
        let unrelated = at(signed("k", "post", "aGk=:[]", "aGk=:[]", 52), 1100);
        capture(&pool, &bad, 1, true, true).await;
        capture(&pool, &unrelated, 1, true, false).await;
        consume(&consumer).await;
        consume(&consumer).await;
        assert!(native_content(&pool, &bad.transaction_id).await.is_none());
        assert!(
            native_content(&pool, &unrelated.transaction_id)
                .await
                .is_some()
        );
        let old_tail = "aGk=:eWVz,bm8=:2500:[]";
        let old = at(
            signed("kchat", "poll", old_tail, &format!("poll:{old_tail}"), 53),
            1200,
        );
        capture(&pool, &old, 1, true, true).await;
        consume(&consumer).await;
        assert_eq!(
            count(&pool, "notification_probe").await,
            0,
            "decoder replay never turns old eligible raw poll into new push"
        );
        let epoch: i64 = sqlx::query_scalar(
            "SELECT since_ms FROM social_shared.decoder_epochs WHERE kind='kchat:1:extensions'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let live_tail = format!("aGk=:eWVz,bm8=:{}:[]", epoch + 10000);
        let live = at(
            signed(
                "kchat",
                "poll",
                &live_tail,
                &format!("poll:{live_tail}"),
                54,
            ),
            epoch + 10,
        );
        capture(&pool, &live, 1, true, true).await;
        consume(&consumer).await;
        assert_eq!(
            count(&pool, "notification_probe").await,
            1,
            "positive control really captures eligible poll insert"
        );
        capture(&pool, &live, 2, true, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "notification_probe").await, 1);
    }

    async fn capture_vector(
        pool: &sqlx::PgPool,
        fixture: &serde_json::Value,
        revision: i64,
        accepted: bool,
        notify: bool,
        proof: bool,
    ) -> Transaction {
        let value = fixture_transaction(&fixture["transaction"]);
        capture(pool, &value, revision, accepted, notify).await;
        let id = hex::decode(&value.transaction_id).unwrap();
        sqlx::query("UPDATE protocol_indexer.canonical SET source_transaction=$1::jsonb WHERE transaction_id=$2")
            .bind(fixture["body"].to_string()).bind(&id).execute(pool).await.unwrap();
        if proof {
            sqlx::query("INSERT INTO protocol_indexer.evidence VALUES($1,$2::jsonb) ON CONFLICT(transaction_id) DO UPDATE SET receipt=EXCLUDED.receipt")
                .bind(id).bind(fixture["receipt"].to_string()).execute(pool).await.unwrap();
        }
        value
    }
    fn old_tip_vector() -> serde_json::Value {
        serde_json::from_str(include_str!("real_tip.json")).unwrap()
    }
    fn parent_vector() -> Transaction {
        fixture_transaction(&serde_json::from_str(include_str!("real_kpost.json")).unwrap())
    }
    async fn ready_all(pool: &sqlx::PgPool) {
        sqlx::query("UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp()")
            .execute(pool)
            .await
            .unwrap();
    }
    async fn drain(consumer: &SharedSocial) {
        for _ in 0..30 {
            if !consumer.run_one().await.unwrap() {
                return;
            }
        }
        panic!("bounded queue drain did not settle");
    }
    async fn tip_probe(pool: &sqlx::PgPool) {
        sqlx::query("UPDATE social_shared.decoder_epochs SET since_ms=0 WHERE kind='kchat:1:tip'")
            .execute(pool)
            .await
            .unwrap();
        sqlx::raw_sql("CREATE TRIGGER probe_tip AFTER INSERT ON k_tips FOR EACH ROW EXECUTE FUNCTION notification_probe_capture()").execute(pool).await.unwrap();
    }
    async fn boot_tip_parent(pool: &sqlx::PgPool, consumer: &SharedSocial) {
        capture(pool, &parent_vector(), 1, true, false).await;
        consume(consumer).await;
    }
    fn synthetic_tip_vector(
        id: u8,
        target: &Transaction,
        amount: u64,
        version: u16,
        author_override: Option<String>,
    ) -> serde_json::Value {
        use serde_json::json;
        let raw =
            String::from_utf8(hex::decode(target.payload.as_ref().unwrap()).unwrap()).unwrap();
        let author = author_override.unwrap_or_else(|| raw.split(':').nth(3).unwrap().to_owned());
        let tail = format!("{}:{author}:0011", target.transaction_id);
        let mut value = signed("kchat", "tip", &tail, &format!("tip:{tail}"), id);
        let script = hex::encode(crate::tips::p2pk_script(
            &crate::tips::xonly_bytes(&author).unwrap(),
        ));
        let payload = value.payload.as_ref().unwrap();
        let (transaction_id, rpc) = synthetic_rpc(
            payload,
            json!([{"amount":amount,"scriptPublicKey":{"version":version,"scriptPublicKey":script}}]),
            id,
        );
        value.transaction_id = transaction_id;
        json!({"transaction":{"transaction_id":value.transaction_id,"payload":payload,"block_time":value.block_time},
          "body":{"transaction_id":format!("\\x{}",value.transaction_id),"payload":format!("\\x{payload}"),"block_time":value.block_time,"outputs":[{"index":0,"amount":amount,"script_public_key":format!("\\x{script}")}]},
          "receipt":{"schema_version":1,"network":"testnet-10","transaction_id":value.transaction_id,"payload":payload,
            "archive_transaction":{"transaction_id":value.transaction_id,"payload":payload,"outputs":[{"index":0,"amount":amount,"script_public_key":script}]},
            "rpc_transaction":rpc}})
    }

    #[tokio::test]
    async fn postgres_tip_missing_parent_or_proof_retains_job_and_other_posts_progress() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let fixture = old_tip_vector();
        capture_vector(&pool, &fixture, 1, true, false, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_tips").await, 0);
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 1);
        assert!(
            !consumer.run_one().await.unwrap(),
            "no missing-data busy loop"
        );
        boot_tip_parent(&pool, &consumer).await;
        ready_all(&pool).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_tips").await, 0);
        let admission: String = sqlx::query_scalar("SELECT last_error FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(admission, "verified_tip_output_pending:1");
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM protocol_indexer.consumer_jobs WHERE consumer='output-evidence-v1'").fetch_one(&pool).await.unwrap(), 1);
        capture_vector(&pool, &fixture, 2, true, false, true).await;
        ready_all(&pool).await;
        drain(&consumer).await;
        assert_eq!(count(&pool, "social_shared.current_tips").await, 1);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1'").fetch_one(&pool).await.unwrap(), 0);
        // The separate harvester acknowledges its own queued proof work after
        // observing the now-terminal exact native revision.
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM protocol_indexer.consumer_jobs WHERE consumer='output-evidence-v1'").fetch_one(&pool).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn postgres_tip_archive_admission_requires_native_signature_and_matching_parent_author() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        boot_tip_parent(&pool, &consumer).await;
        let fixture = old_tip_vector();
        let mut invalid = fixture_transaction(&fixture["transaction"]);
        let wire =
            String::from_utf8(hex::decode(invalid.payload.as_ref().unwrap()).unwrap()).unwrap();
        let mut parts: Vec<String> = wire.split(':').map(str::to_owned).collect();
        parts[4] = "00".repeat(64);
        invalid.payload = Some(hex::encode(parts.join(":")));
        invalid.transaction_id = "ee".repeat(32);
        capture(&pool, &invalid, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 0);
        let status: String =
            sqlx::query_scalar("SELECT status FROM social_shared.actions WHERE transaction_id=$1")
                .bind(hex::decode(&invalid.transaction_id).unwrap())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "unsupported_or_invalid");
        let wrong =
            synthetic_tip_vector(65, &parent_vector(), 10_000_000, 0, Some(parts[3].clone()));
        let value = capture_vector(&pool, &wrong, 1, true, false, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 0);
        let status: String =
            sqlx::query_scalar("SELECT status FROM social_shared.actions WHERE transaction_id=$1")
                .bind(hex::decode(&value.transaction_id).unwrap())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            status, "verified",
            "valid signature with wrong author is terminal without fetching outputs"
        );
        assert_eq!(count(&pool, "k_tips").await, 0);
    }

    #[tokio::test]
    async fn postgres_tip_reorgs_parent_reorgs_restore_cursors_and_do_not_repeat_eligibility() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        tip_probe(&pool).await;
        boot_tip_parent(&pool, &consumer).await;
        let fixture = old_tip_vector();
        let tip = capture_vector(&pool, &fixture, 1, false, false, true).await;
        consume(&consumer).await;
        capture_vector(&pool, &fixture, 2, true, true, true).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_tips").await, 1);
        assert_eq!(count(&pool, "social_shared.current_tips").await, 1);
        assert_eq!(count(&pool, "notification_probe").await, 1);
        let original_id: i64 = sqlx::query_scalar("SELECT id FROM k_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        let original_mention: i64 =
            sqlx::query_scalar("SELECT id FROM k_mentions WHERE content_type='tip'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let amount: i64 = sqlx::query_scalar("SELECT amount FROM k_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(amount > 0);
        capture_vector(&pool, &fixture, 3, false, false, true).await;
        assert_eq!(
            count(&pool, "social_shared.current_tips").await,
            0,
            "current view hides revocation before native queue drains"
        );
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_tips").await, 0);
        capture_vector(&pool, &fixture, 4, true, true, true).await;
        consume(&consumer).await;
        let restored: i64 = sqlx::query_scalar("SELECT id FROM k_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(restored, original_id);
        let parent = parent_vector();
        capture(&pool, &parent, 2, false, false).await;
        assert_eq!(count(&pool, "social_shared.current_tips").await, 0);
        drain(&consumer).await;
        assert_eq!(count(&pool, "k_tips").await, 0);
        assert_eq!(count(&pool, "k_mentions").await, 0);
        capture(&pool, &parent, 3, true, false).await;
        drain(&consumer).await;
        assert_eq!(count(&pool, "social_shared.current_tips").await, 1);
        let restored: i64 = sqlx::query_scalar("SELECT id FROM k_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(restored, original_id);
        let mention: i64 = sqlx::query_scalar("SELECT id FROM k_mentions WHERE content_type='tip'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(mention, original_mention);
        assert_eq!(
            count(&pool, "notification_probe").await,
            1,
            "append-only trigger probe detects every repeated eligible attempt"
        );
        let stored: String = sqlx::query_scalar("SELECT encode(transaction_id,'hex') FROM k_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(stored, tip.transaction_id);
    }

    #[tokio::test]
    async fn postgres_tip_invalid_zero_version_or_author_cannot_poison_a_valid_signature_copy() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        boot_tip_parent(&pool, &consumer).await;
        let parent = parent_vector();
        let bad = synthetic_tip_vector(10, &parent, 7, 1, None);
        capture_vector(&pool, &bad, 1, true, true, true).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_tips").await, 0);
        let good = synthetic_tip_vector(11, &parent, 10_000_000, 0, None);
        capture_vector(&pool, &good, 1, true, true, true).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "social_shared.current_tips").await, 1);
        let winner: String = sqlx::query_scalar("SELECT encode(transaction_id,'hex') FROM k_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            winner,
            good["transaction"]["transaction_id"].as_str().unwrap()
        );
        let key = Keypair::from_seckey_slice(&Secp256k1::new(), &[4; 32])
            .unwrap()
            .x_only_public_key()
            .0
            .to_string();
        let mismatch = synthetic_tip_vector(12, &parent, 20_000_000, 0, Some(format!("02{key}")));
        capture_vector(&pool, &mismatch, 1, true, true, true).await;
        consume(&consumer).await;
        assert_eq!(
            count(&pool, "k_tips").await,
            1,
            "payment to a different claimed author is not a tip to this post"
        );
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 0);
    }

    #[tokio::test]
    async fn postgres_tip_signature_duplicates_keep_rep_and_fall_back_to_verified_paying_copy() {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        tip_probe(&pool).await;
        boot_tip_parent(&pool, &consumer).await;
        let parent = parent_vector();
        let a = synthetic_tip_vector(10, &parent, 10_000_000, 0, None);
        let b = synthetic_tip_vector(11, &parent, 50_000_000, 0, None);
        capture_vector(&pool, &a, 1, true, true, true).await;
        consume(&consumer).await;
        let id: i64 = sqlx::query_scalar("SELECT id FROM k_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        capture_vector(&pool, &b, 1, true, true, true).await;
        consume(&consumer).await;
        let amount: i64 = sqlx::query_scalar("SELECT amount FROM social_shared.current_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(amount, 10_000_000);
        assert_eq!(count(&pool, "k_tips").await, 1);
        capture_vector(&pool, &a, 2, false, false, true).await;
        consume(&consumer).await;
        let amount: i64 = sqlx::query_scalar("SELECT amount FROM social_shared.current_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(amount, 50_000_000);
        capture_vector(&pool, &a, 3, true, true, true).await;
        consume(&consumer).await;
        let amount: i64 = sqlx::query_scalar("SELECT amount FROM social_shared.current_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(amount, 50_000_000);
        capture_vector(&pool, &b, 2, false, false, true).await;
        consume(&consumer).await;
        let restored: i64 = sqlx::query_scalar("SELECT id FROM k_tips")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(restored, id);
        assert_eq!(count(&pool, "notification_probe").await, 1);
    }

    #[tokio::test]
    async fn postgres_tip_targets_a_valid_cross_root_signature_alias_and_pending_revocation_hides_count()
     {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        boot_tip_parent(&pool, &consumer).await;
        let parent = parent_vector();
        let mut alias = parent.clone();
        alias.transaction_id = "21".repeat(32);
        let raw = String::from_utf8(hex::decode(alias.payload.as_ref().unwrap()).unwrap()).unwrap();
        alias.payload = Some(hex::encode(raw.replacen("k:1:", "kchat:1:", 1)));
        capture(&pool, &alias, 1, true, false).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_contents").await, 1);
        let tip = synthetic_tip_vector(22, &alias, 10_000_000, 0, None);
        let transaction = capture_vector(&pool, &tip, 1, true, false, true).await;
        consume(&consumer).await;
        let visible: String = sqlx::query_scalar(
            "SELECT encode(content_transaction_id,'hex') FROM social_shared.current_tips",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(visible, parent.transaction_id);
        sqlx::query("INSERT INTO protocol_indexer.pending(transaction_id,acceptance_changed,accepting_block) VALUES($1,true,NULL)")
            .bind(hex::decode(&transaction.transaction_id).unwrap()).execute(&pool).await.unwrap();
        assert_eq!(count(&pool, "social_shared.current_tips").await, 0);
        sqlx::query("DELETE FROM protocol_indexer.pending")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(count(&pool, "social_shared.current_tips").await, 1);
        capture(&pool, &parent, 2, false, false).await;
        assert_eq!(
            count(&pool, "social_shared.current_tips").await,
            0,
            "revoked native representative cannot remain a current visible target"
        );
        drain(&consumer).await;
        let visible: String = sqlx::query_scalar(
            "SELECT encode(content_transaction_id,'hex') FROM social_shared.current_tips",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            visible, alias.transaction_id,
            "accepted alias becomes current representative after reconciliation"
        );
        capture(&pool, &parent, 3, true, false).await;
        drain(&consumer).await;
        capture(&pool, &alias, 2, false, false).await;
        assert_eq!(count(&pool, "social_shared.current_tips").await, 0);
        drain(&consumer).await;
        assert_eq!(count(&pool, "k_tips").await, 0);
    }

    #[tokio::test]
    async fn postgres_tip_decoder_epoch_and_schema_restart_do_not_push_old_supported_later_history()
    {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        boot_tip_parent(&pool, &consumer).await;
        sqlx::raw_sql("CREATE TRIGGER probe_tip AFTER INSERT ON k_tips FOR EACH ROW EXECUTE FUNCTION notification_probe_capture()").execute(&pool).await.unwrap();
        let epoch: i64 = sqlx::query_scalar(
            "SELECT since_ms FROM social_shared.decoder_epochs WHERE kind='kchat:1:tip'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let tip = old_tip_vector();
        capture_vector(&pool, &tip, 1, true, true, true).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_tips").await, 1);
        assert_eq!(
            count(&pool, "notification_probe").await,
            0,
            "older decoded history is quiet even if old source event was eligible"
        );
        let again = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        assert!(!again.run_one().await.unwrap());
        let after: i64 = sqlx::query_scalar(
            "SELECT since_ms FROM social_shared.decoder_epochs WHERE kind='kchat:1:tip'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(epoch, after);
    }
    #[tokio::test]
    async fn postgres_rooms_dual_roots_revoke_restore_stable_native_cursor_and_quiet_content() {
        let pool=setup().await;
        let consumer=SharedSocial::bind(pool.clone(),"testnet-10",SOURCE).await.unwrap();
        sqlx::raw_sql("CREATE TRIGGER probe_room AFTER INSERT ON kachat_broadcasts FOR EACH ROW EXECUTE FUNCTION notification_probe_capture()").execute(&pool).await.unwrap();
        for (n,root) in [(96,"kchat"),(97,"ciph_msg")] {
            let mut v=room_vector(root,"  EN ","text:verbatim",n,false);
            let value=capture_vector(&pool,&v,1,true,true,true).await;
            consume(&consumer).await;
            let id=hex::decode(&value.transaction_id).unwrap();
            let before:(i64,i64,String,String)=sqlx::query_as("SELECT id,block_time,channel,content FROM social_shared.current_broadcasts WHERE transaction_id=$1").bind(&id).fetch_one(&pool).await.unwrap();
            assert_eq!(before.2,"en");assert_eq!(before.3,"text:verbatim");
            sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)").bind(&id).execute(&pool).await.unwrap();
            assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM social_shared.current_broadcasts WHERE transaction_id=$1").bind(&id).fetch_one(&pool).await.unwrap(),0);
            capture_vector(&pool,&v,2,false,false,false).await;
            consume(&consumer).await;
            assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM kachat_broadcasts WHERE transaction_id=$1").bind(&id).fetch_one(&pool).await.unwrap(),0);
            sqlx::query("DELETE FROM protocol_indexer.pending WHERE transaction_id=$1").bind(&id).execute(&pool).await.unwrap();
            v["body"]["block_time"]=serde_json::json!(1700000005000i64);
            capture_vector(&pool,&v,3,true,true,false).await;consume(&consumer).await;
            let after:(i64,i64,String,String)=sqlx::query_as("SELECT id,block_time,channel,content FROM social_shared.current_broadcasts WHERE transaction_id=$1").bind(&id).fetch_one(&pool).await.unwrap();
            assert_eq!(before,after);
        }
        assert_eq!(count(&pool,"social_shared.current_broadcasts").await,2);
        assert_eq!(count(&pool,"notification_probe").await,0);
        assert_eq!(count(&pool,"protocol_indexer.consumer_jobs").await,0);
        SharedSocial::bind(pool.clone(),"testnet-10",SOURCE).await.unwrap();
        assert_eq!(count(&pool,"social_shared.current_broadcasts").await,2);
    }

    #[tokio::test]
    async fn postgres_rooms_missing_or_sdk_rejected_proof_admits_exact_revision_and_fair_retry() {
        let pool=setup().await;
        let consumer=SharedSocial::bind(pool.clone(),"testnet-10",SOURCE).await.unwrap();
        let mut v=room_vector("kchat","all","proof pending",98,false);
        let value=capture_vector(&pool,&v,1,true,true,false).await;consume(&consumer).await;
        let id=hex::decode(&value.transaction_id).unwrap();
        assert_eq!(count(&pool,"social_shared.actions").await,0,"pending work rolls back native ledger too");
        let marker:String=sqlx::query_scalar("SELECT last_error FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1' AND transaction_id=$1").bind(&id).fetch_one(&pool).await.unwrap();
        assert_eq!(marker,"parsed_room_sender_pending:1");
        v["receipt"]["rpc_transaction"]["outputs"][0]["scriptPublicKey"]["version"]=serde_json::json!(1);
        capture_vector(&pool,&v,2,true,false,true).await;ready_all(&pool).await;consume(&consumer).await;
        let fingerprint:String=sqlx::query_scalar("SELECT encode(sha256(convert_to(receipt::text,'UTF8')),'hex') FROM protocol_indexer.evidence WHERE transaction_id=$1").bind(&id).fetch_one(&pool).await.unwrap();
        let marker:String=sqlx::query_scalar("SELECT last_error FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1' AND transaction_id=$1").bind(&id).fetch_one(&pool).await.unwrap();
        assert_eq!(marker,format!("parsed_room_sender_pending:2:{fingerprint}"));
        let ready=room_vector("ciph_msg","all","ready",99,false);
        capture_vector(&pool,&ready,1,true,false,true).await;consume(&consumer).await;
        assert_eq!(count(&pool,"social_shared.current_broadcasts").await,1);
        assert!(!consumer.run_one().await.unwrap());
        v["receipt"]["rpc_transaction"]["outputs"][0]["scriptPublicKey"]["version"]=serde_json::json!(0);
        capture_vector(&pool,&v,3,true,false,true).await;ready_all(&pool).await;consume(&consumer).await;
        assert_eq!(count(&pool,"social_shared.current_broadcasts").await,2);
    }

    #[tokio::test]
    async fn postgres_rooms_forged_selfsend_is_terminal_and_failure_rolls_back_ack() {
        let pool=setup().await;
        let consumer=SharedSocial::bind(pool.clone(),"testnet-10",SOURCE).await.unwrap();
        let forged=room_vector("kchat","all","not this sender",100,true);
        capture_vector(&pool,&forged,1,true,true,true).await;consume(&consumer).await;
        assert_eq!(count(&pool,"social_shared.current_broadcasts").await,0);
        assert_eq!(count(&pool,"protocol_indexer.consumer_jobs").await,0);
        let v=room_vector("kchat","all","rollback",101,false);
        capture_vector(&pool,&v,1,true,true,true).await;
        sqlx::raw_sql("CREATE FUNCTION reject_room() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected room SQL failure'; END $$; CREATE TRIGGER reject_room BEFORE INSERT ON kachat_broadcasts FOR EACH ROW EXECUTE FUNCTION reject_room()").execute(&pool).await.unwrap();
        assert!(consumer.run_one().await.is_err());
        assert_eq!(count(&pool,"kachat_broadcasts").await,0);
        assert_eq!(count(&pool,"social_shared.room_facts").await,0);
        assert_eq!(count(&pool,"protocol_indexer.consumer_jobs").await,1);
        sqlx::query("DROP TRIGGER reject_room ON kachat_broadcasts").execute(&pool).await.unwrap();
        consume(&consumer).await;assert_eq!(count(&pool,"social_shared.current_broadcasts").await,1);
    }

    #[tokio::test]
    async fn postgres_rooms_upgrade_refuses_unowned_history_and_tip_rejection_binds_receipt_hash() {
        let pool=setup().await;
        let consumer=SharedSocial::bind(pool.clone(),"testnet-10",SOURCE).await.unwrap();
        boot_tip_parent(&pool,&consumer).await;
        let mut v=old_tip_vector();
        v["receipt"]["rpc_transaction"]["outputs"][0]["scriptPublicKey"]["version"]=serde_json::json!(1);
        let value=capture_vector(&pool,&v,1,true,false,true).await;consume(&consumer).await;
        let id=hex::decode(value.transaction_id).unwrap();
        let fp:String=sqlx::query_scalar("SELECT encode(sha256(convert_to(receipt::text,'UTF8')),'hex') FROM protocol_indexer.evidence WHERE transaction_id=$1").bind(&id).fetch_one(&pool).await.unwrap();
        let marker:String=sqlx::query_scalar("SELECT last_error FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1' AND transaction_id=$1").bind(&id).fetch_one(&pool).await.unwrap();
        assert_eq!(marker,format!("verified_tip_output_pending:1:{fp}"));
        sqlx::query("DELETE FROM social_shared.decoder_epochs WHERE kind='kchat:1:bcast'").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO kachat_broadcasts(transaction_id,block_time,channel,sender_address,content) VALUES($1,1,'en','foreign','retained')").bind(vec![123u8;32]).execute(&pool).await.unwrap();
        assert!(SharedSocial::bind(pool.clone(),"testnet-10",SOURCE).await.is_err());
        assert_eq!(count(&pool,"kachat_broadcasts").await,1);
        assert_eq!(count(&pool,"social_shared.decoder_epochs").await,2,"failed bind does not install room epoch");
    }

}

#[tokio::test]
async fn pure_tip_signature_and_output_binding_follow_upstream_without_fee_input_aliases() {
    use crate::tip_evidence::TipOutputEvidence;
    use crate::tips::{p2pk_script, xonly_bytes};
    use serde_json::json;
    let (processor, _) = processor();
    let author = Keypair::from_seckey_slice(&Secp256k1::new(), &[4; 32])
        .unwrap()
        .x_only_public_key()
        .0
        .to_string();
    let author = format!("02{author}");
    let post = "ab".repeat(32);
    let tail = format!("{post}:{author}:0011");
    let message = format!("tip:{tail}");
    let mut transaction = signed("kchat", "tip", &tail, &message, 7);
    let script = hex::encode(p2pk_script(&xonly_bytes(&author).unwrap()));
    let sender_script = hex::encode(p2pk_script(&xonly_bytes(&public_key()).unwrap()));
    let outputs = vec![
        (7u64, 0u16, script.clone()),
        (11, 0, script.clone()),
        (55, 1, script),
        (999, 0, sender_script),
    ];
    let canonical: Vec<_> = outputs
        .iter()
        .enumerate()
        .map(|(i, (n, _, s))| json!({"index":i,"amount":n,"script_public_key":format!("\\x{s}")}))
        .collect();
    let detail: Vec<_> = outputs
        .iter()
        .enumerate()
        .map(|(i, (n, _, s))| json!({"index":i,"amount":n.to_string(),"script_public_key":s}))
        .collect();
    let rpc:Vec<_>=outputs.iter().map(|(n,v,s)|json!({"amount":n.to_string(),"scriptPublicKey":{"version":v,"scriptPublicKey":s}})).collect();
    let payload = transaction.payload.as_ref().unwrap();
    let (id_text, rpc_transaction) = synthetic_rpc(payload, json!(rpc), 7);
    transaction.transaction_id = id_text;
    let verified = processor.verify_tip_transaction(&transaction).unwrap();
    let body = json!({"transaction_id":format!("\\x{}",transaction.transaction_id),"payload":format!("\\x{payload}"),"outputs":canonical});
    let receipt = json!({"schema_version":1,"network":"testnet-10","transaction_id":transaction.transaction_id,"payload":payload,
        "archive_transaction":{"transaction_id":transaction.transaction_id,"payload":payload,"outputs":detail},
        "rpc_transaction":rpc_transaction});
    let id = hex::decode(&transaction.transaction_id).unwrap();
    let proof = TipOutputEvidence::checked("testnet-10", &id, &body, &receipt).unwrap();
    assert_eq!(
        proof.payment_to_author(&verified).unwrap(),
        18,
        "sum all matching version0 author outputs; exclude change/nonzero versions"
    );
    let mut other = transaction.clone();
    other.transaction_id = "08".repeat(32);
    assert!(
        proof
            .payment_to_author(&processor.verify_tip_transaction(&other).unwrap())
            .is_err()
    );
    // Owned proof and verified envelope cannot change under caller mutations.
    let mut changed = receipt.clone();
    changed["rpc_transaction"]["outputs"][0]["amount"] = json!(999);
    assert!(TipOutputEvidence::checked("testnet-10", &id, &body, &changed).is_err());
    assert_eq!(proof.payment_to_author(&verified).unwrap(), 18);
    let mut changed = receipt.clone();
    changed["rpc_transaction"]["outputs"][0]["scriptPublicKey"]["version"] = json!(1);
    assert!(
        TipOutputEvidence::checked("testnet-10", &id, &body, &changed).is_err(),
        "a reported ID cannot hide a script-version mutation"
    );
    for malformed in [
        json!([null]),
        json!([true]),
        json!(["input"]),
        json!([{"previousOutpoint":[]}]),
    ] {
        let mut changed = receipt.clone();
        changed["rpc_transaction"]["inputs"] = malformed;
        assert!(
            TipOutputEvidence::checked("testnet-10", &id, &body, &changed).is_err(),
            "malformed RPC input must reject without panicking"
        );
    }
    for malformed in [json!([null]), json!([true]), json!(["output"])] {
        let mut changed = receipt.clone();
        changed["rpc_transaction"]["outputs"] = malformed;
        assert!(
            TipOutputEvidence::checked("testnet-10", &id, &body, &changed).is_err(),
            "malformed RPC output must reject without panicking"
        );
    }
    for path in ["network", "transaction_id", "payload"] {
        let mut changed = receipt.clone();
        changed[path] = json!("wrong");
        assert!(
            TipOutputEvidence::checked("testnet-10", &id, &body, &changed).is_err(),
            "{path}"
        );
    }
    for value in [json!(true), json!(-1), json!(65536), json!("1.5")] {
        let mut changed = receipt.clone();
        changed["rpc_transaction"]["outputs"][0]["scriptPublicKey"]["version"] = value;
        assert!(TipOutputEvidence::checked("testnet-10", &id, &body, &changed).is_err());
    }
    for value in [
        json!(true),
        json!(-1),
        json!("18446744073709551616"),
        json!("1.5"),
    ] {
        let mut changed = receipt.clone();
        changed["rpc_transaction"]["outputs"][0]["amount"] = value;
        assert!(TipOutputEvidence::checked("testnet-10", &id, &body, &changed).is_err());
    }
    let mut changed = body.clone();
    changed["outputs"][1]["index"] = json!(0);
    assert!(TipOutputEvidence::checked("testnet-10", &id, &changed, &receipt).is_err());
    let raw = String::from_utf8(hex::decode(payload).unwrap()).unwrap();
    for root in ["k:1:", "kchat:2:", "ciph_msg:1:"] {
        let mut changed = transaction.clone();
        changed.payload = Some(hex::encode(raw.replacen("kchat:1:", root, 1)));
        assert!(processor.verify_tip_transaction(&changed).is_none());
    }
    for author in [
        public_key(),
        format!("03{}", public_key()),
        format!("04{}", public_key()),
    ] {
        let tail = format!("{post}:{author}:0011");
        let message = format!("tip:{tail}");
        assert!(
            processor
                .verify_tip_transaction(&signed("kchat", "tip", &tail, &message, 9))
                .is_none(),
            "self/invalid alias"
        );
    }
}

#[tokio::test]
async fn pure_genuine_archived_tip_has_valid_signature_and_matching_author_payment() {
    use crate::tip_evidence::TipOutputEvidence;
    let (processor, _) = processor();
    let fixture: serde_json::Value = serde_json::from_str(include_str!("real_tip.json")).unwrap();
    let transaction: Transaction = fixture_transaction(&fixture["transaction"]);
    let verified = processor
        .verify_tip_transaction(&transaction)
        .expect("actual archived kchat tip signature verifies with original Kaspa SDK");
    let proof = TipOutputEvidence::checked(
        "testnet-10",
        &hex::decode(&transaction.transaction_id).unwrap(),
        &fixture["body"],
        &fixture["receipt"],
    )
    .unwrap();
    assert_eq!(
        verified.tip().post_id,
        "f3941dc64ae4db441c746f7fd6bd7d6d44436ef84cf61c02c6516e6d4f8f4b61"
    );
    assert!(proof.payment_to_author(&verified).unwrap() > 0);
}

#[tokio::test]
async fn pure_release_d4d85e_tip_protocol_schnorr_post_author_and_exact_payment() {
    use crate::tip_evidence::TipOutputEvidence;
    let (processor, _) = processor();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("release_tip.json")).unwrap();
    let transaction: Transaction = fixture_transaction(&fixture["transaction"]);
    assert_eq!(
        transaction.transaction_id,
        "d4d85e13185251e60e5dbb36ccbc2f029ffb2c5e23589a0ef8bd7bcd197df9f3"
    );
    let verified = processor
        .verify_tip_transaction(&transaction)
        .expect("published native tip's original Kaspa PersonalMessage/Schnorr verifies");
    let post: Transaction =
        fixture_transaction(&serde_json::from_str(include_str!("real_kpost.json")).unwrap());
    let parent = processor
        .verify_transaction(&post)
        .expect("genuine referenced post signature verifies");
    let KActionType::Post(post) = parent.action() else {
        panic!("actual referenced content is post")
    };
    assert_eq!(verified.tip().post_id, parent.transaction().transaction_id);
    assert_eq!(verified.tip().author_pubkey, post.sender_pubkey);
    assert_eq!(
        verified.tip().sender_pubkey,
        "03b736ec7820b077f71daee7dae1b62d910983f25b11ff7fb87a79caa2e37d264f"
    );
    let proof = TipOutputEvidence::checked(
        "testnet-10",
        &hex::decode(&transaction.transaction_id).unwrap(),
        &fixture["body"],
        &fixture["receipt"],
    )
    .unwrap();
    assert_eq!(
        proof.payment_to_author(&verified).unwrap(),
        10_000_000,
        ".1TESTKAS to the exact signed target author"
    );
}
