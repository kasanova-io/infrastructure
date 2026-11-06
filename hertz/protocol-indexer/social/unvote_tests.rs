// Included inside shared_consumer: use its real SDK and disposable PostgreSQL
// fixtures. Synthetic transactions exercise projection policy, not chain proof.
fn vote_at(root: &str, post: &str, value: &str, nonce: u8, time: i64) -> Transaction {
    let tail = format!("{post}:{value}:{}", public_key());
    at(signed(root, "vote", &tail, &tail, nonce), time)
}

async fn votes(pool: &sqlx::PgPool) -> String {
    sqlx::query_scalar("SELECT coalesce(jsonb_agg(to_jsonb(v) ORDER BY transaction_id),'[]'::jsonb)::text FROM k_votes v")
        .fetch_one(pool).await.unwrap()
}

async fn vote_mentions(pool: &sqlx::PgPool) -> String {
    sqlx::query_scalar("SELECT coalesce(jsonb_agg(to_jsonb(m) ORDER BY content_id,id),'[]'::jsonb)::text FROM k_mentions m WHERE content_type='vote'")
        .fetch_one(pool).await.unwrap()
}

async fn vote_probe(pool: &sqlx::PgPool) {
    sqlx::raw_sql("CREATE TRIGGER probe_vote AFTER INSERT ON k_votes FOR EACH ROW EXECUTE FUNCTION notification_probe_capture()")
        .execute(pool).await.unwrap();
}

#[tokio::test]
async fn pure_unvote_uses_vote_signing_fields_and_requires_kachat_envelope() {
    let (processor, pool) = processor();
    let post = "ab".repeat(32);
    let value = vote_at("kchat", &post, "unvote", 90, 2000);
    let verified = processor.verify_kaposts_transaction(&value).unwrap();
    assert!(matches!(
        verified.action(),
        crate::kaposts::KaPostsAction::Unvote(_)
    ));
    assert_eq!(
        verified.action().signing_string().unwrap(),
        format!("{post}:unvote:{}", public_key())
    );
    assert!(
        processor.verify_transaction(&value).is_none(),
        "withdrawal cannot enter the ordinary insert path"
    );
    for (root, kind, tail, message) in [
        (
            "k",
            "vote",
            format!("{post}:unvote:{}", public_key()),
            format!("{post}:unvote:{}", public_key()),
        ),
        ("kchat", "unvote", post.clone(), post.clone()),
        (
            "kchat",
            "vote",
            format!("{post}:unvote"),
            format!("{post}:unvote"),
        ),
        (
            "kchat",
            "vote",
            format!("{post}:unvote:{}:extra", public_key()),
            format!("{post}:unvote:{}:extra", public_key()),
        ),
        (
            "kchat",
            "vote",
            format!("not-hex:unvote:{}", public_key()),
            format!("not-hex:unvote:{}", public_key()),
        ),
    ] {
        assert!(
            processor
                .verify_kaposts_transaction(&signed(root, kind, &tail, &message, 91))
                .is_none()
        );
    }
    let wire = String::from_utf8(hex::decode(value.payload.unwrap()).unwrap()).unwrap();
    for field in [3, 5, 7] {
        let mut fields: Vec<_> = wire.split(':').map(str::to_owned).collect();
        fields[field] = "04".repeat(32);
        let mut forged = verified.transaction().clone();
        forged.payload = Some(hex::encode(fields.join(":")));
        assert!(
            processor.verify_kaposts_transaction(&forged).is_none(),
            "signed field {field} is bound"
        );
    }
    pool.close().await;
}

#[tokio::test]
async fn postgres_unvote_withdraws_only_own_sender_post_votes_and_mentions() {
    let pool = setup().await;
    let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
        .await
        .unwrap();
    vote_probe(&pool).await;
    let post = "ab".repeat(32);
    let up = vote_at("k", &post, "upvote", 1, 1000);
    let down = vote_at("kchat", &post, "downvote", 2, 1100);
    let other_post = vote_at("kchat", &"cd".repeat(32), "upvote", 3, 1200);
    let tail = format!("{post}:upvote:{}", public_key());
    let other_sender = at(signed_as(4, "vote", &tail, &tail, 4), 1300);
    for value in [&up, &down, &other_post, &other_sender] {
        capture(&pool, value, 1, true, true).await;
        consume(&consumer).await;
    }
    assert_eq!(
        count(&pool, "k_votes").await,
        4,
        "legacy signature multiplicity stays intact"
    );
    let withdrawal = vote_at("kchat", &post, "unvote", 5, 2000);
    capture(&pool, &withdrawal, 1, true, true).await;
    consume(&consumer).await;
    assert_eq!(count(&pool, "k_votes").await, 2);
    assert_eq!(count(&pool, "social_shared_api.k_votes").await, 2);
    assert_eq!(count(&pool, "k_mentions").await, 2);
    let remaining: Vec<String> = sqlx::query_scalar(
        "SELECT encode(transaction_id,'hex') FROM k_votes ORDER BY transaction_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        remaining,
        vec![other_post.transaction_id, other_sender.transaction_id]
    );
    assert_eq!(
        count(&pool, "notification_probe").await,
        4,
        "unvote creates no new like or mention push"
    );
    assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 0);
}

#[tokio::test]
async fn postgres_unvote_before_vote_and_late_old_withdrawal_converge() {
    for withdrawal_first in [false, true] {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let post = "ab".repeat(32);
        let old = vote_at("kchat", &post, "upvote", 11, 1000);
        let withdrawal = vote_at("kchat", &post, "unvote", 12, 2000);
        let ordered = if withdrawal_first {
            [&withdrawal, &old]
        } else {
            [&old, &withdrawal]
        };
        for value in ordered {
            capture(&pool, value, 1, true, false).await;
            consume(&consumer).await;
        }
        assert_eq!(count(&pool, "k_votes").await, 0);
        let later = vote_at("kchat", &post, "downvote", 13, 3000);
        capture(&pool, &later, 1, true, false).await;
        consume(&consumer).await;
        let before = votes(&pool).await;
        capture(&pool, &withdrawal, 2, true, false).await;
        consume(&consumer).await;
        assert_eq!(
            votes(&pool).await,
            before,
            "old history never withdraws a later vote"
        );
        assert_eq!(count(&pool, "social_shared_api.k_votes").await, 1);
        assert_eq!(count(&pool, "notification_probe").await, 0);
    }
}

#[tokio::test]
async fn postgres_unvote_allows_later_same_signature_alias_then_reorgs_quietly() {
    let pool = setup().await;
    let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
        .await
        .unwrap();
    vote_probe(&pool).await;
    let post = "ab".repeat(32);
    let first = vote_at("kchat", &post, "upvote", 21, 1000);
    capture(&pool, &first, 1, true, true).await;
    consume(&consumer).await;
    let withdrawal = vote_at("kchat", &post, "unvote", 22, 2000);
    capture(&pool, &withdrawal, 1, true, true).await;
    consume(&consumer).await;
    assert_eq!(count(&pool, "k_votes").await, 0);
    let mut later = first.clone();
    later.transaction_id = "17".repeat(32);
    later.block_time = Some(3000);
    capture(&pool, &later, 1, true, true).await;
    consume(&consumer).await;
    let before = votes(&pool).await;
    assert_eq!(count(&pool, "k_votes").await, 1);
    let selected: String = sqlx::query_scalar("SELECT encode(transaction_id,'hex') FROM k_votes")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(selected, later.transaction_id);
    capture(&pool, &later, 2, false, false).await;
    consume(&consumer).await;
    assert_eq!(
        count(&pool, "k_votes").await,
        0,
        "old pre-withdrawal alias cannot rescue a revoked re-vote"
    );
    capture(&pool, &later, 3, true, false).await;
    consume(&consumer).await;
    assert_eq!(votes(&pool).await, before);
    assert_eq!(
        count(&pool, "notification_probe").await,
        2,
        "reacceptance does not replay like pushes"
    );
}

#[tokio::test]
async fn postgres_unvote_revocation_restores_exact_native_ids_times_mentions_without_alerts() {
    let pool = setup().await;
    let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
        .await
        .unwrap();
    vote_probe(&pool).await;
    let post = "ab".repeat(32);
    let old = vote_at("kchat", &post, "upvote", 31, 1000);
    capture(&pool, &old, 1, true, true).await;
    consume(&consumer).await;
    let before = votes(&pool).await;
    let mentions = vote_mentions(&pool).await;
    let withdrawal = vote_at("kchat", &post, "unvote", 32, 2000);
    capture(&pool, &withdrawal, 1, true, true).await;
    consume(&consumer).await;
    assert_eq!(count(&pool, "k_votes").await, 0);
    capture(&pool, &withdrawal, 2, false, false).await;
    consume(&consumer).await;
    assert_eq!(votes(&pool).await, before);
    assert_eq!(vote_mentions(&pool).await, mentions);
    assert_eq!(count(&pool, "notification_probe").await, 1);
    capture(&pool, &withdrawal, 3, true, false).await;
    consume(&consumer).await;
    assert_eq!(count(&pool, "k_votes").await, 0);
    assert_eq!(count(&pool, "notification_probe").await, 1);
}

#[tokio::test]
async fn postgres_unvote_latest_revocation_falls_back_to_earlier_withdrawal() {
    let pool = setup().await;
    let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
        .await
        .unwrap();
    let post = "ab".repeat(32);
    let first = vote_at("kchat", &post, "upvote", 41, 1000);
    let early = vote_at("kchat", &post, "unvote", 42, 2000);
    let later = vote_at("kchat", &post, "downvote", 43, 3000);
    let latest = vote_at("kchat", &post, "unvote", 44, 4000);
    for value in [&first, &early, &later, &latest] {
        capture(&pool, value, 1, true, false).await;
        consume(&consumer).await;
    }
    assert_eq!(count(&pool, "k_votes").await, 0);
    capture(&pool, &latest, 2, false, false).await;
    consume(&consumer).await;
    assert_eq!(count(&pool, "k_votes").await, 1);
    let selected: String = sqlx::query_scalar("SELECT encode(transaction_id,'hex') FROM k_votes")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(selected, later.transaction_id);
    capture(&pool, &early, 2, false, false).await;
    consume(&consumer).await;
    assert_eq!(
        count(&pool, "k_votes").await,
        2,
        "both legacy signatures restore when all withdrawals revoke"
    );
    assert_eq!(count(&pool, "social_shared_api.k_votes").await, 2);
}

#[tokio::test]
async fn postgres_unvote_current_reader_rejects_stale_or_revoking_withdrawal() {
    let pool = setup().await;
    let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
        .await
        .unwrap();
    let post = "ab".repeat(32);
    let old = vote_at("kchat", &post, "upvote", 51, 1000);
    capture(&pool, &old, 1, true, false).await;
    consume(&consumer).await;
    let before = votes(&pool).await;
    let withdrawal = vote_at("kchat", &post, "unvote", 52, 2000);
    capture(&pool, &withdrawal, 1, true, false).await;
    consume(&consumer).await;
    // Isolated stale physical row: the read contract must still enforce the
    // current signed withdrawal before another native job repairs that row.
    sqlx::query(
        "INSERT INTO k_votes SELECT * FROM jsonb_populate_recordset(NULL::k_votes,$1::jsonb)",
    )
    .bind(&before)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(count(&pool, "k_votes").await, 1);
    assert_eq!(count(&pool, "social_shared_api.k_votes").await, 0);
    sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
        .bind(hex::decode(&withdrawal.transaction_id).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        count(&pool, "social_shared.current_vote_withdrawals").await,
        0
    );
    assert_eq!(
        count(&pool, "social_shared_api.k_votes").await,
        1,
        "pending revocation removes withdrawal authority immediately"
    );
    sqlx::query("DELETE FROM protocol_indexer.pending")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE social_shared.extensions SET revision=99 WHERE kind='unvote'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        count(&pool, "social_shared_api.k_votes").await,
        1,
        "stale decoded revision cannot withdraw a vote"
    );
}

#[tokio::test]
async fn postgres_unvote_relation_and_subject_contention_roll_back_facts_and_retry() {
    for lock_relation in [true, false] {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let post = "ab".repeat(32);
        let old = vote_at("kchat", &post, "upvote", 61, 1000);
        capture(&pool, &old, 1, true, false).await;
        consume(&consumer).await;
        let before = votes(&pool).await;
        let key = if lock_relation {
            format!("vote_relation:{}:{post}", public_key())
        } else {
            sqlx::query_scalar(
                "SELECT subject FROM social_shared.actions WHERE native_table='k_votes'",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        };
        let mut owner = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,736124))")
            .bind(key)
            .execute(&mut *owner)
            .await
            .unwrap();
        let withdrawal = vote_at("kchat", &post, "unvote", 62, 2000);
        capture(&pool, &withdrawal, 1, true, false).await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), consumer.run_one())
                .await
                .unwrap()
                .unwrap()
        );
        assert_eq!(votes(&pool).await, before);
        assert_eq!(count(&pool, "social_shared.vote_withdrawals").await, 0);
        assert_eq!(
            count(&pool, "social_shared.current_vote_withdrawals").await,
            0
        );
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 1);
        owner.rollback().await.unwrap();
        ready_all(&pool).await;
        consume(&consumer).await;
        assert_eq!(count(&pool, "k_votes").await, 0);
        assert_eq!(count(&pool, "protocol_indexer.consumer_jobs").await, 0);
    }
}

#[tokio::test]
async fn postgres_unvote_replays_old_unsupported_record_through_normal_sdk_without_alerts() {
    let pool = setup().await;
    let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
        .await
        .unwrap();
    vote_probe(&pool).await;
    let post = "ab".repeat(32);
    let old = vote_at("kchat", &post, "upvote", 71, 1000);
    capture(&pool, &old, 1, true, false).await;
    consume(&consumer).await;
    let withdrawal = vote_at("kchat", &post, "unvote", 72, 2000);
    capture(&pool, &withdrawal, 1, true, false).await;
    sqlx::query("INSERT INTO social_shared.actions(transaction_id,revision,source_payload,original_time,status,notification_consumed) VALUES($1,1,$2,2000,'unsupported_or_invalid',false)")
        .bind(hex::decode(&withdrawal.transaction_id).unwrap()).bind(withdrawal.payload.as_ref().unwrap()).execute(&pool).await.unwrap();
    consume(&consumer).await;
    let status: String =
        sqlx::query_scalar("SELECT status FROM social_shared.actions WHERE transaction_id=$1")
            .bind(hex::decode(&withdrawal.transaction_id).unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "verified");
    assert_eq!(count(&pool, "k_votes").await, 0);
    assert_eq!(count(&pool, "notification_probe").await, 0);
}

#[tokio::test]
async fn postgres_unvote_upgrade_of_two_column_vote_metadata_preserves_native_history() {
    let pool = setup().await;
    let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
        .await
        .unwrap();
    let post = "ab".repeat(32);
    let old = vote_at("k", &post, "upvote", 81, 1000);
    capture(&pool, &old, 1, true, false).await;
    consume(&consumer).await;
    let before = votes(&pool).await;
    let mentions = vote_mentions(&pool).await;
    // Reproduce the previous deployed metadata shape in this disposable DB.
    // The native table/snapshots/signatures are left intact for the upgrade.
    sqlx::query("ALTER TABLE social_shared.ordinary_vote_targets DROP COLUMN sender")
        .execute(&pool)
        .await
        .unwrap();
    let upgraded = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
        .await
        .unwrap();
    assert_eq!(votes(&pool).await, before);
    assert_eq!(vote_mentions(&pool).await, mentions);
    let sender: String =
        sqlx::query_scalar("SELECT encode(sender,'hex') FROM social_shared.ordinary_vote_targets")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(sender, public_key());
    let withdrawal = vote_at("kchat", &post, "unvote", 82, 2000);
    capture(&pool, &withdrawal, 1, true, false).await;
    consume(&upgraded).await;
    assert_eq!(count(&pool, "k_votes").await, 0);
    capture(&pool, &withdrawal, 2, false, false).await;
    consume(&upgraded).await;
    assert_eq!(votes(&pool).await, before);
    assert_eq!(vote_mentions(&pool).await, mentions);
    assert_eq!(count(&pool, "notification_probe").await, 0);
}

#[tokio::test]
async fn postgres_unvote_same_time_transaction_id_order_is_arrival_independent() {
    for later_first in [false, true] {
        let pool = setup().await;
        let consumer = SharedSocial::bind(pool.clone(), "testnet-10", SOURCE)
            .await
            .unwrap();
        let post = "ab".repeat(32);
        let old = vote_at("kchat", &post, "upvote", 91, 1000);
        let withdrawal = vote_at("kchat", &post, "unvote", 92, 1000);
        let mut later = old.clone();
        later.transaction_id = "5d".repeat(32);
        let order = if later_first {
            [&later, &old, &withdrawal]
        } else {
            [&old, &withdrawal, &later]
        };
        for value in order {
            capture(&pool, value, 1, true, false).await;
            consume(&consumer).await;
        }
        assert_eq!(count(&pool, "k_votes").await, 1);
        let selected: String =
            sqlx::query_scalar("SELECT encode(transaction_id,'hex') FROM k_votes")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(selected, later.transaction_id);
        assert_eq!(count(&pool, "social_shared_api.k_votes").await, 1);
        assert_eq!(count(&pool, "notification_probe").await, 0);
    }
}

#[tokio::test]
async fn pure_actual_kachat_unvotes_verify_in_owning_decoder_and_bind_value() {
    let (processor, pool) = processor();
    // Public chain receipts, signature verification only. Current chain/source
    // acceptance is verified separately through the normal live consumer.
    let cases = [
        (
            "0697c151f956d1c2daabb599aae9bb6e35111ad11feace7a745975d450a6e6d7",
            "6b636861743a313a766f74653a3032623863643437353438326631313839653134313336326564616530343066396234653633376661306237663536376666616262633463356135666661356632363a34613964633534363563383633623565336537376234633634363838616536393031643935383631626338623438336262636666323966336534366134633164383366626437373935663766376365313537353562323435626336393230623232346235653234623936376564336264346430353634326135613834616134343a333130613139633930393264323864653765663736613162626435633439383931636266323632366465323838393162376166303735343531653137383234313a756e766f74653a303264613431393135663432353439646133656639313034383332663730656433616662313766383939306564303364656539303231663834316465616665313134",
            1788022007878i64,
        ),
        (
            "0131c3cb21e5311654aa86c602b90cd1532e333176ead5c988d9308698b82d0c",
            "6b636861743a313a766f74653a3033373464663766326331633861343161333864623237376366346364366366613837386363396137383732393536396235616637383730646236383338373865313a32646539356538643561363466663731343033366332306464636537363838326334303665393630386533306131656662306162366261643564336263623564343536376434626234653165663762653061623437326637653033643637616339356335616338616463383136346462663465313231316666623861363539613a646165623964363732376263323537336631336664353433613165616265333431336664336266333064653461393739633365353135633064666330346665303a756e766f74653a303337346466376632633163386134316133386462323737636634636436636661383738636339613738373239353639623561663738373064623638333837386531",
            1790425927230i64,
        ),
    ];
    for (id, payload, time) in cases {
        let value = Transaction {
            transaction_id: id.into(),
            payload: Some(payload.into()),
            block_time: Some(time),
        };
        let result = processor
            .verify_kaposts_transaction(&value)
            .expect("actual KaChat unvote signature");
        assert!(matches!(
            result.action(),
            crate::kaposts::KaPostsAction::Unvote(_)
        ));
        assert!(processor.verify_transaction(&value).is_none());
        let wire = String::from_utf8(hex::decode(payload).unwrap()).unwrap();
        let mut fields: Vec<_> = wire.split(':').map(str::to_owned).collect();
        fields[6] = "upvote".into();
        let mut changed = value.clone();
        changed.payload = Some(hex::encode(fields.join(":")));
        assert!(
            processor.verify_transaction(&changed).is_none(),
            "signed value cannot change"
        );
        assert!(processor.verify_kaposts_transaction(&changed).is_none());
    }
    pool.close().await;
}
