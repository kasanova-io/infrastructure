// These tests call the real signature parser and broadcast persistence method.
// The public signed fixture is unchanged. Alternate IDs/chain times are fixture
// envelopes, prepared only inside the separately approved isolated database.
#[cfg(test)]
mod profile_guard_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use tokio::time::{timeout, Duration};

    async fn pool() -> DbPool {
        let url = std::env::var("KSNV411_PROFILE_FIXTURE_DATABASE_URL")
            .expect("The reviewed private fixture URL is required");
        let pool = PgPoolOptions::new().max_connections(6).connect(&url).await
            .expect("Private fixture connection failed");
        let name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(name, "social_history_profile_guard_fixture_ksnv411_20261010");
        sqlx::query("TRUNCATE k_broadcasts RESTART IDENTITY")
            .execute(&pool).await.unwrap();
        pool
    }

    fn record(processor: &KProtocolProcessor, id: u8, time: i64) -> (Transaction, KBroadcast) {
        let fixture: serde_json::Value = serde_json::from_str(include_str!("signed-profile-fixture.json"))
            .expect("Reviewed public signed fixture differs");
        let mut transaction = Transaction {
            transaction_id: fixture["transaction_id"].as_str().unwrap().to_owned(),
            payload: Some(fixture["payload"].as_str().unwrap().to_owned()),
            block_time: Some(fixture["block_time"].as_i64().unwrap()),
        };
        let payload = String::from_utf8(hex::decode(transaction.payload.as_ref().unwrap()).unwrap()).unwrap();
        let broadcast = match processor.parse_k_protocol_payload(&payload).unwrap() {
            KActionType::Broadcast(broadcast) => broadcast,
            _ => panic!("Fixture is not a broadcast"),
        };
        transaction.transaction_id = hex::encode([id; 32]);
        transaction.block_time = Some(time);
        (transaction, broadcast)
    }

    async fn save(processor: &KProtocolProcessor, id: u8, time: i64) {
        let (transaction, broadcast) = record(processor, id, time);
        timeout(Duration::from_secs(10), processor.save_k_broadcast_to_database(&transaction, broadcast))
            .await.expect("Native method exceeded fixture limit").unwrap();
    }

    async fn full_rows(pool: &DbPool) -> String {
        sqlx::query_scalar("SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY id),'[]')::text FROM k_broadcasts t")
            .fetch_one(pool).await.unwrap()
    }

    async fn assert_current(pool: &DbPool, id: u8, time: i64) {
        let rows: Vec<(String,i64)> = sqlx::query_as("SELECT encode(transaction_id,'hex'),block_time FROM k_broadcasts ORDER BY id")
            .fetch_all(pool).await.unwrap();
        assert_eq!(rows, vec![(hex::encode([id;32]),time)]);
    }

    #[tokio::test]
    async fn delayed_fetched_old_cannot_replace_newer() {
        let pool=pool().await; let processor=KProtocolProcessor::new(pool.clone());
        let (old, broadcast)=record(&processor,1,100);
        save(&processor,2,101).await;
        let before=full_rows(&pool).await;
        processor.save_k_broadcast_to_database(&old,broadcast).await.unwrap();
        assert_eq!(before,full_rows(&pool).await);assert_current(&pool,2,101).await;
    }

    #[tokio::test]
    async fn exact_duplicate_preserves_destination_id_and_all_fields() {
        let pool=pool().await; let processor=KProtocolProcessor::new(pool.clone());
        save(&processor,1,100).await;let before=full_rows(&pool).await;
        save(&processor,1,100).await;assert_eq!(before,full_rows(&pool).await);
    }

    #[tokio::test]
    async fn concurrent_newer_writes_keep_strictly_latest() {
        let pool=pool().await;let first=KProtocolProcessor::new(pool.clone());let second=KProtocolProcessor::new(pool.clone());
        tokio::join!(save(&first,1,100),save(&second,2,101));assert_current(&pool,2,101).await;
    }

    #[tokio::test]
    async fn equal_chain_times_preserve_incumbent_without_id_order() {
        let pool=pool().await;let processor=KProtocolProcessor::new(pool.clone());
        save(&processor,9,100).await;save(&processor,1,100).await;assert_current(&pool,9,100).await;
        sqlx::query("TRUNCATE k_broadcasts RESTART IDENTITY").execute(&pool).await.unwrap();
        save(&processor,1,100).await;save(&processor,9,100).await;assert_current(&pool,1,100).await;
    }

    #[tokio::test]
    async fn invalid_signature_cannot_delete_current_profile() {
        let pool=pool().await;let processor=KProtocolProcessor::new(pool.clone());
        save(&processor,1,100).await;let before=full_rows(&pool).await;
        let (transaction,mut broadcast)=record(&processor,2,101);broadcast.sender_signature="00".repeat(64);
        processor.save_k_broadcast_to_database(&transaction,broadcast).await.unwrap();
        assert_eq!(before,full_rows(&pool).await);
    }

    #[tokio::test]
    async fn importer_table_lock_precedes_fresh_worker_snapshot() {
        let pool=pool().await;let processor=KProtocolProcessor::new(pool.clone());
        let (old,broadcast)=record(&processor,1,100);
        let mut importer=pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE k_broadcasts IN SHARE ROW EXCLUSIVE MODE").execute(&mut *importer).await.unwrap();
        // Fixture setup represents a newer row committed by the missing-only importer.
        sqlx::query("INSERT INTO k_broadcasts(transaction_id,block_time,sender_pubkey,sender_signature,base64_encoded_nickname,base64_encoded_profile_image,base64_encoded_message) VALUES($1,101,$2,$3,$4,$5,$6)")
            .bind(vec![2u8;32]).bind(hex::decode(&broadcast.sender_pubkey).unwrap())
            .bind(hex::decode(&broadcast.sender_signature).unwrap()).bind(&broadcast.base64_encoded_nickname)
            .bind(&broadcast.base64_encoded_profile_image).bind(&broadcast.base64_encoded_message)
            .execute(&mut *importer).await.unwrap();
        let worker=tokio::spawn(async move {processor.save_k_broadcast_to_database(&old,broadcast).await});
        timeout(Duration::from_secs(5),async {
            loop {
                let blocked: i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query='LOCK TABLE k_broadcasts IN ROW EXCLUSIVE MODE'")
                    .fetch_one(&pool).await.unwrap();
                if blocked==1 {break;}
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("Worker table-lock wait was not observed");
        importer.commit().await.unwrap();
        timeout(Duration::from_secs(5),worker).await.unwrap().unwrap().unwrap();
        assert_current(&pool,2,101).await;
    }
}
