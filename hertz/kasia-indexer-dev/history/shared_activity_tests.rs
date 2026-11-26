use super::*;
use kaspa_addresses::{Address, Version};
use kaspa_rpc_core::{RpcHeader, RpcScriptPublicKey};
use std::collections::HashSet;

fn block() -> Result<RpcBlock> {
    let mut script = vec![32];
    script.extend([8; 32]);
    script.push(0xac);
    let tx: RpcTransaction = serde_json::from_value(serde_json::json!({
        "version":0,"inputs":[],"outputs":[{"value":123,"scriptPublicKey":RpcScriptPublicKey::from_vec(0,script),"verboseData":null}],
        "lockTime":0,"subnetworkId":"0000000000000000000000000000000000000000","gas":0,"payload":"","mass":0,"storageMass":0,"verboseData":null
    }))?;
    Ok(RpcBlock {
        header: RpcHeader {
            hash: Default::default(),
            version: 1,
            parents_by_level: vec![],
            hash_merkle_root: Default::default(),
            accepted_id_merkle_root: Default::default(),
            utxo_commitment: Default::default(),
            timestamp: 2000,
            bits: 0,
            nonce: 0,
            daa_score: 55,
            blue_work: Default::default(),
            blue_score: 0,
            pruning_point: Default::default(),
        },
        transactions: vec![tx],
        verbose_data: None,
    })
}

fn address(network: &str) -> Result<(indexer_db::AddressPayload, String)> {
    let a = Address::new(configured_prefix(network)?, Version::PubKey, &[8; 32]);
    Ok((indexer_db::AddressPayload::try_from(&a)?, a.to_string()))
}

fn raw(block: &RpcBlock, network: &str) -> Result<Value> {
    let tx = &block.transactions[0];
    Ok(
        serde_json::json!({"transaction_id":format!("\\x{}",faster_hex::hex_string(&funds_transaction_id(tx)?)),
        "payload":null,"block_hash":format!("\\x{}",faster_hex::hex_string(&block.header.hash.as_bytes())),
        "block_time":block.header.timestamp,"inputs":null,"outputs":tx.outputs.iter().enumerate().map(|(i,o)|serde_json::json!({
            "index":i,"amount":o.value,"script_public_key":format!("\\x{}",faster_hex::hex_string(o.script_public_key.script())),
            "script_public_key_address":address(network).unwrap().1.split_once(':').unwrap().1
        })).collect::<Vec<_>>()}),
    )
}

#[test]
fn activity_proof_preserves_native_scripts_outputs_and_real_header() -> Result<()> {
    let (watched_address, _) = address("mainnet")?;
    indexer_actors::set_watch_only_addresses(HashSet::from([watched_address]));
    let mut b = block()?;
    let output = b.transactions[0].outputs[0].clone();
    b.transactions[0].outputs.push(output);
    let candidate = Candidate {
        id: funds_transaction_id(&b.transactions[0])?.to_vec(),
        body: raw(&b, "mainnet")?,
        accepting: vec![9; 32],
    };
    let tx = proof(&candidate, &b)?;
    let event = funds_event(tx, funds_transaction_id(tx)?, &b.header, || {
        Some(watched_address)
    })
    .unwrap();
    assert_eq!(
        event.credited,
        vec![(watched_address, 123), (watched_address, 123)]
    );
    assert_eq!(event.sender, Some(watched_address));
    assert_eq!(event.timestamp, 2000);
    assert_eq!(event.daa_score, 55);
    for mutation in [0, 1, 2, 3] {
        let mut changed = candidate.clone();
        match mutation {
            0 => changed.id[0] ^= 1,
            1 => changed.body["block_time"] = serde_json::json!(3000),
            2 => changed.body["outputs"][0]["amount"] = serde_json::json!(124),
            _ => changed.body["payload"] = serde_json::json!("\\x01"),
        }
        assert!(proof(&changed, &b).is_err());
    }
    for output in &mut b.transactions[0].outputs {
        output.script_public_key =
            RpcScriptPublicKey::from_vec(1, output.script_public_key.script().to_vec());
    }
    assert!(
        proof(&candidate, &b).is_err(),
        "SDK ID binds the omitted script version"
    );
    assert!(
        funds_event(
            &b.transactions[0],
            funds_transaction_id(&b.transactions[0])?,
            &b.header,
            || None
        )
        .is_none()
    );
    indexer_actors::set_watch_only_addresses(HashSet::new());
    Ok(())
}

fn fixture(network: &str) -> Result<(tempfile::TempDir, Activity)> {
    use std::str::FromStr;
    let url = std::env::var("PROTOCOL_NATIVE_NOTIFICATION_TEST_DSN")?;
    let mut client = postgres::Client::connect(&url, postgres::NoTls)?;
    let row=client.query_one("SELECT current_database(),system_identifier::text,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) FROM pg_control_system()",&[])?;
    let name: String = row.get(0);
    ensure!(
        name.starts_with("protocol_test_"),
        "refuse non-test database"
    );
    let config = postgres::Config::from_str(&url)?;
    let [postgres::config::Host::Tcp(host)] = config.get_hosts() else {
        anyhow::bail!("one private test host required")
    };
    let binding = serde_json::json!({"schema_version":1,"network":network,"source_revision":"test-pinned-source",
        "database":{"host":host,"port":config.get_ports().first().copied().unwrap_or(5432),"name":name,
            "system_identifier":row.get::<_,String>(1),"oid":row.get::<_,i64>(2)}});
    client.batch_execute("DROP SCHEMA IF EXISTS protocol_indexer CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public")?;
    client.batch_execute(include_str!("shared_activity_schema.sql"))?;
    let journal = "a0a0a0a0-1111-4444-8888-012345678901".to_owned();
    client.execute("INSERT INTO protocol_indexer.config(network,journal_id,source_revision,activated_at_ms) VALUES($1,$2::text::uuid,'test-pinned-source',1000)",&[&network,&journal])?;
    client.execute(
        "INSERT INTO protocol_indexer.notification_consumers VALUES($1,1000)",
        &[&CONSUMER],
    )?;
    let temp = tempfile::tempdir()?;
    let native = fjall::Config::new(temp.path()).open_transactional()?;
    apply_batch(
        &native,
        network,
        &JournalBatch {
            schema_version: 1,
            network: network.to_owned(),
            source_id: journal.clone(),
            notification: false,
            transactions: vec![],
        },
    )?;
    let acceptance = TxIDToAcceptancePartition::new(&native)?;
    Ok((
        temp,
        Activity {
            source: Source { client, binding },
            network: network.to_owned(),
            journal,
            native,
            acceptance,
        },
    ))
}

fn insert(activity: &mut Activity, b: &RpcBlock) -> Result<Vec<u8>> {
    let tx = &b.transactions[0];
    let id = funds_transaction_id(tx)?.to_vec();
    let time = b.header.timestamp as i64;
    let amount = tx.outputs[0].value as i64;
    let addr = address(&activity.network)?.1;
    let bare = addr.split_once(':').unwrap().1;
    activity.source.client.execute("INSERT INTO public.transactions(transaction_id,payload,block_time,block_hash,inputs,outputs)
        VALUES($1,NULL,$2,$3,NULL,ARRAY[ROW(0,$4,$5,$6,NULL,NULL)::transactions_outputs])",
        &[&id,&time,&b.header.hash.as_bytes().to_vec(),&amount,&tx.outputs[0].script_public_key.script(),&bare])?;
    Ok(id)
}

fn ready(activity: &mut Activity) -> Result<()> {
    activity.source.client.batch_execute(
        "UPDATE protocol_indexer.activity_pending SET available_at=clock_timestamp()",
    )?;
    Ok(())
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source"]
fn activity_real_source_queue_retry_epoch_reopen_and_reacceptance() -> Result<()> {
    for network in ["mainnet", "testnet-10"] {
        let (temp, mut activity) = fixture(network)?;
        let (watched_address, _) = address(network)?;
        indexer_actors::set_watch_only_addresses(HashSet::from([watched_address]));
        let b = block()?;
        let (sender, receiver) = flume::bounded(4);
        let id = funds_transaction_id(&b.transactions[0])?.to_vec();
        if network == "mainnet" {
            activity.source.client.execute(
                "INSERT INTO public.transactions_acceptances VALUES($1,$2,false)",
                &[&id, &vec![9u8; 32]],
            )?;
            assert!(activity.run_one_with_fetch(
                |_| panic!("acceptance without a committed body must not fetch"),
                &sender
            )?);
            insert(&mut activity, &b)?;
        } else {
            insert(&mut activity, &b)?;
            assert!(
                activity
                    .run_one_with_fetch(|_| panic!("unaccepted body must not fetch"), &sender)?
            );
            activity.source.client.execute(
                "INSERT INTO public.transactions_acceptances VALUES($1,$2,false)",
                &[&id, &vec![9u8; 32]],
            )?;
        }
        assert!(receiver.try_recv().is_err());
        let (full, _) = flume::bounded(0);
        assert!(
            activity
                .run_one_with_fetch(|_| Ok(b.clone()), &full)
                .is_err()
        );
        assert!(
            !activity
                .source
                .client
                .query_one(
                    "SELECT handled FROM protocol_indexer.activity_transactions",
                    &[]
                )?
                .get::<_, bool>(0)
        );
        ready(&mut activity)?;
        assert!(activity.run_one_with_fetch(|_| Ok(b.clone()), &sender)?);
        assert_eq!(receiver.try_recv()?.credited, vec![(watched_address, 123)]);
        let binding = activity.source.binding.clone();
        let journal = activity.journal.clone();
        drop(activity);
        let native = fjall::Config::new(temp.path()).open_transactional()?;
        let client = postgres::Client::connect(
            &std::env::var("PROTOCOL_NATIVE_NOTIFICATION_TEST_DSN")?,
            postgres::NoTls,
        )?;
        let mut activity = Activity {
            source: Source { client, binding },
            network: network.to_owned(),
            journal,
            acceptance: TxIDToAcceptancePartition::new(&native)?,
            native,
        };
        activity.source.client.execute(
            "DELETE FROM public.transactions_acceptances WHERE transaction_id=$1",
            &[&id],
        )?;
        activity.source.client.execute(
            "INSERT INTO public.transactions_acceptances VALUES($1,$2,false)",
            &[&id, &vec![10u8; 32]],
        )?;
        assert!(activity.run_one_with_fetch(
            |_| panic!("handled payment must not refetch on reopen/reacceptance"),
            &sender
        )?);
        assert!(receiver.try_recv().is_err());
        // An epoch advanced by the source makes queued current-body work quiet.
        activity.source.client.execute(
            "UPDATE protocol_indexer.activity_transactions SET handled=false",
            &[],
        )?;
        activity.source.client.execute("UPDATE protocol_indexer.notification_consumers SET activated_at_ms=3000 WHERE consumer=$1",&[&CONSUMER])?;
        activity.source.client.execute(
            "UPDATE public.transactions_acceptances SET block_hash=$1",
            &[&vec![11u8; 32]],
        )?;
        assert!(activity.run_one_with_fetch(|_| panic!("old epoch/body must not fetch"), &sender)?);
        assert!(receiver.try_recv().is_err());
    }
    indexer_actors::set_watch_only_addresses(HashSet::new());
    Ok(())
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source"]
fn activity_changed_source_and_locks_refuse_stale_rpc_proof() -> Result<()> {
    let (_temp, mut activity) = fixture("testnet-10")?;
    indexer_actors::set_watch_only_addresses(HashSet::from([address("testnet-10")?.0]));
    let b = block()?;
    let id = insert(&mut activity, &b)?;
    activity.source.client.execute(
        "INSERT INTO public.transactions_acceptances VALUES($1,$2,false)",
        &[&id, &vec![9u8; 32]],
    )?;
    let url = std::env::var("PROTOCOL_NATIVE_NOTIFICATION_TEST_DSN")?;
    let mut other = postgres::Client::connect(&url, postgres::NoTls)?;
    let (sender, receiver) = flume::bounded(4);
    {
        let mut guards = activity.source.client.transaction()?;
        assert!(
            fence(
                &mut guards,
                &activity.source.binding,
                &activity.network,
                &activity.journal,
                &activity.native,
                &id
            )?
            .is_some()
        );
        other.batch_execute("SET lock_timeout='100ms'")?;
        for statement in [
            "DELETE FROM public.transactions_acceptances",
            "UPDATE public.transactions SET block_time=3000",
            "UPDATE protocol_indexer.activity_transactions SET handled=true",
        ] {
            let error = other.batch_execute(statement).unwrap_err();
            assert_eq!(
                error.code(),
                Some(&postgres::error::SqlState::LOCK_NOT_AVAILABLE)
            );
        }
        guards.rollback()?;
    }

    assert!(activity.run_one_with_fetch(
        |_| {
            other.execute(
                "DELETE FROM public.transactions_acceptances WHERE transaction_id=$1",
                &[&id],
            )?;
            Ok(b.clone())
        },
        &sender
    )?);
    assert!(receiver.try_recv().is_err());
    other.execute(
        "INSERT INTO public.transactions_acceptances VALUES($1,$2,false)",
        &[&id, &vec![9u8; 32]],
    )?;
    let mut held = other.transaction()?;
    held.query_one(
        "SELECT transaction_id FROM public.transactions_acceptances FOR UPDATE",
        &[],
    )?;
    let start = std::time::Instant::now();
    assert!(activity.run_one_with_fetch(|_| panic!("busy acceptance must not fetch"), &sender)?);
    assert!(start.elapsed() < Duration::from_secs(1));
    held.rollback()?;
    ready(&mut activity)?;
    assert!(activity.run_one_with_fetch(
        |_| {
            other.execute(
                "UPDATE public.transactions SET block_time=3000 WHERE transaction_id=$1",
                &[&id],
            )?;
            Ok(b.clone())
        },
        &sender
    )?);
    assert!(receiver.try_recv().is_err());
    activity.source.binding["database"]["system_identifier"] = serde_json::json!("wrong-system");
    assert!(
        activity
            .run_one_with_fetch(|_| panic!("wrong source must not fetch"), &sender)
            .is_err()
    );
    indexer_actors::set_watch_only_addresses(HashSet::new());
    Ok(())
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source"]
fn activity_unwatched_batches_do_not_starve_watched_payment() -> Result<()> {
    let (_temp, mut activity) = fixture("testnet-10")?;
    indexer_actors::set_watch_only_addresses(HashSet::from([address("testnet-10")?.0]));
    let other_address =
        Address::new(configured_prefix("testnet-10")?, Version::PubKey, &[9; 32]).to_string();
    let bare = other_address.split_once(':').unwrap().1;
    let mut script = vec![32u8];
    script.extend([9u8; 32]);
    script.push(0xac);
    activity.source.client.execute(
        "INSERT INTO public.transactions(transaction_id,block_time,outputs)
        SELECT decode(lpad(to_hex(i),64,'0'),'hex'),2000,
            ARRAY[ROW(0,123,$1,$2,NULL,NULL)::transactions_outputs] FROM generate_series(1,10000) i",
        &[&script,&bare],
    )?;
    let b = block()?;
    let id = insert(&mut activity, &b)?;
    activity.source.client.execute(
        "INSERT INTO public.transactions_acceptances VALUES($1,$2,false)",
        &[&id, &vec![9u8; 32]],
    )?;
    let (sender, receiver) = flume::bounded(4);
    let mut fetches = 0;
    for _ in 0..3 {
        assert!(activity.run_one_with_fetch(
            |_| {
                fetches += 1;
                Ok(b.clone())
            },
            &sender
        )?);
        if fetches > 0 {
            break;
        }
    }
    assert_eq!(fetches, 1, "quiet work cannot starve a watched payment");
    assert_eq!(receiver.try_recv()?.credited[0].1, 123);
    while activity.run_one_with_fetch(|_| panic!("unwatched work must not fetch"), &sender)? {}
    assert_eq!(
        activity
            .source
            .client
            .query_one(
                "SELECT count(*) FROM protocol_indexer.activity_pending",
                &[]
            )?
            .get::<_, i64>(0),
        0
    );
    indexer_actors::set_watch_only_addresses(HashSet::new());
    Ok(())
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* source and configured native environment"]
fn activity_configured_epoch_stays_stable_and_upgrade_replay_stays_quiet() -> Result<()> {
    use std::io::Write;
    let (_temp, mut activity) = fixture("testnet-10")?;
    ensure!(
        std::env::var("PROTOCOL_DATABASE_URL")?
            == std::env::var("PROTOCOL_NATIVE_NOTIFICATION_TEST_DSN")?,
        "exact disposable source required"
    );
    let path = std::path::PathBuf::from(std::env::var("PROTOCOL_SOURCE_BINDING")?);
    ensure!(
        path == std::path::Path::new("/tmp/protocol-native-notification-source-binding.json"),
        "owned temporary binding required"
    );
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    struct Owned(std::path::PathBuf);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _owned = Owned(path);
    output.write_all(&serde_json::to_vec(&activity.source.binding)?)?;
    drop(output);
    activity.source.client.batch_execute("DELETE FROM protocol_indexer.notification_consumers WHERE consumer='native-address-push-v1';
        DROP TABLE protocol_indexer.activity_pending; DROP TABLE protocol_indexer.activity_transactions")?;
    assert!(Activity::configured(&activity.native, &Mutex::new(())).is_err());
    assert_eq!(
        activity
            .source
            .client
            .query_one(
                "SELECT count(*) FROM protocol_indexer.notification_consumers WHERE consumer=$1",
                &[&CONSUMER]
            )?
            .get::<_, i64>(0),
        0
    );
    let fixture_sql = include_str!("shared_activity_schema.sql");
    activity.source.client.batch_execute(
        &fixture_sql[fixture_sql
            .find("CREATE TABLE IF NOT EXISTS protocol_indexer.activity_transactions")
            .unwrap()..],
    )?;
    let mut b = block()?;
    b.header.timestamp = activity
        .source
        .client
        .query_one(
            "SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint+60000",
            &[],
        )?
        .get::<_, i64>(0) as u64;
    insert(&mut activity, &b)?;
    activity.source.client.batch_execute("DELETE FROM protocol_indexer.notification_consumers WHERE consumer='native-address-push-v1';
        UPDATE protocol_indexer.activity_transactions SET observed_at=to_timestamp(1);
        UPDATE protocol_indexer.activity_pending SET queued_at=to_timestamp(1)")?;
    let lock = Mutex::new(());
    let mut configured = Activity::configured(&activity.native, &lock)?;
    let epoch: i64 = activity
        .source
        .client
        .query_one(
            "SELECT activated_at_ms FROM protocol_indexer.notification_consumers WHERE consumer=$1",
            &[&CONSUMER],
        )?
        .get(0);
    assert!(epoch > 1000);
    drop(Activity::configured(&activity.native, &lock)?);
    assert_eq!(activity.source.client.query_one("SELECT activated_at_ms FROM protocol_indexer.notification_consumers WHERE consumer=$1",&[&CONSUMER])?.get::<_,i64>(0),epoch);
    indexer_actors::set_watch_only_addresses(HashSet::from([address("testnet-10")?.0]));
    let (sender, receiver) = flume::bounded(4);
    assert!(configured.run_one_with_fetch(
        |_| panic!(
            "pre-activation observed/queued work must remain quiet despite future block time"
        ),
        &sender
    )?);
    assert!(receiver.try_recv().is_err());
    indexer_actors::set_watch_only_addresses(HashSet::new());
    Ok(())
}
