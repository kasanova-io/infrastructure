use super::*;
use super::super::history::HistoryTx;
use super::super::shared_ingestion::apply_batch;
use kaspa_addresses::{Address, Prefix, Version};

fn batch(network: &str, payload: &[u8]) -> JournalBatch {
    let prefix = configured_prefix(network).unwrap();
    JournalBatch {
        schema_version: 1,
        source_id: "a0a0a0a0-1111-4444-8888-012345678901".into(),
        network: network.into(),
        notification: false,
        transactions: vec![JournalTx { revision: 1, transaction: HistoryTx {
            tx_id: "01".repeat(32),
            payload: faster_hex::hex_string(payload),
            sender: Address::new(prefix, Version::PubKey, &[7; 32]).to_string(),
            receiver: Address::new(prefix, Version::PubKey, &[8; 32]).to_string(),
            amount: 123,
            block_hash: "ab".repeat(32),
            block_time: 123456,
            accepting_block: Some("cd".repeat(32)),
            accepting_daa_score: Some(55),
        }}],
    }
}

fn source_fixture(batch: &JournalBatch) -> Result<Source> {
    let url = std::env::var("PROTOCOL_NATIVE_NOTIFICATION_TEST_DSN")
        .context("explicit disposable PostgreSQL source required")?;
    let mut client = postgres::Client::connect(&url, postgres::NoTls)?;
    let row=client.query_one("SELECT current_database(),system_identifier::text,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) FROM pg_control_system()", &[])?;
    let name:String=row.get(0);
    ensure!(name.starts_with("protocol_test_"), "refuse non-test database");
    let binding=serde_json::json!({"schema_version":1,"network":batch.network,"source_revision":"test-pinned-source",
        "database":{"name":name,"system_identifier":row.get::<_,String>(1),"oid":row.get::<_,i64>(2)}});
    client.batch_execute("DROP SCHEMA IF EXISTS protocol_indexer CASCADE;
        DROP TABLE IF EXISTS public.transactions_acceptances;
        CREATE SCHEMA protocol_indexer;
        CREATE TABLE protocol_indexer.config(network text,journal_id uuid,source_revision text);
        CREATE TABLE protocol_indexer.canonical(transaction_id bytea PRIMARY KEY,revision bigint,source_transaction jsonb,accepting_block bytea);
        CREATE TABLE public.transactions_acceptances(transaction_id bytea PRIMARY KEY,block_hash bytea,is_archive_seed boolean DEFAULT false);
        CREATE TABLE protocol_indexer.pending(transaction_id bytea PRIMARY KEY,acceptance_changed boolean);
        CREATE TABLE protocol_indexer.events(event_id bigint PRIMARY KEY,transaction_id bytea,notify_allowed boolean,recorded_at timestamptz DEFAULT clock_timestamp(),source_transaction jsonb);
        CREATE TABLE protocol_indexer.deliveries(consumer text,event_id bigint,PRIMARY KEY(consumer,event_id));
        CREATE TABLE protocol_indexer.notification_consumers(consumer text PRIMARY KEY,activated_at_ms bigint);
        INSERT INTO protocol_indexer.notification_consumers VALUES('native-chat-push-v1',1000)")?;
    client.execute("INSERT INTO protocol_indexer.config VALUES($1,$2::text::uuid,'test-pinned-source')", &[&batch.network,&batch.source_id])?;
    let t=&batch.transactions[0];let id=fixed::<32>(t.transaction.tx_id.as_bytes())?.to_vec();
    let body=serde_json::json!({"payload":format!("\\x{}",t.transaction.payload),"block_time":t.transaction.block_time}).to_string();
    let block=fixed::<32>(t.transaction.accepting_block.as_deref().unwrap().as_bytes())?.to_vec();
    client.execute("INSERT INTO protocol_indexer.canonical VALUES($1,$2,$3::text::jsonb,$4)", &[&id,&(t.revision as i64),&body,&block])?;
    client.execute("INSERT INTO public.transactions_acceptances VALUES($1,$2,false)", &[&id,&block])?;
    client.execute("INSERT INTO protocol_indexer.events(event_id,transaction_id,notify_allowed,source_transaction) VALUES(1,$1,true,$2::text::jsonb)", &[&id,&body])?;
    Ok(Source { client, binding })
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source"]
fn imports_stay_quiet_and_live_receipts_survive_native_restart() -> Result<()> {
    for network in ["mainnet", "testnet-10"] {
        let temp = tempfile::tempdir()?;
        let db = fjall::Config::new(temp.path()).open_transactional()?;
        let mut request = batch(network, b"ciph_msg:1:handshake:sealed");
        let mut source = source_fixture(&request)?;
        let (tx, rx) = flume::bounded(4);
        apply_batch(&db, network, &request)?;
        assert!(rx.try_recv().is_err(), "imports never enqueue a push");
        assert!(notify_with_source(&db, network, &request, Some(&tx), &mut source).is_err());
        request.notification = true;
        assert!(apply_batch(&db, network, &request).is_err());
        assert_eq!(notify_with_source(&db, network, &request, Some(&tx), &mut source)?.applied, 1);
        let sent = rx.try_recv()?;
        assert!(matches!(sent.kind, PushEventKind::Handshake));
        assert_eq!(sent.watched_address, address_on_network(&request.transactions[0].transaction.receiver, configured_prefix(network)?)?);
        assert_eq!(sent.payload.as_deref(), Some("sealed"));
        assert_eq!(sent.tx_id, [1; 32]);
        assert_eq!(sent.daa_score, 55);
        assert_eq!(notify_with_source(&db, network, &request, Some(&tx), &mut source)?.duplicates, 1);
        drop(db);
        let db = fjall::Config::new(temp.path()).open_transactional()?;
        assert_eq!(notify_with_source(&db, network, &request, Some(&tx), &mut source)?.duplicates, 1);
        assert!(rx.try_recv().is_err(), "HTTP retry and reopen do not requeue");
    }
    Ok(())
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source"]
fn rejected_queue_admission_keeps_retry_possible() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = fjall::Config::new(temp.path()).open_transactional()?;
    let mut request = batch("testnet-10", b"kchat:1:comm:alias:sealed");
        let mut source = source_fixture(&request)?;
    apply_batch(&db, "testnet-10", &request)?;
    request.notification = true;
    assert!(notify_with_source(&db, "testnet-10", &request, None, &mut source).is_err());
    let (tx, rx) = flume::bounded(1);
    tx.try_send(event(&request.transactions[0], "testnet-10")?.unwrap())?;
    assert!(notify_with_source(&db, "testnet-10", &request, Some(&tx), &mut source).is_err());
    rx.try_recv()?;
    assert_eq!(notify_with_source(&db, "testnet-10", &request, Some(&tx), &mut source)?.applied, 1);
    assert!(rx.try_recv().is_ok());
    Ok(())
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source"]
fn forged_future_revoked_or_foreign_source_cannot_notify() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = fjall::Config::new(temp.path()).open_transactional()?;
    let mut request = batch("testnet-10", b"kchat:1:handshake:sealed");
        let mut source = source_fixture(&request)?;
    let (tx, rx) = flume::bounded(4);
    request.notification = true;
    assert!(notify_with_source(&db, "testnet-10", &request, Some(&tx), &mut source).is_err());
    request.notification = false;
    apply_batch(&db, "testnet-10", &request)?;
    request.notification = true;
    for changed in [
        { let mut b=request.clone(); b.network="mainnet".into(); b },
        { let mut b=request.clone(); b.source_id="b0b0b0b0-1111-4444-8888-012345678901".into(); b },
        { let mut b=request.clone(); b.transactions[0].revision+=1; b },
        { let mut b=request.clone(); b.transactions[0].transaction.amount+=1; b },
        { let mut b=request.clone(); b.transactions[0].transaction.accepting_daa_score=Some(56); b },
    ] {
        assert!(notify_with_source(&db, "testnet-10", &changed, Some(&tx), &mut source).is_err());
    }
    let mut revoked = request.clone();
    revoked.notification=false;
    revoked.transactions[0].revision=2;
    revoked.transactions[0].transaction.accepting_block=None;
    revoked.transactions[0].transaction.accepting_daa_score=None;
    apply_batch(&db, "testnet-10", &revoked)?;
    assert!(notify_with_source(&db, "testnet-10", &request, Some(&tx), &mut source).is_err());
    revoked.notification=true;
    assert!(notify_with_source(&db, "testnet-10", &revoked, Some(&tx), &mut source).is_err());
    assert!(rx.try_recv().is_err());
    Ok(())
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source"]
fn same_tx_reacceptance_and_already_notified_history_do_not_repeat() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = fjall::Config::new(temp.path()).open_transactional()?;
    let mut request=batch("testnet-10", b"kchat:1:handshake:sealed");
        let mut source = source_fixture(&request)?;
    let (tx, rx)=flume::bounded(4);
    apply_batch(&db, "testnet-10", &request)?;
    request.notification=true;
    notify_with_source(&db, "testnet-10", &request, Some(&tx), &mut source)?;
    rx.try_recv()?;
    request.notification=false;
    request.transactions[0].revision=2;
    request.transactions[0].transaction.accepting_block=None;
    request.transactions[0].transaction.accepting_daa_score=None;
    apply_batch(&db, "testnet-10", &request)?;
    request.transactions[0].revision=3;
    request.transactions[0].transaction.accepting_block=Some("ef".repeat(32));
    request.transactions[0].transaction.accepting_daa_score=Some(57);
    apply_batch(&db, "testnet-10", &request)?;
    source.client.execute("UPDATE protocol_indexer.canonical SET revision=3,accepting_block=decode(repeat('ef',32),'hex')", &[])?;
    source.client.execute("UPDATE public.transactions_acceptances SET block_hash=decode(repeat('ef',32),'hex')", &[])?;
    request.notification=true;
    assert_eq!(notify_with_source(&db, "testnet-10", &request, Some(&tx), &mut source)?.duplicates, 1);
    assert!(rx.try_recv().is_err());
    Ok(())
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source"]
fn timed_out_http_work_rechecks_source_before_late_native_admission() -> Result<()> {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    let temp = tempfile::tempdir()?;
    let db = fjall::Config::new(temp.path()).open_transactional()?;
    let mut request = batch("testnet-10", b"kchat:1:handshake:sealed");
    let mut source = source_fixture(&request)?;
    apply_batch(&db, "testnet-10", &request)?;
    request.notification = true;
    let (push_tx, push_rx) = flume::bounded(4);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let body = serde_json::to_vec(&request)?;
    let db_copy = db.clone();
    let worker = std::thread::spawn(move || -> Result<()> {
        let (mut socket, _) = listener.accept()?;
        socket.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
        let mut bytes = Vec::new();
        let split = loop {
            let mut byte = [0]; socket.read_exact(&mut byte)?; bytes.push(byte[0]);
            if bytes.ends_with(b"\r\n\r\n") { break bytes.len(); }
            ensure!(bytes.len() < 4096, "bounded fixture headers");
        };
        let mut received = vec![0; body.len()]; socket.read_exact(&mut received)?;
        assert_eq!(received, body);
        assert!(split > 0);
        let incoming: JournalBatch = serde_json::from_slice(&received)?;
        ready_tx.send(())?;
        // Represents queued blocking handler work after its caller times out.
        release_rx.recv_timeout(std::time::Duration::from_secs(3))?;
        assert!(notify_with_source(&db_copy, "testnet-10", &incoming, Some(&push_tx), &mut source).is_err());
        let _ = socket.write_all(b"HTTP/1.1 422 Unprocessable Entity\r\nContent-Length: 0\r\n\r\n");
        Ok(())
    });
    let mut caller = TcpStream::connect(address)?;
    caller.set_read_timeout(Some(std::time::Duration::from_millis(50)))?;
    let body = serde_json::to_vec(&request)?;
    caller.write_all(format!("POST /fixture-notify HTTP/1.1\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes())?;
    caller.write_all(&body)?;
    ready_rx.recv_timeout(std::time::Duration::from_secs(3))?;
    let mut byte = [0];
    let error = caller.read(&mut byte).unwrap_err();
    assert!(matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut));
    drop(caller);
    let mut vcp = postgres::Client::connect(&std::env::var("PROTOCOL_NATIVE_NOTIFICATION_TEST_DSN")?, postgres::NoTls)?;
    vcp.execute("DELETE FROM public.transactions_acceptances", &[])?;
    release_tx.send(())?;
    worker.join().map_err(|_| anyhow::anyhow!("delayed fixture handler panicked"))??;
    assert!(push_rx.try_recv().is_err(), "late work cannot emit from stale native acceptance");
    assert_eq!(vcp.query_one("SELECT count(*) FROM protocol_indexer.deliveries", &[])?.get::<_,i64>(0), 0);
    Ok(())
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source"]
fn notification_epoch_suppresses_old_eligible_native_history_but_allows_new_acceptance() -> Result<()> {
    let temp=tempfile::tempdir()?;
    let db=fjall::Config::new(temp.path()).open_transactional()?;
    let mut old=batch("testnet-10",b"kchat:1:handshake:sealed");
    let mut source=source_fixture(&old)?;
    apply_batch(&db,"testnet-10",&old)?;
    source.client.execute("UPDATE protocol_indexer.notification_consumers SET activated_at_ms=123457", &[])?;
    old.notification=true;
    let (tx,rx)=flume::bounded(4);
    assert!(notify_with_source(&db,"testnet-10",&old,Some(&tx),&mut source).is_err());
    let mut fresh=old.clone();fresh.notification=false;
    fresh.transactions[0].transaction.tx_id="02".repeat(32);
    fresh.transactions[0].transaction.block_time=123458;
    apply_batch(&db,"testnet-10",&fresh)?;
    source.client.batch_execute("INSERT INTO protocol_indexer.canonical
        SELECT decode(repeat('02',32),'hex'),revision,jsonb_set(source_transaction,'{block_time}','123458'),accepting_block FROM protocol_indexer.canonical;
        INSERT INTO public.transactions_acceptances SELECT decode(repeat('02',32),'hex'),block_hash,false FROM public.transactions_acceptances;
        INSERT INTO protocol_indexer.events SELECT 2,decode(repeat('02',32),'hex'),true,clock_timestamp(),jsonb_set(source_transaction,'{block_time}','123458') FROM protocol_indexer.events")?;
    fresh.notification=true;
    assert_eq!(notify_with_source(&db,"testnet-10",&fresh,Some(&tx),&mut source)?.applied,1);
    assert_eq!(rx.try_recv()?.tx_id,[2;32]);
    assert!(rx.try_recv().is_err());
    Ok(())
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source"]
fn sql_source_revision_pending_removal_binding_and_missing_epoch_fail_closed() -> Result<()> {
    let temp=tempfile::tempdir()?;
    let db=fjall::Config::new(temp.path()).open_transactional()?;
    let mut request=batch("testnet-10",b"kchat:1:handshake:sealed");
    let mut source=source_fixture(&request)?;
    apply_batch(&db,"testnet-10",&request)?;request.notification=true;
    let (tx,rx)=flume::bounded(4);
    for (mutation,restore) in [
        ("UPDATE protocol_indexer.canonical SET revision=2","UPDATE protocol_indexer.canonical SET revision=1"),
        ("UPDATE protocol_indexer.events SET recorded_at=to_timestamp(0)","UPDATE protocol_indexer.events SET recorded_at=clock_timestamp()"),
        ("INSERT INTO protocol_indexer.pending SELECT transaction_id,true FROM protocol_indexer.canonical","DELETE FROM protocol_indexer.pending"),
        ("UPDATE public.transactions_acceptances SET is_archive_seed=true","UPDATE public.transactions_acceptances SET is_archive_seed=false"),
        ("UPDATE protocol_indexer.config SET network='mainnet'","UPDATE protocol_indexer.config SET network='testnet-10'"),
        ("UPDATE protocol_indexer.config SET journal_id='b0b0b0b0-1111-4444-8888-012345678901'","UPDATE protocol_indexer.config SET journal_id='a0a0a0a0-1111-4444-8888-012345678901'"),
        ("DELETE FROM protocol_indexer.notification_consumers","INSERT INTO protocol_indexer.notification_consumers VALUES('native-chat-push-v1',1000)"),
    ] {
        source.client.batch_execute(mutation)?;
        assert!(notify_with_source(&db,"testnet-10",&request,Some(&tx),&mut source).is_err());
        source.client.batch_execute(restore)?;
        assert!(rx.try_recv().is_err());
    }
    let actual=source.binding["database"]["system_identifier"].clone();
    source.binding["database"]["system_identifier"]=serde_json::json!("wrong-system");
    assert!(notify_with_source(&db,"testnet-10",&request,Some(&tx),&mut source).is_err());
    source.binding["database"]["system_identifier"]=actual;
    assert_eq!(notify_with_source(&db,"testnet-10",&request,Some(&tx),&mut source)?.applied,1);
    Ok(())
}

#[test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source"]
fn actual_admission_holds_source_locks_and_skips_busy_source_without_inversion() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = fjall::Config::new(temp.path()).open_transactional()?;
    let mut request = batch("testnet-10", b"kchat:1:handshake:sealed");
    let mut source = source_fixture(&request)?;
    apply_batch(&db, "testnet-10", &request)?; request.notification = true;
    let url = std::env::var("PROTOCOL_NATIVE_NOTIFICATION_TEST_DSN")?;
    let mut other = postgres::Client::connect(&url, postgres::NoTls)?;
    let mut held = other.transaction()?;
    held.query_one("SELECT transaction_id FROM public.transactions_acceptances FOR UPDATE", &[])?;
    let (push_tx, push_rx) = flume::bounded(4);
    let start = std::time::Instant::now();
    assert!(notify_with_source(&db, "testnet-10", &request, Some(&push_tx), &mut source).is_err());
    assert!(start.elapsed() < std::time::Duration::from_secs(1), "busy source skips instead of reversing lock order");
    held.rollback()?;
    let incoming = &request.transactions[0];
    let id = fixed::<32>(incoming.transaction.tx_id.as_bytes())?.to_vec();
    let accepted = incoming.transaction.accepting_block.as_deref().unwrap();
    let sql = source.client.transaction()?;
    let result = admit(sql, &source.binding, "testnet-10", &request, incoming, &id, accepted, |push| {
        // Both independent writers attempt the actual source rows exactly
        // while production admission holds its guards at queue insertion.
        for statement in ["DELETE FROM public.transactions_acceptances", "UPDATE protocol_indexer.canonical SET revision=2"] {
            let mut writer = postgres::Client::connect(&url, postgres::NoTls)?;
            writer.batch_execute("SET lock_timeout='100ms'")?;
            let error = writer.batch_execute(statement).unwrap_err();
            assert_eq!(error.code(), Some(&postgres::error::SqlState::LOCK_NOT_AVAILABLE));
        }
        push_tx.try_send(push)?;
        Ok(())
    })?;
    assert_eq!(result.applied, 1);
    assert_eq!(push_rx.try_recv()?.tx_id, [1; 32]);
    other.batch_execute("UPDATE protocol_indexer.canonical SET revision=2; DELETE FROM public.transactions_acceptances")?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit disposable protocol_test_* PostgreSQL source and configured private handler environment"]
async fn actual_private_handler_checks_configured_binding_and_uses_existing_push_queue() -> Result<()> {
    use axum::{Router, routing::post};
    let temp = tempfile::tempdir()?;
    let db = fjall::Config::new(temp.path()).open_transactional()?;
    let mut request = batch("testnet-10", b"kchat:1:handshake:sealed");
    let setup = request.clone();
    let binding = tokio::task::spawn_blocking(move || -> Result<serde_json::Value> {
        let mut source = source_fixture(&setup)?;
        let test_url = std::env::var("PROTOCOL_NATIVE_NOTIFICATION_TEST_DSN")?;
        ensure!(std::env::var("PROTOCOL_DATABASE_URL")? == test_url, "handler must use exact disposable source");
        use std::str::FromStr;
        let config = postgres::Config::from_str(&test_url)?;
        let [postgres::config::Host::Tcp(host)] = config.get_hosts() else { anyhow::bail!("one private test host required") };
        source.binding["database"]["host"] = serde_json::json!(host);
        source.binding["database"]["port"] = serde_json::json!(config.get_ports().first().copied().unwrap_or(5432));
        Ok(source.binding.clone())
    }).await??;
    let path = std::path::PathBuf::from(std::env::var("PROTOCOL_SOURCE_BINDING")?);
    ensure!(path == std::path::Path::new("/tmp/protocol-native-notification-source-binding.json"), "owned temporary binding path required");
    {
        use std::io::Write;
        let mut output = std::fs::OpenOptions::new().write(true).create_new(true).open(&path)?;
        output.write_all(&serde_json::to_vec(&binding)?)?;
    }
    let (push_tx, push_rx) = flume::bounded(4);
    let mut state = super::super::export::ExportApi::new(db.clone());
    state.shared_push_tx = Some(push_tx);
    let app = Router::new().route("/history-import", post(super::super::history::import_history).with_state(state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).with_graceful_shutdown(async { let _ = stop_rx.await; }).await
    });
    let client = reqwest::Client::new();
    let url = format!("http://{address}/history-import");
    let secret = std::env::var("KASANOVA_SHARED_INGEST_TOKEN")?;
    let rejected = client.post(&url).json(&request).send().await?;
    assert_eq!(rejected.status(), reqwest::StatusCode::FORBIDDEN);
    let imported = client.post(&url).bearer_auth(&secret).json(&request).send().await?;
    assert_eq!(imported.status(), reqwest::StatusCode::OK);
    assert!(push_rx.try_recv().is_err());
    request.notification = true;
    let mut wrong = binding.clone(); wrong["database"]["host"] = serde_json::json!("wrong-source");
    std::fs::write(&path, serde_json::to_vec(&wrong)?)?;
    let rejected = client.post(&url).bearer_auth(&secret).json(&request).send().await?;
    assert_eq!(rejected.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let message = rejected.text().await?;
    ensure!(!message.contains(&secret) && !message.contains(&std::env::var("PROTOCOL_DATABASE_URL")?), "credential-free error response required");
    assert!(push_rx.try_recv().is_err());
    std::fs::write(&path, serde_json::to_vec(&binding)?)?;
    let admitted = client.post(&url).bearer_auth(&secret).json(&request).send().await?;
    assert_eq!(admitted.status(), reqwest::StatusCode::OK);
    assert_eq!(admitted.json::<serde_json::Value>().await?["applied"], 1);
    assert_eq!(push_rx.try_recv()?.tx_id, [1; 32]);
    let retry = client.post(&url).bearer_auth(&secret).json(&request).send().await?;
    assert_eq!(retry.status(), reqwest::StatusCode::OK);
    assert_eq!(retry.json::<serde_json::Value>().await?["duplicates"], 1);
    assert!(push_rx.try_recv().is_err());
    stop_tx.send(()).map_err(|_| anyhow::anyhow!("fixture server already stopped"))?;
    server.await??;
    std::fs::remove_file(path)?;
    Ok(())
}

#[test]
fn contextual_alias_and_binary_payload_match_live_event_contract() -> Result<()> {
    for network in ["mainnet", "testnet-10"] {
        for root in ["kchat", "ciph_msg"] {
            for alias in [b"abcdefghijklmnop_LONG_ALIAS".as_slice(), b"a\0b", b""] {
                let mut wire=format!("{root}:1:comm:").into_bytes();
                wire.extend_from_slice(alias);wire.push(b':');wire.extend_from_slice(b"\xff\0:sealed");
                let request=batch(network,&wire);
                let sent=event(&request.transactions[0], network)?.unwrap();
                assert!(matches!(sent.kind, PushEventKind::Contextual));
                assert_eq!(sent.alias,Some(String::from_utf8_lossy(alias).into_owned()));
                assert_eq!(sent.payload,Some(String::from_utf8_lossy(b"\xff\0:sealed").into_owned()));
                assert_eq!(sent.watched_address,sent.sender);
            }
        }
    }
    Ok(())
}

#[test]
fn tip_notification_preserves_real_ciphertext_wire_representation() -> Result<()> {
    // Existing accepted public TN10 7cb3e349 vector; no chain broadcast.
    let wire = "kchat:1:tip:03b736ec7820b077f71daee7dae1b62d910983f25b11ff7fb87a79caa2e37d264f:32c541d3a9e6d58bc867f3e4d5d788a362ca29a94c533b2d51b97eae76abc9adaef2dc45640e190974b4734f2836781c402ccccf16b990098aa81048780b1909:f3941dc64ae4db441c746f7fd6bd7d6d44436ef84cf61c02c6516e6d4f8f4b61:022037bed70bc8f3d71a5696616c4f1a5cdf4239ea5c6e1774a0b48a3a6b473943:77a9b66575b85cbeff35deee0290e59cd739af5c965b558fd6858434193ef97236a7549128f482d796728dd75d997d5ea35cb4755ea8ab5fe098a6a7e8752a28855035d4f9da8df915007bbab9748c7c9b30c16fbf9f33f53803c92dab9281f2e855792b36d97dc1fd89fb05bfb9ed635826d2bc710ef4e08fccf1b8655d85d73224d7f7d7936f9287654ac5c94c286486669241";
    let request = batch("testnet-10", wire.as_bytes());
    let sent = event(&request.transactions[0], "testnet-10")?.unwrap();
    assert!(matches!(sent.kind, PushEventKind::Payment));
    assert_eq!(sent.payload.as_deref(), wire.rsplit(':').next());
    for bytes in [b"kchat:1:pay:77a9b665".as_slice(), b"ciph_msg:1:pay:\xff\0:sealed"] {
        let request = batch("testnet-10", bytes);
        let sent = event(&request.transactions[0], "testnet-10")?.unwrap();
        let tail = bytes.splitn(4, |b| *b == b':').nth(3).unwrap();
        assert_eq!(sent.payload, Some(String::from_utf8_lossy(tail).into_owned()));
    }
    Ok(())
}

#[test]
fn dm_payment_stash_and_group_events_keep_native_recipient_rules() -> Result<()> {
    let network="testnet-10";
    let dm=batch(network,format!("kchat:1:dm:{}:alias:sealed","ab".repeat(16)).as_bytes());
    assert!(matches!(event(&dm.transactions[0],network)?.unwrap().kind,PushEventKind::Contextual));
    let mut payment=batch(network,b"kchat:1:pay:sealed");
    let sent=event(&payment.transactions[0],network)?.unwrap();
    assert!(matches!(sent.kind,PushEventKind::Payment));
    assert_eq!(sent.watched_address,sent.receiver);
    assert_eq!(sent.amount,Some(123));
    payment.transactions[0].transaction.receiver=payment.transactions[0].transaction.sender.clone();
    assert!(event(&payment.transactions[0],network)?.is_none());
    let stash=batch(network,b"ciph_msg:1:self_stash:scope:sealed");
    let sent=event(&stash.transactions[0],network)?.unwrap();
    assert!(matches!(sent.kind,PushEventKind::SelfStash));
    assert_eq!(sent.alias.as_deref(),Some("scope"));
    assert_eq!(sent.watched_address,sent.sender);
    let control=batch(network,format!("kchat:1:gctl:{}:aabb","08".repeat(32)).as_bytes());
    let sent=event(&control.transactions[0],network)?.unwrap();
    assert!(matches!(sent.kind,PushEventKind::GroupControl));
    assert_eq!(sent.group_control_recipient,Some(AddressPayload::from_xonly_pubkey([8;32])));
    assert_eq!(sent.payload.as_deref(),Some("aabb"));
    let group=batch(network,format!("kchat:1:gcomm:{}:1:{}:{}:{}:aabb:{}",
        "ab".repeat(32),"09".repeat(32),"07".repeat(32),"01".repeat(24),"02".repeat(64)).as_bytes());
    let sent=event(&group.transactions[0],network)?.unwrap();
    assert!(matches!(sent.kind,PushEventKind::GroupMessage));
    assert_eq!(sent.blinded_group_id,Some([0xab;32]));
    assert_eq!(sent.watched_address,AddressPayload::default());
    // All prefixes use the configured network, never a hardcoded TN10 address.
    assert!(configured_prefix("mainnet")? == Prefix::Mainnet);
    Ok(())
}
