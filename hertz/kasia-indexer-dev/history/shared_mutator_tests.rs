//! Actual native handlers + production middleware decision, no global env changes.
use super::super::{contextual_messages::ContextualMessageApi, export};
use super::*;
use axum::{Router, middleware::from_fn, routing::post};
use indexer_db::messages::contextual_message::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn snapshot(db: &TxKeyspace) -> Result<Vec<(String, Vec<u8>, Vec<u8>)>> {
    let mut rows = Vec::new();
    let tx = db.read_tx();
    for name in export::EXPORT_PARTITIONS.iter().copied().chain([
        "kasanova_shared_ingestion",
        "kasanova_shared_ingestion_config",
        "kasanova_history_import",
        "tx_id_to_acceptance",
    ]) {
        let part = db.open_partition(name, PartitionCreateOptions::default())?;
        for value in tx.iter(&part) {
            let (k, v) = value?;
            rows.push((name.into(), k.to_vec(), v.to_vec()));
        }
    }
    Ok(rows)
}

async fn serve_handlers(
    db: &TxKeyspace,
    shared: bool,
    temp: &std::path::Path,
) -> Result<(
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<std::io::Result<()>>,
)> {
    let config = serde_json::from_value(
        serde_json::json!({"kasia_indexer_db_root":temp.join("context"),"kaspa_node_wborsh_url":"ws://127.0.0.1:1"}),
    )?;
    let state = ContextualMessageApi::new(
        db.clone(),
        ContextualMessageBySenderPartition::new(db)?,
        TxIDToAcceptancePartition::new(db)?,
        TxIdToContextualMessagePartition::new(db)?,
        std::sync::Arc::new(indexer_actors::metrics::IndexerMetrics::new()),
        crate::context::get_indexer_context(&config)?,
    );
    // Use the real contextual router; apply the same production decision with a
    // fixed mode for this isolated server, avoiding process-wide env mutation.
    let contextual =
        super::super::contextual_messages::ContextualMessageApi::router().with_state(state);
    let export = export::ExportApi::new(db.clone());
    let app = Router::new()
        .nest("/contextual-messages", contextual)
        .route(
            "/import-file",
            post(export::import_file).with_state(export.clone()),
        )
        .route(
            "/personal/purge-all",
            post(export::purge_all).with_state(export.clone()),
        )
        .route(
            "/self-stash-gc-orphans",
            post(export::gc_self_stash_orphans).with_state(export),
        )
        .layer(from_fn(move |request, next| {
            reject_alternate_writer_for_mode(shared, request, next)
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, signal) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = signal.await;
            })
            .await
    });
    Ok((address, stop, task))
}

#[tokio::test]
async fn shared_alternate_writers_reject_before_any_body_and_preserve_native_rows() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = fjall::Config::new(temp.path().join("db")).open_transactional()?;
    let mut wire = format!("kchat:1:dm:{}:alias:", "ab".repeat(16)).into_bytes();
    wire.extend_from_slice(b"\xff\x00:binary");
    let transaction = super::tests::fixture(77, Prefix::Testnet, &wire);
    let batch = super::tests::batch("testnet-10", vec![transaction]);
    apply_batch(&db, "testnet-10", &batch)?;
    let before = snapshot(&db)?;
    let (address, stop, server) = serve_handlers(&db, true, temp.path()).await?;
    for route in [
        "/contextual-messages/import",
        "/import-file",
        "/personal/purge-all",
        "/self-stash-gc-orphans",
    ] {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        // Send only headers claiming a huge body. A body extractor/handler would
        // wait forever; receiving403 before sending ANY bytes proves ordering.
        stream.write_all(format!("POST {route} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 1000000000\r\nConnection: close\r\n\r\n").as_bytes()).await?;
        let mut head = [0; 256];
        let length =
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut head))
                .await??;
        assert!(
            std::str::from_utf8(&head[..length])?.starts_with("HTTP/1.1 403"),
            "{route}"
        );
        drop(stream);
        assert_eq!(
            snapshot(&db)?,
            before,
            "{route} cannot mutate partitions or native journal"
        );
    }
    // Canonical writer stays usable after every denial; original duplicate
    // proves its ledger remained intact, then another normal record commits.
    assert_eq!(apply_batch(&db, "testnet-10", &batch)?.duplicates, 1);
    let next = super::tests::batch(
        "testnet-10",
        vec![super::tests::fixture(
            78,
            Prefix::Testnet,
            b"ciph_msg:1:comm:alias:sealed",
        )],
    );
    assert_eq!(apply_batch(&db, "testnet-10", &next)?.applied, 1);
    let _ = stop.send(());
    tokio::time::timeout(std::time::Duration::from_secs(3), server).await???;
    Ok(())
}

#[tokio::test]
async fn standalone_legacy_mutators_still_reach_existing_handlers() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = fjall::Config::new(temp.path().join("db")).open_transactional()?;
    let (address, stop, server) = serve_handlers(&db, false, temp.path()).await?;
    let client = reqwest::Client::new();
    for route in [
        "/contextual-messages/import",
        "/import-file",
        "/personal/purge-all",
        "/self-stash-gc-orphans",
    ] {
        let request = client.post(format!("http://{address}{route}"));
        let response = if route == "/contextual-messages/import" {
            request.json(&serde_json::json!([])).send().await?
        } else {
            request.body("").send().await?
        };
        assert_eq!(response.status().as_u16(), 200, "{route}");
    }
    let response = client
        .get(format!(
            "http://{address}/contextual-messages/by-inbox?tag={}",
            "ab".repeat(16)
        ))
        .send()
        .await?;
    assert_eq!(
        response.status().as_u16(),
        404,
        "standalone scanner cannot advertise shared-only DM support"
    );
    let _ = stop.send(());
    tokio::time::timeout(std::time::Duration::from_secs(3), server).await???;
    Ok(())
}
