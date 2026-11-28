//! Real HTTP/SQLite and canonical PostgreSQL contracts. The provider is a local
//! deterministic server; these tests do not claim Google translation quality.
use super::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
    routing::post,
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;

struct CacheDirectory(PathBuf);
impl CacheDirectory {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "social-translation-test-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn database(&self) -> PathBuf {
        self.0.join("cache.sqlite")
    }
}
impl Drop for CacheDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Provider {
    endpoint: String,
    calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    mode: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Provider {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Provider {
    async fn start() -> Self {
        let calls = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let mode = Arc::new(AtomicUsize::new(0));
        let (capture, behavior) = (calls.clone(), mode.clone());
        let app=Router::new().route("/translate",post(move |headers:axum::http::HeaderMap,Json(body):Json<Value>|{
            let (calls,mode)=(capture.clone(),behavior.clone());
            async move {
                assert_eq!(headers.get("X-Goog-Api-Key").unwrap(),"isolated-test-key");
                assert_eq!(body["format"],"html");
                assert!(body["q"].as_array().unwrap().len()<=128);
                assert!(body["q"].as_array().unwrap().iter().map(|v|v.as_str().unwrap().chars().count()).sum::<usize>()<=25000);
                assert!(body.get("source").is_none(),"source language must be detected");
                calls.lock().await.push(body.clone());
                tokio::time::sleep(Duration::from_millis(75)).await;
                match mode.load(Ordering::Relaxed) {
                    1=>(StatusCode::TOO_MANY_REQUESTS,Json(json!({"error":"private provider diagnostic"}))),
                    2=>(StatusCode::OK,Json(json!({"data":{"translations":[]}}))),
                    _=>{
                        let target=body["target"].as_str().unwrap();
                        let results:Vec<Value>=body["q"].as_array().unwrap().iter().map(|text|{
                            let text=text.as_str().unwrap();
                            let output=if mode.load(Ordering::Relaxed)==3 {text.replace("ZXQ","CORRUPTED")}
                            else if mode.load(Ordering::Relaxed)==4 {format!("<script>{text}</script>")}
                            else if let Some(tokens)=text.strip_prefix("Ich werde morgen ").and_then(|v|v.strip_suffix(" kaufen.")) {format!("I will buy {tokens} tomorrow.")}
                            else if text.contains("こんにちは") {text.replace("こんにちは",if target=="en" {"Hello"} else {"Hola"})}
                            else if text=="l'ami" {"friend &amp; colleague".into()}
                            else {format!("{target}:{text}")};
                            json!({"translatedText":output,"detectedSourceLanguage":"ja"})
                        }).collect();
                        (StatusCode::OK,Json(json!({"data":{"translations":results}})))
                    }
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/translate", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            endpoint,
            calls,
            mode,
            task,
        }
    }
    async fn translator(&self, path: &Path) -> Translator {
        Translator::new(path, "isolated-test-key".into(), self.endpoint.clone())
            .await
            .unwrap()
    }
    async fn count(&self) -> usize {
        self.calls.lock().await.len()
    }
}
fn input(text: &str) -> PublicText {
    PublicText {
        namespace: "testnet-10:isolated-source".into(),
        text: text.into(),
    }
}

#[test]
fn pure_translation_accepts_only_english_spanish_and_indexed_ids() {
    for code in ["en", "es"] {
        assert!(
            serde_json::from_value::<TranslationQuery>(json!({"id":"ab".repeat(32),"target":code}))
                .is_ok()
        );
    }
    for code in ["fr", "EN", "es-MX", ""] {
        assert!(
            serde_json::from_value::<TranslationQuery>(json!({"id":"ab".repeat(32),"target":code}))
                .is_err()
        );
    }
    assert!(
        serde_json::from_value::<TranslationQuery>(
            json!({"id":"ab".repeat(32),"target":"en","text":"arbitrary unindexed input"})
        )
        .is_err()
    );
    assert!(!valid_id("../../private"));
    assert!(!valid_id(&"zz".repeat(32)));
}
#[test]
fn pure_translation_extracts_only_readable_public_broadcast_text() {
    assert_eq!(broadcast_text("hola"), Some("hola".into()));
    let reply=json!({"type":"reply","replyToId":"ab".repeat(32),"text":"こんにちは","replyToPreview":"quoted metadata"}).to_string();
    assert_eq!(broadcast_text(&reply), Some("こんにちは".into()));
    for body in [
        json!({"type":"file","content":"binary"}),
        json!({"fileData":{"content":"binary"}}),
        json!({"type":"edit","text":"new text"}),
        json!({"type":"payment_notice","amountSompi":100}),
        json!({"type":"reaction","emoji":"👍"}),
        json!({"type":"reply","replyToId":"not-a-txid","text":"hola"}),
    ] {
        assert!(broadcast_text(&body.to_string()).is_none());
    }
    let attachment = json!({"type":"reply","replyToId":"ab".repeat(32),"text":json!({"type":"image","content":"binary"}).to_string()});
    assert!(broadcast_text(&attachment.to_string()).is_none());
    assert_eq!(
        broadcast_text("{\"example\":true}"),
        Some("{\"example\":true}".into())
    );
}
#[test]
fn pure_translation_preserves_tokens_and_allows_sentence_reordering() {
    let text = "HOLA @alice #kaspa 123.45 KAS $NACHO kaspa:qabcdefgh123 https://example.test/image.png#alt=Hola `x + 1`";
    let protected = ProtectedText::new(text);
    assert_eq!(protected.restore(&protected.html).unwrap(), text);
    assert!(protected.html.starts_with("HOLA "));
    for token in [
        "@alice",
        "#kaspa",
        "123.45",
        "KAS",
        "$NACHO",
        "kaspa:qabcdefgh123",
        "https://example.test/image.png#alt=Hola",
        "`x + 1`",
    ] {
        assert!(protected.tokens.contains(&token.into()), "{token}");
    }
    let literal = "<b>Hola & adiós</b>";
    let safe = ProtectedText::new(literal);
    assert!(safe.html.contains("&lt;b&gt;"));
    assert_eq!(safe.restore(&safe.html).unwrap(), literal);
    assert!(safe.restore("<script>bad</script>").is_err());
    let long = "こんにちは & <bonjour> ".repeat(1500);
    let protected = ProtectedText::new(&long);
    let chunks = html_chunks(&protected.html);
    assert!(chunks.len() > 1);
    let reconstructed: String = chunks
        .iter()
        .map(|c| format!("{}{}{}", c.before, c.html, c.after))
        .collect();
    assert_eq!(protected.restore(&reconstructed).unwrap(), long);
    assert!(chunks.iter().all(|c| c.html.chars().count() <= 4000));
}
#[test]
fn pure_translation_rejects_changed_repeated_missing_and_unknown_placeholders() {
    let protected = ProtectedText::new("buy 12 KAS");
    assert_eq!(protected.restore(&protected.html).unwrap(), "buy 12 KAS");
    assert!(
        protected
            .restore(&protected.html.replace("ZXQ", "changed"))
            .is_err()
    );
    assert!(
        protected
            .restore(&(protected.html.clone() + &protected.html))
            .is_err()
    );
    assert!(protected.restore("missing").is_err());
    assert!(
        protected
            .restore(&(protected.html.clone() + &format!("{}999QXZ", protected.prefix)))
            .is_err()
    );
    let source = "<script>alert('literal example')</script> 12 KAS";
    let escaped = ProtectedText::new(source);
    assert_eq!(escaped.restore(&escaped.html).unwrap(), source);
    for value in [Value::Null, json!(false), json!(0), json!("")] {
        let source = json!({"type":value,"example":"Hallo"}).to_string();
        assert_eq!(broadcast_text(&source), Some(source.clone()));
    }
    let long = ProtectedText::new(&"こんにちは & < > 123 KAS ".repeat(1000));
    let chunks = html_chunks(&long.html);
    assert!(chunks.len() > 1);
    assert!(chunks.iter().all(|c| c.html.chars().count() <= 4000));
    let restored = chunks
        .into_iter()
        .map(|c| c.before + &c.html + &c.after)
        .collect::<String>();
    assert_eq!(
        long.restore(&restored).unwrap(),
        "こんにちは & < > 123 KAS ".repeat(1000)
    );
}
#[tokio::test]
async fn pure_translation_cache_survives_restart_and_scopes_target_text_and_source() {
    let provider = Provider::start().await;
    let dir = CacheDirectory::new();
    let translator = provider.translator(&dir.database()).await;
    let (en, cached) = translator
        .translate(&input("こんにちは"), Target::En)
        .await
        .unwrap();
    assert_eq!(en.text, "Hello");
    assert_eq!(en.language.as_deref(), Some("ja"));
    assert!(!cached);
    assert!(
        translator
            .translate(&input("こんにちは"), Target::En)
            .await
            .unwrap()
            .1
    );
    assert_eq!(
        translator
            .translate(&input("こんにちは"), Target::Es)
            .await
            .unwrap()
            .0
            .text,
        "Hola"
    );
    translator
        .translate(&input("edited message"), Target::En)
        .await
        .unwrap();
    let mut mainnet = input("こんにちは");
    mainnet.namespace = "mainnet:another-source".into();
    translator.translate(&mainnet, Target::En).await.unwrap();
    assert_eq!(provider.count().await, 4);
    translator.cache.close().await;
    drop(translator);
    let restarted = provider.translator(&dir.database()).await;
    assert!(
        restarted
            .translate(&input("こんにちは"), Target::En)
            .await
            .unwrap()
            .1
    );
    assert_eq!(provider.count().await, 4);
    restarted.cache.close().await;
}
#[tokio::test]
async fn pure_translation_concurrent_processes_share_one_generation() {
    let provider = Provider::start().await;
    let dir = CacheDirectory::new();
    let one = provider.translator(&dir.database()).await;
    let two = provider.translator(&dir.database()).await;
    let text = input("こんにちは");
    let (first, second) = tokio::join!(
        one.translate(&text, Target::En),
        two.translate(&text, Target::En)
    );
    assert_eq!(first.unwrap().0.text, "Hello");
    assert_eq!(second.unwrap().0.text, "Hello");
    assert_eq!(provider.count().await, 1);
    one.cache.close().await;
    two.cache.close().await;
}
#[tokio::test]
async fn pure_translation_failures_are_not_cached_and_expired_claims_retry() {
    let provider = Provider::start().await;
    let dir = CacheDirectory::new();
    let translator = provider.translator(&dir.database()).await;
    provider.mode.store(1, Ordering::Relaxed);
    assert!(
        translator
            .translate(&input("こんにちは"), Target::En)
            .await
            .is_err()
    );
    let saved: Option<String> = sqlx::query_scalar("SELECT result FROM translations")
        .fetch_one(&translator.cache)
        .await
        .unwrap();
    assert!(saved.is_none());
    provider.mode.store(0, Ordering::Relaxed);
    sqlx::query("UPDATE translations SET lease_until=0")
        .execute(&translator.cache)
        .await
        .unwrap();
    assert_eq!(
        translator
            .translate(&input("こんにちは"), Target::En)
            .await
            .unwrap()
            .0
            .text,
        "Hello"
    );
    provider.mode.store(2, Ordering::Relaxed);
    assert!(
        translator
            .translate(&input("bad shape"), Target::Es)
            .await
            .is_err()
    );
    assert_eq!(provider.count().await, 3);
    translator.cache.close().await;
}
#[tokio::test]
async fn pure_translation_retains_tokens_and_decodes_provider_entities() {
    let provider = Provider::start().await;
    let dir = CacheDirectory::new();
    let translator = provider.translator(&dir.database()).await;
    let original = "こんにちは 100 KAS @alice https://example.test/#hello";
    let result = translator
        .translate(&input(original), Target::Es)
        .await
        .unwrap()
        .0;
    assert_eq!(
        result.text,
        "Hola 100 KAS @alice https://example.test/#hello"
    );
    let calls = provider.calls.lock().await;
    assert_eq!(calls[0]["q"].as_array().unwrap().len(), 1);
    assert!(calls[0]["q"][0].as_str().unwrap().contains("こんにちは"));
    assert!(
        calls[0]["q"][0]
            .as_str()
            .unwrap()
            .contains("translate=\"no\"")
    );
    drop(calls);
    assert_eq!(
        translator
            .translate(&input("l'ami"), Target::En)
            .await
            .unwrap()
            .0
            .text,
        "friend & colleague"
    );
    let before = provider.count().await;
    assert_eq!(
        translator
            .translate(&input("123 KAS 👍"), Target::Es)
            .await
            .unwrap()
            .0
            .text,
        "123 KAS 👍"
    );
    assert_eq!(provider.count().await, before);
    translator.cache.close().await;
}

#[tokio::test]
async fn pure_translation_keeps_sentence_context_batches_and_rejects_corrupt_tokens() {
    let provider = Provider::start().await;
    let dir = CacheDirectory::new();
    let translator = provider.translator(&dir.database()).await;
    assert_eq!(
        translator
            .translate(&input("Ich werde morgen 2 KAS kaufen."), Target::En)
            .await
            .unwrap()
            .0
            .text,
        "I will buy 2 KAS tomorrow."
    );
    let busy = (0..350)
        .map(|i| format!("bonjour {i} KAS "))
        .collect::<String>();
    let before = provider.count().await;
    let translated = translator
        .translate(&input(&busy), Target::Es)
        .await
        .unwrap()
        .0
        .text;
    assert!(translated.contains("349 KAS"));
    assert!(
        provider.count().await > before + 1,
        "large payload must use bounded batches"
    );
    provider.mode.store(3, Ordering::Relaxed);
    assert!(
        translator
            .translate(&input("こんにちは 123 KAS"), Target::En)
            .await
            .is_err()
    );
    provider.mode.store(4, Ordering::Relaxed);
    assert!(
        translator
            .translate(&input("こんにちは 456 KAS"), Target::En)
            .await
            .is_err()
    );
    translator.cache.close().await;
}

async fn get(router: &Router, path: &str) -> (StatusCode, axum::http::HeaderMap, Value) {
    let request = Request::builder()
        .uri(path)
        .extension(ConnectInfo(
            "127.0.0.1:49000".parse::<SocketAddr>().unwrap(),
        ))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (
        status,
        headers,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
async fn room(
    db: &PostgresDbManager,
    n: u8,
    time: i64,
    channel: &str,
    sender: &str,
    content: &str,
) {
    let id = vec![n; 32];
    sqlx::query(
        "INSERT INTO protocol_indexer.canonical VALUES($1,1,'{}',decode(repeat('ab',32),'hex'))",
    )
    .bind(&id)
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO social_shared.actions(transaction_id,revision,subject,native_table,source_payload,original_time,status,state_action,snapshot) VALUES($1,1,$2,'kachat_broadcasts','fixture',$3,'verified','signed','{}')")
        .bind(&id).bind(format!("room:{n}")).bind(time).execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO public.kachat_broadcasts(transaction_id,block_time,channel,sender_address,content) VALUES($1,$2,$3,$4,$5)").bind(&id).bind(time).bind(channel).bind(sender).bind(content).execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO social_shared.room_facts VALUES($1,1,$2)")
        .bind(&id)
        .bind(sender)
        .execute(&db.pool)
        .await
        .unwrap();
}
#[tokio::test]
async fn postgres_translation_router_uses_current_public_text_edits_revocations_and_source() {
    use crate::{
        config::ServerConfig,
        shared_read_tests::{extension, post, setup, show},
        web_server::WebServer,
    };
    let db = setup().await;
    post(&db.pool, 1, 1000).await;
    room(&db, 10, 1000, "kaspa", "author", "こんにちは").await;
    room(&db,11,1000,"kaspa","author",&json!({"type":"reply","replyToId":"0a".repeat(32),"text":"こんにちは","replyToPreview":"not the message"}).to_string()).await;
    room(
        &db,
        12,
        2000,
        "kaspa",
        "impostor",
        &json!({"type":"edit","targetTxId":"0a".repeat(32),"text":"forged"}).to_string(),
    )
    .await;
    room(
        &db,
        13,
        2000,
        "other",
        "author",
        &json!({"type":"edit","targetTxId":"0a".repeat(32),"text":"wrong channel"}).to_string(),
    )
    .await;
    let provider = Provider::start().await;
    let dir = CacheDirectory::new();
    let translator = Arc::new(provider.translator(&dir.database()).await);
    let server = WebServer::new(
        Arc::new(PostgresDbManager {
            pool: db.pool.clone(),
            shared: db.shared.clone(),
        }),
        ServerConfig {
            bind_address: "127.0.0.1:0".into(),
            request_timeout: 10,
            rate_limit: 1000,
        },
    )
    .await
    .with_translator(Some(translator.clone()));
    let router = server.create_router();
    let endpoint = format!(
        "/get-translation?id={}&kind=broadcast&target=es",
        "0a".repeat(32)
    );
    let (status, headers, response) = get(&router, &endpoint).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(response["translatedText"], "Hola");
    assert_eq!(response["originalText"], "こんにちは");
    assert_eq!(get(&router, &endpoint).await.2["cached"], true);
    assert_eq!(provider.count().await, 1);
    let reply = format!(
        "/get-translation?id={}&kind=broadcast&target=en",
        "0b".repeat(32)
    );
    assert_eq!(get(&router, &reply).await.2["originalText"], "こんにちは");
    room(
        &db,
        14,
        3000,
        "kaspa",
        "author",
        &json!({"type":"edit","targetTxId":"0a".repeat(32),"text":"changed"}).to_string(),
    )
    .await;
    assert_eq!(get(&router, &endpoint).await.2["originalText"], "changed");
    sqlx::query("INSERT INTO protocol_indexer.pending VALUES($1,true,NULL)")
        .bind(vec![14u8; 32])
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        get(&router, &endpoint).await.2["originalText"],
        "こんにちは"
    );
    // Malformed jsonb must not poison a previous message's edit lookup.
    room(&db, 15, 4000, "kaspa", "author", r#"{"example":"\u0000"}"#).await;
    room(
        &db,
        16,
        5000,
        "kaspa",
        "author",
        &json!({"type":"edit","targetTxId":"0a".repeat(32),"text":" \n\t\u{000b}\u{00a0}\u{feff}"})
            .to_string(),
    )
    .await;
    assert_eq!(
        get(&router, &endpoint).await.2["originalText"],
        "こんにちは"
    );
    room(
        &db,
        17,
        6000,
        "kaspa",
        "author",
        &json!({"type":"edit","targetTxId":"0a".repeat(32),"text":"v"}).to_string(),
    )
    .await;
    assert_eq!(get(&router, &endpoint).await.2["originalText"], "v");
    room(
        &db,
        18,
        5000,
        "kaspa",
        "author",
        r#"{"type":"file","content":"opaque"}"#,
    )
    .await;
    let attachment = format!(
        "/get-translation?id={}&kind=broadcast&target=en",
        "12".repeat(32)
    );
    assert_eq!(get(&router, &attachment).await.0, StatusCode::NOT_FOUND);
    room(
        &db,
        19,
        6000,
        "kaspa",
        "author",
        &json!({"type":"edit","targetTxId":"12".repeat(32),"text":"こんにちは"}).to_string(),
    )
    .await;
    assert_eq!(
        get(&router, &attachment).await.2["originalText"],
        "こんにちは"
    );
    let post_url = format!("/get-translation?id={}&target=en", "01".repeat(32));
    assert_eq!(get(&router, &post_url).await.2["originalText"], "hi");
    let edited = Base64::encode_string("こんにちは".as_bytes());
    extension(&db.pool, 2, 1, "edit", 2000, &edited).await;
    show(&db.pool, 1, Some(2000), &edited).await;
    assert_eq!(
        get(&router, &post_url).await.2["originalText"],
        "こんにちは"
    );
    // A source change while the provider is running must beat its response.
    room(
        &db,
        70,
        1000,
        "race",
        "author",
        "network-race-unique-message",
    )
    .await;
    let race_url = format!(
        "/get-translation?id={}&kind=broadcast&target=en",
        "46".repeat(32)
    );
    let captured = provider.count().await;
    let race_router = router.clone();
    let race = tokio::spawn(async move { get(&race_router, &race_url).await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while provider.count().await == captured {
        assert!(
            tokio::time::Instant::now() < deadline,
            "provider did not start"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    sqlx::query(
        "UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=$1",
    )
    .bind(vec![70u8; 32])
    .execute(&db.pool)
    .await
    .unwrap();
    assert_eq!(race.await.unwrap().0, StatusCode::NOT_FOUND);
    // Exercise the actual lookup against a busy room, using the same read
    // session with a strict DB-side one-second budget (synthetic materialization).
    sqlx::raw_sql("WITH source AS (SELECT decode(lpad(to_hex(g+100000),64,'0'),'hex') AS tx FROM generate_series(1,20000) g), canonical AS (INSERT INTO protocol_indexer.canonical SELECT tx,1,'{}',decode(repeat('ab',32),'hex') FROM source), actions AS (INSERT INTO social_shared.actions(transaction_id,revision,subject,native_table,source_payload,original_time,status,state_action,snapshot) SELECT tx,1,'room:'||encode(tx,'hex'),'kachat_broadcasts','fixture',10000,'verified','signed','{}' FROM source RETURNING transaction_id), native AS (INSERT INTO public.kachat_broadcasts(transaction_id,block_time,channel,sender_address,content) SELECT tx,10000,'kaspa','other-author','unrelated public text' FROM source) INSERT INTO social_shared.room_facts SELECT transaction_id,1,'other-author' FROM actions").execute(&db.pool).await.unwrap();
    sqlx::raw_sql("ANALYZE public.kachat_broadcasts; ANALYZE protocol_indexer.canonical; ANALYZE social_shared.room_facts").execute(&db.pool).await.unwrap();
    let mut binding = db.shared.clone().unwrap();
    binding.statement_timeout_ms = 1000;
    let bounded = PostgresDbManager {
        pool: db.pool.clone(),
        shared: Some(binding),
    };
    let started = std::time::Instant::now();
    assert_eq!(
        bounded
            .read_translation_text(&"0a".repeat(32), Kind::Broadcast)
            .await
            .unwrap()
            .unwrap()
            .text,
        "v"
    );
    eprintln!(
        "20,000-row broadcast translation lookup: {:?}",
        started.elapsed()
    );
    let before = provider.count().await;
    sqlx::query(
        "UPDATE protocol_indexer.canonical SET accepting_block=NULL WHERE transaction_id=$1",
    )
    .bind(vec![1u8; 32])
    .execute(&db.pool)
    .await
    .unwrap();
    assert_eq!(get(&router, &post_url).await.0, StatusCode::NOT_FOUND);
    assert_eq!(provider.count().await, before);
    assert_eq!(
        get(&router, "/get-translation?id=bad&target=en").await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        get(&router, &endpoint.replace("target=es", "target=fr"))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let control = format!(
        "/get-translation?id={}&kind=broadcast&target=en",
        "0e".repeat(32)
    );
    assert_eq!(get(&router, &control).await.0, StatusCode::NOT_FOUND);
    sqlx::query("UPDATE protocol_indexer.config SET network='mainnet'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        get(&router, &endpoint).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(provider.count().await, before);
    sqlx::query("UPDATE protocol_indexer.config SET network='testnet-10'")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::raw_sql("UPDATE protocol_indexer.config SET network='mainnet'; UPDATE social_shared.config SET network='mainnet'; UPDATE public.k_vars SET value='mainnet' WHERE key='network'").execute(&db.pool).await.unwrap();
    let mut mainnet = db.shared.clone().unwrap();
    mainnet.network = "mainnet".into();
    let mainnet_db = PostgresDbManager {
        pool: db.pool.clone(),
        shared: Some(mainnet),
    };
    let current = mainnet_db
        .read_translation_text(&"0a".repeat(32), Kind::Broadcast)
        .await
        .unwrap()
        .unwrap();
    assert!(current.namespace.starts_with("mainnet:"));
    let mut legacy = mainnet_db.shared.clone().unwrap();
    legacy.read_contract = ReadContract::LegacyK;
    assert!(
        PostgresDbManager {
            pool: db.pool.clone(),
            shared: Some(legacy)
        }
        .read_translation_text(&"0a".repeat(32), Kind::Broadcast)
        .await
        .is_err()
    );
    translator.cache.close().await;
    db.pool.close().await;
}
