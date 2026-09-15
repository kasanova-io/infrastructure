use indexer_db::FromBytes;

/// Versioned, route-bound native keys preserve ordering even when timestamps tie.
/// Tokens are opaque to clients; they must return the last row's token unchanged.
pub(super) fn encode_cursor(kind: &str, bytes: &[u8]) -> String {
    format!("{kind}:1:{}", faster_hex::hex_string(bytes))
}

pub(super) fn decode_cursor<K: FromBytes>(
    cursor: Option<&str>,
    block_time: Option<u64>,
    kind: &str,
    belongs_to_query: impl FnOnce(&K) -> bool,
) -> Result<Option<K>, String> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    if block_time.is_some() {
        return Err("Use either cursor or block_time, not both".into());
    }
    let hex = cursor
        .strip_prefix(&format!("{kind}:1:"))
        .ok_or("Cursor has the wrong route or version")?;
    if hex.len() != std::mem::size_of::<K>() * 2 {
        return Err("Invalid cursor length".into());
    }
    let mut bytes = vec![0; std::mem::size_of::<K>()];
    faster_hex::hex_decode(hex.as_bytes(), &mut bytes).map_err(|_| "Invalid cursor hex")?;
    let key = K::read_from_bytes(&bytes).map_err(|_| "Invalid cursor key")?;
    if !belongs_to_query(&key) {
        return Err("Cursor does not belong to this query".into());
    }
    Ok(Some(key))
}
