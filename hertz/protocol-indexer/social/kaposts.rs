//! Pure KaChat extension types/parser from the pinned upstream implementation.
//! Source: KaspaSilver/KaChat-Indexer e2f462171bee80ff26c6fe14ddafe06614f05efa,
//! kachat-transaction-processor/src/k_protocol.rs (ISC notice LICENSE.K-indexer).
//! No scanner, pool escape, app-content filters, RPC or direct notification.
//! Native Kaspa SDK verification produces the owning opaque envelope separately.

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(Debug, Clone)]
pub enum KaPostsAction {
    Unquote(KUnquote),
    Edit(KEdit),
    Delete(KDelete),
    Poll(KPoll),
    PollVote(KPollVote),
}

/// Removal counter-action for a quote/repost (fork addition). The sender withdraws their
/// prior quote or repost of `content_id`; on receipt the processor deletes the matching
/// k_contents row(s) and quotesCount (computed live) drops automatically.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KUnquote {
    pub sender_pubkey: String,
    pub sender_signature: String,
    pub content_id: String,
}

/// Author edit of an existing post/reply/quote (fork addition, §5.7). The signature is over
/// `edit:<post_id>:<b64_message>:<mentions_json>` (note the literal `edit:` prefix inside the
/// signed string — it prevents a reply to your own post being replayed as an edit of it).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KEdit {
    pub sender_pubkey: String,
    pub sender_signature: String,
    pub post_id: String,
    pub base64_encoded_message: String,
    pub mentioned_pubkeys: Vec<String>,
    /// Whether the payload arrived under the canonical `kchat:1:` prefix (vs legacy `k:1:`).
    #[serde(default)]
    pub is_kchat: bool,
}

/// Author delete of an existing post/reply/quote (fork addition, §5.8). The signature is over
/// `delete:<post_id>` (literal `delete:` prefix, mirroring the edit action, so a bare-content-id
/// signature from another action can't be replayed as a delete).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KDelete {
    pub sender_pubkey: String,
    pub sender_signature: String,
    pub post_id: String,
}

/// A poll (fork addition, §5.9). The poll IS a post: `base64_encoded_question` is stored as the
/// post's content (content_type `poll`), so an older client with no poll support shows it as a
/// plain post. `options_b64_csv` is 2–4 options, each base64 of its UTF-8 text, joined with `,`
/// (base64 has no `,`/`:`, so the payload still splits cleanly). Signature is over
/// `poll:<b64_question>:<options_b64_csv>:<closes_at_ms>:<mentions_json>`.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KPoll {
    pub sender_pubkey: String,
    pub sender_signature: String,
    pub base64_encoded_question: String,
    pub options_b64_csv: String,
    pub closes_at_ms: i64,
    pub mentioned_pubkeys: Vec<String>,
    /// Whether the payload arrived under the canonical `kchat:1:` prefix (vs legacy `k:1:`).
    #[serde(default)]
    pub is_kchat: bool,
}

/// A vote in a poll (fork addition, §5.9). Signature is over
/// `pollvote:<poll_id>:<option_index>`. One current vote per pubkey — a later vote (by chain time)
/// replaces the earlier one.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KPollVote {
    pub sender_pubkey: String,
    pub sender_signature: String,
    pub poll_id: String,
    pub option_index: i32,
}

pub fn parse_parts(parts: &[&str]) -> Result<KaPostsAction> {
    let action = *parts
        .first()
        .ok_or_else(|| anyhow!("empty KaPosts extension"))?;
    // Called only for the canonical kchat:1: envelope, by the existing native verifier.
    let is_kchat = true;
    match action {
        "unquote" => {
            // Expected format: unquote:sender_pubkey:sender_signature:content_id
            // (fork addition) Removal counter-action for a prior quote/repost.
            if parts.len() < 4 {
                return Err(anyhow::anyhow!(
                    "Invalid unquote format: expected 4 parts, got {}",
                    parts.len()
                ));
            }

            Ok(KaPostsAction::Unquote(KUnquote {
                sender_pubkey: parts[1].to_string(),
                sender_signature: parts[2].to_string(),
                content_id: parts[3].to_string(),
            }))
        }
        "edit" => {
            // Expected format: edit:sender_pubkey:sender_signature:post_id:base64_message:mentioned_pubkeys_json
            if parts.len() < 5 {
                return Err(anyhow::anyhow!(
                    "Invalid edit format: expected at least 5 parts, got {}",
                    parts.len()
                ));
            }

            let sender_pubkey = parts[1].to_string();
            let sender_signature = parts[2].to_string();
            let post_id = parts[3].to_string();
            let base64_encoded_message = parts[4].to_string();

            let mentioned_pubkeys: Vec<String> = if parts.len() > 5 {
                let mentioned_pubkeys_json = parts[5];
                match serde_json::from_str::<Vec<String>>(mentioned_pubkeys_json) {
                    Ok(pubkeys) => pubkeys,
                    Err(err) => {
                        error!(
                            "Failed to parse edit mentioned_pubkeys JSON '{}': {}",
                            mentioned_pubkeys_json, err
                        );
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            };

            Ok(KaPostsAction::Edit(KEdit {
                sender_pubkey,
                sender_signature,
                post_id,
                base64_encoded_message,
                mentioned_pubkeys,
                is_kchat,
            }))
        }
        "delete" => {
            // Expected format: delete:sender_pubkey:sender_signature:post_id
            if parts.len() < 4 {
                return Err(anyhow::anyhow!(
                    "Invalid delete format: expected 4 parts, got {}",
                    parts.len()
                ));
            }

            Ok(KaPostsAction::Delete(KDelete {
                sender_pubkey: parts[1].to_string(),
                sender_signature: parts[2].to_string(),
                post_id: parts[3].to_string(),
            }))
        }
        "poll" => {
            // Expected: poll:pubkey:sig:b64_question:options_b64_csv:closes_at_ms:mentions_json
            if parts.len() < 6 {
                return Err(anyhow::anyhow!(
                    "Invalid poll format: expected at least 6 parts, got {}",
                    parts.len()
                ));
            }
            let closes_at_ms: i64 = parts[5]
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid poll closes_at_ms '{}'", parts[5]))?;
            let mentioned_pubkeys: Vec<String> = if parts.len() > 6 {
                serde_json::from_str::<Vec<String>>(parts[6]).unwrap_or_default()
            } else {
                Vec::new()
            };
            Ok(KaPostsAction::Poll(KPoll {
                sender_pubkey: parts[1].to_string(),
                sender_signature: parts[2].to_string(),
                base64_encoded_question: parts[3].to_string(),
                options_b64_csv: parts[4].to_string(),
                closes_at_ms,
                mentioned_pubkeys,
                is_kchat,
            }))
        }
        "pollvote" => {
            // Expected: pollvote:pubkey:sig:poll_id:option_index
            if parts.len() < 5 {
                return Err(anyhow::anyhow!(
                    "Invalid pollvote format: expected 5 parts, got {}",
                    parts.len()
                ));
            }
            let option_index: i32 = parts[4]
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid pollvote option_index '{}'", parts[4]))?;
            Ok(KaPostsAction::PollVote(KPollVote {
                sender_pubkey: parts[1].to_string(),
                sender_signature: parts[2].to_string(),
                poll_id: parts[3].to_string(),
                option_index,
            }))
        }
        _ => Err(anyhow!("not a supported KaPosts extension")),
    }
}

impl KaPostsAction {
    /// Exact upstream signing strings; unquote intentionally signs the bare ID.
    pub fn signing_string(&self) -> Result<String> {
        Ok(match self {
            Self::Unquote(v) => v.content_id.clone(),
            Self::Edit(v) => format!(
                "edit:{}:{}:{}",
                v.post_id,
                v.base64_encoded_message,
                serde_json::to_string(&v.mentioned_pubkeys)?
            ),
            Self::Delete(v) => format!("delete:{}", v.post_id),
            Self::Poll(v) => format!(
                "poll:{}:{}:{}:{}",
                v.base64_encoded_question,
                v.options_b64_csv,
                v.closes_at_ms,
                serde_json::to_string(&v.mentioned_pubkeys)?
            ),
            Self::PollVote(v) => format!("pollvote:{}:{}", v.poll_id, v.option_index),
        })
    }

    pub fn signature_fields(&self) -> (&str, &str) {
        match self {
            Self::Unquote(v) => (&v.sender_signature, &v.sender_pubkey),
            Self::Edit(v) => (&v.sender_signature, &v.sender_pubkey),
            Self::Delete(v) => (&v.sender_signature, &v.sender_pubkey),
            Self::Poll(v) => (&v.sender_signature, &v.sender_pubkey),
            Self::PollVote(v) => (&v.sender_signature, &v.sender_pubkey),
        }
    }

    /// Match native hex decoding before SQL; target/time/payment authorization
    /// and structural poll validity still belong to canonical materialization.
    pub fn storage_fields_are_decodable(&self) -> bool {
        let hex_ok = |s: &str| hex::decode(s).is_ok();
        match self {
            Self::Unquote(v) => hex_ok(&v.content_id),
            Self::Edit(v) => hex_ok(&v.post_id) && v.mentioned_pubkeys.iter().all(|s| hex_ok(s)),
            Self::Delete(v) => hex_ok(&v.post_id),
            Self::Poll(v) => v.mentioned_pubkeys.iter().all(|s| hex_ok(s)),
            Self::PollVote(v) => hex_ok(&v.poll_id),
        }
    }
}
