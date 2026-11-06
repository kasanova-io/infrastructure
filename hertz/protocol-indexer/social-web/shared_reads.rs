//! Existing database methods keep their SQL/DTO/cursors on one read session.
//! Shared mode has no public-table fallback; legacy mode keeps a plain checkout.
use crate::database_postgres_impl::PostgresDbManager;
use crate::database_trait::{DatabaseError, DatabaseResult};
use sqlx::{PgConnection, Postgres, Transaction, pool::PoolConnection};
use std::ops::{Deref, DerefMut};

pub(crate) enum ReadSession<'a> {
    Legacy(PoolConnection<Postgres>),
    Shared(Transaction<'a, Postgres>),
}
impl Deref for ReadSession<'_> {
    type Target = PgConnection;
    fn deref(&self) -> &PgConnection {
        match self {
            Self::Legacy(c) => &**c,
            Self::Shared(tx) => &**tx,
        }
    }
}
impl DerefMut for ReadSession<'_> {
    fn deref_mut(&mut self) -> &mut PgConnection {
        match self {
            Self::Legacy(c) => &mut **c,
            Self::Shared(tx) => &mut **tx,
        }
    }
}
fn error(e: sqlx::Error) -> DatabaseError {
    DatabaseError::QueryError(e.to_string())
}

impl PostgresDbManager {
    pub(crate) async fn read_session(&self) -> DatabaseResult<ReadSession<'_>> {
        let Some(binding) = &self.shared else {
            return Ok(ReadSession::Legacy(
                self.pool.acquire().await.map_err(error)?,
            ));
        };
        // Existing binding uses the SAME repeatable-read, read-only transaction
        // for ownership, request-derived SQL timeout and the eventual data read.
        let mut tx = binding.begin(self).await?;
        // These bounded API reads expand several authorization views. JIT's
        // compilation cost exceeds their request budget even for one page.
        // Keep this local to the read transaction and restore the pool setting.
        sqlx::query("SET LOCAL jit=off")
            .execute(&mut *tx)
            .await
            .map_err(error)?;
        // Resolve and lock every required view until this read ends. There is
        // no window in which a missing/dropped view can use a raw public table.
        let schema = match binding.read_contract {
            crate::shared_rooms::ReadContract::LegacyK => "social_shared_legacy_api",
            crate::shared_rooms::ReadContract::Unified => "social_shared_api",
        };
        // Schema names come only from the two compile-time contract variants.
        sqlx::query(&format!("SELECT 1 FROM {schema}.k_contents, {schema}.k_votes, {schema}.k_broadcasts, {schema}.k_follows, {schema}.k_blocks, {schema}.k_mentions, {schema}.k_hashtags, {schema}.k_vars, social_shared.current_tips, social_shared.current_retained_wire_roots LIMIT 0"))
            .execute(&mut *tx).await.map_err(error)?;
        if binding.read_contract == crate::shared_rooms::ReadContract::Unified {
            sqlx::query("SELECT 1 FROM social_shared_api.current_polls, social_shared.current_poll_votes, social_shared_legacy_api.k_contents LIMIT 0")
                .execute(&mut *tx)
                .await
                .map_err(error)?;
        }
        sqlx::query(&format!(
            "SET LOCAL search_path=pg_catalog,{schema},pg_temp"
        ))
        .execute(&mut *tx)
        .await
        .map_err(error)?;
        Ok(ReadSession::Shared(tx))
    }

    pub(crate) async fn resolve_current_content_alias(
        &self,
        content_id: &str,
    ) -> DatabaseResult<Option<String>> {
        let Some(binding) = &self.shared else {
            return Ok(None);
        };
        if binding.read_contract != crate::shared_rooms::ReadContract::Unified {
            return Ok(None);
        }
        let id = hex::decode(content_id)
            .map_err(|_| DatabaseError::InvalidInput("Invalid post ID".into()))?;
        if id.len() != 32 {
            return Ok(None);
        }
        let mut read = self.read_session().await?;
        // Both sides must be current and the selected native card must remain
        // visible under all deletion/edit/source/pending/reorg fences. Match
        // original signed envelope, not only a possibly shared signature.
        let query = r#"
            SELECT encode(visible.transaction_id,'hex')
            FROM social_shared_api.current_actions literal
            JOIN LATERAL (
                SELECT social_shared_api.selected_k_contents(literal.subject) AS id
                OFFSET 0
            ) picked ON true
            JOIN social_shared_api.k_contents visible ON visible.transaction_id=picked.id
            JOIN social_shared_api.current_actions chosen ON chosen.transaction_id=picked.id
            WHERE literal.transaction_id=$1 AND literal.native_table='k_contents'
              AND literal.provenance='canonical' AND picked.id<>literal.transaction_id
              AND chosen.subject=literal.subject
              -- The owning writer stores the full immutable verified wire
              -- payload on every action, including aliases never materialized.
              -- Its native snapshot is optional and contains edited state.
              -- Compare the entire envelope, not a fabricated snapshot shape.
              AND lower(literal.source_payload)=lower(chosen.source_payload)
              AND NOT EXISTS (
                SELECT 1 FROM social_shared.extensions malformed
                WHERE malformed.transaction_id=literal.transaction_id
                  AND malformed.kind='poll' AND NOT malformed.shape_valid
              )
              AND (visible.content_type<>'poll' OR EXISTS (
                SELECT 1 FROM social_shared.extensions original_poll
                JOIN social_shared.extensions selected_poll ON selected_poll.transaction_id=chosen.transaction_id
                WHERE original_poll.transaction_id=literal.transaction_id
                  AND original_poll.kind='poll' AND selected_poll.kind='poll'
                  AND original_poll.shape_valid AND selected_poll.shape_valid
                  AND original_poll.revision=literal.revision AND selected_poll.revision=chosen.revision
                  AND original_poll.options=selected_poll.options AND original_poll.closes_at=selected_poll.closes_at
              ))
              AND (visible.content_type NOT IN ('quote','repost') OR EXISTS (
                SELECT 1 FROM social_shared.content_keys k
                WHERE k.transaction_id=literal.transaction_id AND k.signature=visible.sender_signature
                  AND k.author=visible.sender_pubkey AND k.kind=visible.content_type
                  AND k.reference IS NOT DISTINCT FROM visible.referenced_content_id
              ))
              AND NOT EXISTS (
                SELECT 1 FROM social_shared.content_keys k
                JOIN social_shared.extensions e ON e.kind='unquote' AND e.sender=k.author AND e.target=k.reference
                JOIN social_shared_api.current_actions mutation ON mutation.transaction_id=e.transaction_id
                WHERE k.transaction_id=literal.transaction_id AND k.kind IN('quote','repost')
                  AND e.shape_valid AND e.revision=mutation.revision AND __UNQUOTE_ORDER__
              )
            LIMIT 1
        "#.replace("__UNQUOTE_ORDER__", include_str!("shared_unquote_order.sql"));
        sqlx::query_scalar(&query)
            .bind(id)
            .fetch_optional(&mut *read)
            .await
            .map_err(error)
    }

    pub(crate) async fn validate_shared_reads(&self) -> DatabaseResult<()> {
        let _read = self.read_session().await?;
        Ok(())
    }
}
