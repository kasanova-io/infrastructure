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
        sqlx::query(&format!("SELECT 1 FROM {schema}.k_contents, {schema}.k_votes, {schema}.k_broadcasts, {schema}.k_follows, {schema}.k_blocks, {schema}.k_mentions, {schema}.k_hashtags, {schema}.k_vars, social_shared.current_tips LIMIT 0"))
            .execute(&mut *tx).await.map_err(error)?;
        if binding.read_contract == crate::shared_rooms::ReadContract::Unified {
            sqlx::query("SELECT 1 FROM social_shared_api.current_polls LIMIT 0")
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

    pub(crate) async fn validate_shared_reads(&self) -> DatabaseResult<()> {
        let _read = self.read_session().await?;
        Ok(())
    }
}
