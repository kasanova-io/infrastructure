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
        // Resolve and lock every required view until this read ends. There is
        // no window in which a missing/dropped view can use a raw public table.
        sqlx::query("SELECT 1 FROM social_shared_api.k_contents, social_shared_api.k_votes, social_shared_api.k_broadcasts, social_shared_api.k_follows, social_shared_api.k_blocks, social_shared_api.k_mentions, social_shared_api.k_hashtags, social_shared_api.k_vars LIMIT 0")
            .execute(&mut *tx).await.map_err(error)?;
        sqlx::query("SET LOCAL search_path=pg_catalog,social_shared_api,pg_temp")
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
