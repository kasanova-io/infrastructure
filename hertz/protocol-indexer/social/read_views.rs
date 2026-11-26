//! Read projections reuse native action selection and edit authorization.
//! They do not write history, choose new identities, or authorize notifications.
use anyhow::Result;
use sqlx::{Postgres, Transaction};

pub async fn install(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let sql = include_str!("read_views.sql")
        .replace(
            "__AUTHORIZED_EDIT__",
            crate::kaposts_projection::AUTHORIZED_EDIT,
        )
        .replace(
            "__UNQUOTE_ORDER__",
            crate::kaposts_projection::UNQUOTE_ORDER,
        );
    sqlx::raw_sql(&sql).execute(&mut **tx).await?;
    Ok(())
}

#[test]
fn pure_reader_selection_functions_match_the_native_projection_policy() {
    let sql = include_str!("read_views.sql");
    let normalize = |value: &str| value.split_whitespace().collect::<Vec<_>>().join(" ");
    for table in [
        "k_contents",
        "k_votes",
        "k_broadcasts",
        "k_follows",
        "k_blocks",
    ] {
        let delimiter = format!("$selection_{table}$");
        let body = sql
            .split(&delimiter)
            .nth(1)
            .expect("reader selector exists");
        let native = crate::shared_social::selection_sql(table);
        assert_eq!(
            normalize(body),
            normalize(&format!(
                "SELECT picked.transaction_id FROM ({native}) picked"
            )),
            "{table}"
        );
    }
    assert_eq!(sql.matches("__AUTHORIZED_EDIT__").count(), 1);
    assert_eq!(sql.matches("__UNQUOTE_ORDER__").count(), 1);
}
