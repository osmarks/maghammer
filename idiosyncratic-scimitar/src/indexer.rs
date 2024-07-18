use std::{collections::HashSet, sync::Arc};
use anyhow::{Context, Result};
use compact_str::CompactString;
use deadpool_postgres::Pool;
use futures::{pin_mut, TryStreamExt};

use crate::util::hash_str;

#[derive(Default)]
pub struct ColumnSpec {
    pub name: &'static str,
    pub fts: bool,
    pub fts_short: bool,
    pub trigram: bool,
    pub is_array: bool
}

pub struct TableSpec {
    pub name: &'static str,
    pub parent: Option<(&'static str, &'static str)>,
    pub columns: &'static [ColumnSpec],
    pub url_source_column: Option<&'static str>,
    pub title_source_column: &'static str,
    pub summary_columns: &'static [&'static str]
}

impl sea_query::Iden for &TableSpec {
    fn unquoted(&self, s: &mut dyn std::fmt::Write) {
        write!(s, "{}", self.name).unwrap();
    }
}

impl sea_query::Iden for &ColumnSpec {
    fn unquoted(&self, s: &mut dyn std::fmt::Write) {
        write!(s, "{}", self.name).unwrap();
    }
}

impl TableSpec {
    pub fn column(&self, name: &str) -> Option<&'static ColumnSpec> {
        self.columns.iter().find(|x| x.name == name)
    }
}

pub struct Ctx {
    pub pool: Pool
}

#[async_trait::async_trait]
pub trait Indexer: Sync + Send {
    async fn run(&self, ctx: Arc<Ctx>) -> Result<()>;

    fn name(&self) -> &'static str;
    fn schemas(&self) -> &'static [&'static str];
    fn tables(&self) -> &'static [TableSpec];
    fn url_for(&self, table: &str, column_content: &str) -> String;

    fn table(&self, name: &str) -> Option<&'static TableSpec> {
        self.tables().iter().find(|x| x.name == name)
    }
}

pub async fn delete_nonexistent_files(ctx: Arc<Ctx>, select_paths: &str, delete_by_id: &str, existing: &HashSet<CompactString>) -> Result<()> {
    let conn = ctx.pool.get().await?;
    let it = conn.query_raw(select_paths, [""; 0]).await?;
    pin_mut!(it);
    while let Some(row) = it.try_next().await? {
        let path: String = row.get(0);
        let path = CompactString::from(path);
        if !existing.contains(&path) {
            conn.execute(delete_by_id, &[&hash_str(&path)]).await?;
        }
    }
    Ok(())
}