use std::sync::Arc;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use crate::indexer::{Ctx, Indexer, TableSpec, ColumnSpec};
use chrono::prelude::*;
use rusqlite::OpenFlags;

#[derive(Serialize, Deserialize)]
struct Config {
    db_path: String
}

#[derive(Clone)]
pub struct AnkiIndexer {
    config: Arc<Config>
}

#[async_trait::async_trait]
impl Indexer for AnkiIndexer {
    fn name(&self) -> &'static str {
        "anki"
    }

    fn schemas(&self) -> &'static [&'static str] {
        &[r#"
CREATE TABLE cards (
    id BIGINT PRIMARY KEY,
    front TEXT NOT NULL,
    back TEXT NOT NULL,
    timestamp TIMESTAMPTZ
);
        "#]
    }

    fn tables(&self) -> &'static [TableSpec] {
        &[
            TableSpec {
                name: "cards",
                parent: None,
                columns: &[
                    ColumnSpec {
                        name: "front",
                        fts: true,
                        fts_short: false,
                        trigram: false,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "back",
                        fts: true,
                        fts_short: false,
                        trigram: false,
                        is_array: false
                    }
                ],
                url_source_column: None,
                title_source_column: "front",
                summary_columns: &["front", "back", "timestamp"]
            }
        ]
    }

    async fn run(&self, ctx: Arc<Ctx>) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let self = self.clone();
        let bg = tokio::task::spawn_blocking(move || self.read_database(tx));

        let mut conn = ctx.pool.get().await?;
        while let Some((id, fields, modtime)) = rx.recv().await {
            let (front, back): (&str, &str) = fields.split_once('\x1F').context("parse fail")?;
            let modtime = DateTime::from_timestamp(modtime, 0).context("parse fail")?;
            let row = conn.query_opt("SELECT timestamp FROM cards WHERE id = $1", &[&id]).await?;
            if let Some(row) = row {
                let last_modtime: DateTime<Utc> = row.get(0);
                if last_modtime >= modtime {
                    continue;
                }
            }
            let tx = conn.transaction().await?;
            tx.execute("DELETE FROM cards WHERE id = $1", &[&id]).await?;
            tx.execute("INSERT INTO cards VALUES ($1, $2, $3, $4)",
                &[&id, &front, &back, &modtime])
                .await?;
            tx.commit().await?;
        }

        // TODO at some point: delete removed notes

        bg.await??;

        Ok(())
    }

    fn url_for(&self, _table: &str, _column_content: &str) -> String {
        unimplemented!()
    }
}

impl AnkiIndexer {
    pub async fn new(config: toml::Table) -> Result<Box<Self>> {
        let config: Config = config.try_into()?;
        Ok(Box::new(AnkiIndexer {
            config: Arc::new(config)
        }))
    }

    fn read_database(&self, target: tokio::sync::mpsc::Sender<(i64, String, i64)>) -> Result<()> {
        let conn = rusqlite::Connection::open_with_flags(&self.config.db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let mut notes_stmt = conn.prepare("SELECT id, flds, mod FROM notes")?;
        for row in notes_stmt.query_map([], |row| {
            let id = row.get(0)?;
            let fields: String = row.get(1)?;
            let modtime: i64 = row.get(2)?;
            Ok((id, fields, modtime))
        })? {
            let row = row?;
            target.blocking_send(row)?;
        }
        Ok(())
    }
}