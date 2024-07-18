use std::sync::Arc;
use anyhow::Result;
use futures::pin_mut;
use serde::{Deserialize, Serialize};
use crate::{indexer::{ColumnSpec, Ctx, Indexer, TableSpec}, util::hash_str};
use chrono::prelude::*;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type};
use rusqlite::OpenFlags;

#[derive(Serialize, Deserialize)]
struct Config {
    db_path: String
}

#[derive(Clone)]
pub struct AtuinIndexer {
    config: Arc<Config>
}

#[async_trait::async_trait]
impl Indexer for AtuinIndexer {
    fn name(&self) -> &'static str {
        "atuin"
    }

    fn schemas(&self) -> &'static [&'static str] {
        &[r#"
CREATE TABLE shell_history (
    id BIGINT PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    command TEXT NOT NULL,
    cwd TEXT,
    duration BIGINT,
    exit INTEGER,
    hostname TEXT NOT NULL,
    session TEXT NOT NULL
);
        "#]
    }

    fn tables(&self) -> &'static [TableSpec] {
        &[
            TableSpec {
                name: "shell_history",
                parent: None,
                columns: &[
                    ColumnSpec {
                        name: "command",
                        fts: true,
                        fts_short: false,
                        trigram: false,
                        is_array: false
                    },
                ],
                url_source_column: None,
                title_source_column: "command",
                summary_columns: &["timestamp", "hostname", "command", "cwd", "duration", "exit"]
            }
        ]
    }

    async fn run(&self, ctx: Arc<Ctx>) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
        let self = self.clone();

        let conn = ctx.pool.get().await?;

        let max_timestamp = conn.query_one("SELECT max(timestamp) FROM shell_history", &[]).await?.get::<_, Option<DateTime<Utc>>>(0);
        let max_timestamp = max_timestamp.map(|x| x.timestamp_nanos_opt().unwrap() + 999).unwrap_or(0); // my code had better not be deployed unchanged in 2262; we have to add 1000 because of Postgres timestamp rounding
        let bg = tokio::task::spawn_blocking(move || self.read_database(tx, max_timestamp));
        
        let sink = conn.copy_in("COPY shell_history (id, timestamp, command, cwd, duration, exit, hostname, session) FROM STDIN BINARY").await?;

        let writer = BinaryCopyInWriter::new(sink, &[Type::INT8, Type::TIMESTAMPTZ, Type::TEXT, Type::TEXT, Type::INT8, Type::INT4, Type::TEXT, Type::TEXT]);
        pin_mut!(writer);

        while let Some((id, timestamp, duration, exit, command, cwd, session, hostname)) = rx.recv().await {
            let id = hash_str(&id);
            let timestamp = DateTime::from_timestamp_nanos(timestamp);
            let duration = if duration != -1 { Some(duration) } else { None };
            let exit = if exit != -1 { Some(exit) } else { None };
            let cwd = if cwd != "unknown" { Some(cwd) } else { None };
            writer.as_mut().write(&[&id, &timestamp, &command, &cwd, &duration, &exit, &hostname, &session]).await?;
        }

        writer.finish().await?;

        bg.await??;

        Ok(())
    }

    fn url_for(&self, _table: &str, _column_content: &str) -> String {
        unimplemented!()
    }
}

impl AtuinIndexer {
    pub async fn new(config: toml::Table) -> Result<Box<Self>> {
        let config: Config = config.try_into()?;
        Ok(Box::new(AtuinIndexer {
            config: Arc::new(config)
        }))
    }

    fn read_database(&self, target: tokio::sync::mpsc::Sender<(String, i64, i64, i32, String, String, String, String)>, min_timestamp: i64) -> Result<()> {
        let conn = rusqlite::Connection::open_with_flags(&self.config.db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let mut fetch_stmt = conn.prepare("SELECT id, timestamp, duration, exit, command, cwd, session, hostname FROM history WHERE timestamp > ? ORDER BY timestamp ASC")?;
        for row in fetch_stmt.query_map([min_timestamp], |row| {
            let id = row.get(0)?;
            let timestamp = row.get(1)?;
            let duration = row.get(2)?;
            let exit = row.get(3)?;
            let command = row.get(4)?;
            let cwd = row.get(5)?;
            let session = row.get(6)?;
            let hostname = row.get(7)?;
            Ok((id, timestamp, duration, exit, command, cwd, session, hostname))
        })? {
            let row = row?;
            target.blocking_send(row)?;
        }
        Ok(())
    }
}