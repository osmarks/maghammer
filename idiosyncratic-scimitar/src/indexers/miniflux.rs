use std::sync::Arc;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;
use crate::{indexer::{ColumnSpec, Ctx, Indexer, TableSpec}, util::parse_html};
use chrono::prelude::*;

#[derive(Serialize, Deserialize)]
struct Config {
    source_database: String
}

#[derive(Clone)]
pub struct MinifluxIndexer {
    config: Arc<Config>
}

#[async_trait::async_trait]
impl Indexer for MinifluxIndexer {
    fn name(&self) -> &'static str {
        "miniflux"
    }

    fn schemas(&self) -> &'static [&'static str] {
        &[r#"
CREATE TABLE rss_items (
    id BIGINT PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    content_html TEXT NOT NULL,
    content TEXT NOT NULL,
    author TEXT,
    feed_title TEXT NOT NULL
);
        "#]
    }

    fn tables(&self) -> &'static [TableSpec] {
        &[
            TableSpec {
                name: "rss_items",
                parent: None,
                columns: &[
                    ColumnSpec {
                        name: "title",
                        fts: true,
                        fts_short: false,
                        trigram: true,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "content",
                        fts: true,
                        fts_short: false,
                        trigram: false,
                        is_array: false
                    }
                ],
                url_source_column: Some("url"),
                title_source_column: "title",
                summary_columns: &["timestamp", "title", "url", "feed_title"]
            }
        ]
    }

    async fn run(&self, ctx: Arc<Ctx>) -> Result<()> {
        let (src_conn, connection) = tokio_postgres::connect(&self.config.source_database, NoTls).await?;

        tokio::spawn(connection);

        let dest_conn = ctx.pool.get().await?;
        let max_id = dest_conn.query_one("SELECT max(id) FROM rss_items", &[]).await?.get::<_, Option<i64>>(0).unwrap_or(-1);
        let rows = src_conn.query("SELECT entries.id, entries.published_at, entries.title, entries.url, entries.content, entries.author, feeds.title FROM entries JOIN feeds ON feeds.id = entries.feed_id WHERE entries.id > $1 ORDER BY entries.id", &[&max_id]).await?;

        for row in rows {
            let id: i64 = row.get(0);
            let pubdate: DateTime<Utc> = row.get(1);
            let title: String = row.get(2);
            let url: String = row.get(3);
            let content_html: String = row.get(4);
            let author: String = row.get(5);
            let feed_title: String = row.get(6);
            let content = parse_html(content_html.as_bytes(), false).0;
            dest_conn.execute("INSERT INTO rss_items VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", &[&id, &pubdate, &title, &url, &content_html, &content, &author, &feed_title]).await?;
        }

        Ok(())
    }

    fn url_for(&self, _table: &str, column_content: &str) -> String {
        column_content.to_string()
    }
}

impl MinifluxIndexer {
    pub async fn new(config: toml::Table) -> Result<Box<Self>> {
        let config: Config = config.try_into()?;
        Ok(Box::new(MinifluxIndexer {
            config: Arc::new(config)
        }))
    }
}