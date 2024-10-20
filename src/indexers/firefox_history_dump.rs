use std::sync::Arc;
use anyhow::{Context, Result};
use futures::pin_mut;
use serde::{Deserialize, Serialize};
use crate::{indexer::{ColumnSpec, Ctx, Indexer, TableSpec}, util::hash_str};
use chrono::prelude::*;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type};
use rusqlite::{types::ValueRef, OpenFlags};
use tokio::sync::mpsc::Sender;
use tracing::instrument;

#[derive(Serialize, Deserialize)]
struct Config {
    db_path: String,
    include_untimestamped_records: bool
}

#[derive(Clone)]
pub struct FirefoxHistoryDumpIndexer {
    config: Arc<Config>
}

#[async_trait::async_trait]
impl Indexer for FirefoxHistoryDumpIndexer {
    fn name(&self) -> &'static str {
        "browser_history"
    }

    fn schemas(&self) -> &'static [&'static str] {
        &[r#"
CREATE TABLE browser_places (
    id BIGINT PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    url TEXT NOT NULL,
    title TEXT,
    visit_count BIGINT NOT NULL,
    description TEXT,
    preview_image TEXT
);

CREATE TABLE bookmarks (
    id BIGINT PRIMARY KEY,
    place BIGINT NOT NULL REFERENCES browser_places(id),
    title TEXT NOT NULL,
    added TIMESTAMPTZ NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE TABLE history (
    id BIGINT PRIMARY KEY,
    place BIGINT NOT NULL REFERENCES browser_places(id),
    timestamp TIMESTAMPTZ NOT NULL,
    ty INTEGER NOT NULL
);
        "#]
    }

    fn tables(&self) -> &'static [TableSpec] {
        &[
            TableSpec {
                name: "browser_places",
                parent: None,
                columns: &[
                    ColumnSpec {
                        name: "title",
                        fts: true,
                        fts_short: true,
                        trigram: true,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "description",
                        fts: true,
                        fts_short: true,
                        trigram: false,
                        is_array: false
                    }
                ],
                url_source_column: Some("url"),
                title_source_column: "title",
                summary_columns: &["url", "timestamp", "title", "visit_count", "description"]
            },
            TableSpec {
                name: "bookmarks",
                columns: &[
                    ColumnSpec {
                        name: "title",
                        fts: true,
                        fts_short: false,
                        trigram: true,
                        is_array: false
                    }
                ],
                url_source_column: None,
                parent: Some(("place", "browser_places")),
                title_source_column: "title",
                summary_columns: &["title", "added", "timestamp"]
            },
            TableSpec {
                name: "history",
                parent: Some(("place", "browser_places")),
                title_source_column: "id",
                columns: &[],
                summary_columns: &["timestamp", "ty"],
                url_source_column: None
            }
        ]
    }

    async fn run(&self, ctx: Arc<Ctx>) -> Result<()> {
        let (places_tx, mut places_rx) = tokio::sync::mpsc::channel(1000);
        let (bookmarks_tx, mut bookmarks_rx) = tokio::sync::mpsc::channel(1000);
        let (history_tx, mut history_rx) = tokio::sync::mpsc::channel(1000);
        let self = self.clone();

        let conn = ctx.pool.get().await?;

        let max_places_timestamp = conn.query_one("SELECT max(timestamp) FROM browser_places", &[]).await?.get::<_, Option<DateTime<Utc>>>(0);
        let max_places_timestamp = max_places_timestamp.map(|x| x.timestamp_micros()).unwrap_or(0);
        let max_bookmarks_timestamp = conn.query_one("SELECT max(timestamp) FROM bookmarks", &[]).await?.get::<_, Option<DateTime<Utc>>>(0);
        let max_bookmarks_timestamp = max_bookmarks_timestamp.map(|x| x.timestamp_micros()).unwrap_or(0);
        let max_history_timestamp = conn.query_one("SELECT max(timestamp) FROM history", &[]).await?.get::<_, Option<DateTime<Utc>>>(0);
        let max_history_timestamp = max_history_timestamp.map(|x| x.timestamp_micros()).unwrap_or(0);
        let bg = tokio::task::spawn_blocking(move || self.read_database(places_tx, max_places_timestamp, bookmarks_tx, max_bookmarks_timestamp, history_tx, max_history_timestamp));

        while let Some((id, url, title, visit_count, last_visit_date, description, preview_image_url)) = places_rx.recv().await {
            conn.execute("INSERT INTO browser_places VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO UPDATE SET timestamp = $2, visit_count = $5", &[&hash_str(&id), &DateTime::from_timestamp_micros(last_visit_date.unwrap_or_default()).context("invalid time")?, &url, &title.map(|x| x.replace("\0", "")), &visit_count, &description.map(|x| x.replace("\0", "")), &preview_image_url]).await?;
        }

        while let Some((id, place, title, date_added, last_modified)) = bookmarks_rx.recv().await {
            conn.execute("INSERT INTO bookmarks VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET title = $3, timestamp = $5", &[&hash_str(&id), &hash_str(&place), &title, &DateTime::from_timestamp_micros(date_added).context("invalid time")?, &DateTime::from_timestamp_micros(last_modified).context("invalid time")?]).await?;
        }

        let sink = conn.copy_in("COPY history (id, place, timestamp, ty) FROM STDIN BINARY").await?;

        let writer = BinaryCopyInWriter::new(sink, &[Type::INT8, Type::INT8, Type::TIMESTAMPTZ, Type::INT4]);
        pin_mut!(writer);

        while let Some((id, place, ts, ty)) = history_rx.recv().await {
            let id: i64 = hash_str(&id);
            let place = hash_str(&place);
            let timestamp = DateTime::from_timestamp_micros(ts).context("invalid time")?;
            writer.as_mut().write(&[&id, &place, &timestamp, &ty]).await?;
        }

        writer.finish().await?;

        bg.await??;

        Ok(())
    }

    fn url_for(&self, _table: &str, column_content: &str) -> String {
        column_content.to_string()
    }
}

impl FirefoxHistoryDumpIndexer {
    pub async fn new(config: toml::Table) -> Result<Box<Self>> {
        let config: Config = config.try_into()?;
        Ok(Box::new(FirefoxHistoryDumpIndexer {
            config: Arc::new(config)
        }))
    }

    #[instrument(skip(places, bookmarks, history, self))]
    fn read_database(&self, places: Sender<(String, String, Option<String>, i64, Option<i64>, Option<String>, Option<String>)>, places_min_ts: i64, bookmarks: Sender<(String, String, String, i64, i64)>, bookmarks_min_ts: i64, history: Sender<(String, String, i64, i32)>, history_min_ts: i64) -> Result<()> {
        let conn = rusqlite::Connection::open_with_flags(&self.config.db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        // Apparently some records in my history database have no last_visit_date. I don't know what happened to it or why, but this is not fixable, so add an ugly hack for them.
        let mut fetch_places = conn.prepare(if self.config.include_untimestamped_records { "SELECT guid, url, title, visit_count, last_visit_date, description, preview_image_url FROM places WHERE last_visit_date > ? OR last_visit_date IS NULL ORDER BY last_visit_date ASC" } else { "SELECT guid, url, title, visit_count, last_visit_date, description, preview_image_url FROM places WHERE last_visit_date > ? OR last_visit_date IS NULL ORDER BY last_visit_date ASC" })?;
        for row in fetch_places.query_map([places_min_ts], |row| {
            let description = row.get_ref(5)?;
            let description = match description {
                ValueRef::Null => None,
                ValueRef::Text(x) => Some(String::from_utf8_lossy(x).to_string()),
                _ => panic!("unexpected type")
            };
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, description, row.get(6)?))
        })? {
            let row = row?;
            places.blocking_send(row)?;
        }
        std::mem::drop(places);

        let mut fetch_bookmarks = conn.prepare("SELECT guid, bookmark, title, dateAdded, lastModified FROM bookmarks WHERE lastModified > ? AND title IS NOT NULL ORDER BY lastModified ASC")?;
        for row in fetch_bookmarks.query_map([bookmarks_min_ts], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
        })? {
            let row = row?;
            bookmarks.blocking_send(row)?;
        }
        std::mem::drop(bookmarks);

        let mut fetch_places = conn.prepare("SELECT id, place, date, type FROM historyvisits WHERE date > ? ORDER BY date ASC")?;
        for row in fetch_places.query_map([history_min_ts], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })? {
            let row = row?;
            history.blocking_send(row)?;
        }
        std::mem::drop(history);
        Ok(())
    }
}
