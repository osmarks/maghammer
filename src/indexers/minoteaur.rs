use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use crate::util::hash_str;
use crate::indexer::{Ctx, Indexer, TableSpec, ColumnSpec};
use chrono::prelude::*;
use rusqlite::OpenFlags;
use tracing::instrument;

#[derive(Serialize, Deserialize)]
struct Config {
    db_path: String,
    base_url: String
}

#[derive(Clone)]
pub struct MinoteaurIndexer {
    config: Arc<Config>
}

// https://github.com/osmarks/minoteaur-8/blob/master/src/storage.rs
// https://github.com/osmarks/minoteaur-8/blob/master/src/util.rs
mod minoteaur_types {
    use serde::{Deserialize, Serialize};
    use ulid::Ulid;
    use chrono::Utc;
    use std::collections::{BTreeSet, HashMap};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub enum Value {
        Text(String),
        Number(f64)
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    pub struct ContentSize {
        pub words: usize,
        pub bytes: usize,
        pub lines: usize
    }

    pub type StructuredData = Vec<(String, Value)>;

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct File {
        #[serde(with="ulid::serde::ulid_as_u128")]
        pub page: Ulid,
        pub filename: String,
        #[serde(with="chrono::serde::ts_milliseconds")]
        pub created: chrono::DateTime<Utc>,
        pub storage_path: String,
        pub size: u64,
        pub mime_type: String,
        pub metadata: HashMap<String, String>
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct Page {
        #[serde(with="ulid::serde::ulid_as_u128")]
        pub id: Ulid,
        #[serde(with="chrono::serde::ts_milliseconds")]
        pub updated: chrono::DateTime<Utc>,
        #[serde(with="chrono::serde::ts_milliseconds")]
        pub created: chrono::DateTime<Utc>,
        pub title: String,
        pub names: BTreeSet<String>,
        pub content: String,
        pub tags: BTreeSet<String>,
        pub size: ContentSize,
        #[serde(default)]
        pub files: HashMap<String, File>,
        #[serde(default)]
        pub icon_filename: Option<String>,
        #[serde(default)]
        pub structured_data: StructuredData,
        #[serde(default)]
        pub theme: Option<String>
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub enum RevisionType {
        AddName(String),
        AddTag(String),
        ContentUpdate { new_content_size: ContentSize, edit_distance: Option<u32> },
        PageCreated,
        RemoveName(String),
        RemoveTag(String),
        AddFile(String),
        RemoveFile(String),
        SetIconFilename(Option<String>),
        SetStructuredData(StructuredData),
        SetTheme(Option<String>),
        Rename(String)
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct PageView {
        #[serde(with="ulid::serde::ulid_as_u128")]
        pub page: Ulid,
        #[serde(with="chrono::serde::ts_milliseconds")]
        pub time: chrono::DateTime<Utc>
    }

        #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct RevisionHeader {
        #[serde(with="ulid::serde::ulid_as_u128")]
        pub id: Ulid,
        #[serde(with="ulid::serde::ulid_as_u128")]
        pub page: Ulid,
        pub ty: RevisionType,
        #[serde(with="chrono::serde::ts_milliseconds")]
        pub time: chrono::DateTime<Utc>
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub enum Object {
        Page(Page),
        Revision(RevisionHeader),
        PageView(PageView)
    }
}

#[async_trait::async_trait]
impl Indexer for MinoteaurIndexer {
    fn name(&self) -> &'static str {
        "minoteaur"
    }

    fn schemas(&self) -> &'static [&'static str] {
        &[r#"
CREATE TABLE IF NOT EXISTS mino_pages (
    id BIGINT PRIMARY KEY,
    ulid TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    created TIMESTAMPTZ NOT NULL,
    title TEXT NOT NULL,
    words INTEGER NOT NULL,
    tags TEXT[] NOT NULL,
    names TEXT[] NOT NULL,
    content TEXT NOT NULL,
    view_count BIGINT NOT NULL,
    revision_count BIGINT NOT NULL,
    last_view_timestamp TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS mino_structured_data (
    id BIGSERIAL PRIMARY KEY,
    page BIGINT NOT NULL REFERENCES mino_pages(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    numeric_value DOUBLE PRECISION,
    text_value TEXT
);
CREATE TABLE IF NOT EXISTS mino_files (
    id BIGSERIAL PRIMARY KEY,
    page BIGINT NOT NULL REFERENCES mino_pages(id) ON DELETE CASCADE,
    filename TEXT NOT NULL,
    size INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL
);
        "#]
    }

    fn tables(&self) -> &'static [TableSpec] {
        &[
            TableSpec {
                name: "mino_pages",
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
                        name: "content",
                        fts: true,
                        fts_short: false,
                        trigram: false,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "tags",
                        fts: true,
                        fts_short: true,
                        trigram: true,
                        is_array: true
                    },
                    ColumnSpec {
                        name: "names",
                        fts: true,
                        fts_short: true,
                        trigram: true,
                        is_array: true
                    }
                ],
                url_source_column: Some("ulid"),
                title_source_column: "title",
                summary_columns: &["title", "tags", "timestamp", "created", "words", "view_count", "revision_count"]
            },
            TableSpec {
                name: "mino_structured_data",
                parent: Some(("page", "mino_pages")),
                columns: &[
                    ColumnSpec {
                        name: "key",
                        fts: false,
                        fts_short: false,
                        trigram: true,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "text_value",
                        fts: true,
                        fts_short: true,
                        trigram: true,
                        is_array: false
                    }
                ],
                title_source_column: "key",
                url_source_column: None,
                summary_columns: &["key", "text_value", "numeric_value"]
            },
            TableSpec {
                name: "mino_files",
                parent: Some(("page", "mino_pages")),
                columns: &[
                    ColumnSpec {
                        name: "filename",
                        fts: true,
                        fts_short: true,
                        trigram: true,
                        is_array: false
                    }
                ],
                url_source_column: None,
                title_source_column: "filename",
                summary_columns: &["filename", "size", "timestamp", "metadata"]
            }
        ]
    }

    async fn run(&self, ctx: Arc<Ctx>) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(200);
        let self = self.clone();
        let bg = tokio::task::spawn_blocking(move || self.read_database(tx));

        let mut timestamps = HashMap::new();
        let mut conn = ctx.pool.get().await?;

        for row in conn.query("SELECT ulid, timestamp, last_view_timestamp FROM mino_pages", &[]).await? {
            let ulid: String = row.get(0);
            let updated: DateTime<Utc> = row.get(1);
            let last_view_timestamp: DateTime<Utc> = row.get(2);
            timestamps.insert(ulid::Ulid::from_string(&ulid)?, (updated, last_view_timestamp));
        }

        while let Some((id, object)) = rx.recv().await {
            MinoteaurIndexer::write_object(&mut conn, id, object, &timestamps).await?;
        }

        // Minoteaur doesn't have a delete button so not supporting deletes is clearly fine, probably.

        bg.await??;

        Ok(())
    }

    fn url_for(&self, _table: &str, column_content: &str) -> String {
        format!("{}#/page/{}", self.config.base_url, column_content)
    }
}

impl MinoteaurIndexer {
    pub async fn new(config: toml::Table) -> Result<Box<Self>> {
        let config: Config = config.try_into()?;
        Ok(Box::new(MinoteaurIndexer {
            config: Arc::new(config)
        }))
    }

    fn read_database(&self, target: tokio::sync::mpsc::Sender<(ulid::Ulid, minoteaur_types::Object)>) -> Result<()> {
        let conn = rusqlite::Connection::open_with_flags(&self.config.db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        // Minoteaur databases are structured so that the system state can be understood by reading objects in ID order, as ID increases with timestamp.
        // Technically the clocks might not be perfectly monotonic but this is unlikely enough to ever be significant that I don't care.
        let mut objs_stmt = conn.prepare("SELECT id, data FROM objects ORDER BY id ASC")?;
        for row in objs_stmt.query_map([], |row| {
            let id: Vec<u8> = row.get(0)?;
            let data: Vec<u8> = row.get(1)?;
            Ok((id, data))
        })? {
            let (id, data) = row?;
            target.blocking_send((ulid::Ulid::from_bytes(id.try_into().map_err(|_| anyhow!("conversion failure"))?), rmp_serde::decode::from_slice(&data).context("parse object")?))?;
        }
        Ok(())
    }

    #[instrument(skip(conn, timestamps))]
    async fn write_object(conn: &mut tokio_postgres::Client, id: ulid::Ulid, object: minoteaur_types::Object, timestamps: &HashMap<ulid::Ulid, (DateTime<Utc>, DateTime<Utc>)>) -> Result<()> {
        match object {
            minoteaur_types::Object::Page(page) => {
                // If we already have the latest information on this page, skip it.
                if let Some((updated_timestamp, _last_view_timestamp)) = timestamps.get(&id) {
                    if *updated_timestamp >= page.updated {
                        return Ok(());
                    }
                }
                let ulid = id.to_string();
                let int_id = hash_str(&ulid);
                let tx = conn.transaction().await?;
                tx.execute("DELETE FROM mino_pages WHERE id = $1", &[&int_id]).await?;
                tx.execute("INSERT INTO mino_pages VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, 0, $10)",
                    &[&int_id, &ulid, &page.updated, &page.created, &page.title, &(page.size.words as i32), &page.tags.into_iter().collect::<Vec<String>>(), &page.names.into_iter().collect::<Vec<String>>(), &page.content, &page.created])
                    .await?;
                for (key, value) in page.structured_data {
                    let (num, text) = match value {
                        minoteaur_types::Value::Number(x) => (Some(x), None),
                        minoteaur_types::Value::Text(t) => (None, Some(t))
                    };
                    tx.execute("INSERT INTO mino_structured_data (page, key, numeric_value, text_value) VALUES ($1, $2, $3, $4)", &[&int_id, &key, &num, &text]).await?;
                }
                for (_key, file) in page.files {
                    tx.execute("INSERT INTO mino_files (page, filename, size, timestamp, metadata) VALUES ($1, $2, $3, $4, $5)", &[&int_id, &file.filename, &(file.size as i32), &file.created, &tokio_postgres::types::Json(file.metadata)]).await?;
                }
                tx.commit().await?;
            },
            // These should only occur after the page's record, with the exception of page creation.
            minoteaur_types::Object::PageView(view) => {
                if let Some((_updated_timestamp, last_view_timestamp)) = timestamps.get(&view.page) {
                    if *last_view_timestamp >= view.time {
                        return Ok(());
                    }
                }
                let int_id = hash_str(&view.page.to_string());
                conn.execute("UPDATE mino_pages SET view_count = view_count + 1, last_view_timestamp = $2 WHERE id = $1", &[&int_id, &view.time]).await?;
            },
            minoteaur_types::Object::Revision(rev) => {
                // There's no separate "last revision timestamp" because revisions should always be associated with the updated field being adjusted.
                if let Some((updated_timestamp, _last_view_timestamp)) = timestamps.get(&rev.page) {
                    if *updated_timestamp >= rev.time {
                        return Ok(());
                    }
                }
                if let minoteaur_types::RevisionType::PageCreated = rev.ty {
                    return Ok(());
                }
                let int_id = hash_str(&rev.page.to_string());
                conn.execute("UPDATE mino_pages SET revision_count = revision_count + 1 WHERE id = $1", &[&int_id]).await?;
            }
        }
        Ok(())
    }
}
