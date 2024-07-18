use std::collections::HashSet;
use std::ffi::OsStr;
use std::sync::Arc;
use anyhow::Result;
use compact_str::CompactString;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use crate::util::{hash_str, parse_html, parse_pdf, systemtime_to_utc, urlencode, CONFIG};
use crate::indexer::{Ctx, Indexer, TableSpec, delete_nonexistent_files, ColumnSpec};
use async_walkdir::WalkDir;
use chrono::prelude::*;
use regex::RegexSet;

#[derive(Serialize, Deserialize)]
struct Config {
    path: String,
    #[serde(default)]
    ignore_regexes: Vec<String>,
    base_url: String
}

pub struct TextFilesIndexer {
    config: Config,
    ignore: RegexSet
}

lazy_static::lazy_static! {
    static ref VALID_EXTENSIONS: HashSet<&'static str> = ["pdf", "txt", "html", "htm", "xhtml"].into_iter().collect();
}

#[async_trait::async_trait]
impl Indexer for TextFilesIndexer {
    fn name(&self) -> &'static str {
        "text_files"
    }

    fn schemas(&self) -> &'static [&'static str] {
        &[r#"
CREATE TABLE text_files (
    id BIGINT PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    title TEXT,
    content TEXT NOT NULL,
    timestamp TIMESTAMPTZ
);
        "#]
    }

    fn tables(&self) -> &'static [TableSpec] {
        &[
            TableSpec {
                name: "text_files",
                parent: None,
                columns: &[
                    ColumnSpec {
                        name: "path",
                        fts: true,
                        fts_short: true,
                        trigram: true,
                        is_array: false
                    },
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
                    }
                ],
                url_source_column: Some("path"),
                title_source_column: "title",
                summary_columns: &["path", "title", "timestamp"]
            }
        ]
    }

    async fn run(&self, ctx: Arc<Ctx>) -> Result<()> {
        let entries = WalkDir::new(&self.config.path); // TODO
        let ignore = &self.ignore;
        let base_path = &self.config.path;

        let existing_files = Arc::new(RwLock::new(HashSet::new()));

        entries.map_err(|e| anyhow::Error::from(e)).try_for_each_concurrent(Some(CONFIG.concurrency), |entry| 
            {
                let ctx = ctx.clone();
                let existing_files = existing_files.clone();
                async move {
                    let real_path = entry.path();
                    let path = if let Some(path) = real_path.strip_prefix(base_path)?.to_str() {
                        path
                    } else {
                        return Result::Ok(());
                    };
                    let ext = real_path.extension().and_then(OsStr::to_str);
                    if ignore.is_match(path) || !entry.file_type().await?.is_file() || !VALID_EXTENSIONS.contains(ext.unwrap_or_default()) {
                        return Ok(());
                    }
                    let mut conn = ctx.pool.get().await?;
                    existing_files.write().await.insert(CompactString::from(path));
                    let metadata = entry.metadata().await?;
                    let row = conn.query_opt("SELECT timestamp FROM text_files WHERE id = $1", &[&hash_str(path)]).await?;
                    let timestamp: DateTime<Utc> = row.map(|r| r.get(0)).unwrap_or(DateTime::<Utc>::MIN_UTC);
                    let modtime = systemtime_to_utc(metadata.modified()?)?;
                    if modtime > timestamp {
                        let parse = match ext {
                            Some("pdf") => {
                                parse_pdf(&real_path).await.map(Some)
                            },
                            Some("txt") => {
                                let content = tokio::fs::read(&real_path).await?;
                                Ok(Some((String::from_utf8_lossy(&content).to_string(), String::new())))
                            },
                            Some("htm") | Some("html") | Some("xhtml") => {
                                let content = tokio::fs::read(&real_path).await?;
                                Ok(Some(tokio::task::block_in_place(|| parse_html(&content, true))))
                            },
                            _ => Ok(None),
                        };
                        match parse {
                            Ok(None) => (),
                            Ok(Some((content, title))) => {
                                // Null bytes aren't legal in Postgres strings despite being valid UTF-8.
                                let tx = conn.transaction().await?;
                                tx.execute("DELETE FROM text_files WHERE id = $1", &[&hash_str(path)]).await?;
                                tx.execute("INSERT INTO text_files VALUES ($1, $2, $3, $4, $5)",
                                    &[&hash_str(path), &path, &title.replace("\0", ""), &content.replace("\0", ""), &modtime])
                                    .await?;
                                tx.commit().await?;
                            },
                            Err(e) => log::warn!("File parse for {}: {}", path, e)
                        }
                    }
                    Result::Ok(())
                }
        }).await?;

        {
            let existing = existing_files.read().await;
            delete_nonexistent_files(ctx, "SELECT path FROM text_files", "DELETE FROM text_files WHERE id = $1", &existing).await?;
        }

        Ok(())
    }

    fn url_for(&self, _table: &str, column_content: &str) -> String {
        format!("{}{}", self.config.base_url, urlencode(column_content))
    }
}

impl TextFilesIndexer {
    pub async fn new(config: toml::Table) -> Result<Box<Self>> {
        let config: Config = config.try_into()?;
        Ok(Box::new(TextFilesIndexer {
            ignore: RegexSet::new(&config.ignore_regexes)?,
            config
        }))
    }
}