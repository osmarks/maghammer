use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;
use anyhow::{Context, Result};
use async_walkdir::{WalkDir, DirEntry};
use compact_str::{CompactString, ToCompactString};
use epub::doc::EpubDoc;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use crate::util::{hash_str, parse_html, parse_date, systemtime_to_utc, CONFIG};
use crate::indexer::{delete_nonexistent_files, Ctx, Indexer, TableSpec, ColumnSpec};
use chrono::prelude::*;
use std::str::FromStr;
use tracing::instrument;

#[derive(Serialize, Deserialize, Clone)]
struct Config {
    path: String
}

pub struct BooksIndexer {
    config: Config
}

#[derive(Clone, Debug)]
pub struct EpubParse {
    title: String,
    author: Vec<String>,
    description: String,
    series: Option<String>,
    series_index: Option<f32>,
    publication_date: DateTime<Utc>,
    chapters: Vec<(String, String)>,
    tags: Vec<String>
}

#[instrument]
pub fn parse_epub(path: &PathBuf) -> Result<EpubParse> {
    let mut epub = EpubDoc::new(path).context("initial doc load")?;

    let description = epub.metadata.get("description")
        .and_then(|v| v.iter().map(|x| x.as_str()).next())
        .unwrap_or_default();
    let description = parse_html(description.as_bytes(), false).0;
    let publication_date = epub.metadata.get("date").context("invalid book (no date)")?.get(0).context("invalid book")?;
    let publication_date = parse_date(&publication_date)?;
    let series_index = epub.metadata.get("calibre:series_index").and_then(|x| x.get(0)).and_then(|x| f32::from_str(x.as_str()).ok());
    let series = epub.metadata.get("calibre:series").and_then(|x| x.get(0)).cloned();

    let mut parse = EpubParse {
        title: epub.metadata.get("title").context("invalid book (no title)")?.get(0).context("invalid book")?.to_string(),
        author: epub.metadata.get("creator").map(|x| x.clone()).unwrap_or_default(),
        description,
        series_index: series.as_ref().and(series_index),
        series,
        publication_date,
        chapters: vec![],
        tags: epub.metadata.get("subject").cloned().unwrap_or_else(Vec::new)
    };

    tracing::trace!("read epub {:?}", path);

    let mut seen = HashSet::new();
    for navpoint in epub.toc.clone() {
        let path = navpoint.content;
        let mut last = path.to_str().context("invalid path")?;
        if let Some((fst, _snd)) = last.split_once('#') {
            last = fst;
        }
        // It's valid for a single XHTML file in a book to contain multiple semantic "chapters".
        // Splitting at the link target would be annoying, so we discard repeats like that.
        if !seen.contains(last) {
            seen.insert(last.to_string());
            let resource = epub.get_resource_str_by_path(last).with_context(|| format!("resource {} nonexistent", last))?;
            let html = parse_html(resource.as_bytes(), true);
            tracing::trace!("read epub chapter {:?}", navpoint.label);
            parse.chapters.push((
                html.0,
                navpoint.label.clone()
            ));
        }
    }

    Ok(parse)
}

#[instrument(skip(ctx, existing_files))]
async fn handle_epub(relpath: CompactString, ctx: Arc<Ctx>, entry: DirEntry, existing_files: Arc<RwLock<HashSet<CompactString>>>) -> Result<()> {
    let epub = entry.path();

    let mut conn = ctx.pool.get().await?;
    let metadata = entry.metadata().await?;
    let row = conn.query_opt("SELECT timestamp FROM books WHERE id = $1", &[&hash_str(&relpath)]).await?;
    existing_files.write().await.insert(relpath.clone());
    let timestamp: DateTime<Utc> = row.map(|r| r.get(0)).unwrap_or(DateTime::<Utc>::MIN_UTC);
    let modtime = systemtime_to_utc(metadata.modified()?)?;

    if modtime > timestamp {
        let parse_result = match tokio::task::spawn_blocking(move || parse_epub(&epub)).await? {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("Failed parse for {}: {}", relpath, e);
                return Ok(())
            }
        };
        let tx = conn.transaction().await?;
        let id = hash_str(&relpath);
        tx.execute("DELETE FROM books WHERE id = $1", &[&id]).await?;
        tx.execute("INSERT INTO books VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", &[
            &id,
            &relpath.as_str(),
            &parse_result.title,
            &parse_result.author,
            &parse_result.description,
            &parse_result.series,
            &parse_result.series_index,
            &parse_result.publication_date,
            &parse_result.tags,
            &modtime
        ]).await?;
        for (ix, (content, title)) in parse_result.chapters.into_iter().enumerate() {
            tx.execute("INSERT INTO chapters (book, chapter_index, chapter_title, content) VALUES ($1, $2, $3, $4)", &[
                &id,
                &(ix as i16),
                &title,
                &content
            ]).await?;
        }
        tx.commit().await?;
    }
    Result::Ok(())
}

#[async_trait::async_trait]
impl Indexer for BooksIndexer {
    fn name(&self) -> &'static str {
        "books"
    }

    fn schemas(&self) -> &'static [&'static str] {
        &[r#"
CREATE TABLE books (
    id BIGINT PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    author TEXT[] NOT NULL,
    description TEXT NOT NULL,
    series TEXT,
    series_index REAL,
    publication_date TIMESTAMPTZ,
    tags TEXT[],
    timestamp TIMESTAMPTZ
);

CREATE TABLE chapters (
    id BIGSERIAL PRIMARY KEY,
    book BIGINT NOT NULL REFERENCES books (id) ON DELETE CASCADE,
    chapter_index SMALLINT NOT NULL,
    chapter_title TEXT NOT NULL,
    content TEXT NOT NULL
);
        "#]
    }

    fn tables(&self) -> &'static [TableSpec] {
        &[
            TableSpec {
                name: "books",
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
                        name: "author",
                        fts: false,
                        fts_short: false,
                        trigram: true,
                        is_array: true
                    },
                    ColumnSpec {
                        name: "description",
                        fts: true,
                        fts_short: false,
                        trigram: false,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "tags",
                        fts: false,
                        fts_short: false,
                        trigram: true,
                        is_array: true
                    }
                ],
                url_source_column: None,
                title_source_column: "title",
                summary_columns: &["title", "author", "tags", "series", "series_index", "publication_date"]
            },
            TableSpec {
                name: "chapters",
                parent: Some(("book", "books")),
                columns: &[
                    ColumnSpec {
                        name: "chapter_title",
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
                url_source_column: None,
                title_source_column: "chapter_title",
                summary_columns: &["chapter_index", "chapter_title"]
            }
        ]
    }

    async fn run(&self, ctx: Arc<Ctx>) -> Result<()> {
        let existing_files = Arc::new(RwLock::new(HashSet::new()));

        let entries = WalkDir::new(&self.config.path);
        let base_path = &self.config.path;

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
                if !entry.file_type().await?.is_file() || "epub" != ext.unwrap_or_default() {
                    return Ok(());
                }
                let conn = ctx.pool.get().await?;
                existing_files.write().await.insert(CompactString::from(path));
                let metadata = entry.metadata().await?;
                let row = conn.query_opt("SELECT timestamp FROM books WHERE id = $1", &[&hash_str(path)]).await?;
                let timestamp: DateTime<Utc> = row.map(|r| r.get(0)).unwrap_or(DateTime::<Utc>::MIN_UTC);
                let modtime = systemtime_to_utc(metadata.modified()?)?;
                if modtime > timestamp {
                    let relpath = entry.path().as_path().strip_prefix(&base_path)?.as_os_str().to_str().context("invalid path")?.to_compact_string();
                    handle_epub(relpath.clone(), ctx, entry, existing_files).await
                } else {
                    Ok(())
                }
            }
        }).await?;

        {
            let existing = existing_files.read().await;
            delete_nonexistent_files(ctx, "SELECT path FROM books", "DELETE FROM books WHERE id = $1", &existing).await?;
        }

        Ok(())
    }

    fn url_for(&self, _table: &str, _column_content: &str) -> String {
        unimplemented!()
    }
}

impl BooksIndexer {
    pub async fn new(config: toml::Table) -> Result<Box<Self>> {
        let config: Config = config.try_into()?;
        Ok(Box::new(BooksIndexer {
            config
        }))
    }
}
