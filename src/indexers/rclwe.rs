use std::{collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};
use anyhow::{Context, Result};
use futures::pin_mut;
use serde::{Deserialize, Serialize};
use crate::{indexer::{ColumnSpec, Ctx, Indexer, TableSpec}, util::{hash_str, parse_html, systemtime_to_utc}};
use chrono::prelude::*;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type};

#[derive(Serialize, Deserialize)]
struct Config {
    input: String,
    output: String
}

#[derive(Clone)]
pub struct RclweIndexer {
    config: Arc<Config>
}

fn parse_circache_meta(s: &str) -> Option<HashMap<String, String>> {
    let mut out = HashMap::new();
    for line in s.lines() {
        if !line.is_empty() {
            let (fst, snd) = line.split_once(" = ")?;
            out.insert(fst.to_string(), snd.to_string());
        }
    }
    Some(out)
}

async fn move_file(from: &PathBuf, to: PathBuf) -> Result<()> {
    tokio::fs::copy(from, to).await?;
    tokio::fs::remove_file(from).await?;
    Ok(())
}

#[async_trait::async_trait]
impl Indexer for RclweIndexer {
    fn name(&self) -> &'static str {
        "rclwe"
    }

    fn schemas(&self) -> &'static [&'static str] {
        &[r#"
CREATE TABLE webpages (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    url TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    html TEXT NOT NULL
);
        "#]
    }

    fn tables(&self) -> &'static [TableSpec] {
        &[
            TableSpec {
                name: "webpages",
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
                    }
                ],
                url_source_column: Some("url"),
                title_source_column: "title",
                summary_columns: &["timestamp", "url", "title"]
            }
        ]
    }

    async fn run(&self, ctx: Arc<Ctx>) -> Result<()> {
        let conn = ctx.pool.get().await?;        
        let sink = conn.copy_in("COPY webpages (timestamp, url, title, content, html) FROM STDIN BINARY").await?;

        let input = PathBuf::from(&self.config.input);
        let mut readdir = tokio::fs::read_dir(&self.config.input).await?;

        let output = PathBuf::from(&self.config.output);

        let writer = BinaryCopyInWriter::new(sink, &[Type::TIMESTAMPTZ, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT]);
        pin_mut!(writer);

        while let Some(entry) = readdir.next_entry().await? {
            if entry.file_type().await?.is_file() {
                let path = entry.path();
                let name = path.file_name().unwrap().to_str().context("invalid path")?;
                // for some reason there are several filename formats in storage
                let mut metadata = None;
                if name.starts_with("firefox-recoll-web") {
                    let meta_file = "_".to_owned() + name;
                    let mtime = systemtime_to_utc(entry.metadata().await?.modified()?)?;
                    if let Some(url) = tokio::fs::read_to_string(input.join(&meta_file)).await?.lines().next() {
                        metadata = Some((mtime, url.to_string()));
                        tokio::fs::rename(input.join(&meta_file), output.join(&meta_file)).await?;
                    } else {
                        log::warn!("Metadata error for {}", name);
                    }                    
                }
                if let Some(rem) = name.strip_suffix(".html") {
                    let meta_file = format!("{}.dic", rem);
                    let meta = parse_circache_meta(&tokio::fs::read_to_string(input.join(&meta_file)).await?).context("invalid metadata format")?;
                    let mtime = i64::from_str(meta.get("fmtime").context("invalid metadata format")?)?;
                    metadata = Some((DateTime::from_timestamp(mtime, 0).context("time is broken")?, meta.get("url").context("invalid metadata format")?.to_string()));
                    move_file(&input.join(&meta_file), output.join(&meta_file)).await?;
                }
                if let Some(rem) = name.strip_prefix("recoll-we-c") {
                    let meta_file = format!("recoll-we-m{}", rem);
                    let mtime = systemtime_to_utc(entry.metadata().await?.modified()?)?;
                    if tokio::fs::try_exists(input.join(&meta_file)).await? {
                        if let Some(url) = tokio::fs::read_to_string(input.join(&meta_file)).await?.lines().next() {
                            metadata = Some((mtime, url.to_string()));
                            move_file(&input.join(&meta_file), output.join(&meta_file)).await?;
                        } else {
                            log::warn!("Metadata error for {}", name);
                        }
                    } else {
                        log::warn!("No metadata for {}", name);
                    }
                }

                if let Some((mtime, url)) = metadata {
                    let html = tokio::fs::read(&path).await?;
                    move_file(&path, output.join(name)).await?;
                    let (content, title) = parse_html(html.as_slice(), true);
                    writer.as_mut().write(&[&mtime, &url, &title.replace("\0", ""), &content.replace("\0", ""), &String::from_utf8_lossy(&html).replace("\0", "")]).await?;
                }
            }
        }

        writer.finish().await?;

        Ok(())
    }

    fn url_for(&self, _table: &str, column_content: &str) -> String {
        column_content.to_string()
    }
}

impl RclweIndexer {
    pub async fn new(config: toml::Table) -> Result<Box<Self>> {
        let config: Config = config.try_into()?;
        Ok(Box::new(RclweIndexer {
            config: Arc::new(config)
        }))
    }
}