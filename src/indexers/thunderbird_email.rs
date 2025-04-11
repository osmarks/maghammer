use std::collections::HashSet;
use std::{collections::HashMap, path::PathBuf};
use std::sync::Arc;
use anyhow::{Context, Result};
use compact_str::ToCompactString;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;
use crate::indexer::{Ctx, Indexer, TableSpec, ColumnSpec};
use crate::util::{hash_thing, parse_html, async_walkdir};
use chrono::prelude::*;
use tokio::{fs::File, io::{AsyncBufReadExt, BufReader}};
use mail_parser::MessageParser;
use std::future::Future;
use compact_str::CompactString;
use tracing::instrument;

#[derive(Serialize, Deserialize)]
struct Config {
    mboxes_path: String,
    account_mapping: HashMap<String, String>,
    ignore_mboxes: HashSet<String>
}

#[derive(Clone)]
pub struct EmailIndexer {
    config: Arc<Config>
}

#[derive(Debug, Serialize, Deserialize)]
enum Body {
    Plain(String),
    Html(String)
}

#[derive(Debug, Serialize, Deserialize)]
struct Email {
    message_id: String,
    reply_to: Option<String>,
    date: DateTime<Utc>,
    raw: Vec<u8>,
    from: String,
    from_address: String,
    subject: String,
    body: Body
}

lazy_static::lazy_static! {
    static ref MULTIPART_REGEX: regex::bytes::Regex = regex::bytes::RegexBuilder::new(r#"content-type:\s*multipart/(mixed|alternative);\s*boundary="?([A-Za-z0-9=_-]+)"?;?\r\n$"#).case_insensitive(true).build().unwrap();
}

#[instrument(skip(callback))]
async fn read_mbox<U: Future<Output=Result<()>>, F: FnMut(Email) -> U>(path: &PathBuf, mut callback: F) -> Result<()> {
    let input = File::open(path).await?;
    let mut buf = Vec::new();
    let parse_current = |buf: &mut Vec<u8>| -> Result<Option<Email>> {
        let message = MessageParser::default().parse(buf.as_slice()).context("parse error")?;
        if message.date().is_none() || message.from().is_none() { buf.clear(); return Ok(None); }
        let email = Email {
            message_id: message.header("Message-ID").map(|h| h.as_text().context("parse error")).transpose()?.or_else(|| message.subject()).context("no message ID or subject")?.to_string(),
            reply_to: message.header("In-Reply-To").map(|s| s.as_text().context("parse error").map(|s| s.to_string())).transpose()?,
            date: DateTime::from_timestamp(message.date().context("missing date")?.to_timestamp(), 0).context("invalid time")?,
            raw: buf.clone(),
            from: message.from().map(|c| {
                let g = c.clone().into_group()[0].clone();
                g.name.map(|x| x.to_string()).unwrap_or(g.addresses[0].name.as_ref().map(|x| x.to_string()).unwrap_or(g.addresses[0].address.as_ref().unwrap().to_string()))
            }).context("missing from")?,
            from_address: message.from().map(|c| {
                let g = c.clone().into_group()[0].clone();
                g.addresses[0].address.as_ref().unwrap().to_string()
            }).context("missing from")?,
            subject: message.subject().map(|x| x.to_string()).unwrap_or_default(),
            body: message.body_html(0).map(|s| s.to_string()).map(Body::Html)
                .or_else(|| message.body_text(0).map(|i| i.to_string()).map(Body::Plain)).context("they will never find the body")?
        };

        buf.clear();
        Ok(Some(email))
    };
    let mut reader = BufReader::new(input);
    let mut line = Vec::new();
    let mut current_delim = None;
    while let Ok(x) = reader.read_until(0x0A, &mut line).await {
        if x == 0 {
            break;
        }
        if line.is_empty() {
            break;
        }
        // sorry.
        // Thunderbird doesn't seem to escape "From " in message bodies at all.
        // I originally thought I could just detect lines containing only "From ", but it turns out that some of them contain further text!
        // I don't know how Thunderbird itself parses these, but my best guess is that it's parsing the mbox and MIME at the same time.
        // I don't want to so I'm accursedly partially parsing the MIME.
        if let Some(cap) = MULTIPART_REGEX.captures(&buf) {
            if let Some(m) = cap.get(2) {
                current_delim = Some(m.as_bytes().to_vec());
            }
        }
        if let Some(ref delim) = current_delim {
            if line.len() > 6 && &line[2..(line.len()-4)] == delim.as_slice() && line.starts_with(b"--") && line.ends_with(b"--\r\n") {
                current_delim = None;
            }
        }

        if current_delim.is_none() && line.starts_with(b"From ") && !buf.is_empty() {
            if let Ok(Some(mail)) = parse_current(&mut buf) {
                callback(mail).await?;
            }
        } else {
            buf.extend(&line);
        }
        line.clear();
    }
    if !buf.is_empty() { if let Ok(Some(mail)) = parse_current(&mut buf) { callback(mail).await? } }
    Ok(())
}

#[async_trait::async_trait]
impl Indexer for EmailIndexer {
    fn name(&self) -> &'static str {
        "email"
    }

    fn schemas(&self) -> &'static [&'static str] {
        &[r#"
CREATE TABLE IF NOT EXISTS emails (
    id BIGINT PRIMARY KEY,
    message_id TEXT,
    reply_to TEXT,
    timestamp TIMESTAMPTZ NOT NULL,
    raw BYTEA NOT NULL,
    account TEXT NOT NULL,
    box TEXT NOT NULL,
    from_ TEXT,
    from_addr TEXT,
    subject TEXT NOT NULL,
    body TEXT NOT NULL
);
        "#]
    }

    fn tables(&self) -> &'static [TableSpec] {
        &[
            TableSpec {
                name: "emails",
                parent: None,
                columns: &[
                    ColumnSpec {
                        name: "from_",
                        fts: true,
                        fts_short: true,
                        trigram: true,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "subject",
                        fts: true,
                        fts_short: true,
                        trigram: true,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "body",
                        fts: true,
                        fts_short: false,
                        trigram: false,
                        is_array: false
                    }
                ],
                url_source_column: None,
                title_source_column: "subject",
                summary_columns: &["account", "box", "subject", "from_", "timestamp"]
            }
        ]
    }

    async fn run(&self, ctx: Arc<Ctx>) -> Result<()> {
        let mut js: tokio::task::JoinSet<Result<()>> = tokio::task::JoinSet::new();
        let mut entries = async_walkdir(self.config.mboxes_path.clone().into(), false, |_| true);
        let config = self.config.clone();
        while let Some(entry) = entries.try_next().await? {
            let path = entry.path();
            let mbox = path.file_stem().and_then(|x| x.to_str()).context("invalid path")?.to_compact_string();
            let folder = path.parent().unwrap().file_name().unwrap().to_str().unwrap().to_compact_string();
            if let Some(account) = config.account_mapping.get(&*folder) {
                let account = account.to_compact_string();
                let ext = path.extension();
                if let None = ext {
                    if !self.config.ignore_mboxes.contains(mbox.as_str()) {
                        let ctx = ctx.clone();
                        js.spawn(EmailIndexer::process_mbox(ctx.clone(), path.to_path_buf(), mbox, folder, account.clone()));
                    }
                }
            }
        }

        while let Some(next) = js.join_next().await {
            next??;
        }

        Ok(())
    }

    fn url_for(&self, _table: &str, _column_content: &str) -> String {
        unimplemented!()
    }
}

impl EmailIndexer {
    pub async fn new(config: toml::Table) -> Result<Box<Self>> {
        let config: Config = config.try_into()?;
        Ok(Box::new(EmailIndexer {
            config: Arc::new(config)
        }))
    }

    #[instrument(skip(ctx))]
    async fn process_mbox(ctx: Arc<Ctx>, path: PathBuf, mbox: CompactString, folder: CompactString, account: CompactString) -> Result<()> {
        tracing::trace!("reading mailbox");
        read_mbox(&path, move |mail| {
            let ctx = ctx.clone();
            let account = account.clone();
            let mbox = mbox.trim_end_matches("-1").to_compact_string();
            async move {
                let conn = ctx.pool.get().await?;
                let id = hash_thing(&mail.raw.as_slice());
                let body = match mail.body {
                    Body::Plain(t) => t,
                    Body::Html(h) => parse_html(h.as_bytes(), false).0
                };
                conn.execute(r#"INSERT INTO emails VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (id) DO UPDATE SET box = $7"#, &[
                    &id,
                    &mail.message_id,
                    &mail.reply_to,
                    &mail.date,
                    &mail.raw,
                    &account.as_str(),
                    &mbox.as_str(),
                    &mail.from.replace("\0", ""),
                    &mail.from_address.replace("\0", ""),
                    &mail.subject.replace("\0", ""),
                    &body.replace("\0", "")
                ]).await?;
                Ok(())
            }
        }).await
    }
}
