use std::{collections::{HashMap, HashSet}, hash::{Hash, Hasher}, path::PathBuf, time::{SystemTime, UNIX_EPOCH}};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc, NaiveDate, NaiveTime};
use regex::Regex;
use seahash::SeaHasher;
use serde::{Serialize, Deserialize};
use tokio_postgres::Row;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

const FRAGMENT: &AsciiSet = &CONTROLS.add(b' ').add(b'"').add(b'<').add(b'>').add(b'`');

use crate::indexer::ColumnSpec;

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub database: String,
    pub indexers: HashMap<String, toml::Table>,
    #[serde(default="num_cpus::get")]
    pub concurrency: usize,
    pub semantic: crate::semantic::SemanticSearchConfig
}

fn load_config() -> Result<Config> {
    let data = String::from_utf8(std::fs::read("./config.toml")?)?;
    toml::from_str(&data).context("config parse failed")
}

lazy_static::lazy_static! {
    pub static ref CONFIG: Config = load_config().unwrap();
}

pub fn hash_thing<H: Hash>(s: &H) -> i64 {
    let mut h = SeaHasher::new();
    s.hash(&mut h);
    i64::from_ne_bytes(h.finish().to_ne_bytes())
}

pub fn hash_str(s: &str) -> i64 {
    hash_thing(&s)
}

pub fn hash_to_color(property: &str, s: &str) -> String {
    format!("{}: hsl({}deg, 100%, 40%)", property, hash_str(s) % 360)
}

pub fn systemtime_to_utc(s: SystemTime) -> Result<DateTime<Utc>> {
    let duration = s.duration_since(UNIX_EPOCH)?.as_micros();
    Ok(DateTime::from_timestamp_micros(duration as i64).context("invalid time")?)
}

lazy_static::lazy_static! {
    static ref HEADER_TAGS: HashSet<&'static [u8]> = ["h1", "h2", "h3", "h4", "h5", "h6"].into_iter().map(str::as_bytes).collect();
    static ref INLINE_TAGS: HashSet<&'static [u8]> = ["span", "sub", "sup", "small", "i", "b", "em", "strong", "strike", "d", "a", "link", "head", "font"].into_iter().map(str::as_bytes).collect();
    static ref IGNORE_TAGS: HashSet<&'static [u8]> = ["style", "script", "nav", "iframe", "svg"].into_iter().map(str::as_bytes).collect();
    static ref SELF_CLOSING_NEWLINE_TAGS: HashSet<&'static [u8]> = ["hr", "br"].into_iter().map(str::as_bytes).collect();
    static ref NEWLINES: Regex = Regex::new(r"\n{3,}").unwrap();
    static ref SPACE_ON_NEWLINES: Regex = Regex::new(r"\n\s+").unwrap();
}

pub fn parse_html(html: &[u8], prefer_title_tag: bool) -> (String, String) {
    use html5gum::Token;

    let mut text = String::new();
    let mut title_from_header = String::new();
    let mut title_from_tag = String::new();

    let mut ignore_stack = vec![];
    let mut current_header: Option<Vec<u8>> = None;
    let mut in_title = false;

    for token in html5gum::Tokenizer::new(html).infallible() {
        match token {
            Token::StartTag(tag) => {
                let name = tag.name.0.as_slice();
                if IGNORE_TAGS.contains(name) {
                    ignore_stack.push(tag.name.0);
                    continue;
                }
                if HEADER_TAGS.contains(name) {
                    current_header = Some(name.to_vec());
                }
                if name == b"title" || name == b"dc:title" {
                    in_title = true;
                }
                if SELF_CLOSING_NEWLINE_TAGS.contains(name) && !text.ends_with("\n\n") {
                    text.push('\n');
                }
            },
            Token::String(s) => {
                let s = String::from_utf8_lossy(&s);
                let s = s.trim_matches('\n');
                if ignore_stack.is_empty() {
                    if !in_title {
                        text.push_str(s);
                    }
                    if in_title {
                        title_from_tag.push_str(s);
                    }
                    if current_header.is_some() {
                        title_from_header.push_str(s);
                    }
                }
            },
            Token::EndTag(tag) => {
                let name = tag.name.0;
                if let Some(ref header) = current_header {
                    if header == &name {
                        current_header = None;
                    }
                }
                if ignore_stack.is_empty() && !INLINE_TAGS.contains(name.as_slice()) {
                    if !text.ends_with("\n\n") {
                        text.push('\n');
                    }
                }
                if let Some(last) = ignore_stack.last() {
                    if last == &name {
                        ignore_stack.pop();
                    }
                }
                if name.as_slice() == b"title" || name == b"dc:title" {
                    in_title = false;
                }
            },
            _ => ()
        }
    }

    let title = if prefer_title_tag {
        if !title_from_tag.is_empty() { title_from_tag } else { title_from_header }
    } else {
        if !title_from_header.is_empty() { title_from_header } else { title_from_tag }
    };

    (NEWLINES.replace_all(&SPACE_ON_NEWLINES.replace_all(&text, "\n"), "\n\n").trim().to_string(), title)
}

pub async fn parse_pdf(path: &PathBuf) -> Result<(String, String)> {
    // Rust does not seem to have a robust library for this.
    let res = tokio::process::Command::new("pdftotext")
        .arg("-htmlmeta")
        .arg(path)
        .arg("/dev/stdout")
        .output().await?;
    if !res.status.success() {
        return Err(anyhow!("PDF parsing: {}", String::from_utf8_lossy(&res.stderr)));
    }
    Ok(parse_html(&res.stdout, true))
}

// Wrong but probably-good-enough parsing of dates.
fn parse_naive_date(s: &str) -> Result<DateTime<Utc>> {
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").or_else(|_| NaiveDate::parse_from_str(s, "%Y%m%d"))?;
    let datetime = date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    Ok(DateTime::from_naive_utc_and_offset(datetime, Utc))
}

pub fn parse_date(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s).map(|x| x.into()).or_else(|_| parse_naive_date(s))
}

pub fn get_column_string(row: &Row, index: usize, spec: &ColumnSpec) -> Option<String> {
    if spec.is_array {
        let mut out = String::new();
        let xs: Option<Vec<String>> = row.get(index);
        for x in xs? {
            out.push_str(&x);
            out.push('\n');
        }
        Some(out)
    } else {
        row.get(index)
    }
}

pub fn urlencode(s: &str) -> String {
    utf8_percent_encode(s, FRAGMENT).to_string()
}