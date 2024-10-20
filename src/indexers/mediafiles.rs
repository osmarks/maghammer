use std::fmt;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::str::FromStr;
use anyhow::{anyhow, Context, Result};
use compact_str::CompactString;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tokio::sync::RwLock;
use crate::util::{hash_str, parse_date, parse_html, systemtime_to_utc, urlencode, CONFIG};
use crate::indexer::{Ctx, Indexer, TableSpec, delete_nonexistent_files, ColumnSpec};
use async_walkdir::{Filtering, WalkDir};
use chrono::prelude::*;
use regex::{RegexSet, Regex};
use tracing::instrument;

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    path: String,
    #[serde(default)]
    ignore_regexes: Vec<String>,
    #[serde(default)]
    ignore_metadata: Vec<String>,
    base_url: String
}

pub struct MediaFilesIndexer {
    config: Config,
    ignore_files: RegexSet,
    ignore_metadata: Arc<RegexSet>
}

#[derive(Debug)]
enum AutoSubsState {
    NotRequired = 0,
    Pending = 1,
    Generated = 2
}

impl Default for AutoSubsState {
    fn default() -> Self {
        AutoSubsState::Pending
    }
}

#[derive(Debug, Default)]
struct MediaParse {
    format: Vec<String>,
    creation_timestamp: Option<DateTime<Utc>>,
    title: Option<String>,
    description: Option<String>,
    series: Option<String>,
    season: Option<i32>,
    episode: Option<i32>,
    creator: Option<String>,
    url: Option<String>,
    duration: f32,
    subs: Option<String>,
    chapters: String,
    raw_parse: serde_json::Value,
    auto_subs: AutoSubsState
}

#[derive(Deserialize, Debug)]
struct Chapter {
    start_time: String,
    end_time: String,
    tags: HashMap<String, String>
}

#[derive(Deserialize, Debug)]
struct Disposition {
    default: u8
}

#[derive(Deserialize, Debug)]
struct Stream {
    index: usize,
    codec_name: Option<String>,
    duration: Option<String>,
    codec_type: String,
    #[serde(default)]
    tags: HashMap<String, String>,
    width: Option<u32>,
    disposition: Option<Disposition>
}

#[derive(Deserialize, Debug)]
struct Format {
    duration: String,
    #[serde(default)]
    tags: HashMap<String, String>
}

#[derive(Deserialize, Debug)]
struct Probe {
    streams: Vec<Stream>,
    chapters: Vec<Chapter>,
    format: Format
}

lazy_static::lazy_static! {
    static ref SEASON_EPNUM: Regex = Regex::new(r"(?i:s(\d+)e(\d+)[e0-9-]*\s*)").unwrap();
    static ref DURATION_STRING: Regex = Regex::new(r"(\d+):(\d+):(\d+[.,]\d+)").unwrap();
}

fn parse_duration(s: &str) -> Result<f32> {
    let mat = DURATION_STRING.captures(s).context("duration misformatted")?;
    let duration = f32::from_str(mat.get(1).unwrap().as_str())? * 3600.0
        + f32::from_str(mat.get(2).unwrap().as_str())? * 60.0
        + f32::from_str(&mat.get(3).unwrap().as_str().replace(",", "."))?;

   Ok(duration)
}

fn format_duration<W: fmt::Write>(mut seconds: f32, mut f: W) {
    let hours = (seconds / 3600.0).floor();
    seconds -= 3600.0 * hours;
    let minutes = (seconds / 60.0).floor();
    seconds -= 60.0 * minutes;
    let full_seconds = seconds.floor();
    write!(&mut f, "{:02}:{:02}:{:02}", hours, minutes, full_seconds).unwrap();
}

#[derive(Debug)]
enum SRTParseState {
    ExpectSeqNumOrData,
    ExpectTime,
    ExpectData
}

#[instrument(skip(srt))]
fn parse_srt(srt: &str) -> Result<String> {
    use SRTParseState::*;

    let mut out = String::new();
    let mut time = (0.0, 0.0);
    let mut state = ExpectSeqNumOrData;
    for line in srt.lines() {
        match state {
            ExpectSeqNumOrData => {
                if usize::from_str(line).is_ok() { state = ExpectTime }
                else { state = ExpectData }
            },
            ExpectTime => {
                let (fst, snd) = line.split_once(" --> ").context("invalid time line")?;
                time = (parse_duration(fst)?, parse_duration(snd)?);
                state = ExpectData;
            },
            ExpectData => {
                if line == "" {
                    state = ExpectSeqNumOrData;
                } else {
                    let line = &parse_html(line.as_bytes(), false).0; // SRT can contain basic HTML for some reason
                    if !out.trim_end().ends_with(line.trim_end()) {
                        out.push_str("[");
                        format_duration(time.0, &mut out);
                        out.push_str(" -> ");
                        format_duration(time.1, &mut out);
                        out.push_str("] ");
                        out.push_str(line);
                        out.push_str("\n");
                    }
                }
            }
        }
    }
    Ok(out)
}

fn parse_filename(path: &PathBuf) -> Option<(String, String, Option<i32>, Option<i32>)> {
    let stem = path.file_stem()?.to_str()?;
    let dirname = path.parent()?.file_name()?.to_str()?;
    if let Some(mat) = SEASON_EPNUM.captures(stem) {
        let name = &stem[mat.get(0).unwrap().end()..];
        let se = i32::from_str(mat.get(1).unwrap().as_str()).unwrap();
        let ep = i32::from_str(mat.get(2).unwrap().as_str()).unwrap();
        Some((dirname.to_string(), name.to_string(), Some(se), Some(ep)))
    } else {
        Some((dirname.to_string(), stem.to_string(), None, None))
    }
}

mod test {
    #[test]
    fn test_filename_parser() {
        use std::{path::PathBuf, str::FromStr};

        assert_eq!(super::parse_filename(&PathBuf::from_str("Test 1/s01e02-03 Example.mkv").unwrap()), Some((
            String::from("Test 1"),
            String::from("Example"),
            Some(1),
            Some(2)
        )));

        assert_eq!(super::parse_filename(&PathBuf::from_str("Test 2/s9e9 Simple.mkv").unwrap()), Some((
            String::from("Test 2"),
            String::from("Simple"),
            Some(9),
            Some(9)
        )));

        assert_eq!(super::parse_filename(&PathBuf::from_str("Invalid").unwrap()), None);
    }

    #[test]
    fn test_duration() {
        use super::{parse_duration, format_duration};

        let mut s = String::new();
        format_duration(parse_duration("00:01:02,410").unwrap(), &mut s);
        assert_eq!(s, "00:01:02");
    }
}

fn score_subtitle_stream(stream: &Stream, others: &Vec<Stream>, parse_so_far: &MediaParse) -> i8 {
    if stream.width.is_some() {
        return i8::MIN; // video subtitle track - unusable
    }
    if stream.codec_type != "subtitle" {
        return i8::MIN;
    }
    // subtitles from these videos are problematic - TODO this is not a very elegant way to do this
    match &parse_so_far.description {
        Some(x) if x.starts_with("https://web.microsoftstream.com/video/") => return i8::MIN,
        _ => ()
    };
    // YouTube subtitle tracks are weird, at least as I download them
    match &parse_so_far.url {
        Some(x) if x.starts_with("https://www.youtube.com") => {
            // Due to something, I appear to sometimes have streams duplicated under different labels.
            for other in others {
                if other.index != stream.index && stream.tags.get("DURATION") == other.tags.get("DURATION") && stream.tags.get("language") == other.tags.get("language") && stream.codec_name == other.codec_name {
                    let result = score_subtitle_stream(other, &vec![], parse_so_far);
                    if result < 0 {
                        return result;
                    }
                }
            }
            let title = stream.tags.get("title").map(String::as_str).unwrap_or_default();
            if title == "English (Original)" {
                return -1
            }
            if title.starts_with("English from") && title != "English from English" {
                return -2
            }
        },
        _ => ()
    }
    match stream.disposition {
        Some(Disposition { default: 1 }) => return 2,
        _ => ()
    }
    1
}

#[instrument]
async fn parse_media(path: &PathBuf, ignore: Arc<RegexSet>) -> Result<MediaParse> {
    tracing::trace!("starting ffprobe");
    let ffmpeg = Command::new("ffprobe")
        .arg("-hide_banner")
        .arg("-print_format").arg("json")
        .arg("-v").arg("error")
        .arg("-show_chapters")
        .arg("-show_format")
        .arg("-show_streams")
        .arg(path)
        .output().await?;
    if !ffmpeg.status.success() {
        return Err(anyhow!("ffmpeg failure: {}", String::from_utf8_lossy(&ffmpeg.stderr)))
    }
    tracing::trace!("ffprobe successful");
    let probe: Probe = serde_json::from_slice(&ffmpeg.stdout).context("ffmpeg parse")?;

    let mut result = MediaParse {
        raw_parse: serde_json::from_slice(&ffmpeg.stdout)?,
        ..Default::default()
    };

    if let Some((series, title, season, episode)) = parse_filename(path) {
        result.series = Some(series);
        result.title = Some(title);
        result.season = season;
        result.episode = episode;
    }

    {
        let mut tags = probe.format.tags;

        let mut drop = vec![];
        for (k, v) in tags.iter() {
            if ignore.is_match(v) {
                drop.push(k.to_string());
            }
        }
        for x in drop {
            tags.remove(&x);
        }

        if let Some(date) = tags.get("creation_time").or_else(|| tags.get("DATE")).or_else(|| tags.get("date")) {
            result.creation_timestamp = parse_date(date).ok();
        }

        result.title = result.title.or(tags.get("title").cloned());
        result.url = tags.get("PURL").cloned();
        result.description = tags.get("DESCRIPTION")
            .or_else(|| tags.get("comment"))
            .or_else(|| tags.get("synopsis"))
            .cloned();
        result.creator = tags.get("ARTIST").or_else(|| tags.get("artist")).cloned();

        if let Some(season) = tags.get("season_number") {
            result.season = Some(i32::from_str(season)?);
        }
        if let Some(episode) = tags.get("episode_sort") {
            result.episode = Some(i32::from_str(episode)?);
        }
    }

    result.duration = f32::from_str(&probe.format.duration)?;

    let mut best_subtitle_track = (0, i8::MIN);

    // For some reason I forgot using stream duration was more accurate in some circumstance.
    for stream in probe.streams.iter() {
        if let Some(dur) = &stream.duration {
            result.duration = result.duration.max(f32::from_str(&dur)?);
        }
        if let Some(dur) = stream.tags.get("DURATION") {
            result.duration = result.duration.max(parse_duration(&dur)?);
        }

        result.format.push(stream.codec_name.as_ref().unwrap_or(&stream.codec_type).to_string());

        let score = score_subtitle_stream(stream, &probe.streams, &result);
        if score > best_subtitle_track.1 {
            best_subtitle_track = (stream.index, score)
        }
    }

    // if we have any remotely acceptable subtitles, use ffmpeg to read them out
    if best_subtitle_track.1 > i8::MIN {
        tracing::trace!("reading subtitle track {:?}", best_subtitle_track);

        let ffmpeg = Command::new("ffmpeg")
            .arg("-hide_banner").arg("-v").arg("quiet")
            .arg("-i").arg(path)
            .arg("-map").arg(format!("0:{}", best_subtitle_track.0))
            .arg("-f").arg("srt")
            .arg("-")
            .output().await?;
        if ffmpeg.status.success() {
            let subs = parse_srt(&String::from_utf8_lossy(&ffmpeg.stdout))?;
            if !subs.is_empty() {
                result.subs = Some(subs);
                // don't schedule automatic subtitling if existing ones okay
                if best_subtitle_track.1 > 0 {
                    result.auto_subs = AutoSubsState::NotRequired;
                }
            }
        }
    }

    for chapter in probe.chapters {
        result.chapters.push_str("[");
        format_duration(f32::from_str(&chapter.start_time)?, &mut result.chapters);
        result.chapters.push_str(" -> ");
        format_duration(f32::from_str(&chapter.end_time)?, &mut result.chapters);
        result.chapters.push_str("] ");
        result.chapters.push_str(&chapter.tags.get("title").map(String::as_str).unwrap_or(""));
        result.chapters.push_str("\n");
    }

    Ok(result)
}

#[async_trait::async_trait]
impl Indexer for MediaFilesIndexer {
    fn name(&self) -> &'static str {
        "media_files"
    }

    fn schemas(&self) -> &'static [&'static str] {
        &[r#"
CREATE TABLE media_files (
    id BIGINT PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    timestamp TIMESTAMPTZ NOT NULL,
    format TEXT[] NOT NULL,
    creation_timestamp TIMESTAMPTZ,
    title TEXT,
    description TEXT,
    series TEXT,
    season INTEGER,
    episode INTEGER,
    creator TEXT,
    url TEXT,
    duration REAL NOT NULL,
    subs TEXT,
    auto_subs_state SMALLINT NOT NULL,
    chapters TEXT,
    probe JSONB NOT NULL
);
        "#]
    }

    fn tables(&self) -> &'static [TableSpec] {
        &[
            TableSpec {
                name: "media_files",
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
                        name: "description",
                        fts: true,
                        fts_short: false,
                        trigram: false,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "series",
                        fts: false,
                        fts_short: false,
                        trigram: true,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "creator",
                        fts: false,
                        fts_short: false,
                        trigram: true,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "subs",
                        fts: true,
                        fts_short: false,
                        trigram: false,
                        is_array: false
                    },
                    ColumnSpec {
                        name: "chapters",
                        fts: true,
                        fts_short: false,
                        trigram: false,
                        is_array: false
                    }
                ],
                url_source_column: Some("path"),
                title_source_column: "title",
                summary_columns: &["path", "timestamp", "format", "creation_timestamp", "title", "series", "season", "episode", "url", "duration"]
            }
        ]
    }

    async fn run(&self, ctx: Arc<Ctx>) -> Result<()> {
        let entries = WalkDir::new(&self.config.path);
        let ignore = Arc::new(self.ignore_files.clone());
        let base_path = Arc::new(self.config.path.clone());
        let base_path_ = base_path.clone();
        let ignore_metadata = self.ignore_metadata.clone();

        let existing_files = Arc::new(RwLock::new(HashSet::new()));
        let existing_files_ = existing_files.clone();
        let ctx_ = ctx.clone();

        entries
            .filter(move |entry| {
                let ignore = ignore.clone();
                let base_path = base_path.clone();
                let path = entry.path();
                tracing::trace!("filtering {:?}", path);
                if let Some(path) = path.strip_prefix(&*base_path).ok().and_then(|x| x.to_str()) {
                    if ignore.is_match(path) {
                        return std::future::ready(Filtering::IgnoreDir);
                    }
                } else {
                    return std::future::ready(Filtering::IgnoreDir);
                }
                std::future::ready(Filtering::Continue)
            })
            .map_err(|e| anyhow::Error::from(e))
            .filter(|r| {
                // ignore permissions errors because things apparently break otherwise
                let keep = match r {
                    Err(_e) => false,
                    _ => true
                };
                async move { keep }
            })
            .try_for_each_concurrent(Some(CONFIG.concurrency), move |entry| {
                tracing::trace!("got file {:?}", entry.path());
                let ctx = ctx_.clone();
                let base_path = base_path_.clone();
                let existing_files = existing_files_.clone();
                let ignore_metadata = ignore_metadata.clone();
                async move {
                    let real_path = entry.path();
                    let path = if let Some(path) = real_path.strip_prefix(&*base_path)?.to_str() {
                        path
                    } else {
                        return Result::Ok(());
                    };
                    if !entry.file_type().await?.is_file() {
                        return Ok(());
                    }
                    let mut conn = ctx.pool.get().await?;
                    existing_files.write().await.insert(CompactString::from(path));
                    let metadata = entry.metadata().await?;
                    let row = conn.query_opt("SELECT timestamp FROM media_files WHERE id = $1", &[&hash_str(path)]).await?;
                    let timestamp: DateTime<Utc> = row.map(|r| r.get(0)).unwrap_or(DateTime::<Utc>::MIN_UTC);
                    let modtime = systemtime_to_utc(metadata.modified()?)?;
                    tracing::trace!("timestamp {:?}", timestamp);
                    if modtime > timestamp {
                        match parse_media(&real_path, ignore_metadata).await {
                            Ok(x) => {
                                let tx = conn.transaction().await?;
                                tx.execute("DELETE FROM media_files WHERE id = $1", &[&hash_str(path)]).await?;
                                tx.execute("INSERT INTO media_files VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)", &[
                                    &hash_str(path),
                                    &path,
                                    &modtime,
                                    &x.format,
                                    &x.creation_timestamp,
                                    &x.title,
                                    &x.description,
                                    &x.series,
                                    &x.season,
                                    &x.episode,
                                    &x.creator,
                                    &x.url,
                                    &x.duration,
                                    &x.subs,
                                    &(x.auto_subs as i16),
                                    &x.chapters,
                                    &tokio_postgres::types::Json(x.raw_parse)
                                ]).await?;
                                tx.commit().await?;
                            },
                            Err(e) => {
                                tracing::warn!("Media parse {}: {:?}", &path, e)
                            }
                        }
                    } else {
                        tracing::trace!("skipping {:?}", path);
                    }
                    Result::Ok(())
                }
            }).await?;

        {
            let existing = existing_files.read().await;
            delete_nonexistent_files(ctx, "SELECT path FROM media_files", "DELETE FROM media_files WHERE id = $1", &existing).await?;
        }

        Ok(())
    }

    fn url_for(&self, _table: &str, column_content: &str) -> String {
        format!("{}{}", self.config.base_url, urlencode(column_content))
    }
}

impl MediaFilesIndexer {
    pub async fn new(config: toml::Table) -> Result<Box<Self>> {
        let config: Config = config.try_into()?;
        Ok(Box::new(MediaFilesIndexer {
            ignore_files: RegexSet::new(&config.ignore_regexes)?,
            ignore_metadata: Arc::new(RegexSet::new(&config.ignore_metadata)?),
            config
        }))
    }
}
