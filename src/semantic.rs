use std::{collections::HashMap, sync::Arc};
use futures::{StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use pgvector::HalfVector;
use tokenizers::{tokenizer::{Error, Tokenizer}, Encoding};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use reqwest::{header::CONTENT_TYPE, Client};
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use std::fmt::Write;
use half::f16;
use tracing::instrument;

use crate::{indexer::{ColumnSpec, Indexer, TableSpec}, util::{self, get_column_string, CONFIG}};

// sorry.
// https://gist.github.com/hanxiao/3f60354cf6dc5ac698bc9154163b4e6a
lazy_static! {
    static ref CHUNKING_REGEX: pcre2::bytes::Regex = pcre2::bytes::RegexBuilder::new().utf(true).jit_if_available(true).multi_line(true).build(r#"((?:^(?:[#*=-]{1,7}|\w[^\r\n]{0,200}\r?\n[-=]{2,200}|<h[1-6][^>]{0,100}>)[^\r\n]{1,200}(?:<\/h[1-6]>)?(?:\r?\n|$))|(?:\[[0-9]+\][^\r\n]{1,800})|(?:(?:^|\r?\n)[ \t]{0,3}(?:[-*+•]|\d{1,3}\.\w\.|\[[ xX]\])[ \t]+(?![.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]\s)(?:[^\r\n]{1,200}(?:(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)|(?=[\r\n]|$))|[^\r\n]{1,200}(?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]|(?:'(?=`)|''(?=``)))(?:(?:(?!(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)).){1,100}(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$))?)[\s\]})>,']*(?:(?:\r?\n[ \t]{2,5}(?:[-*+•]|\d{1,3}\.\w\.|\[[ xX]\])[ \t]+(?![.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]\s)(?:[^\r\n]{1,200}(?:(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)|(?=[\r\n]|$))|[^\r\n]{1,200}(?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]|(?:'(?=`)|''(?=``)))(?:(?:(?!(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)).){1,100}(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$))?)[\s\]})>,']*){0,6}(?:\r?\n[ \t]{4,7}(?:[-*+•]|\d{1,3}\.\w\.|\[[ xX]\])[ \t]+(?![.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]\s)(?:[^\r\n]{1,200}(?:(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)|(?=[\r\n]|$))|[^\r\n]{1,200}(?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]|(?:'(?=`)|''(?=``)))(?:(?:(?!(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)).){1,100}(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$))?)[\s\]})>,']*){0,6})?)|(?:(?:^>(?:>|\s{2,}){0,2}(?![.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]\s)(?:[^\r\n]{1,200}(?:(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)|(?=[\r\n]|$))|[^\r\n]{1,200}(?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]|(?:'(?=`)|''(?=``)))(?:(?:(?!(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)).){1,100}(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$))?)[\s\]})>,']*\r?\n?){1,15})|(?:(?:^|\r?\n)(?:```|~~~)(?:\w{0,20})?\r?\n[\s\S]{0,1500}?(?:```|~~~)\r?\n?|(?:(?:^|\r?\n)(?: {4}|\t)[^\r\n]{0,200}(?:\r?\n(?: {4}|\t)[^\r\n]{0,200}){0,20}\r?\n?)|(?:<pre>(?:<code>)?[\s\S]{0,1500}?(?:<\/code>)?<\/pre>))|(?:(?:^|\r?\n)(?:\|[^\r\n]{0,200}\|(?:\r?\n\|[-:]{1,200}\|){0,1}(?:\r?\n\|[^\r\n]{0,200}\|){0,20}|<table>[\s\S]{0,2000}?<\/table>))|(?:^(?:[-*_]){3,}\s*$|<hr\s*\/?>)|(?![\s\]})>,'])(?:^(?:<[a-zA-Z][^>]{0,100}>)?(?![.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]\s)(?:[^\r\n]{1,800}(?:(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)|(?=[\r\n]|$))|[^\r\n]{1,800}(?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]|(?:'(?=`)|''(?=``)))(?:(?:(?!(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)).){1,100}(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$))?)[\s\]})>,']*(?:<\/[a-zA-Z]+>)?(?:\r?\n|$))|(?![\s\]})>,'])(?![.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]\s)(?:[^\r\n]{1,400}(?:(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)|(?=[\r\n]|$))|[^\r\n]{1,400}(?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]|(?:'(?=`)|''(?=``)))(?:(?:(?!(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)).){1,100}(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$))?)[\s\]})>,']*|(?:(?<!\w)"""[^"]{0,300}"""(?!\w)|(?<!\w)(?:['"`'"])[^\r\n]{0,300}\1(?!\w)|(?<!\w)`[^\r\n]{0,300}'(?!\w)|(?<!\w)``[^\r\n]{0,300}''(?!\w)|\([^\r\n()]{0,200}(?:\([^\r\n()]{0,200}\)[^\r\n()]{0,200}){0,5}\)|\[[^\r\n\[\]]{0,200}(?:\[[^\r\n\[\]]{0,200}\][^\r\n\[\]]{0,200}){0,5}\]|\$[^\r\n$]{0,100}\$|`[^`\r\n]{0,100}`)|(?![\s\]})>,'])(?:(?:^|\r?\n\r?\n)(?:<p>)?(?![.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]\s)(?:[^\r\n]{1,1000}(?:(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)|(?=[\r\n]|$))|[^\r\n]{1,1000}(?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]|(?:'(?=`)|''(?=``)))(?:(?:(?!(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)).){1,100}(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$))?)[\s\]})>,']*(?:<\/p>)?(?=\r?\n\r?\n|$))|(?:<[a-zA-Z][^>]{0,100}(?:>[\s\S]{0,1000}?<\/[a-zA-Z]+>|\s*\/>))|(?:(?:\$\$[\s\S]{0,500}?\$\$)|(?:\$[^\$\r\n]{0,100}\$))|(?![\s\]})>,'])(?![.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]\s)(?:[^\r\n]{1,800}(?:(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)|(?=[\r\n]|$))|[^\r\n]{1,800}(?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]|(?:'(?=`)|''(?=``)))(?:(?:(?!(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$)).){1,100}(?:[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?<![\s\]})>,'](?=[.!?…]|\.{3}|[\x{2026}\x{2047}-\x{2049}]|[\p{Emoji_Presentation}\p{Extended_Pictographic}]))|(?:'(?=`)|''(?=``)))(?=\S|$))?)[\s\]})>,']*)"#).unwrap();
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct SemanticSearchConfig {
    tokenizer: String,
    backend: String,
    pub embedding_dim: u32,
    max_tokens: usize,
    batch_size: usize,
    document_prefix: String,
    query_prefix: String
}

fn convert_tokenizer_error(e: Error) -> anyhow::Error {
    anyhow!("tokenizer: {}", e)
}

pub fn load_tokenizer() -> Result<Tokenizer> {
    let tokenizer = Tokenizer::from_pretrained(&CONFIG.semantic.tokenizer, None)
        .map_err(convert_tokenizer_error)?;
    Ok(tokenizer)
}

#[instrument(skip(t))]
pub fn tokenize_chunk_text(t: &Tokenizer, s: &str) -> Result<Vec<(usize, usize, String)>> {
    let mut result = vec![];

    let mut raw_regions = vec![];

    // use inscrutable semantic chunking regex, then use tokenizer to split in case some chunks are too long still
    for mat in CHUNKING_REGEX.find_iter(s.as_bytes()) {
        let mat = mat?;
        let range = mat.start()..mat.end();
        let region = &s[range];
        raw_regions.push((mat.start(), t.encode(region, false).map_err(convert_tokenizer_error)?.len()));
    }

    raw_regions.push((s.len(), CONFIG.semantic.max_tokens));

    let mut new_chunk_start = 0;
    let mut new_chunk_token_length = 0;

    for ((_region_start, token_length), (next_region_start, next_token_length)) in raw_regions.iter().zip(raw_regions.iter().skip(1)) {
        new_chunk_token_length += token_length;
        if new_chunk_token_length + next_token_length > util::CONFIG.semantic.max_tokens {
            let enc = t.encode(&s[new_chunk_start..*next_region_start], false).map_err(convert_tokenizer_error)?;

            let mut write = |enc: &Encoding| -> Result<()> {
                if !enc.get_offsets().is_empty() {
                    let offsets: Vec<(usize, usize)> = enc.get_offsets().into_iter().copied().filter(|(a, b)| *a != 0 || *b != 0).collect();
                    result.push((
                        new_chunk_start + offsets[0].0,
                        new_chunk_start + offsets[offsets.len() - 1].1,
                        t.decode(enc.get_ids(), true).map_err(convert_tokenizer_error)?
                    ));
                }
                Ok(())
            };

            write(&enc)?;
            for overflowing in enc.get_overflowing() {
                write(overflowing)?;
            }

            new_chunk_start = *next_region_start;
            new_chunk_token_length = 0;
        }
    }

    Ok(result)
}

#[derive(Serialize)]
struct EmbeddingRequest<'a> {
    text: Vec<&'a str>
}

#[derive(Deserialize)]
struct EmbeddingResponse(Vec<serde_bytes::ByteBuf>);

fn decode_fp16_buffer(buf: &[u8]) -> Vec<f16> {
    buf.chunks_exact(2)
        .map(|chunk| half::f16::from_le_bytes([chunk[0], chunk[1]]))
        .collect()
}

fn contains_nan(buf: &[f16]) -> bool {
    buf.iter().any(|x| x.is_nan())
}

#[instrument(skip(client))]
async fn send_batch(client: &Client, batch: Vec<&str>) -> Result<Vec<Vec<f16>>> {
    let res = client.post(&CONFIG.semantic.backend)
        .body(rmp_serde::to_vec_named(&EmbeddingRequest { text: batch })?)
        .header(CONTENT_TYPE, "application/msgpack")
        .send()
        .await?;
    let data: EmbeddingResponse = rmp_serde::from_read(&*res.bytes().await?)?;
    Ok(data.0.into_iter().map(|x| decode_fp16_buffer(&*x)).collect())
}

struct Chunk {
    id: i64,
    col: &'static str,
    start: i32,
    length: i32,
    text: String
}

#[derive(Debug)]
pub struct SemanticCtx {
    tokenizer: Tokenizer,
    client: reqwest::Client,
    pool: deadpool_postgres::Pool
}

impl SemanticCtx {
    pub fn new(pool: deadpool_postgres::Pool) -> Result<Self> {
        Ok(SemanticCtx { tokenizer: load_tokenizer()?, client: Client::new(), pool })
    }
}

// This is only called when we have all chunks for a document ready, so we delete the change record
// and all associated FTS chunks.
#[instrument(skip(table, ctx, chunks))]
async fn insert_fts_chunks(id: i64, chunks: Vec<(Chunk, Vec<f16>)>, table: &TableSpec, ctx: Arc<SemanticCtx>) -> Result<()> {
    let mut conn = ctx.pool.get().await?;
    let tx = conn.transaction().await?;
    tx.execute(&format!("DELETE FROM {}_change_tracker WHERE id = $1", table.name), &[&id]).await?;
    for col in table.columns {
        if !col.fts { continue }
        tx.execute(&format!("DELETE FROM {}_{}_fts_chunks WHERE document = $1", table.name, col.name), &[&id]).await?;
    }
    for (chunk, embedding) in chunks {
        tx.execute(&format!("INSERT INTO {}_{}_fts_chunks VALUES ($1, $2, $3, $4)", table.name, chunk.col), &[
            &id,
            &chunk.start,
            &chunk.length,
            &pgvector::HalfVector::from(embedding)
        ]).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub async fn embed_query(q: &str, ctx: Arc<SemanticCtx>) -> Result<(HalfVector, HalfVector)> {
    let query_prefixed = format!("{}{}", CONFIG.semantic.query_prefix, q);
    let doc_prefixed = format!("{}{}", CONFIG.semantic.document_prefix, q);
    let mut result = send_batch(&ctx.client, vec![
        &query_prefixed,
        &doc_prefixed
    ]).await?.into_iter();
    Ok((HalfVector::from(result.next().unwrap()), HalfVector::from(result.next().unwrap())))
}

#[instrument(skip(ctx))]
pub async fn fts_for_indexer(i: &Box<dyn Indexer>, ctx: Arc<SemanticCtx>) -> Result<()> {
    let conn = ctx.pool.get().await?;
    for table in i.tables() {
        let fts_columns: Arc<Vec<&ColumnSpec>> = Arc::new(table.columns.iter().filter(|x| x.fts).collect());
        if !fts_columns.is_empty() {
            let mut select_one_query = format!("SELECT id");
            for column in fts_columns.iter() {
                write!(&mut select_one_query, ", {}", column.name).unwrap();
            }
            write!(&mut select_one_query, " FROM {} WHERE id = $1", table.name).unwrap();
            let select_one_query = Arc::new(select_one_query);

            let (encoded_row_tx, encoded_row_rx) = mpsc::channel::<Chunk>(CONFIG.semantic.batch_size * 4);

            let pending = Arc::new(RwLock::new(HashMap::new()));

            let ctx_ = ctx.clone();
            let pending_ = pending.clone();
            let get_inputs = conn.query_raw(&format!("SELECT id FROM {}_change_tracker", table.name), [""; 0])
                .await?
                .map_err(anyhow::Error::from)
                .try_for_each_concurrent(CONFIG.concurrency * 2, move |row| {
                    let ctx = ctx_.clone();
                    let pending = pending_.clone();
                    let select_one_query = select_one_query.clone();
                    let encoded_row_tx = encoded_row_tx.clone();
                    let fts_columns = fts_columns.clone();
                    async move {
                        let conn = ctx.pool.get().await?;
                        let id: i64 = row.get(0);
                        let row = conn.query_opt(&*select_one_query, &[&id]).await?;
                        if let Some(row) = row {
                            let mut buffer = vec![];
                            for (i, col) in fts_columns.iter().enumerate() {
                                let s: Option<String> = get_column_string(&row, i + 1, col);
                                if let Some(s) = s {
                                    let ctx = ctx.clone();
                                    let chunks = tokio::task::spawn_blocking(move || {
                                        tokenize_chunk_text(&ctx.tokenizer, &s)
                                    }).await??;
                                    for chunk in chunks {
                                        buffer.push(Chunk {
                                            id,
                                            col: col.name,
                                            start: chunk.0 as i32,
                                            length: (chunk.1 - chunk.0) as i32,
                                            text: format!("{}{}", CONFIG.semantic.document_prefix, chunk.2)
                                        });
                                    }
                                }
                            }
                            pending.write().await.insert(id, (vec![], buffer.len()));
                            for chunk in buffer {
                                encoded_row_tx.send(chunk).await?;
                            }
                        }
                        Ok(())
                    }
                });

            let get_inputs = tokio::task::spawn(get_inputs);

            let ctx_ = ctx.clone();
            let pending = pending.clone();
            let make_embeddings =
                ReceiverStream::new(encoded_row_rx).chunks(CONFIG.semantic.batch_size)
                .map(Ok)
                .try_for_each_concurrent(3, move |batch| {
                    let ctx = ctx_.clone();
                    let pending = pending.clone();
                    async move {
                        let embs = send_batch(&ctx.client, batch.iter().map(|c| c.text.as_str()).collect()).await?;
                        let mut pending = pending.write().await;

                        for (embedding, chunk) in embs.into_iter().zip(batch.into_iter()) {
                            // ugly hack
                            if contains_nan(&embedding) {
                                // write no entries
                                tokio::task::spawn(insert_fts_chunks(chunk.id, vec![], table, ctx.clone()));
                            }

                            let record = pending.get_mut(&chunk.id).unwrap();
                            record.1 -= 1;
                            let id = chunk.id;
                            record.0.push((chunk, embedding));

                            // No pending chunks to embed for record: insert into database.
                            if record.1 == 0 {
                                let record = pending.remove(&id).unwrap();
                                // This should generally not be rate-limiting, so don't bother with backpressure.
                                tokio::task::spawn(insert_fts_chunks(id, record.0, table, ctx.clone()));
                            }
                        }

                        Result::Ok(())
                    }
                });

            let make_embeddings: tokio::task::JoinHandle<Result<()>> = tokio::task::spawn(make_embeddings);

            make_embeddings.await??;
            get_inputs.await??;
        }
    }
    Ok(())
}

#[test]
fn test_tokenize() {
    println!("{:?}", tokenize_chunk_text(&load_tokenizer().unwrap(), include_str!("../tokenizer_test_input.txt")));
}
