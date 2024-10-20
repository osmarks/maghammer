use chrono::{DateTime, Utc};
use compact_str::{CompactString, ToCompactString};
use deadpool_postgres::{Manager, ManagerConfig, RecyclingMethod, Pool};
use indexer::{ColumnSpec, TableSpec};
use pgvector::HalfVector;
use semantic::SemanticCtx;
use tokio_postgres::{NoTls, Row};
use anyhow::{Context, Result};
use tracing::Instrument;
use util::{get_column_string, urlencode};
use std::collections::{BTreeMap, HashMap};
use std::{str::FromStr, sync::Arc, fmt::Write};
use ntex::web;
use maud::{html, Markup, Render, DOCTYPE};
use serde::{Deserialize, Serialize};
use rs_abbreviation_number::NumericAbbreviate;
use sea_query_postgres::PostgresBinder;
use tracing_subscriber::prelude::*;

mod indexer;
mod indexers;
mod util;
mod semantic;
mod error;

use crate::error::{Error, EResult};
use crate::util::CONFIG;
use crate::indexer::Indexer;

fn generate_auxiliary_tables(ix: &Box<dyn Indexer>) -> String {
    let mut buf = String::new();
    for tbl in ix.tables().iter() {
        write!(&mut buf, "
CREATE TABLE {name}_change_tracker (
    id BIGINT PRIMARY KEY REFERENCES {name} (id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE FUNCTION {name}_track() RETURNS trigger AS $$
    BEGIN
        IF NEW IS DISTINCT FROM OLD THEN
            INSERT INTO {name}_change_tracker VALUES (
                new.id,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (id) DO UPDATE SET timestamp = CURRENT_TIMESTAMP;
        END IF;
        RETURN NULL;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER {name}_track_insert AFTER INSERT ON {name}
FOR EACH ROW
EXECUTE FUNCTION {name}_track();

CREATE TRIGGER {name}_track_update AFTER UPDATE ON {name}
FOR EACH ROW
EXECUTE FUNCTION {name}_track();
        ", name=tbl.name).unwrap();
        for col in tbl.columns {
            if col.fts {
                write!(&mut buf, "
CREATE TABLE {name}_{col}_fts_chunks (
    document BIGINT NOT NULL REFERENCES {name} (id) ON DELETE CASCADE,
    start INTEGER NOT NULL,
    length INTEGER NOT NULL,
    embedding HALFVEC({dim})
);
                ", name=tbl.name, col=col.name, dim=CONFIG.semantic.embedding_dim).unwrap();
            }
        }
    }
    buf
}

fn page(title: &str, body: Markup) -> web::HttpResponse {
    let mut response = web::HttpResponse::Ok();
    let body = html! {
        (DOCTYPE)
        meta charset="utf-8";
        link rel="stylesheet" href="/style.css";
        title { "Maghammer " (title) }
        @if title != "Index" {
            a href="/" { "←" }
        }
        h1 { (title) }
        main {
            (body)
        }
    }.render().0;
    response.content_type("text/html").body(body)
}

fn search_bar(ctx: &ServerState, value: &SearchQuery) -> Markup {
    html! {
        form.search-bar action="/search" {
            input type="search" placeholder="Query" value=(value.q) name="q";
            select name="src_mode" {
                option selected[value.src_mode == SearchSourceMode::Mix] { "Mix" }
                option selected[value.src_mode == SearchSourceMode::Titles] { "Titles" }
                option selected[value.src_mode == SearchSourceMode::Content] { "Content" }
            }
            select name="table" {
                option selected[value.table.as_ref().map(|x| x == "All").unwrap_or(true)] { "All" }
                @for ix in ctx.indexers.iter() {
                    @for table in ix.tables() {
                        option selected[value.table.as_ref().map(|x| x == table.name).unwrap_or(false)] { (table.name) }
                    }
                }
            }
            input type="submit" value="Search";
        }
    }
}

#[web::get("/")]
async fn index_page(state: web::types::State<ServerState>) -> impl web::Responder {
    let table_names: Vec<&str> = state.indexers.iter()
        .flat_map(|i| i.tables())
        .map(|t| t.name)
        .collect();
    let conn = state.pool.get().await?;
    let mut counts: HashMap<String, i64> = HashMap::new();
    for row in conn.query("SELECT relname, reltuples::BIGINT FROM pg_class WHERE relname = ANY($1)", &[&table_names]).await? {
        counts.insert(row.get(0), row.get(1));
    }
    let body = html! {
        (search_bar(&state, &Default::default()))
        @for indexer in state.indexers.iter() {
            div {
                h2.colborder style=(util::hash_to_color("border-color", indexer.name())) { (indexer.name()) }
                @for table in indexer.tables() {
                    div {
                        a href=(url_for_table(indexer.name(), table.name, None, None)) { (table.name) } " / " (counts[table.name])
                    }
                }
            }
        }
    };
    EResult::Ok(page("Index", body))
}

fn with_newlines(s: &str) -> Markup {
    html! {
        @for line in s.lines() {
            (line)
            br;
        }
    }
}

fn url_for_table(indexer: &str, table: &str, record: Option<i64>, controls: Option<TableViewControls>) -> String {
    let mut out = format!("/t/{}/{}", indexer, table);
    if let Some(record) = record {
        write!(out, "/{}", record).unwrap();
    }
    if let Some(controls) = controls {
        write!(out, "?c={}", urlencode(&serde_json::to_string(&controls).unwrap())).unwrap();
    }
    out
}

fn render_json_value(j: serde_json::Value) -> Markup {
    use serde_json::Value::*;
    match j {
        Number(x) => html! { (x) },
        Array(xs) => html! {
            ul {
                @for x in xs {
                    li {
                        (render_json_value(x))
                    }
                }
            }
        },
        String(s) => html! { (s) },
        Bool(b) => html! { (b) },
        Object(kv) => html! {
            ul {
                @for (k, v) in kv {
                    li {
                        div { (k) }
                        div { (render_json_value(v)) }
                    }
                }
            }
        },
        Null => html! { "null" }
    }
}

fn render_column<F: Fn(&str) -> Markup>(row: &Row, index: usize, table: &TableSpec, indexer: &Box<dyn Indexer>, short_form: bool, generate_filter: F) -> Option<Markup> {
    use tokio_postgres::types::Type;
    let name = row.columns()[index].name();
    let type_ = row.columns()[index].type_().clone();
    Some(match type_ {
        Type::BOOL => html! { (row.get::<usize, Option<bool>>(index)?) },
        Type::TEXT => {
            let content = &row.get::<usize, Option<String>>(index)?;
            if let Some(usource) = table.url_source_column {
                if name == usource {
                    html! {
                        (generate_filter(&content))
                        a href=(indexer.url_for(table.name, content)) {
                            (with_newlines(&content))
                        }
                    }
                } else {
                    html! {
                        (generate_filter(&content))
                        (with_newlines(&content))
                    }
                }
            } else {
                html! {
                    (generate_filter(&content))
                    (with_newlines(&content))
                }
            }
        },
        Type::TIMESTAMPTZ => {
            let text = row.get::<usize, Option<DateTime<Utc>>>(index)?.format("%Y-%m-%d %H:%M:%S").to_compact_string();
            html! { (generate_filter(&text)) (text) }
        },
        Type::TEXT_ARRAY => {
            let texts = row.get::<usize, Option<Vec<String>>>(index)?;
            let len = texts.len();
            html! {
                @if short_form {
                    @for (i, x) in texts.into_iter().enumerate() {
                        (generate_filter(&x))
                        (x)
                        @if i != len - 1 {
                            ", "
                        }
                    }
                } @else {
                    ul {
                        @for x in texts {
                            li {
                                (generate_filter(&x))
                                (x)
                            }
                        }
                    }
                }
            }
        },
        Type::INT2 => {
            let text = row.get::<usize, Option<i16>>(index)?.to_compact_string();
            html! { (generate_filter(&text)) (text) }
        },
        Type::INT4 => {
            let text = row.get::<usize, Option<i32>>(index)?.to_compact_string();
            html! { (generate_filter(&text)) (text) }
        },
        Type::INT8 => {
            let text = row.get::<usize, Option<i64>>(index)?.to_compact_string();
            html! { (generate_filter(&text)) (text) }
        },
        Type::FLOAT4 => {
            let text = row.get::<usize, Option<f32>>(index)?.abbreviate_number(&Default::default());
            html! { (generate_filter(&text)) (text) }
        },
        Type::FLOAT8 => {
            let text = row.get::<usize, Option<f64>>(index)?.abbreviate_number(&Default::default());
            html! { (generate_filter(&text)) (text) }
        },
        Type::JSONB => {
            let value = row.get::<usize, Option<serde_json::Value>>(index)?;
            html! { (render_json_value(value)) }
        },
        Type::BYTEA => {
            let value: Vec<u8> = row.get::<usize, Option<Vec<u8>>>(index)?;
            html! { (with_newlines(&String::from_utf8_lossy(&value))) }
        },
        _ => html! { span.error { (format!("cannot render type {:?}", type_)) } }
    })
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
enum Filter {
    Contains,
    Equal,
    Gt,
    Lt,
    NotNull
}

impl Filter {
    const VALUES: &'static [Filter] = &[Filter::Contains, Filter::Equal, Filter::Gt, Filter::Lt, Filter::NotNull];
    fn name(&self) -> &'static str {
        match self {
            Filter::Contains => "Contains",
            Filter::Equal => "Equal",
            Filter::Gt => "Gt",
            Filter::Lt => "Lt",
            Filter::NotNull => "NotNull"
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "Contains" => Filter::Contains,
            "Equal" => Filter::Equal,
            "Gt" => Filter::Gt,
            "Lt" => Filter::Lt,
            "NotNull" => Filter::NotNull,
            _ => return None
        })
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
struct TableViewControls {
    #[serde(default)]
    sort: Option<CompactString>,
    #[serde(default)]
    sort_desc: bool,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    filters: im::Vector<(CompactString, Filter, CompactString)>
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
struct TableViewControlsWrapper {
    #[serde(default)]
    c: String,
    index: Option<usize>,
    column: Option<String>,
    filter: Option<String>,
    value: Option<String>,
    ctrl: Option<String>
}

const COUNT: usize = 100;

#[web::get("/{indexer}/{table}")]
async fn table_view<'a>(state: web::types::State<ServerState>, path: web::types::Path<(String, String)>, config: web::types::Query<TableViewControlsWrapper>) -> impl web::Responder {
    use sea_query::*;

    let mut controls: TableViewControls = serde_json::from_str(&config.c).ok().unwrap_or_default();

    if let (Some(index), Some(column), Some(filter), Some(value)) = (config.index.as_ref(), config.column.as_ref(), config.filter.as_ref(), config.value.as_ref()) {
        if config.ctrl.as_ref().map(|x| x.as_str()).unwrap_or_default() == "Delete" {
            controls.filters.remove(*index);
        } else {
            let record = (column.to_compact_string(), Filter::from_str(filter).context("filter parse error")?, value.to_compact_string());
            if *index < controls.filters.len() {
                controls.filters[*index] = record;
            } else {
                controls.filters.push_back(record);
            }
        }
    }

    let (indexer, table) = path.into_inner();
    let indexer = state.indexer(&indexer)?;
    let table = indexer.table(&table).ok_or(Error::NotFound)?;
    let conn = state.pool.get().await?;

    let offset = &(controls.offset as i64);

    let rows = conn.query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", &[&table.name]).await?;
    let mut typemap = BTreeMap::new();
    for row in rows.into_iter() {
        let column: String = row.get(0);
        let mut ty: String = row.get(1);
        // TODO: information schema returns ARRAY for all arrays, and we handwave arrays later on in the code
        if &ty == "ARRAY" {
            ty = String::from("TEXT")
        }
        typemap.insert(column, ty);
    }

    let mut query = sea_query::Query::select();
    query.from(table);
    query.column((table, Alias::new("id")));
    for col in table.summary_columns {
        query.column((table, Alias::new(*col)));
    }
    if let Some((this_column, other_table)) = table.parent {
        let other = indexer.table(other_table).unwrap();
        query.column((other, Alias::new(other.title_source_column)));
        query.column((table, Alias::new(this_column)));
        query.join(JoinType::InnerJoin, Alias::new(other_table), Expr::col((other, Alias::new("id"))).eq(Expr::col((table, Alias::new(this_column)))));
    }
    query.limit((COUNT + 1) as u64);
    query.offset(*offset as u64);

    if let Some(sort) = &controls.sort {
        let sort = Alias::new(sort.as_str());
        query.order_by(sort, if controls.sort_desc { Order::Desc } else { Order::Asc });
    }

    for (column, filter_type, value) in controls.filters.iter() {
        let column_det = table.column(column);
        let column_alias = Alias::new(column.as_str());

        let column_expr = if column_det.map(|x| x.is_array).unwrap_or_default() {
            Expr::expr(SimpleExpr::FunctionCall(PgFunc::any(Expr::col(column_alias))))
        } else {
            Expr::col(column_alias)
        };
        // As a very ugly hack to avoid our ability to deal with dynamic types neatly, force the database to take in strings and internally cast them.
        let value_expr = Expr::value(value.to_string()).cast_as(Alias::new("TEXT")).cast_as(Alias::new(typemap[column.as_str()].to_string()));
        let expr = match filter_type {
            Filter::Contains => column_expr.like(format!("%{}%", value)),
            Filter::Equal => value_expr.eq(column_expr),
            Filter::Gt => Expr::expr(value_expr).lte(column_expr),
            Filter::Lt => Expr::expr(value_expr).gte(column_expr),
            Filter::NotNull => column_expr.is_not_null()
        };
        query.and_where(expr);
    }

    let query = query.build_postgres(sea_query::PostgresQueryBuilder);

    let rows = conn.query(&query.0, &query.1.as_params()).await?;

    let control_link = |label: &str, new_controls: TableViewControls| {
        let url = url_for_table(indexer.name(), table.name, None, Some(new_controls));
        html! {
            a href=(url) { (label) }
        }
    };

    let new_control_string = serde_json::to_string(&controls).unwrap();

    let body = html! {
        div.filters {
            @for (i, (column, filter_type, value)) in controls.filters.iter().enumerate() {
                div {
                    form {
                        select name="column" {
                            @for ncolumn in typemap.keys() {
                                option selected[column.as_str() == ncolumn] { (ncolumn) }
                            }
                        }
                        select name="filter" {
                            @for filter in Filter::VALUES {
                                option selected[filter == filter_type] { (filter.name()) }
                            }
                        }
                        input type="text" name="value" value=(value);
                        input type="submit" name="ctrl" value="Set";
                        input type="submit" name="ctrl" value="Delete";
                        input type="hidden" name="index" value=(i);
                        input type="hidden" name="c" value=(&new_control_string);
                    }
                }
            }
            div {
                form {
                    select name="column" {
                        @for column in typemap.keys() {
                            option { (column) }
                        }
                    }
                    select name="filter" {
                        @for filter in Filter::VALUES {
                            option { (filter.name()) }
                        }
                    }
                    input type="text" name="value";
                    input type="submit" value="Add";
                    input type="hidden" name="index" value=(controls.filters.len());
                    input type="hidden" name="c" value=(&new_control_string);
                }
            }
        }
        table {
            tr {
                td;
                @for col in table.summary_columns.iter() {
                    td {
                        (col)
                        " "
                        (control_link("↑", TableViewControls {
                            sort: Some(col.to_compact_string()),
                            sort_desc: true,
                            ..controls.clone()
                        }))
                        " "
                        (control_link("↓", TableViewControls {
                            sort: Some(col.to_compact_string()),
                            sort_desc: false,
                            ..controls.clone()
                        }))
                    }
                }
                @if let Some((parent, _other_table)) = table.parent {
                    td {
                        (parent)
                    }
                }
            }
            @for row in &rows[0..COUNT.min(rows.len())] {
                @let id: i64 = row.get(0);
                tr {
                    td {
                        a href=(url_for_table(indexer.name(), table.name, Some(id), None)) { "Full" }
                    }
                    @for i in 1..(table.summary_columns.len() + 1) {
                        td {
                            @let generate_filter = |s: &str| {
                                html! {
                                    (control_link("⊆", TableViewControls {
                                        filters: controls.filters.clone() + im::Vector::unit((table.summary_columns[i - 1].to_compact_string(), Filter::Equal, s.to_compact_string())),
                                        ..controls.clone()
                                    }))
                                    " "
                                }
                            };
                            (render_column(&row, i, table, indexer, true, generate_filter).unwrap_or(html!{}))
                        }
                    }
                    @if let Some((_parent, other_table)) = table.parent {
                        td {
                            @let title: Option<String> = row.get(table.summary_columns.len() + 1);
                            @let parent_id: Option<i64> = row.get(table.summary_columns.len() + 2);
                            @if let Some(parent_id) = parent_id {
                                a href=(url_for_table(indexer.name(), other_table, Some(parent_id), None)) { (title.unwrap_or_default()) }
                            }
                        }
                    }
                }
            }
        }
        div.footer {
            @if controls.offset > 0 {
                (control_link("Prev", TableViewControls {
                    offset: controls.offset - COUNT,
                    ..controls.clone()
                }))
                " "
            }
            @if rows.len() > COUNT {
                (control_link("Next", TableViewControls {
                    offset: controls.offset + COUNT,
                    ..controls.clone()
                }))
            }
        }
    };
    EResult::Ok(page(&format!("Table {}/{}", indexer.name(), table.name), body))
}

#[web::get("/{indexer}/{table}/{record}")]
async fn record_view(state: web::types::State<ServerState>, path: web::types::Path<(String, String, i64)>) -> impl web::Responder {
    let (indexer, table, record) = path.into_inner();
    let conn = state.pool.get().await?;
    let row = conn.query_one(&format!("SELECT * FROM {} WHERE id = $1", table), &[&record]).await?;
    let indexer = state.indexer(&indexer)?;
    let table = indexer.table(&table).ok_or(Error::NotFound)?;
    let fk_ref = if let Some((this_column, other_table)) = table.parent {
        let other_tablespec = indexer.table(other_table).unwrap();
        let mut other_id: i64 = 0;
        for (i, col) in row.columns().iter().enumerate() {
            if col.name() == this_column {
                other_id = row.get(i);
            }
        }
        let row = conn.query_one(&format!("SELECT {} FROM {} WHERE id = $1", other_tablespec.title_source_column, other_table), &[&other_id]).await?;
        Some((this_column, row.get::<usize, String>(0), other_table))
    } else { None };
    let body = html! {
        @for (i, col) in row.columns()[1..].iter().enumerate() {
            div.wide-column {
                div.column-name { (col.name()) }
                @if let Some(ref fk_ref) = fk_ref {
                    @let (name, value, tablename) = fk_ref;
                    @if name == &col.name() {
                        div.content {
                            a href=(url_for_table(indexer.name(), tablename, row.get(i + 1), None)) { (value) }
                        }
                    } @else {
                        div.content { (render_column(&row, i + 1, table, indexer, false, |_| html!{}).unwrap_or(html!{})) }
                    }
                } @else {
                    div.content { (render_column(&row, i + 1, table, indexer, false, |_| html!{}).unwrap_or(html!{})) }
                }
            }
        }
    };
    EResult::Ok(page(&format!("Record {}/{}/{}", indexer.name(), table.name, record), body))
}

#[derive(Deserialize, PartialEq, Eq, Clone, Copy, Debug)]
enum SearchSourceMode {
    Mix,
    Titles,
    Content
}

impl Default for SearchSourceMode {
    fn default() -> Self {
        SearchSourceMode::Content
    }
}

#[derive(Deserialize, Default, Debug)]
struct SearchQuery {
    q: String,
    #[serde(default)]
    src_mode: SearchSourceMode,
    #[serde(default)]
    table: Option<String>
}

struct SearchResult {
    docid: i64,
    indexer: &'static str,
    table: &'static str,
    column: &'static str,
    title: Option<String>,
    url: Option<String>,
    match_quality: i32,
    text: String
}

async fn query_one_table(table: &TableSpec, indexer: &'static str, col: &ColumnSpec, state: ServerState, count: usize, embedding_choice: Arc<HalfVector>) -> Result<Vec<SearchResult>> {
    let conn = state.pool.get().await?;
    let rows = conn.query(&format!("SELECT document, start, length, embedding <#> $1 AS match FROM {}_{}_fts_chunks ORDER BY match LIMIT {}", table.name, col.name, count as i64), &[&*embedding_choice]).await?;
    let mut chunks = HashMap::new();
    for row in rows {
        let docid: i64 = row.get(0);
        let offset: i32 = row.get(1);
        let length: i32 = row.get(2);
        let matchq: f64 = row.get(3);
        chunks.entry(docid).or_insert_with(Vec::new).push((offset, length, matchq));
    }

    let docids: Vec<i64> = chunks.keys().copied().collect();
    let mut select_one_query = format!("SELECT id, {}, {} ", col.name, table.title_source_column);
    if let Some(usource) = table.url_source_column {
        write!(&mut select_one_query, ", {} ", usource).unwrap();
    }
    write!(&mut select_one_query, "FROM {} WHERE id = ANY($1)", table.name).unwrap();
    let rows = conn.query(&select_one_query, &[&docids]).await?;
    let mut results = Vec::with_capacity(count);
    for row in rows {
        let doc: i64 = row.get(0);
        let text: String = get_column_string(&row, 1, col).context("missing document")?;
        let title: Option<String> = row.get(2);
        let usource: Option<String> = table.url_source_column.map(|_| row.get(3));
        for (offset, length, matchq) in chunks[&doc].iter() {
            // This is slightly deranged, but floats aren't Ord while ints are.
            // We know that -1 <= matchq <= 1 so just convert to integers (slightly lossy but negligible).
            let matchq_int = (*matchq * (i32::MAX as f64)) as i32;
            results.push(SearchResult {
                docid: doc,
                indexer: indexer,
                table: table.name,
                column: col.name,
                title: title.clone(),
                url: usource.clone(),
                match_quality: matchq_int,
                text: text[*offset as usize..(offset + length) as usize].to_string()
            })
        }
    }
    Ok(results)
}

#[web::get("/search")]
async fn fts_page(state: web::types::State<ServerState>, query: web::types::Query<SearchQuery>) -> impl web::Responder {
    let state = (*state).clone();
    let (prefixed, unprefixed) = semantic::embed_query(&query.q, state.semantic.clone()).await?;
    let prefixed = Arc::new(prefixed);
    let unprefixed = Arc::new(unprefixed);
    let mut results = HashMap::new();

    let mut set = tokio::task::JoinSet::new();
    for ix in state.indexers.iter() {
        for table in ix.tables() {
            for col in table.columns {
                if !col.fts { continue }
                if query.table.as_ref().map(|x| x != table.name && x != "All").unwrap_or(false) { continue }
                match (query.src_mode, col.fts_short) {
                    (SearchSourceMode::Content, true) => continue,
                    (SearchSourceMode::Titles, false) => continue,
                    _ => ()
                }
                // We get a batch of a prefixed and unprefixed query.
                // The prefixed one is better for asymmetric search, where the passages are longer than the queries.
                // Some columns are not like this (their spec says so) so we use the model for symmetric search.
                // This does result in a different distribution of dot products.
                let (embedding_choice, count) = if col.fts_short {
                    (unprefixed.clone(), if query.src_mode == SearchSourceMode::Mix { 1 } else { 20 })
                } else {
                    (prefixed.clone(), 20)
                };
                set.spawn(query_one_table(table, ix.name(), col, state.clone(), count, embedding_choice));
            }
        }
    }

    while let Some(res) = set.join_next().await {
        let res = res.context("internal error")??;
        for r in res {
            let entry = results.entry((r.docid, r.indexer, r.table, r.column, r.title, r.url)).or_insert_with(|| (BTreeMap::new(), i32::MIN));
            entry.1 = entry.1.max(r.match_quality);
            // don't flip sign: PGVector already returns inner products negated
            entry.0.insert(r.match_quality, r.text);
        }
    }

    let mut results: Vec<_> = results.into_iter().collect();
    results.sort_by_key(|x| x.1.1);

    let body = html! {
        (search_bar(&state, &query))
        div {
            @for ((docid, indexer, table, column, title, maybe_url), (chunks, _match_quality)) in results {
                div.search-result {
                    div.colborder.result-header style=(util::hash_to_color("border-color", indexer)) {
                        a href=(url_for_table(indexer, table, None, None)) {
                            (table) "/" (column)
                        }
                        " / "
                        a href=(url_for_table(indexer, table, Some(docid), None)) {
                            @match title.as_ref().map(String::as_str) {
                                Some("") => "Record",
                                Some(x) => (x),
                                None => "Record"
                            }
                        }
                        @if let Some(url_base) = maybe_url {
                            " / "
                            a href=(state.indexer(indexer)?.url_for(table, url_base.as_str())) {
                                (url_base)
                            }
                        }
                    }
                    @let len = chunks.len();
                    @let mut chunks = chunks.into_iter();
                    div.result-content {
                        (with_newlines(&chunks.next().unwrap().1))
                    }
                    @if len > 1 {
                        details {
                            summary { "Expand" }
                            @for chunk in chunks {
                                div.result-content {
                                    (with_newlines(&chunk.1))
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    EResult::Ok(page("Search", body))
}

#[derive(Clone)]
struct ServerState {
    indexers: Arc<Vec<Box<dyn Indexer>>>,
    pool: Pool,
    semantic: Arc<SemanticCtx>
}

impl ServerState {
    fn indexer(&self, name: &str) -> EResult<&Box<dyn Indexer>> {
        self.indexers.iter().find(|x| x.name() == name).ok_or(Error::NotFound)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    console_subscriber::ConsoleLayer::builder()
        .with_default_env()
        .init();

    let pg_config = tokio_postgres::Config::from_str(&CONFIG.database)?;
    let mgr_config = ManagerConfig { recycling_method: RecyclingMethod::Fast };
    let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
    let pool = Pool::builder(mgr).max_size(20).build()?;

    let indexers: Arc<Vec<Box<dyn Indexer>>> = Arc::new(vec![
        indexers::mediafiles::MediaFilesIndexer::new(CONFIG.indexers["media_files"].clone()).await?,
        indexers::firefox_history_dump::FirefoxHistoryDumpIndexer::new(CONFIG.indexers["browser_history"].clone()).await?,
        indexers::rclwe::RclweIndexer::new(CONFIG.indexers["rclwe"].clone()).await?,
        indexers::atuin::AtuinIndexer::new(CONFIG.indexers["atuin"].clone()).await?,
        indexers::miniflux::MinifluxIndexer::new(CONFIG.indexers["miniflux"].clone()).await?,
        indexers::thunderbird_email::EmailIndexer::new(CONFIG.indexers["email"].clone()).await?,
        indexers::books::BooksIndexer::new(CONFIG.indexers["books"].clone()).await?,
        indexers::textfiles::TextFilesIndexer::new(CONFIG.indexers["text_files"].clone()).await?,
        indexers::anki::AnkiIndexer::new(CONFIG.indexers["anki"].clone()).await?,
        indexers::minoteaur::MinoteaurIndexer::new(CONFIG.indexers["minoteaur"].clone()).await?
    ]);

    {
        let mut conn = pool.get().await?;
        conn.execute("CREATE TABLE IF NOT EXISTS versions (
            indexer VARCHAR(256) PRIMARY KEY,
            version INTEGER NOT NULL
        )", &[]).await?;
        for indexer in indexers.iter() {
            let name = indexer.name();
            let version = conn
                .query_opt("SELECT version FROM versions WHERE indexer = $1", &[&name])
                .await?.map(|row| row.get(0)).unwrap_or(0);
            for (index, sql) in indexer.schemas().iter().enumerate() {
                let index = index as i32;
                if index >= version {
                    tracing::info!("Migrating {} to {}.", name, index);
                    let tx = conn.transaction().await?;
                    tx.batch_execute(*sql).await.context("execute migration")?;
                    tx.execute("INSERT INTO versions VALUES ($1, $2) ON CONFLICT (indexer) DO UPDATE SET version = $2", &[&name, &(index + 1)]).await.context("update migrations database")?;
                    if index == 0 {
                        tx.batch_execute(&generate_auxiliary_tables(indexer)).await.context("write auxiliary tables")?;
                    }
                    tx.commit().await?;
                }
            }
        }
    }

    match std::env::args().nth(1).unwrap().as_str() {
        "index" => {
            let sctx = Arc::new(SemanticCtx::new(pool.clone())?);
            for indexer in indexers.iter() {
                let ctx = Arc::new(indexer::Ctx {
                    pool: pool.clone()
                });
                tracing::info!("indexing {}", indexer.name());
                indexer.run(ctx).instrument(tracing::info_span!("index", indexer = indexer.name())).await.context(indexer.name())?;
                tracing::info!("FTS indexing {}", indexer.name());
                semantic::fts_for_indexer(indexer, sctx.clone()).await?;
            }
            Ok(())
        },
        "serve" => {
            let state = ServerState {
                indexers: indexers.clone(),
                pool: pool.clone(),
                semantic: Arc::new(SemanticCtx::new(pool.clone())?)
            };
            tokio::task::block_in_place(|| {
                let sys = ntex::rt::System::new("httpserver");
                sys.run(|| {
                    web::HttpServer::new(move || {
                        web::App::new()
                            .state(state.clone())
                            .service(index_page)
                            .service(web::scope("/t").service(table_view).service(record_view))
                            .service(fts_page)
                            .service(ntex_files::Files::new("/", "./static"))
                        })
                        .bind(("127.0.0.1", 7403))?
                        .stop_runtime()
                        .run();
                    Ok(())
                }).context("init fail")
            })
        },
        "sql" => {
            println!("delete semantic indices:");
            for indexer in indexers.iter() {
                for table in indexer.tables() {
                    for column in table.columns {
                        if column.fts {
                            println!("DELETE FROM {}_{}_fts_chunks;", table.name, column.name);
                            println!("DROP INDEX IF EXISTS {}_{}_fts_chunks_embedding_idx;", table.name, column.name);
                        }
                    }

                    println!("DELETE FROM {}_change_tracker;", table.name);
                    println!("INSERT INTO {}_change_tracker SELECT id, CURRENT_TIMESTAMP FROM {};", table.name, table.name);
                }
            }

            println!("create semantic indices:");
            for indexer in indexers.iter() {
                for table in indexer.tables() {
                    for column in table.columns {
                        if column.fts {
                            println!("CREATE INDEX {}_{}_fts_chunks_embedding_idx ON {}_{}_fts_chunks USING hnsw (embedding halfvec_ip_ops);", table.name, column.name, table.name, column.name);
                        }
                    }
                }
            }

            println!("create document index:");
            for indexer in indexers.iter() {
                for table in indexer.tables() {
                    for column in table.columns {
                        if column.fts {
                            println!("CREATE INDEX {}_{}_fts_chunks_document_idx ON {}_{}_fts_chunks (document);", table.name, column.name, table.name, column.name);
                        }
                    }
                }
            }
            Ok(())
        },
        _ => Ok(())
    }
}
