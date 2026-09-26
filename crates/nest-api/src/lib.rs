//! The management API (ADR-006): HTTP+JSON served by every daemon on a
//! local Unix socket (`<state_dir>/api.sock`, mode 0600). The `nest` CLI
//! and (later) the web UI use only this.
//!
//! Paths in requests are namespace paths ("/hub/...") or paths under this
//! node's mountpoint, which are translated.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use nest_data::Vfs;
use nest_meta::{SealPolicy, query};
use nest_place::import::{ImportOptions, SealMode};
use nest_place::selector::{self, Manifest};
use nest_place::{Placer, RuleSpec, Selector};
use nest_types::{FileKind, GenState, NestError};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct Api {
    pub vfs: Arc<Vfs>,
    pub placer: Arc<Placer>,
    pub mountpoint: Option<String>,
    pub hub: String,
}

struct ApiError(StatusCode, String);

impl From<NestError> for ApiError {
    fn from(e: NestError) -> Self {
        let code = match &e {
            NestError::NotFound => StatusCode::NOT_FOUND,
            NestError::Exists | NestError::Stale | NestError::Busy(_) => StatusCode::CONFLICT,
            NestError::Invalid(_)
            | NestError::NotDir
            | NestError::IsDir
            | NestError::NameTooLong => StatusCode::BAD_REQUEST,
            NestError::NotPermitted(_) => StatusCode::FORBIDDEN,
            NestError::NoQuorum | NestError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError(code, e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

type R<T> = Result<Json<T>, ApiError>;

fn bad(msg: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, msg.into())
}

impl Api {
    /// Namespace path for a namespace path or a path under our mountpoint.
    fn ns(&self, p: &str) -> String {
        if let Some(mp) = &self.mountpoint {
            let mp = mp.trim_end_matches('/');
            if let Some(rest) = p.strip_prefix(mp)
                && (rest.is_empty() || rest.starts_with('/'))
            {
                return if rest.is_empty() {
                    "/".into()
                } else {
                    rest.to_string()
                };
            }
        }
        p.to_string()
    }

    fn selector(&self, s: &str) -> Result<Selector, ApiError> {
        let s = if s.starts_with('/') {
            self.ns(s)
        } else {
            s.to_string()
        };
        Selector::parse(&s, &self.hub).map_err(bad)
    }

    fn conn(&self) -> Result<rusqlite::Connection, ApiError> {
        self.vfs
            .data()
            .meta()
            .open_reader()
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
    }
}

pub fn router(api: Api) -> Router {
    Router::new()
        .route("/v1/status", get(status))
        .route("/v1/ls", get(ls))
        .route("/v1/seal", post(seal))
        .route("/v1/seal-policy", post(seal_policy))
        .route("/v1/where", get(where_))
        .route("/v1/replicate", post(replicate))
        .route("/v1/evict", post(evict))
        .route("/v1/rules", get(rules))
        .route("/v1/rules/{name}", put(set_rule).delete(delete_rule))
        .route("/v1/reconcile", post(reconcile))
        .route("/v1/jobs", get(jobs))
        .route("/v1/jobs/{id}", get(job))
        .route("/v1/import", post(import))
        .with_state(api)
}

/// Serve on a Unix socket until the task is dropped.
pub async fn serve_unix(api: Api, path: std::path::PathBuf) -> anyhow::Result<()> {
    let _ = std::fs::remove_file(&path);
    let listener = tokio::net::UnixListener::bind(&path)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    tracing::info!(socket = %path.display(), "management API listening");
    axum::serve(listener, router(api)).await?;
    Ok(())
}

async fn status(State(api): State<Api>) -> R<serde_json::Value> {
    let nodes = api.placer.status().await?;
    let d = api.vfs.data();
    let t = api.vfs.statfs().map(|(_, n)| n).unwrap_or(0);
    Ok(Json(json!({
        "me": d.id(),
        "leader": d.meta().leader(),
        "applied": d.meta().applied_index(),
        "mountpoint": api.mountpoint,
        "entries": t,
        "nodes": nodes,
        "rules": api.placer.rules()?.len(),
    })))
}

#[derive(Deserialize)]
struct PathQ {
    path: String,
}

#[derive(Serialize)]
struct LsEntry {
    name: String,
    kind: FileKind,
    size: u64,
    sealed: bool,
    writing_on: Option<String>,
    hosts: Vec<String>,
    target: Option<String>,
}

async fn ls(State(api): State<Api>, Query(q): Query<PathQ>) -> R<serde_json::Value> {
    let path = api.ns(&q.path);
    let c = api.conn()?;
    let (id, _) = selector::resolve_path(&c, &path)?;
    let names: HashMap<u64, String> = api
        .placer
        .nodes()?
        .into_iter()
        .map(|h| (h.node.0, h.name))
        .collect();
    let host = |n: u64| names.get(&n).cloned().unwrap_or_else(|| format!("node{n}"));
    let entry = |name: String, a: nest_types::FileAttr| -> Result<LsEntry, ApiError> {
        let hosts = query::replicas(&c, a.id)
            .map_err(|e| bad(e.to_string()))?
            .into_iter()
            .filter(|r| r.generation == a.generation)
            .map(|r| host(r.store.0))
            .collect();
        Ok(LsEntry {
            name,
            kind: a.kind,
            size: a.size,
            sealed: a.sealed,
            writing_on: (a.gen_state == GenState::Owned)
                .then(|| host(a.owner.map(|o| o.0).unwrap_or(0))),
            hosts,
            target: query::readlink(&c, a.id)
                .ok()
                .flatten()
                .map(|t| String::from_utf8_lossy(&t).into_owned()),
        })
    };
    let a = query::getattr(&c, id)
        .map_err(|e| bad(e.to_string()))?
        .ok_or(ApiError(StatusCode::NOT_FOUND, path.clone()))?;
    let mut out = Vec::new();
    if a.kind == FileKind::Directory {
        let mut after = 0;
        loop {
            let batch = query::readdir(&c, id, after, 1024).map_err(|e| bad(e.to_string()))?;
            if batch.is_empty() {
                break;
            }
            for (cookie, e) in batch {
                after = cookie;
                if let Some(ea) = query::getattr(&c, e.id).map_err(|e| bad(e.to_string()))? {
                    out.push(entry(String::from_utf8_lossy(&e.name).into_owned(), ea)?);
                }
            }
        }
    } else {
        out.push(entry(path.rsplit('/').next().unwrap_or("").to_string(), a)?);
    }
    Ok(Json(json!({ "path": path, "entries": out })))
}

#[derive(Deserialize)]
struct SealReq {
    path: String,
    #[serde(default = "yes")]
    sealed: bool,
    #[serde(default)]
    recursive: bool,
}

fn yes() -> bool {
    true
}

async fn seal(State(api): State<Api>, Json(r): Json<SealReq>) -> R<serde_json::Value> {
    let path = api.ns(&r.path);
    let m: Manifest = if r.recursive {
        selector::resolve_tree(&api.conn()?, &path)?
    } else {
        let (id, _) = selector::resolve_path(&api.conn()?, &path)?;
        Manifest {
            entries: vec![selector::Entry {
                file: id,
                generation: Default::default(),
                size: 0,
                stable: true,
                path: path.clone(),
            }],
            dangling: vec![],
        }
    };
    let mut changed = 0;
    let mut errors = Vec::new();
    for e in m.entries {
        match api.vfs.seal(e.file, r.sealed).await {
            Ok(_) => changed += 1,
            Err(err) => errors.push(format!("{}: {err}", e.path)),
        }
    }
    Ok(Json(json!({ "changed": changed, "errors": errors })))
}

#[derive(Deserialize)]
struct PolicyReq {
    path: String,
    policy: SealPolicy,
}

async fn seal_policy(State(api): State<Api>, Json(r): Json<PolicyReq>) -> R<serde_json::Value> {
    let path = api.ns(&r.path);
    let (id, _) = selector::resolve_path(&api.conn()?, &path)?;
    api.vfs
        .data()
        .meta()
        .propose(nest_meta::Command::SetSealPolicy {
            dir: id,
            policy: r.policy,
            now: nest_types::Timestamp::now(),
        })
        .await?;
    Ok(Json(json!({ "path": path, "policy": r.policy })))
}

#[derive(Deserialize)]
struct WhereQ {
    selector: String,
    #[serde(default)]
    hosts: Option<String>,
}

fn split_hosts(h: &Option<String>) -> Vec<String> {
    h.as_deref()
        .map(|s| {
            s.split(',')
                .filter(|x| !x.is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

async fn where_(State(api): State<Api>, Query(q): Query<WhereQ>) -> R<serde_json::Value> {
    let sel = api.selector(&q.selector)?;
    let (m, ready) = api.placer.readiness(&sel, &split_hosts(&q.hosts)).await?;
    Ok(Json(json!({
        "selector": sel.describe(),
        "files": m.entries.len(),
        "bytes": m.bytes(),
        "writing": m.entries.iter().filter(|e| !e.stable).count(),
        "dangling": m.dangling,
        "hosts": ready,
    })))
}

#[derive(Deserialize)]
struct PlaceReq {
    selector: String,
    hosts: Vec<String>,
    #[serde(default = "par")]
    parallel: usize,
}

fn par() -> usize {
    8
}

async fn replicate(State(api): State<Api>, Json(r): Json<PlaceReq>) -> R<serde_json::Value> {
    let sel = api.selector(&r.selector)?;
    let job = api.placer.replicate(sel, r.hosts, r.parallel).await?;
    Ok(Json(json!({ "job": job })))
}

async fn evict(State(api): State<Api>, Json(r): Json<PlaceReq>) -> R<serde_json::Value> {
    let sel = api.selector(&r.selector)?;
    let reports = api.placer.evict(&sel, &r.hosts).await?;
    Ok(Json(json!({ "hosts": reports })))
}

async fn rules(State(api): State<Api>) -> R<serde_json::Value> {
    let rules: Vec<_> = api
        .placer
        .rules()?
        .into_iter()
        .map(|(name, spec, rev)| json!({ "name": name, "selector": spec.selector.describe(), "hosts": spec.hosts, "revision": rev }))
        .collect();
    Ok(Json(json!({ "rules": rules })))
}

#[derive(Deserialize)]
struct RuleReq {
    selector: String,
    hosts: Vec<String>,
}

async fn set_rule(
    State(api): State<Api>,
    Path(name): Path<String>,
    Json(r): Json<RuleReq>,
) -> R<serde_json::Value> {
    let spec = RuleSpec {
        selector: api.selector(&r.selector)?,
        hosts: r.hosts,
    };
    let rev = api.placer.set_rule(&name, &spec).await?;
    Ok(Json(json!({ "name": name, "revision": rev })))
}

async fn delete_rule(State(api): State<Api>, Path(name): Path<String>) -> R<serde_json::Value> {
    api.placer.delete_rule(&name).await?;
    Ok(Json(json!({ "deleted": name })))
}

#[derive(Deserialize)]
struct ReconcileReq {
    #[serde(default)]
    name: Option<String>,
    #[serde(default = "par")]
    parallel: usize,
}

async fn reconcile(State(api): State<Api>, Json(r): Json<ReconcileReq>) -> R<serde_json::Value> {
    let jobs = api.placer.reconcile(r.name.as_deref(), r.parallel).await?;
    Ok(Json(json!({ "jobs": jobs })))
}

async fn jobs(State(api): State<Api>) -> R<serde_json::Value> {
    Ok(Json(json!({ "jobs": api.placer.jobs() })))
}

async fn job(State(api): State<Api>, Path(id): Path<u64>) -> R<serde_json::Value> {
    let j = api
        .placer
        .job(id)
        .ok_or(ApiError(StatusCode::NOT_FOUND, format!("no job {id}")))?;
    Ok(Json(
        json!({ "job": j, "import": api.placer.import_progress(id) }),
    ))
}

#[derive(Deserialize)]
struct ImportReq {
    src: String,
    dst: String,
    #[serde(default)]
    r#move: bool,
    #[serde(default)]
    seal: Option<SealMode>,
}

async fn import(State(api): State<Api>, Json(r): Json<ImportReq>) -> R<serde_json::Value> {
    let opts = ImportOptions {
        src: r.src.into(),
        dst: api.ns(&r.dst),
        r#move: r.r#move,
        seal: r.seal.unwrap_or(SealMode::Auto),
    };
    Ok(Json(json!({ "job": api.placer.import(opts) })))
}
