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
    /// Bearer token for the network listener (and the web UI).
    pub web_token: String,
    /// Network listener address, if any (for `nest ui`).
    pub web_addr: Option<std::net::SocketAddr>,
    /// This host's name (for lost+found).
    pub host: String,
}

/// The web/API bearer token: HMAC of a fixed label under the cluster secret,
/// so every node accepts the same token and it never equals the secret.
pub fn web_token(secret: &[u8]) -> String {
    use hmac::Mac;
    let mut m = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret).expect("any key length");
    m.update(b"sparknest-web-v1");
    m.finalize()
        .into_bytes()
        .iter()
        .take(20)
        .map(|b| format!("{b:02x}"))
        .collect()
}

const INDEX_HTML: &str = include_str!("../web/index.html");

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
        .route("/v1/rm", post(remove))
        .route("/v1/fsck", post(fsck))
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
        .route("/v1/stores", get(stores).post(add_store))
        .route("/v1/offload", post(offload))
        .route("/v1/plans", post(make_plan))
        .route("/v1/plans/{id}/apply", post(apply_plan))
        .route("/v1/groups", get(groups))
        .route("/v1/groups/{name}", put(set_group).delete(delete_group))
        .route("/v1/backups", get(backups).post(backup_create))
        .route("/v1/backups/{id}/restore", post(backup_restore))
        .route("/v1/backups/{id}", axum::routing::delete(backup_delete))
        .route("/v1/backups/meta", post(meta_snapshot))
        .route("/v1/hf", get(hf_repos))
        .route("/v1/web", get(web_info))
        .route("/v1/cluster", get(cluster))
        .route("/v1/cluster/remove", post(cluster_remove))
        .route("/v1/cluster/add", post(cluster_add))
        .with_state(api)
}

/// Serve the web UI and the token-protected API on a TCP listener.
pub async fn serve_tcp(api: Api, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    use axum::middleware::{self, Next};
    let token = api.web_token.clone();
    let protected = router(api).layer(middleware::from_fn(
        move |req: axum::extract::Request, next: Next| {
            let token = token.clone();
            async move {
                let ok = req
                    .headers()
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .is_some_and(|v| v.strip_prefix("Bearer ").is_some_and(|t| t == token));
                if ok {
                    next.run(req).await
                } else {
                    (
                        StatusCode::UNAUTHORIZED,
                        Json(json!({ "error": "missing or wrong token" })),
                    )
                        .into_response()
                }
            }
        },
    ));
    let app = Router::new()
        .route("/", get(|| async { axum::response::Html(INDEX_HTML) }))
        .merge(protected);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "web UI and API listening");
    axum::serve(listener, app).await?;
    Ok(())
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
        // This host serves: caught up, admitted after any recovery, lease valid.
        "serving": d.caught_up() && d.lease_valid(),
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
    // Store ids to names: nodes' live stores and archive stores.
    let mut names: HashMap<u64, String> = api
        .placer
        .nodes()?
        .into_iter()
        .map(|h| (h.node.0, h.name))
        .collect();
    names.extend(
        api.placer
            .archive_stores()?
            .into_iter()
            .map(|(id, name, _)| (id.0, name)),
    );
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
struct FsckReq {
    /// Apply the standard, non-destructive resolutions.
    #[serde(default)]
    repair: bool,
    #[serde(default)]
    deep: bool,
}

/// Check this host's objects against the metadata (ADR-026).
async fn fsck(State(api): State<Api>, Json(r): Json<FsckReq>) -> R<serde_json::Value> {
    use nest_data::fsck::{FsckOptions, Orphans};
    let rep = api
        .vfs
        .fsck(&FsckOptions {
            repair: r.repair,
            // Never delete what might be the only copy of data the metadata
            // forgot: keep it in lost+found.
            orphans: if r.repair {
                Orphans::Quarantine
            } else {
                Orphans::Report
            },
            deep: r.deep,
            settle_owned: false,
            min_age: std::time::Duration::from_secs(600),
            host: api.host.clone(),
            stamp: nest_data::fsck::stamp_now(),
        })
        .await?;
    Ok(Json(serde_json::json!({
        "host": api.host,
        "objects": rep.objects,
        "lost": rep.lost(),
        "findings": rep.findings,
    })))
}

#[derive(Deserialize)]
struct RmReq {
    path: String,
    #[serde(default)]
    recursive: bool,
    #[serde(default)]
    dry_run: bool,
}

/// Remove a file or (recursive) a tree, many entries per commit.
async fn remove(State(api): State<Api>, Json(r): Json<RmReq>) -> R<serde_json::Value> {
    let path = api.ns(&r.path);
    let name = path
        .rsplit('/')
        .find(|c| !c.is_empty())
        .ok_or_else(|| NestError::Invalid("refusing to remove the root".into()))?
        .as_bytes()
        .to_vec();
    let (_, parent) = selector::resolve_path(&api.conn()?, &path)?;
    let rep = api
        .vfs
        .remove_tree(parent, &name, r.recursive, r.dry_run)
        .await?;
    Ok(Json(serde_json::to_value(rep).unwrap_or_default()))
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
        .map(|(name, spec, rev)| json!({ "name": name, "selector": spec.selector.describe(), "hosts": spec.hosts, "auto": spec.auto, "revision": rev }))
        .collect();
    Ok(Json(json!({ "rules": rules })))
}

#[derive(Deserialize)]
struct RuleReq {
    selector: String,
    hosts: Vec<String>,
    #[serde(default)]
    auto: bool,
}

async fn set_rule(
    State(api): State<Api>,
    Path(name): Path<String>,
    Json(r): Json<RuleReq>,
) -> R<serde_json::Value> {
    let spec = RuleSpec {
        selector: api.selector(&r.selector)?,
        hosts: r.hosts,
        auto: r.auto,
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

async fn cluster(State(api): State<Api>) -> R<serde_json::Value> {
    Ok(Json(api.placer.membership()?))
}

#[derive(Deserialize)]
struct RemoveReq {
    host: String,
}

async fn cluster_remove(State(api): State<Api>, Json(r): Json<RemoveReq>) -> R<serde_json::Value> {
    let h = api
        .placer
        .resolve_hosts(std::slice::from_ref(&r.host))?
        .pop()
        .ok_or(bad("unknown host"))?;
    if h.node == api.vfs.data().id() {
        return Err(bad(
            "refusing to remove the node serving this request; ask another node",
        ));
    }
    api.placer
        .change_membership(nest_place::admin::MembershipChange::Remove { node: h.node })
        .await?;
    Ok(Json(json!({ "removed": h.name })))
}

#[derive(Deserialize)]
struct AddReq {
    id: u64,
    addr: String,
    #[serde(default = "yes")]
    voter: bool,
}

async fn cluster_add(State(api): State<Api>, Json(r): Json<AddReq>) -> R<serde_json::Value> {
    api.placer
        .change_membership(nest_place::admin::MembershipChange::Add {
            node: nest_types::NodeId(r.id),
            addr: r.addr.clone(),
            voter: r.voter,
        })
        .await?;
    Ok(Json(json!({ "added": r.id, "voter": r.voter })))
}

async fn stores(State(api): State<Api>) -> R<serde_json::Value> {
    Ok(Json(json!({ "stores": api.placer.stores().await? })))
}

#[derive(Deserialize)]
struct AddStoreReq {
    name: String,
    path: String,
    gateways: Vec<String>,
}

async fn add_store(State(api): State<Api>, Json(r): Json<AddStoreReq>) -> R<serde_json::Value> {
    let results = api.placer.add_store(&r.name, &r.path, &r.gateways).await?;
    let gateways: Vec<serde_json::Value> = results
        .into_iter()
        .map(|(g, r)| json!({ "gateway": g, "ok": r.is_ok(), "error": r.err() }))
        .collect();
    Ok(Json(json!({ "name": r.name, "gateways": gateways })))
}

#[derive(Deserialize)]
struct OffloadReq {
    selector: String,
    store: String,
    #[serde(default = "par")]
    parallel: usize,
}

async fn offload(State(api): State<Api>, Json(r): Json<OffloadReq>) -> R<serde_json::Value> {
    let sel = api.selector(&r.selector)?;
    Ok(Json(
        json!({ "job": api.placer.offload(sel, r.store, r.parallel).await? }),
    ))
}

/// Every Hugging Face repo in the hub with where it is complete.
async fn hf_repos(State(api): State<Api>) -> R<serde_json::Value> {
    let hub = api.hub.clone();
    let c = api.conn()?;
    let (hub_id, _) = match selector::resolve_path(&c, &hub) {
        Ok(x) => x,
        Err(_) => return Ok(Json(json!({ "hub": hub, "repos": [] }))),
    };
    let mut repos = Vec::new();
    let mut after = 0;
    loop {
        let batch = query::readdir(&c, hub_id, after, 1024).map_err(|e| bad(e.to_string()))?;
        if batch.is_empty() {
            break;
        }
        for (cookie, e) in batch {
            after = cookie;
            let name = String::from_utf8_lossy(&e.name).into_owned();
            let (kind, rest) = if let Some(r) = name.strip_prefix("models--") {
                ("model", r)
            } else if let Some(r) = name.strip_prefix("datasets--") {
                ("dataset", r)
            } else {
                continue;
            };
            repos.push((kind, rest.replacen("--", "/", 1), name));
        }
    }
    drop(c);
    let mut out = Vec::new();
    for (kind, repo, dir) in repos {
        let sel = Selector::Hf {
            hub: hub.clone(),
            repo: repo.clone(),
            revision: None,
            repo_type: kind.to_string(),
        };
        let revisions = {
            let c = api.conn()?;
            selector::resolve_path(&c, &format!("{hub}/{dir}/snapshots"))
                .ok()
                .and_then(|(id, _)| query::readdir(&c, id, 0, 1000).ok())
                .map(|v| v.len())
                .unwrap_or(0)
        };
        match api.placer.readiness(&sel, &[]).await {
            Ok((m, ready)) => out.push(json!({
                "repo": repo,
                "kind": kind,
                "selector": sel.describe(),
                "files": m.entries.len(),
                "bytes": m.bytes(),
                "writing": m.entries.iter().filter(|e| !e.stable).count(),
                "revisions": revisions,
                "hosts": ready,
            })),
            Err(e) => out.push(json!({ "repo": repo, "kind": kind, "error": e.to_string() })),
        }
    }
    Ok(Json(json!({ "hub": hub, "repos": out })))
}

/// For `nest ui` (Unix socket only): where the web UI is and its token.
async fn web_info(State(api): State<Api>) -> R<serde_json::Value> {
    Ok(Json(
        json!({ "addr": api.web_addr, "token": api.web_token }),
    ))
}

async fn backups(State(api): State<Api>) -> R<serde_json::Value> {
    let stores: HashMap<u64, String> = api
        .placer
        .archive_stores()?
        .into_iter()
        .map(|(id, n, _)| (id.0, n))
        .collect();
    let list: Vec<serde_json::Value> = api
        .placer
        .backups()?
        .into_iter()
        .map(|b| json!({ "id": b.id, "name": b.name, "store": stores.get(&b.store.0), "created": b.created.0 / 1_000_000_000,
            "selector": b.selector, "files": b.files, "bytes": b.bytes }))
        .collect();
    Ok(Json(json!({ "backups": list })))
}

#[derive(Deserialize)]
struct BackupReq {
    selector: String,
    name: String,
    store: String,
}

async fn backup_create(State(api): State<Api>, Json(r): Json<BackupReq>) -> R<serde_json::Value> {
    let sel = api.selector(&r.selector)?;
    Ok(Json(
        json!({ "job": api.placer.backup_create(sel, r.name, r.store).await? }),
    ))
}

#[derive(Deserialize)]
struct RestoreReq {
    dst: String,
}

async fn backup_restore(
    State(api): State<Api>,
    Path(id): Path<u64>,
    Json(r): Json<RestoreReq>,
) -> R<serde_json::Value> {
    Ok(Json(
        json!({ "job": api.placer.backup_restore(id, api.ns(&r.dst)).await? }),
    ))
}

async fn backup_delete(State(api): State<Api>, Path(id): Path<u64>) -> R<serde_json::Value> {
    Ok(Json(
        json!({ "deleted": id, "objects_removed": api.placer.backup_delete(id).await? }),
    ))
}

#[derive(Deserialize)]
struct MetaReq {
    store: String,
}

async fn meta_snapshot(State(api): State<Api>, Json(r): Json<MetaReq>) -> R<serde_json::Value> {
    Ok(Json(
        json!({ "snapshot": api.placer.meta_snapshot(&r.store).await? }),
    ))
}

#[derive(Deserialize)]
struct PlanReq {
    /// (host or @group, desired free bytes)
    free: Vec<(String, u64)>,
}

async fn make_plan(State(api): State<Api>, Json(r): Json<PlanReq>) -> R<serde_json::Value> {
    Ok(Json(
        serde_json::to_value(api.placer.plan(&r.free).await?).unwrap_or_default(),
    ))
}

async fn apply_plan(State(api): State<Api>, Path(id): Path<u64>) -> R<serde_json::Value> {
    Ok(Json(json!({ "job": api.placer.apply_plan(id).await? })))
}

async fn groups(State(api): State<Api>) -> R<serde_json::Value> {
    let g: Vec<_> = api
        .placer
        .groups()?
        .into_iter()
        .map(|(n, m)| json!({ "name": n, "members": m }))
        .collect();
    Ok(Json(json!({ "groups": g })))
}

#[derive(Deserialize)]
struct GroupReq {
    members: Vec<String>,
}

async fn set_group(
    State(api): State<Api>,
    Path(name): Path<String>,
    Json(r): Json<GroupReq>,
) -> R<serde_json::Value> {
    api.placer.set_group(&name, r.members).await?;
    Ok(Json(json!({ "group": name })))
}

async fn delete_group(State(api): State<Api>, Path(name): Path<String>) -> R<serde_json::Value> {
    api.placer.delete_group(&name).await?;
    Ok(Json(json!({ "deleted": name })))
}
