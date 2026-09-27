//! The management API (ADR-006): HTTP+JSON served by every daemon on a
//! local Unix socket (`<state_dir>/api.sock`, mode 0600). The `nest` CLI
//! and (later) the web UI use only this.
//!
//! Paths in requests are namespace paths ("/hub/...") or paths under this
//! node's mountpoint, which are translated.

mod transfer;

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
use nest_types::{FileKind, GenState, NestError, StoreId};
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

pub(crate) struct ApiError(pub(crate) StatusCode, pub(crate) String);

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

pub(crate) type R<T> = Result<Json<T>, ApiError>;

pub(crate) fn bad(msg: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, msg.into())
}

impl Api {
    /// Namespace path for a namespace path or a path under our mountpoint.
    pub(crate) fn ns(&self, p: &str) -> String {
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

    pub(crate) fn conn(&self) -> Result<rusqlite::Connection, ApiError> {
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
        .route("/v1/jobs/{id}/cancel", post(cancel_job))
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
        .route("/v1/hf/detail", get(hf_detail))
        .route("/v1/hf/import", post(hf_import))
        .route("/v1/logs", get(logs))
        .route("/v1/space/tree", get(space_tree))
        .route("/v1/download", get(transfer::download))
        .route(
            "/v1/upload",
            put(transfer::upload).layer(axum::extract::DefaultBodyLimit::disable()),
        )
        .route("/v1/mkdir", post(transfer::mkdir))
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
                    .is_some_and(|v| v.strip_prefix("Bearer ").is_some_and(|t| t == token))
                    // A download is a plain link, which cannot send a
                    // header: it may carry the token in its query.
                    || (req.uri().path() == "/v1/download"
                        && req.uri().query().is_some_and(|q| {
                            q.split('&').any(|kv| kv.strip_prefix("token=") == Some(token.as_str()))
                        }));
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
    /// Copy files that cannot be hard-linked (another filesystem).
    #[serde(default)]
    copy: bool,
}

async fn import(State(api): State<Api>, Json(r): Json<ImportReq>) -> R<serde_json::Value> {
    let opts = ImportOptions {
        src: r.src.into(),
        dst: api.ns(&r.dst),
        r#move: r.r#move,
        seal: r.seal.unwrap_or(SealMode::Auto),
        copy: r.copy,
    };
    Ok(Json(json!({ "job": api.placer.import(opts) })))
}

#[derive(Deserialize)]
struct HfImportReq {
    src: String,
    #[serde(default)]
    r#move: bool,
    /// The `hf` program to finalize with; absent: mirror snapshots offline.
    hf: Option<String>,
    #[serde(default = "yes")]
    copy: bool,
}

async fn hf_import(State(api): State<Api>, Json(r): Json<HfImportReq>) -> R<serde_json::Value> {
    let mount_hub = api
        .mountpoint
        .as_ref()
        .map(|m| std::path::Path::new(m).join(api.hub.trim_start_matches('/')));
    let opts = nest_place::hfimport::HfImportOptions {
        src: r.src.into(),
        hub: api.hub.clone(),
        mount_hub,
        hf: r.hf.map(Into::into),
        r#move: r.r#move,
        copy: r.copy,
    };
    Ok(Json(json!({ "job": api.placer.hf_import(opts) })))
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
/// Usage summed over `files`, per host name: opens, last open, bytes read
/// locally and over the network (other hosts or archives).
fn usage_by_host(
    usage: &HashMap<nest_types::NodeId, HashMap<nest_types::FileId, nest_data::usage::FileUsage>>,
    names: &HashMap<nest_types::NodeId, String>,
    files: &std::collections::HashSet<nest_types::FileId>,
) -> HashMap<String, serde_json::Value> {
    let mut out = HashMap::new();
    for (node, per) in usage {
        let (mut opens, mut last, mut local, mut net) = (0u64, 0u64, 0u64, 0u64);
        for f in files {
            if let Some(u) = per.get(f) {
                opens += u.opens;
                last = last.max(u.last_open_ms);
                local += u.local_bytes;
                net += u.remote_bytes + u.archive_bytes;
            }
        }
        if let Some(n) = names.get(node) {
            out.insert(
                n.clone(),
                json!({ "opens": opens, "last_open_ms": last, "local_bytes": local, "net_bytes": net }),
            );
        }
    }
    out
}

const MONTH_MS: u64 = 30 * 86_400_000;

#[derive(Deserialize)]
struct DetailQ {
    selector: String,
}

/// Everything about one Hugging Face repo: its files as the snapshots name
/// them and where each lives, revisions and the refs pointing at them, the
/// rules that place it, and per-host usage over the last 30 days.
async fn hf_detail(State(api): State<Api>, Query(q): Query<DetailQ>) -> R<serde_json::Value> {
    let sel = Selector::parse(&q.selector, &api.hub).map_err(bad)?;
    let Selector::Hf {
        repo, repo_type, ..
    } = &sel
    else {
        return Err(bad("expected hf:org/name or hf-dataset:org/name"));
    };
    let dir = format!(
        "{}/{}--{}",
        api.hub.trim_end_matches('/'),
        if repo_type == "dataset" {
            "datasets"
        } else {
            "models"
        },
        repo.replace('/', "--")
    );
    let (_m, ready) = api.placer.readiness(&sel, &[]).await?;
    // Files and where they live, from the space tree's models shape.
    let nodes = api.placer.nodes()?;
    let mut store_names: HashMap<StoreId, String> = nodes
        .iter()
        .map(|h| (h.node.live_store(), h.name.clone()))
        .collect();
    store_names.extend(
        api.placer
            .archive_stores()?
            .into_iter()
            .map(|(id, n, _)| (id, n)),
    );
    let req = nest_place::space::TreeReq {
        store: None,
        weight: nest_place::space::Weight::Logical,
        models: true,
        root: "/".into(),
        depth: 1,
        max_children: 100_000,
        hub: api.hub.clone(),
        store_names,
    };
    let c = api.conn()?;
    let want = sel.describe();
    let tree = tokio::task::spawn_blocking(move || nest_place::space::tree(&c, &req))
        .await
        .map_err(|e| bad(e.to_string()))??;
    let node = tree
        .children
        .into_iter()
        .find(|n| n.selector.as_deref() == Some(want.as_str()))
        .unwrap_or_default();
    // Revisions and refs.
    let mut revisions: Vec<serde_json::Value> = Vec::new();
    let mut refs: Vec<(String, nest_types::FileId)> = Vec::new();
    {
        let c = api.conn()?;
        if let Ok((sid, _)) = selector::resolve_path(&c, &format!("{dir}/snapshots")) {
            for (_, e) in query::readdir(&c, sid, 0, 1000).map_err(|e| bad(e.to_string()))? {
                let name = String::from_utf8_lossy(&e.name).into_owned();
                if name != "." && name != ".." {
                    revisions.push(json!({ "commit": name, "refs": [] }));
                }
            }
        }
        if let Ok((rid, _)) = selector::resolve_path(&c, &format!("{dir}/refs")) {
            for (_, e) in query::readdir(&c, rid, 0, 1000).map_err(|e| bad(e.to_string()))? {
                let name = String::from_utf8_lossy(&e.name).into_owned();
                if name != "." && name != ".." {
                    refs.push((name, e.id));
                }
            }
        }
    }
    for (name, id) in refs {
        let Ok((fh, _)) = api.vfs.open(id, 0).await else {
            continue;
        };
        let body = api.vfs.read(fh, 0, 256).await.unwrap_or_default();
        api.vfs.release(fh, None).await;
        let commit = String::from_utf8_lossy(&body).trim().to_string();
        if let Some(r) = revisions
            .iter_mut()
            .find(|r| r["commit"] == commit.as_str())
        {
            r["refs"].as_array_mut().expect("array").push(json!(name));
        }
    }
    let rules: Vec<_> = api
        .placer
        .rules()?
        .into_iter()
        .filter(|(_, spec, _)| matches!(&spec.selector, Selector::Hf { repo: r, repo_type: t, .. } if r == repo && t == repo_type))
        .map(|(name, spec, _)| json!({ "name": name, "hosts": spec.hosts, "auto": spec.auto }))
        .collect();
    // Usage per host, for the repo and per file.
    let names: HashMap<_, _> = nodes.iter().map(|h| (h.node, h.name.clone())).collect();
    let usage = api
        .placer
        .usage(nest_data::usage::now_ms().saturating_sub(MONTH_MS))
        .await
        .unwrap_or_default();
    fn leaves(n: &nest_place::space::TreeNode, out: &mut Vec<nest_types::FileId>) {
        out.extend(n.file);
        for c in &n.children {
            leaves(c, out);
        }
    }
    let mut ids = Vec::new();
    leaves(&node, &mut ids);
    let all: std::collections::HashSet<_> = ids.iter().copied().collect();
    let per_file: HashMap<String, HashMap<String, serde_json::Value>> = ids
        .iter()
        .map(|f| {
            (
                f.0.to_string(),
                usage_by_host(&usage, &names, &[*f].into_iter().collect())
                    .into_iter()
                    .filter(|(_, v)| {
                        v["opens"].as_u64() != Some(0) || v["net_bytes"].as_u64() != Some(0)
                    })
                    .collect(),
            )
        })
        .collect();
    Ok(Json(json!({
        "repo": repo,
        "kind": repo_type,
        "selector": want,
        "path": dir,
        "hosts": ready,
        "tree": node,
        "revisions": revisions,
        "rules": rules,
        "usage": usage_by_host(&usage, &names, &all),
        "file_usage": per_file,
    })))
}

async fn hf_repos(State(api): State<Api>) -> R<serde_json::Value> {
    let hub = api.hub.clone();
    let names: HashMap<_, _> = api
        .placer
        .nodes()?
        .into_iter()
        .map(|h| (h.node, h.name))
        .collect();
    let usage = api
        .placer
        .usage(nest_data::usage::now_ms().saturating_sub(MONTH_MS))
        .await
        .unwrap_or_default();
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
            Ok((m, ready)) => {
                let files = m.entries.iter().map(|e| e.file).collect();
                let used = usage_by_host(&usage, &names, &files);
                let last = used
                    .values()
                    .filter_map(|u| u["last_open_ms"].as_u64())
                    .max()
                    .unwrap_or(0);
                out.push(json!({
                    "repo": repo,
                    "kind": kind,
                    "selector": sel.describe(),
                    "files": m.entries.len(),
                    "bytes": m.bytes(),
                    "writing": m.entries.iter().filter(|e| !e.stable).count(),
                    "revisions": revisions,
                    "hosts": ready,
                    "usage": used,
                    "last_open_ms": last,
                }))
            }
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

/// A goal (`{"goal": "free" | "tidy" | "speedup", ...}`), or the older
/// free-space form without `goal`.
#[derive(Deserialize)]
#[serde(untagged)]
enum PlanReq {
    Goal(nest_place::plan::Goal),
    Free {
        /// (host or @group, desired free bytes)
        free: Vec<(String, u64)>,
        /// Archive stores sole copies may be offloaded into, filled in
        /// order; none means the plan only removes redundant copies.
        #[serde(default)]
        archives: Vec<String>,
    },
}

#[derive(Deserialize)]
struct TreeQ {
    /// A host or archive store name; the whole cluster when absent.
    scope: Option<String>,
    /// `logical` (default) or `copies` (size times copies).
    weight: Option<String>,
    /// `models` (default) or `path`.
    shape: Option<String>,
    root: Option<String>,
    depth: Option<u32>,
    max: Option<usize>,
}

/// A size-weighted tree of what sparknest holds, for treemaps, plus the
/// free space of the stores in scope.
async fn space_tree(State(api): State<Api>, Query(q): Query<TreeQ>) -> R<serde_json::Value> {
    let nodes = api.placer.nodes()?;
    let archives = api.placer.archive_stores()?;
    let mut names: HashMap<StoreId, String> = nodes
        .iter()
        .map(|h| (h.node.live_store(), h.name.clone()))
        .collect();
    names.extend(archives.iter().map(|(id, n, _)| (*id, n.clone())));
    let scope = q.scope.filter(|s| !s.is_empty() && s != "all");
    let store = match &scope {
        None => None,
        Some(s) => Some(
            names
                .iter()
                .find(|(_, n)| *n == s)
                .map(|(id, _)| *id)
                .ok_or_else(|| bad(format!("no host or archive store named {s}")))?,
        ),
    };
    let req = nest_place::space::TreeReq {
        store,
        weight: if q.weight.as_deref() == Some("copies") {
            nest_place::space::Weight::Copies
        } else {
            nest_place::space::Weight::Logical
        },
        models: q.shape.as_deref() != Some("path"),
        root: q.root.unwrap_or_else(|| "/".into()),
        depth: q.depth.unwrap_or(6).min(32),
        max_children: q.max.unwrap_or(150).clamp(2, 2000),
        hub: api.hub.clone(),
        store_names: names,
    };
    let c = api.conn()?;
    let tree = tokio::task::spawn_blocking(move || nest_place::space::tree(&c, &req))
        .await
        .map_err(|e| bad(e.to_string()))??;
    // Free space of what is in scope: hosts from their status, archives
    // through a healthy gateway.
    let mut free = Vec::new();
    for s in api.placer.status().await? {
        if scope.as_ref().is_none_or(|x| *x == s.name)
            && let Some(i) = &s.info
        {
            free.push(json!({ "name": s.name, "kind": "host", "free": i.free_bytes, "total": i.total_bytes, "held": i.object_bytes }));
        }
    }
    for st in api.placer.stores().await? {
        if scope.as_ref().is_none_or(|x| *x == st.name)
            && let Some((_, h)) = st.gateways.iter().find(|(_, h)| h.healthy)
        {
            free.push(json!({ "name": st.name, "kind": "archive", "free": h.free_bytes, "total": h.total_bytes, "held": h.object_bytes }));
        }
    }
    Ok(Json(json!({ "tree": tree, "stores": free })))
}

#[derive(Deserialize)]
struct LogsReq {
    /// One host; every node when absent.
    host: Option<String>,
    /// Least severe level shown: error, warn, info (default), debug.
    level: Option<String>,
    /// Case-insensitive text the message or target must contain.
    q: Option<String>,
    limit: Option<usize>,
}

async fn logs(
    State(api): State<Api>,
    axum::extract::Query(r): axum::extract::Query<LogsReq>,
) -> R<serde_json::Value> {
    let (lines, missing) = api
        .placer
        .logs(
            r.host.as_deref().filter(|h| !h.is_empty()),
            nest_place::logs::LogQuery {
                level: r.level,
                contains: r.q.filter(|q| !q.is_empty()),
                after: None,
                limit: r.limit,
            },
        )
        .await?;
    let lines: Vec<_> = lines
        .into_iter()
        .map(|(host, l)| {
            json!({ "host": host, "ts_ms": l.ts_ms, "level": l.level, "target": l.target, "message": l.message })
        })
        .collect();
    Ok(Json(json!({ "lines": lines, "unreachable": missing })))
}

async fn cancel_job(State(api): State<Api>, Path(id): Path<u64>) -> R<serde_json::Value> {
    api.placer.cancel_job(id).await?;
    Ok(Json(json!({ "cancelled": id })))
}

async fn make_plan(State(api): State<Api>, Json(r): Json<PlanReq>) -> R<serde_json::Value> {
    Ok(Json(
        serde_json::to_value(
            api.placer
                .plan(match r {
                    PlanReq::Goal(g) => g,
                    PlanReq::Free { free, archives } => {
                        nest_place::plan::Goal::Free { free, archives }
                    }
                })
                .await?,
        )
        .unwrap_or_default(),
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
