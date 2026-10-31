//! The management API (ADR-006): HTTP+JSON served by every daemon on a
//! local Unix socket (`<state_dir>/api.sock`, mode 0600). The `nest` CLI
//! and (later) the web UI use only this.
//!
//! Paths in requests are namespace paths ("/hub/...") or paths under this
//! node's mountpoint, which are translated.

pub mod counters;
mod events;
mod transfer;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
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
    /// Push loads (ADR-063), for status.
    pub push: Option<Arc<nest_data::push::Push>>,
    pub placer: Arc<Placer>,
    pub mountpoint: Option<String>,
    pub hub: String,
    /// Bearer token for the network listener (and the web UI).
    pub web_token: String,
    /// Network listener address, if any (for `nest ui`).
    pub web_addr: Option<std::net::SocketAddr>,
    /// This host's name (for lost+found).
    pub host: String,
    /// Live updates for web UI subscribers.
    pub events: Arc<events::Events>,
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

/// Which web UI this daemon serves: a page that reconnects to a daemon
/// serving another reloads itself rather than run stale code against it.
pub(crate) fn ui_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| {
        use sha2::Digest;
        sha2::Sha256::digest(INDEX_HTML.as_bytes())
            .iter()
            .take(8)
            .map(|b| format!("{b:02x}"))
            .collect()
    })
}

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

    /// A selector as typed, with mountpoint paths mapped into the namespace.
    fn selector_string(&self, s: &str) -> String {
        if s.starts_with('/') {
            self.ns(s)
        } else {
            s.to_string()
        }
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
        .route(
            "/v1/passthrough",
            get(get_passthrough).post(set_passthrough),
        )
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
        .route("/v1/disks", get(disks).post(add_disk))
        .route("/v1/disks/{name}", delete(remove_disk))
        .route("/v1/disks/{name}/drain", post(drain_disk))
        .route("/v1/offload", post(offload))
        .route("/v1/plans", post(make_plan))
        .route("/v1/plans/{id}", get(get_plan))
        .route("/v1/plans/{id}/select", post(select_plan))
        .route("/v1/plans/{id}/apply", post(apply_plan))
        .route("/v1/groups", get(groups))
        .route("/v1/groups/{name}", put(set_group).delete(delete_group))
        .route("/v1/settings", get(settings))
        .route(
            "/v1/settings/{name}",
            put(set_setting).delete(reset_setting),
        )
        .route("/v1/backups", get(backups).post(backup_create))
        .route("/v1/backups/{id}/restore", post(backup_restore))
        .route("/v1/backups/{id}", axum::routing::delete(backup_delete))
        .route("/v1/backups/meta", post(meta_snapshot))
        .route("/v1/hf", get(hf_repos))
        .route("/v1/hf/detail", get(hf_detail))
        .route("/v1/hf/import", post(hf_import))
        .route("/v1/hf/remove", post(hf_remove))
        .route("/v1/hf/share", post(hf_share))
        .route("/v1/hf/refs", get(hf_refs))
        .route("/v1/hf/search", get(hf_search))
        .route("/v1/hf/size", get(hf_size))
        .route("/v1/hf/download", post(hf_download))
        .route("/v1/logs", get(logs))
        .route("/v1/space/tree", get(space_tree))
        .route("/v1/download", get(transfer::download))
        .route("/v1/ws", get(events::ws))
        .route(
            "/v1/upload",
            put(transfer::upload).layer(axum::extract::DefaultBodyLimit::disable()),
        )
        .route("/v1/mkdir", post(transfer::mkdir))
        .route("/v1/drop-caches", post(drop_caches))
        .route("/v1/web", get(web_info))
        .route("/v1/cluster", get(cluster))
        .route("/v1/cluster/raft", get(cluster_raft))
        .route("/v1/net", get(net))
        .route("/v1/push", get(push_status))
        .route("/v1/push/loads", get(push_loads))
        .route("/v1/push/loads/{group}", get(push_load))
        .route("/v1/cluster/remove", post(cluster_remove))
        .route("/v1/cluster/role", post(cluster_role))
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
                    // A download link and a WebSocket cannot send a
                    // header: they may carry the token in the query.
                    || (matches!(req.uri().path(), "/v1/download" | "/v1/ws")
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

/// Push loads this host takes part in, and what it has sent (ADR-063).
async fn push_status(State(api): State<Api>) -> R<serde_json::Value> {
    let Some(p) = &api.push else {
        return Ok(Json(json!({"groups": [], "rdma_slots": 0, "rpc_slots": 0})));
    };
    let st = p.stats();
    Ok(Json(json!({
        "groups": p
            .groups()
            .into_iter()
            .map(|(g, role)| json!({"group": g, "role": role}))
            .collect::<Vec<_>>(),
        "rdma_slots": st.rdma_slots,
        "rpc_slots": st.rpc_slots,
    })))
}

/// Every host's recent push loads (ADR-063), newest first, without their
/// time bins.
async fn push_loads(State(api): State<Api>) -> R<serde_json::Value> {
    let v = api.placer.push_loads(None).await?;
    Ok(Json(json!({ "loads": v })))
}

/// One push load with its time bins: bytes from each source to each rank.
async fn push_load(
    State(api): State<Api>,
    axum::extract::Path(group): axum::extract::Path<u64>,
) -> R<serde_json::Value> {
    let v = api.placer.push_loads(Some(group)).await?;
    match v.into_iter().find(|r| r.group == group) {
        Some(r) => Ok(Json(
            serde_json::to_value(r).map_err(|e| bad(e.to_string()))?,
        )),
        None => Err(ApiError(StatusCode::NOT_FOUND, "no such load".into())),
    }
}

async fn status(State(api): State<Api>) -> R<serde_json::Value> {
    Ok(Json(status_json(&api).await?))
}

pub(crate) async fn status_json(api: &Api) -> Result<serde_json::Value, ApiError> {
    status_json_with(api, api.placer.status().await?).await
}

/// For live views: busy hosts answer from their last report.
pub(crate) async fn status_json_live(api: &Api) -> Result<serde_json::Value, ApiError> {
    status_json_with(api, api.placer.status_live().await?).await
}

async fn status_json_with(
    api: &Api,
    nodes: Vec<nest_place::placer::NodeStatus>,
) -> Result<serde_json::Value, ApiError> {
    let d = api.vfs.data();
    let t = api.vfs.statfs().map(|(_, n)| n).unwrap_or(0);
    Ok(json!({
        "me": d.id(),
        // This host serves: caught up, admitted after any recovery, lease valid.
        "serving": d.caught_up() && d.lease_valid(),
        "leader": d.meta().leader(),
        "applied": d.meta().applied_index(),
        "mountpoint": api.mountpoint,
        "entries": t,
        "nodes": nodes,
        "rules": api.placer.rules()?.len(),
    }))
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
        .places()?
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
    /// With `deep`: hash copies already verified again.
    #[serde(default)]
    recheck: bool,
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
            recheck: r.recheck,
            settle_owned: false,
            min_age: std::time::Duration::from_secs(600),
            host: api.host.clone(),
            stamp: nest_data::fsck::stamp_now(),
        })
        .await?;
    Ok(Json(serde_json::json!({
        "host": api.host,
        "objects": rep.objects,
        "hashed": rep.hashed,
        "already_verified": rep.already_verified,
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
struct PassthroughReq {
    /// A path (file or tree) or hf:org/name.
    selector: String,
    /// Always passthrough where a complete local copy exists, or back to
    /// each host's policy.
    always: bool,
}

/// The distinct files a selector names.
async fn selected_files(api: &Api, selector: &str) -> Result<Vec<selector::Entry>, ApiError> {
    let sel = api.selector(selector)?;
    let mut entries = api.placer.manifest(&sel).await?.entries;
    entries.sort_by_key(|e| e.file);
    entries.dedup_by_key(|e| e.file);
    Ok(entries)
}

/// `0.4.1` as (0, 4, 1); anything unparsable as 0.
fn semver(v: &str) -> (u64, u64, u64) {
    let mut p = v.split(['.', '-']).map(|x| x.parse().unwrap_or(0));
    (
        p.next().unwrap_or(0),
        p.next().unwrap_or(0),
        p.next().unwrap_or(0),
    )
}

/// Refuse a new kind of log entry until every host runs a version that
/// decodes it (ADR-018): an older host would stop applying the log.
async fn require_every_host_upgraded(api: &Api, what: &str) -> Result<(), ApiError> {
    let mine = env!("CARGO_PKG_VERSION");
    let behind: Vec<String> = api
        .placer
        .status()
        .await?
        .into_iter()
        .filter_map(|n| match &n.info {
            Some(i) if semver(&i.version) >= semver(mine) => None,
            Some(i) => Some(format!("{} runs {}", n.name, i.version)),
            None => Some(format!("{} does not answer", n.name)),
        })
        .collect();
    if behind.is_empty() {
        return Ok(());
    }
    Err(ApiError(
        StatusCode::CONFLICT,
        format!(
            "{what} needs every host on {mine} or later first: {}",
            behind.join(", ")
        ),
    ))
}

/// Mark files always passthrough on hosts with a complete local copy
/// (ADR-053), or return them to each host's policy.
async fn set_passthrough(
    State(api): State<Api>,
    Json(r): Json<PassthroughReq>,
) -> R<serde_json::Value> {
    require_every_host_upgraded(&api, "marking files passthrough").await?;
    let entries = selected_files(&api, &r.selector).await?;
    let mut changed = 0;
    let mut errors = Vec::new();
    for chunk in entries.chunks(512) {
        let cmds = chunk
            .iter()
            .map(|e| nest_meta::Command::SetPassthrough {
                file: e.file,
                always: r.always,
            })
            .collect();
        let nest_meta::Reply::Batch(results) = api
            .vfs
            .data()
            .meta()
            .propose(nest_meta::Command::Batch(cmds))
            .await?
        else {
            return Err(NestError::Io("unexpected reply".into()).into());
        };
        for (res, e) in results.into_iter().zip(chunk) {
            match res {
                Ok(_) => changed += 1,
                Err(err) => errors.push(format!("{}: {err}", e.path)),
            }
        }
    }
    Ok(Json(
        json!({ "files": entries.len(), "changed": changed, "errors": errors }),
    ))
}

/// How many of a selector's files are marked always passthrough.
async fn get_passthrough(State(api): State<Api>, Query(q): Query<DetailQ>) -> R<serde_json::Value> {
    let entries = selected_files(&api, &q.selector).await?;
    let c = api.conn()?;
    let mut always = 0;
    for e in &entries {
        if query::read_hints(&c, e.file)
            .map_err(|e| NestError::Io(e.to_string()))?
            .1
        {
            always += 1;
        }
    }
    Ok(Json(json!({ "files": entries.len(), "always": always })))
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
    Ok(Json(json!({ "jobs": api.placer.all_jobs().await })))
}

async fn job(State(api): State<Api>, Path(id): Path<u64>) -> R<serde_json::Value> {
    let j = api
        .placer
        .find_job(id)
        .await
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
struct DropCachesReq {
    /// Hosts or @groups; every node when empty.
    #[serde(default)]
    hosts: Vec<String>,
}

/// Drop the clean page cache on hosts (benchmarks).
async fn drop_caches(State(api): State<Api>, Json(r): Json<DropCachesReq>) -> R<serde_json::Value> {
    let out: Vec<_> = api
        .placer
        .drop_caches(&r.hosts)
        .await?
        .into_iter()
        .map(|(h, r)| match r {
            Ok(m) => json!({ "host": h, "ok": true, "message": m }),
            Err(e) => json!({ "host": h, "ok": false, "message": e }),
        })
        .collect();
    Ok(Json(json!({ "hosts": out })))
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
    /// Place each blob on the host with the most free space.
    #[serde(default)]
    spread: bool,
    /// Put the blobs in this archive store (this node must be a gateway).
    #[serde(default)]
    archive: Option<String>,
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
        spread: r.spread,
        archive: r.archive.filter(|a| !a.is_empty()),
    };
    if let Some(a) = &opts.archive {
        if opts.spread {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "--archive and --spread do not go together".into(),
            ));
        }
        api.placer.archive_here(a)?;
    }
    Ok(Json(json!({ "job": api.placer.hf_import(opts) })))
}

#[derive(Deserialize)]
struct HfRemoveReq {
    /// Repos: `org/name`, `hf:org/name`, `model/org/name`, `dataset/org/name`.
    targets: Vec<String>,
    /// Only report what would go (hf's dry run), synchronously.
    #[serde(default)]
    dry_run: bool,
    /// The `hf` program; absent: found on this node.
    hf: Option<String>,
}

/// Delete Hugging Face repos properly: `hf cache rm` on sparknest's hub
/// (through this node's mount), which knows the hub's shared blobs.
async fn hf_remove(State(api): State<Api>, Json(r): Json<HfRemoveReq>) -> R<serde_json::Value> {
    let mount_hub = api
        .mountpoint
        .as_ref()
        .map(|m| std::path::Path::new(m).join(api.hub.trim_start_matches('/')))
        .ok_or_else(|| ApiError(StatusCode::CONFLICT, "this node has no mount".into()))?;
    let hf =
        r.hf.map(std::path::PathBuf::from)
            .or_else(nest_place::hfimport::find_hf)
            .ok_or_else(|| {
                ApiError(
                    StatusCode::CONFLICT,
                    "hf is not installed on this node".into(),
                )
            })?;
    if r.targets.is_empty() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "no repos given".into()));
    }
    if r.dry_run {
        let v = nest_place::hfimport::remove(&hf, &mount_hub, &r.targets, true).await?;
        return Ok(Json(json!({ "dry_run": v })));
    }
    Ok(Json(
        json!({ "job": api.placer.hf_remove(hf, mount_hub, r.targets) }),
    ))
}

#[derive(Deserialize)]
struct HfShareReq {
    /// Repo directories (`models--org--name`) or `org/name`; empty: every
    /// repo with its own blobs.
    #[serde(default)]
    repos: Vec<String>,
    /// Report what would change, per repo, synchronously.
    #[serde(default)]
    dry_run: bool,
    /// Check each blob's sha256 before moving it (reads every file).
    #[serde(default)]
    verify: bool,
    hf: Option<String>,
}

/// Move repos' own blobs into the hub's shared store (the layout
/// huggingface_hub ≥ 1.32 writes, ADR-010), with hf's own code, through
/// this node's mount.
async fn hf_share(State(api): State<Api>, Json(r): Json<HfShareReq>) -> R<serde_json::Value> {
    let hub = mount_hub(&api)?;
    let hf = r.hf.map(std::path::PathBuf::from);
    let repos: Vec<String> = r
        .repos
        .iter()
        .map(|t| {
            if t.starts_with("models--") || t.starts_with("datasets--") {
                t.clone()
            } else {
                let p = nest_place::hfimport::parse_repo_ref(t);
                let kind = if p.dataset { "dataset" } else { "model" };
                nest_place::hfimport::repo_dir(kind, &p.repo)
            }
        })
        .collect();
    if r.dry_run {
        let repos = if repos.is_empty() {
            let h = hub.clone();
            tokio::task::spawn_blocking(move || nest_place::hfimport::unshared_repos(&h))
                .await
                .map_err(|e| NestError::Io(e.to_string()))??
        } else {
            repos
        };
        let mut out = Vec::new();
        for repo in repos {
            out.push(
                match nest_place::hfimport::share(hf.as_deref(), &hub, &repo, true, false, |_| {})
                    .await
                {
                    Ok(o) => serde_json::to_value(o).expect("plain JSON"),
                    Err(e) => json!({ "repo": repo, "error": e.to_string() }),
                },
            );
        }
        return Ok(Json(json!({ "dry_run": out })));
    }
    Ok(Json(
        json!({ "job": api.placer.hf_share(hf, hub, repos, r.verify) }),
    ))
}

fn mount_hub(api: &Api) -> Result<std::path::PathBuf, ApiError> {
    api.mountpoint
        .as_ref()
        .map(|m| std::path::Path::new(m).join(api.hub.trim_start_matches('/')))
        .ok_or_else(|| ApiError(StatusCode::CONFLICT, "this node has no mount".into()))
}

/// Every hub repo's refs: repo directory → commit → the branch and tag
/// names (`refs/<name>`, `refs/pr/1`) naming it, read through this node's
/// mount on a blocking thread. Empty without a mount.
async fn hub_refs(api: &Api) -> HashMap<String, HashMap<String, Vec<String>>> {
    let Ok(hub) = mount_hub(api) else {
        return HashMap::new();
    };
    tokio::task::spawn_blocking(move || {
        let mut out: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        for e in std::fs::read_dir(&hub).into_iter().flatten().flatten() {
            let dir = e.file_name().to_string_lossy().into_owned();
            if !(dir.starts_with("models--") || dir.starts_with("datasets--")) {
                continue;
            }
            let root = e.path().join("refs");
            let mut stack = vec![root.clone()];
            while let Some(d) = stack.pop() {
                for f in std::fs::read_dir(&d).into_iter().flatten().flatten() {
                    let p = f.path();
                    if p.is_dir() {
                        stack.push(p);
                        continue;
                    }
                    let Ok(body) = std::fs::read_to_string(&p) else {
                        continue;
                    };
                    let commit = body.trim().to_string();
                    let Ok(name) = p.strip_prefix(&root) else {
                        continue;
                    };
                    out.entry(dir.clone())
                        .or_default()
                        .entry(commit)
                        .or_default()
                        .push(name.to_string_lossy().into_owned());
                }
            }
        }
        // main first, then by name: the first names a revision's selector.
        for m in out.values_mut() {
            for v in m.values_mut() {
                v.sort_by(|a, b| (a != "main").cmp(&(b != "main")).then(a.cmp(b)));
            }
        }
        out
    })
    .await
    .unwrap_or_default()
}

fn hf_program(given: Option<String>) -> Result<std::path::PathBuf, ApiError> {
    given
        .map(std::path::PathBuf::from)
        .filter(|p| p.is_file())
        .or_else(nest_place::hfimport::find_hf)
        .ok_or_else(|| {
            ApiError(
                StatusCode::CONFLICT,
                "hf is not installed on this node".into(),
            )
        })
}

#[derive(Deserialize)]
struct HfSearchQ {
    q: String,
    #[serde(default)]
    kind: String,
    limit: Option<u32>,
}

/// Search the Hub through hf (most downloaded first).
async fn hf_search(State(_api): State<Api>, Query(q): Query<HfSearchQ>) -> R<serde_json::Value> {
    let hf = hf_program(None)?;
    let v =
        nest_place::hfimport::search(&hf, &q.kind, &q.q, q.limit.unwrap_or(20).min(100)).await?;
    Ok(Json(json!({ "results": v })))
}

#[derive(Deserialize)]
struct HfSizeQ {
    repo: String,
    #[serde(default)]
    kind: String,
    revision: Option<String>,
}

/// A repo as given (an id, `repo@branch`, or a huggingface.co link), with
/// a revision and kind given separately winning over those in the text.
fn repo_ref(
    repo: &str,
    kind: &str,
    revision: Option<String>,
) -> Result<(String, String, Option<String>), ApiError> {
    let r = nest_place::hfimport::parse_repo_ref(repo);
    if r.repo.is_empty() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "no repo given".into()));
    }
    let kind = if kind.is_empty() && r.dataset {
        "dataset".to_string()
    } else {
        kind.to_string()
    };
    Ok((
        r.repo,
        kind,
        revision.filter(|v| !v.is_empty()).or(r.revision),
    ))
}

/// Files and bytes of a repo on the Hub (hf's dry run).
async fn hf_size(State(api): State<Api>, Query(q): Query<HfSizeQ>) -> R<serde_json::Value> {
    let hf = hf_program(None)?;
    let (repo, kind, revision) = repo_ref(&q.repo, &q.kind, q.revision)?;
    let s = nest_place::hfimport::download_size(
        &hf,
        &mount_hub(&api)?,
        &repo,
        &kind,
        revision.as_deref(),
    )
    .await?;
    Ok(Json(json!({
        "repo": repo,
        "kind": kind,
        "revision": revision,
        "files": s.files,
        "missing_files": s.missing_files,
        "missing_bytes": s.missing_bytes,
    })))
}

#[derive(Deserialize)]
struct HfRefsQ {
    repo: String,
    #[serde(default)]
    kind: String,
}

/// A repo's branches and tags on the Hub, each marked with whether the
/// hub here holds it (its `refs/<name>` names the same commit) .
async fn hf_refs(State(api): State<Api>, Query(q): Query<HfRefsQ>) -> R<serde_json::Value> {
    let (repo, kind, named) = repo_ref(&q.repo, &q.kind, None)?;
    let hub = mount_hub(&api)?;
    let r = nest_place::hfimport::repo_refs(&hub, &repo, &kind).await?;
    let dir = hub
        .join(nest_place::hfimport::repo_dir(&kind, &repo))
        .join("refs");
    // Read through the mount: off the async executor.
    let (branches, tags) = tokio::task::spawn_blocking(move || {
        let mark = |v: Vec<nest_place::hfimport::HubRef>| -> Vec<serde_json::Value> {
            v.into_iter()
                .map(|b| {
                    let have = std::fs::read_to_string(dir.join(&b.name)).ok();
                    let cached = match have.as_deref().map(str::trim) {
                        Some(c) if c == b.sha => "current",
                        Some(_) => "older",
                        None => "",
                    };
                    json!({ "name": b.name, "sha": b.sha, "cached": cached })
                })
                .collect()
        };
        (mark(r.branches), mark(r.tags))
    })
    .await
    .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(json!({
        "repo": repo,
        "kind": kind,
        "named": named,
        "branches": branches,
        "tags": tags,
    })))
}

#[derive(Deserialize)]
struct HfDownloadReq {
    repo: String,
    #[serde(default)]
    kind: String,
    /// One place to download everything to: a host, one of its disks, or
    /// an off-grid host (ADR-075).
    #[serde(default)]
    host: String,
    /// Places or @groups to spread the files over (default: every serving
    /// fabric host, unless `host` is given).
    #[serde(default)]
    hosts: Vec<String>,
    revision: Option<String>,
    /// Files downloading at once across the cluster (default 4).
    in_flight: Option<usize>,
    /// Download into this archive store instead of hosts (this node must
    /// be one of its gateways).
    #[serde(default)]
    archive: Option<String>,
    /// Unused (the daemon finds hf itself); kept for older CLIs.
    #[allow(dead_code)]
    hf: Option<String>,
}

/// Download a repo: the files no host has, spread over the hosts (or on
/// one), each written through the mount of the host it is assigned to, so
/// its first copy lands there; nothing the cluster holds moves. Returns the
/// job (on this node), with byte progress per host.
async fn hf_download(State(api): State<Api>, Json(r): Json<HfDownloadReq>) -> R<serde_json::Value> {
    let (repo, kind, revision) = repo_ref(&r.repo, &r.kind, r.revision)?;
    let r = HfDownloadReq {
        kind,
        revision,
        ..r
    };
    if let Some(archive) = r.archive.filter(|a| !a.is_empty()) {
        if !r.host.is_empty() || !r.hosts.is_empty() {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "a download goes into an archive or onto hosts, not both".into(),
            ));
        }
        let job = api
            .placer
            .hf_download_archive(api.hub.clone(), repo, r.kind, r.revision, archive)
            .await?;
        return Ok(Json(json!({ "job": job })));
    }
    let hosts = if r.host.is_empty() {
        r.hosts
    } else {
        vec![r.host]
    };
    let (job, attached) = api
        .placer
        .hf_download_spread(
            api.hub.clone(),
            repo,
            r.kind,
            r.revision,
            hosts,
            r.in_flight.unwrap_or(4).clamp(1, 16),
        )
        .await?;
    Ok(Json(json!({ "job": job, "attached": attached })))
}

async fn cluster(State(api): State<Api>) -> R<serde_json::Value> {
    Ok(Json(api.placer.membership()?))
}

/// This host's view of each Raft peer (ADR-057): round trips its Raft
/// network measured per kind of request, and replication progress when it
/// leads.
async fn cluster_raft(State(api): State<Api>) -> R<serde_json::Value> {
    let meta = api.vfs.data().meta();
    let m = meta.metrics();
    Ok(Json(json!({
        "me": meta.id(),
        "leader": m.current_leader,
        "term": m.current_term,
        "peers": meta.raft_peers(),
    })))
}

/// Every host's view of the network: control path, RDMA link, TCP lanes,
/// Raft round trips, NIC loss counters (ADR-057).
async fn net(State(api): State<Api>) -> R<Vec<nest_place::placer::NetStatus>> {
    Ok(Json(api.placer.net().await?))
}

#[derive(Deserialize)]
struct RemoveReq {
    host: String,
    /// The host is gone for good: drop the copies recorded on it first.
    #[serde(default)]
    forget_copies: bool,
}

#[derive(Deserialize)]
struct RoleReq {
    host: String,
    /// "offgrid" or "fabric".
    role: String,
    /// Its control address on the fabric (role "fabric").
    #[serde(default)]
    addr: Option<String>,
}

/// Move a host off the fabric or onto it (ADR-047). Done while the host is
/// stopped for the move; it starts again with the matching node.toml.
async fn cluster_role(State(api): State<Api>, Json(r): Json<RoleReq>) -> R<serde_json::Value> {
    let h = api
        .placer
        .resolve_hosts(std::slice::from_ref(&r.host))?
        .pop()
        .ok_or(bad("unknown host"))?;
    use nest_place::admin::MembershipChange;
    let ok = |a: &str| a.parse::<std::net::SocketAddr>().is_ok();
    let change = match (r.role.as_str(), r.addr) {
        ("offgrid", None) => MembershipChange::SetRole {
            node: h.node,
            addr: String::new(),
        },
        // On the LAN: every host dials it there (ADR-071).
        ("offgrid", Some(a)) if ok(&a) => MembershipChange::OffgridAt {
            node: h.node,
            addr: a,
        },
        ("offgrid", Some(_)) => return Err(bad("addr is its LAN control address (ip:port)")),
        ("fabric", Some(a)) if ok(&a) => MembershipChange::SetRole {
            node: h.node,
            addr: a,
        },
        ("fabric", _) => return Err(bad("role fabric needs addr, its control address (ip:port)")),
        _ => return Err(bad("role is offgrid or fabric")),
    };
    api.placer.change_membership(change).await?;
    Ok(Json(json!({ "host": h.name, "role": r.role })))
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
    let mut forgot = json!(null);
    if r.forget_copies {
        let (retired, kept) = api.placer.forget_copies(&h).await?;
        if !kept.is_empty() {
            return Err(bad(format!(
                "{} held the last copy of {} files (first: file {}); remove them, then retry",
                h.name,
                kept.len(),
                kept[0].0
            )));
        }
        forgot = json!(retired);
    }
    api.placer
        .change_membership(nest_place::admin::MembershipChange::Remove { node: h.node })
        .await?;
    Ok(Json(json!({ "removed": h.name, "forgot_copies": forgot })))
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
    Ok(Json(
        json!({ "stores": api.placer.stores_listing().await? }),
    ))
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

/// Every host's stores: its live store and extra disks (ADR-073).
async fn disks(State(api): State<Api>) -> R<serde_json::Value> {
    let hosts: Vec<serde_json::Value> = api
        .placer
        .disks()
        .await?
        .into_iter()
        .map(|(h, r)| match r {
            Ok(d) => json!({ "host": h.name, "offgrid": h.offgrid, "disks": d, "error": null }),
            Err(e) => json!({ "host": h.name, "offgrid": h.offgrid, "disks": [], "error": e }),
        })
        .collect();
    Ok(Json(json!({ "hosts": hosts })))
}

#[derive(Deserialize)]
struct AddDiskReq {
    host: String,
    path: String,
    #[serde(default)]
    name: Option<String>,
}

async fn add_disk(State(api): State<Api>, Json(r): Json<AddDiskReq>) -> R<serde_json::Value> {
    require_every_host_upgraded(&api, "adding a disk").await?;
    let id = api
        .placer
        .add_disk(&r.host, &r.path, r.name.as_deref())
        .await?;
    let (_, name, _) = api.placer.find_disk(&id.0.to_string())?;
    Ok(Json(json!({ "store": id.0, "name": name })))
}

#[derive(Deserialize, Default)]
struct DrainReq {
    #[serde(default)]
    to: Option<String>,
}

async fn drain_disk(
    State(api): State<Api>,
    Path(name): Path<String>,
    body: Option<Json<DrainReq>>,
) -> R<serde_json::Value> {
    let to = body.and_then(|Json(b)| b.to);
    Ok(Json(
        json!({ "job": api.placer.drain_disk(&name, to.as_deref()).await? }),
    ))
}

#[derive(Deserialize)]
struct RemoveDiskQ {
    #[serde(default)]
    forget_copies: bool,
}

async fn remove_disk(
    State(api): State<Api>,
    Path(name): Path<String>,
    Query(q): Query<RemoveDiskQ>,
) -> R<serde_json::Value> {
    require_every_host_upgraded(&api, "removing a disk").await?;
    let forgot = api.placer.remove_disk(&name, q.forget_copies).await?;
    Ok(Json(json!({ "removed": name, "forgot": forgot })))
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
    let nodes = api.placer.places()?;
    let mut store_names: HashMap<StoreId, String> = nodes
        .iter()
        .map(|h| (StoreId(h.node.0), h.name.clone()))
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
        refs: hub_refs(&api).await,
    };
    let c = api.conn()?;
    let want = sel.describe();
    let tree = tokio::task::spawn_blocking(move || nest_place::space::tree(&c, &req))
        .await
        .map_err(|e| bad(e.to_string()))??;
    // The repo, or for repo@branch that branch's revision in it (a repo
    // with one revision is not split: it is that branch).
    let repo_sel = Selector::Hf {
        hub: api.hub.clone(),
        repo: repo.clone(),
        revision: None,
        repo_type: repo_type.clone(),
    }
    .describe();
    let node = tree
        .children
        .into_iter()
        .find(|n| n.selector.as_deref() == Some(repo_sel.as_str()))
        .unwrap_or_default();
    let node = if want == repo_sel {
        node
    } else {
        let split = node.children.iter().any(|c| c.kind == "revision");
        match node
            .children
            .iter()
            .find(|c| c.kind == "revision" && c.selector.as_deref() == Some(want.as_str()))
        {
            Some(r) => nest_place::space::TreeNode {
                name: node.name.clone(),
                path: node.path.clone(),
                kind: "repo".into(),
                selector: Some(want.clone()),
                ..r.clone()
            },
            None if !split => node,
            None => nest_place::space::TreeNode::default(),
        }
    };
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
    let all_refs = hub_refs(&api).await;
    // A repo with several named revisions cached (quant branches,
    // typically) is one row per branch, named and selected by it. Snapshots
    // no ref names are what updates left behind: they stay in the repo's
    // own row, and a repo with fewer than two named ones is not split.
    let mut rows = Vec::new();
    for (kind, repo, dir) in repos {
        let snaps: Vec<String> = {
            let c = api.conn()?;
            selector::resolve_path(&c, &format!("{hub}/{dir}/snapshots"))
                .ok()
                .and_then(|(id, _)| query::readdir(&c, id, 0, 1000).ok())
                .unwrap_or_default()
                .into_iter()
                .map(|(_, e)| String::from_utf8_lossy(&e.name).into_owned())
                .filter(|n| n != "." && n != "..")
                .collect()
        };
        let names = all_refs.get(&dir);
        let named: Vec<&Vec<String>> = snaps
            .iter()
            .filter_map(|s| names.and_then(|m| m.get(s)))
            .filter(|b| !b.is_empty())
            .collect();
        if named.len() < 2 {
            let branches = named.first().map(|b| b.to_vec()).unwrap_or_default();
            rows.push((kind, repo, None, branches, snaps.len()));
            continue;
        }
        for branches in named {
            rows.push((
                kind,
                repo.clone(),
                Some(branches[0].clone()),
                branches.clone(),
                snaps.len(),
            ));
        }
    }
    let mut out = Vec::new();
    for (kind, repo, branch, branches, revisions) in rows {
        let sel = Selector::Hf {
            hub: hub.clone(),
            repo: repo.clone(),
            revision: branch.clone(),
            repo_type: kind.to_string(),
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
                    // Set on a row that is one revision of a repo with
                    // several; `branches` names every ref pointing at it.
                    "branch": branch,
                    "branches": branches,
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
            Err(e) => out.push(json!({ "repo": repo, "kind": kind, "branch": branch, "selector": sel.describe(), "error": e.to_string() })),
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
    let nodes = api.placer.places()?;
    let archives = api.placer.archive_stores()?;
    let mut names: HashMap<StoreId, String> = nodes
        .iter()
        .map(|h| (StoreId(h.node.0), h.name.clone()))
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
        refs: if q.shape.as_deref() != Some("path") {
            hub_refs(&api).await
        } else {
            HashMap::new()
        },
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
    // Each host's extra disks, places of their own (ADR-073).
    for d in api.placer.disks_listing().await.into_values().flatten() {
        if !d.primary && d.healthy && scope.as_ref().is_none_or(|x| *x == d.name) {
            free.push(json!({ "name": d.name, "kind": "host", "free": d.free_bytes, "total": d.total_bytes, "held": d.object_bytes }));
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
pub(crate) struct LogsReq {
    /// One host; every node when absent.
    pub(crate) host: Option<String>,
    /// Least severe level shown: error, warn, info (default), debug.
    pub(crate) level: Option<String>,
    /// Case-insensitive text the message or target must contain.
    pub(crate) q: Option<String>,
    pub(crate) limit: Option<usize>,
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
                since_ms: None,
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
                .plan_in(
                    match r {
                        PlanReq::Goal(nest_place::plan::Goal::Consolidate {
                            selector,
                            host,
                            ..
                        }) => nest_place::plan::Goal::Consolidate {
                            selector: api.selector_string(&selector),
                            host,
                            hub: api.hub.clone(),
                        },
                        PlanReq::Goal(nest_place::plan::Goal::Place {
                            selector,
                            hosts,
                            factor,
                            by_speed,
                            ..
                        }) => nest_place::plan::Goal::Place {
                            selector: api.selector_string(&selector),
                            hosts,
                            factor,
                            by_speed,
                            hub: api.hub.clone(),
                        },
                        PlanReq::Goal(nest_place::plan::Goal::Offgrid {
                            selector,
                            hosts,
                            spread,
                            ..
                        }) => nest_place::plan::Goal::Offgrid {
                            selector: api.selector_string(&selector),
                            hosts,
                            spread,
                            hub: api.hub.clone(),
                        },
                        PlanReq::Goal(g) => g,
                        PlanReq::Free { free, archives } => {
                            nest_place::plan::Goal::Free { free, archives }
                        }
                    },
                    &api.hub,
                )
                .await?,
        )
        .unwrap_or_default(),
    ))
}

async fn get_plan(State(api): State<Api>, Path(id): Path<u64>) -> R<serde_json::Value> {
    Ok(Json(
        serde_json::to_value(api.placer.get_plan(id)?).unwrap_or_default(),
    ))
}

#[derive(Deserialize)]
struct SelectReq {
    /// One host's copies; every host's when absent.
    #[serde(default)]
    host: Option<String>,
    /// Models (org/name, hf:org/name), directories or files; every copy
    /// (on `host`) when empty.
    #[serde(default)]
    matches: Vec<String>,
    /// true takes copies back into the plan, false skips them.
    on: bool,
}

/// Edit a proposed plan: skip copies or take them back. Returns the plan
/// and how many copies changed.
async fn select_plan(
    State(api): State<Api>,
    Path(id): Path<u64>,
    Json(r): Json<SelectReq>,
) -> R<serde_json::Value> {
    let (plan, changed) = api.placer.select_plan(
        id,
        r.host.as_deref().filter(|h| !h.is_empty()),
        &r.matches,
        r.on,
    )?;
    Ok(Json(json!({ "plan": plan, "changed": changed })))
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

/// Cluster-wide settings (ADR-056): every known one with its value,
/// default, unit and whether it was set.
async fn settings(State(api): State<Api>) -> R<serde_json::Value> {
    let set: std::collections::HashMap<String, u64> = api
        .vfs
        .data()
        .with_reader(nest_meta::query::settings)
        .map_err(|e| NestError::Io(format!("metadata: {e}")))?
        .into_iter()
        .collect();
    let names: HashMap<u64, String> = api
        .placer
        .nodes()
        .map(|n| n.into_iter().map(|h| (h.node.0, h.name)).collect())
        .unwrap_or_default();
    let all: Vec<_> = nest_types::Setting::ALL
        .iter()
        .map(|s| {
            let (min, max) = s.range();
            let value = set.get(s.name()).copied().unwrap_or(s.default_value());
            json!({
                "name": s.name(),
                "value": value,
                // A host-valued setting: the host's name (none when 0).
                "host": s.is_host().then(|| names.get(&value).cloned()).flatten(),
                "default": s.default_value(),
                "set": set.contains_key(s.name()),
                "unit": s.unit(),
                "min": min,
                "max": max,
                "about": s.about(),
            })
        })
        .collect();
    Ok(Json(json!({ "settings": all })))
}

#[derive(Deserialize)]
struct SettingReq {
    value: SettingValue,
}

/// A number in the setting's unit, or for a host-valued setting
/// (`preferred-leader`) a host name, stored as its node id.
#[derive(Deserialize)]
#[serde(untagged)]
enum SettingValue {
    Number(u64),
    Host(String),
}

fn setting_value(api: &Api, s: nest_types::Setting, v: SettingValue) -> Result<u64, ApiError> {
    match v {
        SettingValue::Host(h) if s.is_host() && h.parse::<u64>().is_err() => {
            let hosts: Vec<(u64, String)> = api
                .placer
                .nodes()?
                .into_iter()
                .map(|h| (h.node.0, h.name))
                .collect();
            host_value(s, SettingValue::Host(h), &hosts)
        }
        v => host_value(s, v, &[]),
    }
}

/// A setting's value from a request: a number, or for a host-valued
/// setting a host's name (or `node<id>`) among `hosts`.
fn host_value(
    s: nest_types::Setting,
    v: SettingValue,
    hosts: &[(u64, String)],
) -> Result<u64, ApiError> {
    match v {
        SettingValue::Number(n) => Ok(n),
        SettingValue::Host(h) if s.is_host() => {
            if let Ok(n) = h.parse() {
                return Ok(n);
            }
            hosts
                .iter()
                .find(|(id, name)| *name == h || format!("node{id}") == h)
                .map(|(id, _)| *id)
                .ok_or_else(|| bad(format!("unknown host {h:?}")))
        }
        SettingValue::Host(h) => Err(bad(format!(
            "{} takes a number ({}), not {h:?}",
            s.name(),
            s.unit()
        ))),
    }
}

#[cfg(test)]
mod setting_tests {
    use super::{SettingValue, host_value};
    use nest_types::Setting;

    fn parse(v: serde_json::Value) -> SettingValue {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn a_host_setting_takes_a_name_or_an_id() {
        let hosts = vec![(1, "raptor".to_string()), (4, "kiwi".to_string())];
        let s = Setting::PreferredLeader;
        let v = |j| host_value(s, parse(j), &hosts).map_err(|e| e.1);
        assert_eq!(v(serde_json::json!("raptor")), Ok(1));
        assert_eq!(v(serde_json::json!("node4")), Ok(4));
        assert_eq!(v(serde_json::json!("4")), Ok(4));
        assert_eq!(v(serde_json::json!(7)), Ok(7));
        assert!(v(serde_json::json!("@all")).is_err());
        assert!(
            v(serde_json::json!("emu"))
                .unwrap_err()
                .contains("unknown host")
        );
        let w = host_value(
            Setting::ReadRecoveryWait,
            parse(serde_json::json!("raptor")),
            &hosts,
        );
        assert!(w.is_err());
        assert_eq!(
            host_value(
                Setting::ReadRecoveryWait,
                parse(serde_json::json!(5000)),
                &[]
            )
            .map_err(|e| e.1),
            Ok(5000)
        );
    }
}

fn known_setting(name: &str) -> Result<nest_types::Setting, ApiError> {
    nest_types::Setting::parse(name).ok_or_else(|| {
        let names: Vec<_> = nest_types::Setting::ALL.iter().map(|s| s.name()).collect();
        ApiError(
            StatusCode::NOT_FOUND,
            format!("no setting {name:?}; known: {}", names.join(", ")),
        )
    })
}

async fn put_setting(
    api: &Api,
    s: nest_types::Setting,
    value: Option<u64>,
) -> Result<(), ApiError> {
    require_every_host_upgraded(api, "changing a cluster setting").await?;
    api.vfs
        .data()
        .meta()
        .propose(nest_meta::Command::SetSetting {
            name: s.name().into(),
            value,
        })
        .await?;
    Ok(())
}

async fn set_setting(
    State(api): State<Api>,
    Path(name): Path<String>,
    Json(r): Json<SettingReq>,
) -> R<serde_json::Value> {
    let s = known_setting(&name)?;
    let value = setting_value(&api, s, r.value)?;
    let (min, max) = s.range();
    if !(min..=max).contains(&value) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("{name} must be {min}..={max} {}", s.unit()),
        ));
    }
    if s.is_host() && value != 0 {
        let voter = api
            .vfs
            .data()
            .meta()
            .metrics()
            .membership_config
            .membership()
            .voter_ids()
            .any(|v| v == value);
        if !voter {
            return Err(bad(format!(
                "node {value} is not a voter; only a voter can lead"
            )));
        }
    }
    // 0 for a host means none: back to the default.
    let stored = (!(s.is_host() && value == 0)).then_some(value);
    put_setting(&api, s, stored).await?;
    Ok(Json(json!({ "name": name, "value": value })))
}

async fn reset_setting(State(api): State<Api>, Path(name): Path<String>) -> R<serde_json::Value> {
    let s = known_setting(&name)?;
    put_setting(&api, s, None).await?;
    Ok(Json(json!({ "name": name, "value": s.default_value() })))
}

#[cfg(test)]
mod version_tests {
    #[test]
    fn semver_orders_releases() {
        use super::semver;
        assert!(semver("0.4.1") > semver("0.4.0"));
        assert!(semver("0.10.0") > semver("0.9.9"));
        assert!(semver("1.0.0") >= semver("1.0.0-rc1"));
        assert_eq!(semver("junk"), (0, 0, 0));
    }
}
