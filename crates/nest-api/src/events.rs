//! Live updates for the web UI over one WebSocket per page (`GET /v1/ws`),
//! instead of polling. A WebSocket, not Server-Sent Events: over HTTP/1.1
//! a browser allows six connections per server across all its tabs, and an
//! event stream holds one for good, so a few open tabs starved every
//! request; WebSockets do not count against that limit.
//!
//! Server to page (JSON text frames, `type` first):
//! - `state`: status, jobs, archive stores and groups (`data`), sent when
//!   it differs from the last one (checked every second), computed once per
//!   node for all pages and only while one is connected;
//! - `changed`: the namespace or placement changed (debounced): views
//!   showing files, models or space refetch;
//! - `lines`: new log lines, while the page subscribes.
//!
//! - `reply`: the answer to a call: `id`, HTTP `status`, JSON `body`.
//!
//! Page to server: `{"type":"call", id, method, path, body?}` runs any API
//! request through the same router the HTTP endpoints use (concurrently,
//! so a slow call never holds up others); `{"type":"logs", host?, level?,
//! q?, limit?}` follows logs (recent lines first); `{"type":"logs_off"}`
//! stops. Only the page itself and file uploads and downloads (streamed
//! bodies) use plain HTTP.

use crate::Api;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{broadcast, watch};

pub struct Events {
    state: watch::Sender<Option<Arc<str>>>,
    changed: broadcast::Sender<()>,
    publishing: AtomicBool,
    tapped: OnceLock<()>,
}

impl Default for Events {
    fn default() -> Self {
        Events {
            state: watch::channel(None).0,
            changed: broadcast::channel(64).0,
            publishing: AtomicBool::new(false),
            tapped: OnceLock::new(),
        }
    }
}

/// What the UI's shell shows everywhere.
async fn snapshot(api: &Api) -> Option<String> {
    let status = crate::status_json(api).await.ok()?;
    let stores = api.placer.stores_listing().await.unwrap_or_default();
    let groups: Vec<_> = api
        .placer
        .groups()
        .unwrap_or_default()
        .into_iter()
        .map(|(n, m)| json!({ "name": n, "members": m }))
        .collect();
    Some(
        json!({
            "status": status,
            "jobs": api.placer.jobs(),
            "stores": stores,
            "groups": groups,
        })
        .to_string(),
    )
}

/// Publish snapshots while anyone subscribes; stop a while after the last
/// one leaves.
fn ensure_publisher(api: &Api) {
    let ev = api.events.clone();
    ev.tapped.get_or_init(|| {
        let tx = ev.changed.clone();
        // Runs in the metadata apply path: a send, nothing more.
        api.vfs.data().add_tap(Arc::new(move |_i, _e| {
            let _ = tx.send(());
        }));
    });
    if ev.publishing.swap(true, Ordering::AcqRel) {
        return;
    }
    let api = api.clone();
    tokio::spawn(async move {
        let ev = api.events.clone();
        let mut idle = 0;
        loop {
            if ev.state.receiver_count() == 0 {
                idle += 1;
                if idle > 10 {
                    break;
                }
            } else {
                idle = 0;
                if let Some(s) = snapshot(&api).await {
                    ev.state.send_if_modified(|cur| {
                        if cur.as_deref() == Some(s.as_str()) {
                            false
                        } else {
                            *cur = Some(s.into());
                            true
                        }
                    });
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        ev.publishing.store(false, Ordering::Release);
        // A subscriber may have arrived as we stopped.
        if ev.state.receiver_count() > 0 {
            ensure_publisher(&api);
        }
    });
}

type Out = tokio::sync::mpsc::Sender<String>;

pub(crate) async fn ws(State(api): State<Api>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| session(api, socket))
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum FromPage {
    Call {
        id: u64,
        method: String,
        path: String,
        #[serde(default)]
        body: Option<serde_json::Value>,
    },
    Logs {
        host: Option<String>,
        level: Option<String>,
        q: Option<String>,
        limit: Option<usize>,
    },
    LogsOff,
}

async fn session(api: Api, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(64);
    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(Duration::from_secs(20));
        loop {
            tokio::select! {
                m = rx.recv() => match m {
                    Some(m) => if sink.send(Message::Text(m.into())).await.is_err() { break },
                    None => break,
                },
                // Keeps idle connections (and proxies) alive; a dead peer
                // fails the send and ends the session.
                _ = ping.tick() => if sink.send(Message::Ping(Vec::new().into())).await.is_err() { break },
            }
        }
    });
    let pusher = tokio::spawn(push_state(api.clone(), tx.clone()));
    // Calls run through the API router itself (the socket is authenticated).
    let router = crate::router(api.clone());
    let mut logs: Option<tokio::task::JoinHandle<()>> = None;
    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Text(t) => match serde_json::from_str::<FromPage>(&t) {
                Ok(FromPage::Call {
                    id,
                    method,
                    path,
                    body,
                }) => {
                    let (router, tx) = (router.clone(), tx.clone());
                    tokio::spawn(async move {
                        let (status, body) = call(router, &method, &path, body).await;
                        let m =
                            json!({ "type": "reply", "id": id, "status": status, "body": body });
                        let _ = tx.send(m.to_string()).await;
                    });
                }
                Ok(FromPage::Logs {
                    host,
                    level,
                    q,
                    limit,
                }) => {
                    if let Some(h) = logs.take() {
                        h.abort();
                    }
                    let q = nest_place::logs::LogQuery {
                        level,
                        contains: q.filter(|q| !q.is_empty()),
                        after: None,
                        since_ms: None,
                        limit: Some(limit.unwrap_or(500).min(5000)),
                    };
                    logs = Some(tokio::spawn(follow_logs(
                        api.clone(),
                        host.filter(|h| !h.is_empty()),
                        q,
                        tx.clone(),
                    )));
                }
                Ok(FromPage::LogsOff) => {
                    if let Some(h) = logs.take() {
                        h.abort();
                    }
                }
                Err(_) => {}
            },
            Message::Close(_) => break,
            _ => {}
        }
    }
    pusher.abort();
    if let Some(h) = logs {
        h.abort();
    }
    writer.abort();
}

/// `state` when it changes, `changed` (debounced) when the namespace does.
async fn push_state(api: Api, tx: Out) {
    let mut state_rx = api.events.state.subscribe();
    let mut changed_rx = api.events.changed.subscribe();
    ensure_publisher(&api);
    let state = |s: &str| format!(r#"{{"type":"state","data":{s}}}"#);
    let first = state_rx.borrow_and_update().clone();
    if let Some(s) = first
        && tx.send(state(&s)).await.is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            r = state_rx.changed() => {
                if r.is_err() { return; }
                let s = state_rx.borrow_and_update().clone();
                if let Some(s) = s
                    && tx.send(state(&s)).await.is_err()
                {
                    return;
                }
            }
            r = changed_rx.recv() => {
                if matches!(r, Err(broadcast::error::RecvError::Closed)) { return; }
                // Debounce: a burst of commits is one notice.
                tokio::time::sleep(Duration::from_millis(700)).await;
                while changed_rx.try_recv().is_ok() {}
                if tx.send(r#"{"type":"changed"}"#.to_string()).await.is_err() {
                    return;
                }
            }
        }
    }
}

fn line_json(host: &str, l: &nest_place::logs::LogLine) -> serde_json::Value {
    json!({ "host": host, "seq": l.seq, "ts_ms": l.ts_ms, "level": l.level, "target": l.target, "message": l.message })
}

/// The recent lines, then each node's new ones as they are logged. seq
/// restarts when a node does, so following goes by (time, seq).
async fn follow_logs(api: Api, host: Option<String>, q: nest_place::logs::LogQuery, tx: Out) {
    let start = nest_data::usage::now_ms();
    let mut last: std::collections::HashMap<String, (u64, u64)> = Default::default();
    let mut first = true;
    loop {
        let since: std::collections::HashMap<String, u64> = if first {
            Default::default()
        } else {
            api.placer
                .nodes()
                .unwrap_or_default()
                .into_iter()
                .map(|h| {
                    let t = last.get(&h.name).map_or(start, |x| x.0);
                    (h.name, t)
                })
                .collect()
        };
        if let Ok((lines, _)) = api
            .placer
            .logs_since(host.as_deref(), q.clone(), &since)
            .await
        {
            let fresh: Vec<_> = lines
                .into_iter()
                .filter(|(h, l)| {
                    last.get(h)
                        .is_none_or(|&(t, s)| l.ts_ms > t || (l.ts_ms == t && l.seq > s))
                })
                .collect();
            for (h, l) in &fresh {
                let e = last.entry(h.clone()).or_insert((0, 0));
                if (l.ts_ms, l.seq) > *e {
                    *e = (l.ts_ms, l.seq);
                }
            }
            if first || !fresh.is_empty() {
                let body: Vec<_> = fresh.iter().map(|(h, l)| line_json(h, l)).collect();
                let m = json!({ "type": "lines", "lines": body }).to_string();
                if tx.send(m).await.is_err() {
                    return;
                }
            }
        }
        first = false;
        tokio::time::sleep(Duration::from_millis(700)).await;
        if tx.is_closed() {
            return;
        }
    }
}

/// One API request through the router; (HTTP status, JSON body).
async fn call(
    router: axum::Router,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    use tower::ServiceExt;
    if !path.starts_with("/v1/") {
        return (400, json!({ "error": "not an API path" }));
    }
    let req = axum::http::Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            body.map(|b| b.to_string()).unwrap_or_default(),
        ));
    let req = match req {
        Ok(r) => r,
        Err(e) => return (400, json!({ "error": e.to_string() })),
    };
    let resp = match router.oneshot(req).await {
        Ok(r) => r,
        Err(e) => return (500, json!({ "error": e.to_string() })),
    };
    let status = resp.status().as_u16();
    let bytes = axum::body::to_bytes(resp.into_body(), 256 << 20)
        .await
        .unwrap_or_default();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "error": String::from_utf8_lossy(&bytes) }));
    (status, body)
}
