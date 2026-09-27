//! Live updates for the web UI over Server-Sent Events (`GET /v1/events`),
//! instead of each browser polling several endpoints.
//!
//! - `state`: status, jobs, archive stores and groups as one JSON document,
//!   sent when it differs from the last one (checked every second). It is
//!   computed once per node for every subscriber, and only while someone
//!   listens.
//! - `changed`: the namespace or placement changed (debounced), so views
//!   showing files, models or space refetch what they show.

use crate::Api;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use serde_json::json;
use std::convert::Infallible;
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

pub(crate) async fn events(
    State(api): State<Api>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let mut state_rx = api.events.state.subscribe();
    let mut changed_rx = api.events.changed.subscribe();
    ensure_publisher(&api);
    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(16);
    tokio::spawn(async move {
        // The current state at once, if there is one.
        let first = state_rx.borrow_and_update().clone();
        if let Some(s) = first
            && tx
                .send(Event::default().event("state").data(&*s))
                .await
                .is_err()
        {
            return;
        }
        loop {
            tokio::select! {
                r = state_rx.changed() => {
                    if r.is_err() { return; }
                    let s = state_rx.borrow_and_update().clone();
                    if let Some(s) = s
                        && tx.send(Event::default().event("state").data(&*s)).await.is_err()
                    {
                        return;
                    }
                }
                r = changed_rx.recv() => {
                    if matches!(r, Err(broadcast::error::RecvError::Closed)) { return; }
                    // Debounce: a burst of commits is one notice.
                    tokio::time::sleep(Duration::from_millis(700)).await;
                    while changed_rx.try_recv().is_ok() {}
                    if tx.send(Event::default().event("changed").data("")).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    let stream =
        futures::stream::unfold(
            rx,
            |mut rx| async move { rx.recv().await.map(|e| (Ok(e), rx)) },
        );
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

fn line_json(host: &str, l: &nest_place::logs::LogLine) -> serde_json::Value {
    json!({ "host": host, "seq": l.seq, "ts_ms": l.ts_ms, "level": l.level, "target": l.target, "message": l.message })
}

/// `GET /v1/logs/stream`: the recent lines (as `/v1/logs`), then new ones
/// as the nodes log them, as `lines` events (JSON arrays).
pub(crate) async fn log_stream(
    State(api): State<Api>,
    axum::extract::Query(r): axum::extract::Query<crate::LogsReq>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(16);
    tokio::spawn(async move {
        let host = r.host.filter(|h| !h.is_empty());
        let q = nest_place::logs::LogQuery {
            level: r.level,
            contains: r.q.filter(|q| !q.is_empty()),
            after: None,
            since_ms: None,
            limit: Some(r.limit.unwrap_or(500).min(5000)),
        };
        let start = nest_data::usage::now_ms();
        // Per host: the newest (time, seq) sent. seq restarts with a node,
        // so following goes by time and skips only what was already sent.
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
                    let ev = Event::default()
                        .event("lines")
                        .data(serde_json::Value::from(body).to_string());
                    if tx.send(ev).await.is_err() {
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
    });
    let stream =
        futures::stream::unfold(
            rx,
            |mut rx| async move { rx.recv().await.map(|e| (Ok(e), rx)) },
        );
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}
