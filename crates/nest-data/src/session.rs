//! Session lifecycle and orphan release.
//!
//! Each daemon incarnation holds one session. Handles and orphan holds
//! belong to it; when it expires (the node restarted or was cut off), the
//! cluster releases everything it held.

use crate::DataNode;
use nest_meta::{Command, Reply, query};
use nest_types::{NestError, Timestamp};
use std::sync::{Arc, Weak};
use std::time::Duration;

/// How often a node renews its session.
pub const RENEW_EVERY: Duration = Duration::from_secs(5);
/// A session not renewed for this long is expired by the leader.
pub const SESSION_TTL: Duration = Duration::from_secs(30);
/// Orphan releases are batched over this window.
const RELEASE_BATCH: Duration = Duration::from_millis(20);

pub(crate) fn spawn(d: Arc<DataNode>) {
    tokio::spawn(session_loop(Arc::downgrade(&d)));
    tokio::spawn(release_loop(Arc::downgrade(&d)));
    tokio::spawn(expiry_loop(Arc::downgrade(&d)));
}

async fn sleep_or_stop(d: &Weak<DataNode>, t: Duration) -> bool {
    let Some(mut stop) = d.upgrade().map(|d| d.stopping.subscribe()) else {
        return false;
    };
    tokio::select! {
        _ = tokio::time::sleep(t) => !*stop.borrow(),
        _ = stop.changed() => false,
    }
}

async fn session_loop(w: Weak<DataNode>) {
    loop {
        let Some(d) = w.upgrade() else { return };
        if *d.stopping.borrow() {
            return;
        }
        let meta = d.meta().clone();
        let current = d.session();
        let pause = match current {
            None => match meta
                .propose(Command::OpenSession {
                    node: d.id,
                    now: Timestamp::now(),
                })
                .await
            {
                Ok(Reply::Session(s)) => {
                    tracing::info!(session = %s, "session opened");
                    d.st.lock().session = Some(s);
                    RENEW_EVERY
                }
                other => {
                    tracing::warn!(?other, "opening session failed; retrying");
                    Duration::from_millis(500)
                }
            },
            Some(s) => match meta
                .propose(Command::RenewSession {
                    session: s,
                    now: Timestamp::now(),
                })
                .await
            {
                Ok(_) => RENEW_EVERY,
                Err(NestError::NotFound) => {
                    let mut st = d.st.lock();
                    if st.session == Some(s) {
                        st.session = None;
                    }
                    Duration::ZERO
                }
                Err(e) => {
                    tracing::warn!(error = %e, "renewing session failed");
                    Duration::from_secs(1)
                }
            },
        };
        drop(d);
        if !sleep_or_stop(&w, pause).await {
            return;
        }
    }
}

async fn release_loop(w: Weak<DataNode>) {
    loop {
        let Some(d) = w.upgrade() else { return };
        let notified = d.release_notify.notified();
        let (session, files) = {
            let mut st = d.st.lock();
            (st.session, std::mem::take(&mut st.release))
        };
        if files.is_empty() {
            // Holding the Arc while waiting is fine: shutdown wakes us.
            let mut stop = d.stopping.subscribe();
            tokio::select! {
                _ = notified => {}
                _ = stop.changed() => return,
            }
            drop(d);
            if !sleep_or_stop(&w, RELEASE_BATCH).await {
                return;
            }
            continue;
        }
        // Without a session there is nothing to release: expiry already
        // released everything the old session held.
        if let Some(session) = session
            && let Err(e) = d
                .meta()
                .propose(Command::ReleaseOrphans {
                    session,
                    files: files.clone(),
                })
                .await
        {
            tracing::warn!(error = %e, "releasing orphans failed; will retry");
            d.st.lock().release.extend(files);
            if !sleep_or_stop(&w, Duration::from_millis(500)).await {
                return;
            }
        }
    }
}

/// Leader duty: expire sessions whose node stopped renewing.
async fn expiry_loop(w: Weak<DataNode>) {
    loop {
        if !sleep_or_stop(&w, RENEW_EVERY).await {
            return;
        }
        let Some(d) = w.upgrade() else { return };
        let meta = d.meta().clone();
        if meta.leader() != Some(d.id) {
            continue;
        }
        let Ok(sessions) = meta.open_reader().and_then(|c| query::sessions(&c)) else {
            continue;
        };
        let cutoff = Timestamp::now().0 - SESSION_TTL.as_nanos() as i64;
        for s in sessions.into_iter().filter(|s| s.renewed.0 < cutoff) {
            tracing::info!(session = %s.id, node = %s.node, "expiring stale session");
            let _ = meta.propose(Command::ExpireSession { session: s.id }).await;
        }
    }
}
