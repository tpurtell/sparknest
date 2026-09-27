//! WAL checkpointing on our own schedule (ADR-026).
//!
//! Both databases run in WAL mode with `synchronous=NORMAL` and SQLite's
//! automatic checkpoints off, so a commit costs no fsync. A checkpoint
//! fsyncs the WAL before copying it into the database, so the interval
//! between checkpoints is how much can be lost if every host loses power
//! at once. One thread per database runs a passive checkpoint (it never
//! blocks writers) about once a second when the WAL has grown. Writers set
//! `journal_size_limit` so the WAL file is cut back when it restarts.

use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

/// Default time between checkpoints while there are pending changes.
pub const INTERVAL: Duration = Duration::from_secs(1);
/// Size the WAL file is cut back to when it restarts (`journal_size_limit`).
pub const WAL_LIMIT: i64 = 8 << 20;

/// Keeps the checkpoint thread alive; the thread stops once every clone is
/// dropped.
#[derive(Clone)]
pub struct Checkpointer(#[allow(dead_code)] Arc<()>);

fn wal_len(db: &Path) -> u64 {
    let mut wal = db.as_os_str().to_owned();
    wal.push("-wal");
    std::fs::metadata(PathBuf::from(wal))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Start checkpointing `db` (which must already be in WAL mode).
pub fn start(db: &Path, interval: Duration) -> Checkpointer {
    let token = Arc::new(());
    let weak: Weak<()> = Arc::downgrade(&token);
    let path = db.to_path_buf();
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let _ = std::thread::Builder::new()
        .name(format!("ckpt-{name}"))
        .spawn(move || {
            let mut last = wal_len(&path);
            let step = Duration::from_millis(50);
            'run: loop {
                let mut waited = Duration::ZERO;
                while waited < interval {
                    std::thread::sleep(step);
                    waited += step;
                    if weak.upgrade().is_none() {
                        break 'run;
                    }
                }
                let len = wal_len(&path);
                if len == 0 || len == last {
                    continue;
                }
                // Opened per checkpoint: holding a connection would pin the
                // file after its owner closes it (and confuse SQLite if a
                // wiped database is recreated in this process).
                let Ok(c) = Connection::open(&path) else {
                    continue;
                };
                let _ = c.busy_timeout(Duration::from_millis(200));
                // PASSIVE only: TRUNCATE/RESTART take the write lock, and a
                // writer's read-to-write upgrade then fails with SQLITE_BUSY.
                if let Err(e) = c.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(())) {
                    tracing::debug!(db = %path.display(), error = %e, "checkpoint deferred");
                }
                drop(c);
                last = wal_len(&path);
            }
        });
    Checkpointer(token)
}

/// Make everything committed to `db` so far durable now: fsync its WAL.
/// Used for the metadata barrier behind an application's fsync.
pub fn sync_wal(db: &Path) -> std::io::Result<()> {
    let mut wal = db.as_os_str().to_owned();
    wal.push("-wal");
    match std::fs::File::open(PathBuf::from(wal)) {
        Ok(f) => f.sync_data(),
        // No WAL: everything is already in the (synced) database file.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
