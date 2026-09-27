//! Per-host file usage (ADR-028): when this host last opened each file and
//! how many bytes its reads took from the local store, from other hosts,
//! or from an archive. Kept in `usage.sqlite` next to the objects, never in
//! the replicated metadata: counting reads must not cost a Raft commit, and
//! the numbers are only meaningful per host. Keyed by file id, so a rename
//! keeps its history and a new file under an old name starts afresh (a
//! re-downloaded blob is new content). Daily rows; days older than
//! `KEEP_DAYS` are pruned.
//!
//! Readers count bytes on their open handle (one relaxed atomic add); the
//! VFS folds handle counters in here every few seconds and on close, and a
//! blocking task writes the batch.

use nest_types::FileId;
use parking_lot::Mutex;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub const KEEP_DAYS: u64 = 30;
const DAY_MS: u64 = 86_400_000;

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// What one host did with one file over a window.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileUsage {
    pub file: FileId,
    pub opens: u64,
    /// Unix milliseconds; 0 if never opened in the window.
    pub last_open_ms: u64,
    pub local_bytes: u64,
    /// Read from other hosts over the fabric or TCP.
    pub remote_bytes: u64,
    /// Read from an archive store this host is a gateway for.
    pub archive_bytes: u64,
}

impl FileUsage {
    fn merge(&mut self, o: &FileUsage) {
        self.opens += o.opens;
        self.last_open_ms = self.last_open_ms.max(o.last_open_ms);
        self.local_bytes += o.local_bytes;
        self.remote_bytes += o.remote_bytes;
        self.archive_bytes += o.archive_bytes;
    }
}

/// Where read bytes came from.
#[derive(Clone, Copy, Debug)]
pub enum Source {
    Local,
    Remote,
    Archive,
}

pub struct Usage {
    path: PathBuf,
    /// Not yet written, keyed by (day, file).
    pending: Mutex<HashMap<(u64, FileId), FileUsage>>,
    last_prune_day: Mutex<u64>,
}

fn open_db(path: &Path) -> rusqlite::Result<Connection> {
    let c = Connection::open(path)?;
    c.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = OFF;
         PRAGMA busy_timeout = 2000;
         CREATE TABLE IF NOT EXISTS usage_day (
             day INTEGER NOT NULL,
             file INTEGER NOT NULL,
             opens INTEGER NOT NULL,
             last_open_ms INTEGER NOT NULL,
             local_bytes INTEGER NOT NULL,
             remote_bytes INTEGER NOT NULL,
             archive_bytes INTEGER NOT NULL,
             PRIMARY KEY (day, file)
         ) WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS usage_file ON usage_day(file);",
    )?;
    Ok(c)
}

impl Usage {
    /// Statistics are advisory: a database that cannot be opened is logged
    /// and every write becomes a no-op rather than failing the node.
    pub fn open(dir: &Path) -> Usage {
        let path = dir.join("usage.sqlite");
        if let Err(e) = open_db(&path) {
            tracing::warn!(path = %path.display(), error = %e, "usage statistics disabled");
        }
        Usage {
            path,
            pending: Mutex::new(HashMap::new()),
            last_prune_day: Mutex::new(0),
        }
    }

    pub fn record_open(&self, file: FileId) {
        let now = now_ms();
        let mut p = self.pending.lock();
        let u = p.entry((now / DAY_MS, file)).or_insert_with(|| FileUsage {
            file,
            ..Default::default()
        });
        u.opens += 1;
        u.last_open_ms = now;
    }

    pub fn add_bytes(&self, file: FileId, local: u64, remote: u64, archive: u64) {
        if local == 0 && remote == 0 && archive == 0 {
            return;
        }
        let day = now_ms() / DAY_MS;
        let mut p = self.pending.lock();
        let u = p.entry((day, file)).or_insert_with(|| FileUsage {
            file,
            ..Default::default()
        });
        u.local_bytes += local;
        u.remote_bytes += remote;
        u.archive_bytes += archive;
    }

    /// Write what has accumulated (blocking: call from a blocking pool).
    pub fn flush(&self) -> rusqlite::Result<()> {
        let batch: Vec<_> = self.pending.lock().drain().collect();
        let today = now_ms() / DAY_MS;
        let prune = {
            let mut last = self.last_prune_day.lock();
            let due = *last != today;
            *last = today;
            due
        };
        if batch.is_empty() && !prune {
            return Ok(());
        }
        let mut c = open_db(&self.path)?;
        let tx = c.transaction()?;
        {
            let mut st = tx.prepare_cached(
                "INSERT INTO usage_day (day, file, opens, last_open_ms, local_bytes, remote_bytes, archive_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT (day, file) DO UPDATE SET
                   opens = opens + excluded.opens,
                   last_open_ms = max(last_open_ms, excluded.last_open_ms),
                   local_bytes = local_bytes + excluded.local_bytes,
                   remote_bytes = remote_bytes + excluded.remote_bytes,
                   archive_bytes = archive_bytes + excluded.archive_bytes",
            )?;
            for ((day, file), u) in &batch {
                st.execute(params![
                    *day as i64,
                    file.0 as i64,
                    u.opens as i64,
                    u.last_open_ms as i64,
                    u.local_bytes as i64,
                    u.remote_bytes as i64,
                    u.archive_bytes as i64
                ])?;
            }
        }
        if prune {
            tx.execute(
                "DELETE FROM usage_day WHERE day < ?1",
                params![today.saturating_sub(KEEP_DAYS) as i64],
            )?;
        }
        tx.commit()
    }

    /// Per-file totals since `since_ms` (whole days), including what has
    /// not been written yet.
    pub fn summary(&self, since_ms: u64) -> rusqlite::Result<Vec<FileUsage>> {
        let since_day = since_ms / DAY_MS;
        let mut out: HashMap<FileId, FileUsage> = HashMap::new();
        let c = open_db(&self.path)?;
        let mut st = c.prepare(
            "SELECT file, sum(opens), max(last_open_ms), sum(local_bytes), sum(remote_bytes), sum(archive_bytes)
             FROM usage_day WHERE day >= ?1 GROUP BY file",
        )?;
        let rows = st.query_map(params![since_day as i64], |r| {
            Ok(FileUsage {
                file: FileId(r.get::<_, i64>(0)? as u64),
                opens: r.get::<_, i64>(1)? as u64,
                last_open_ms: r.get::<_, i64>(2)? as u64,
                local_bytes: r.get::<_, i64>(3)? as u64,
                remote_bytes: r.get::<_, i64>(4)? as u64,
                archive_bytes: r.get::<_, i64>(5)? as u64,
            })
        })?;
        for u in rows {
            let u = u?;
            out.insert(u.file, u);
        }
        for ((day, file), u) in self.pending.lock().iter() {
            if *day >= since_day {
                out.entry(*file)
                    .or_insert_with(|| FileUsage {
                        file: *file,
                        ..Default::default()
                    })
                    .merge(u);
            }
        }
        let mut v: Vec<_> = out.into_values().collect();
        v.sort_by_key(|u| u.file);
        Ok(v)
    }

    /// For tests: pretend `file` was used `days_ago`.
    pub fn backdate(&self, file: FileId, days_ago: u64, u: FileUsage) -> rusqlite::Result<()> {
        let day = (now_ms() / DAY_MS).saturating_sub(days_ago);
        self.pending
            .lock()
            .insert((day, file), FileUsage { file, ..u });
        self.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_merges_pending_and_prunes() {
        let dir = std::env::temp_dir().join(format!("nest-usage-{}", rand_u32()));
        std::fs::create_dir_all(&dir).unwrap();
        let u = Usage::open(&dir);
        let f = FileId(7);
        u.record_open(f);
        u.add_bytes(f, 10, 20, 0);
        u.flush().unwrap();
        u.record_open(f);
        u.add_bytes(f, 1, 2, 3);
        // Pending and written rows add up.
        let s = u.summary(0).unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(
            (
                s[0].opens,
                s[0].local_bytes,
                s[0].remote_bytes,
                s[0].archive_bytes
            ),
            (2, 11, 22, 3)
        );
        // A row older than the window is left out, and pruned on the next
        // day's first flush.
        u.backdate(
            FileId(8),
            KEEP_DAYS + 5,
            FileUsage {
                opens: 1,
                last_open_ms: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let week = now_ms() - 7 * DAY_MS;
        assert!(u.summary(week).unwrap().iter().all(|x| x.file == f));
        *u.last_prune_day.lock() = 0;
        u.flush().unwrap();
        assert!(u.summary(0).unwrap().iter().all(|x| x.file == f));
        std::fs::remove_dir_all(&dir).ok();
    }

    fn rand_u32() -> u32 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    }
}
