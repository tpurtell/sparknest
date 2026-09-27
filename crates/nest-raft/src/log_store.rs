//! Raft log in `raft.sqlite` (openraft 0.10, ADR-026).
//!
//! WAL mode with `synchronous=NORMAL`: an append is readable and reported
//! complete as soon as its transaction commits, without an fsync. Durability
//! comes from our checkpoint thread (about once a second), from
//! [`LogStore::sync`] (the barrier behind an application's fsync), and from
//! votes, which are always fsynced: a host that forgot a vote could vote
//! twice in one term. The commit index is stored with the same relaxed
//! durability; openraft allows it to lag.

use crate::{Entry, LogId, RAFT_FORMAT, TypeConfig, Vote};
use openraft::OptionalSend;
use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params};
use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct LogStore {
    db: Arc<Mutex<Connection>>,
    path: PathBuf,
    _ckpt: crate::checkpoint::Checkpointer,
}

fn sql(e: rusqlite::Error) -> io::Error {
    io::Error::other(e)
}

/// A decoding failure surfaces as a storage error (never a panic): the
/// node stops with a clear message instead of crashing mid-apply.
fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> rusqlite::Result<T> {
    postcard::from_bytes(b).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(e))
    })
}

fn enc<T: serde::Serialize>(v: &T) -> Vec<u8> {
    postcard::to_stdvec(v).expect("raft types always encode")
}

/// What `raft.sqlite` holds, as seen before opening it for Raft.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Found {
    /// No Raft state yet.
    Empty,
    /// This build's format.
    Current,
    /// Another format (an older or newer build wrote it).
    Other(u32),
}

impl LogStore {
    /// Inspect `raft.sqlite` without changing it.
    pub fn probe(path: &Path) -> rusqlite::Result<Found> {
        if !path.exists() {
            return Ok(Found::Empty);
        }
        let c = Connection::open(path)?;
        let has_tables: bool = c.query_row(
            "SELECT count(*) = 2 FROM sqlite_master WHERE type = 'table' AND name IN ('log', 'state')",
            [],
            |r| r.get(0),
        )?;
        if !has_tables {
            return Ok(Found::Empty);
        }
        // Format 1 (openraft 0.9) stored it under "format".
        let v2: Option<u32> = Self::get_state(&c, "raft_format")?;
        let v1: Option<u32> = Self::get_state(&c, "format")?;
        let has_state: bool = c.query_row(
            "SELECT EXISTS (SELECT 1 FROM log) OR EXISTS (SELECT 1 FROM state)",
            [],
            |r| r.get(0),
        )?;
        Ok(match (v2, v1) {
            (Some(v), _) if v == RAFT_FORMAT => Found::Current,
            (Some(v), _) => Found::Other(v),
            (None, Some(v)) => Found::Other(v),
            (None, None) if !has_state => Found::Empty,
            (None, None) => Found::Other(0),
        })
    }

    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        match Self::probe(path)? {
            Found::Empty | Found::Current => {}
            Found::Other(v) => {
                return Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISMATCH),
                    Some(format!(
                        "{} holds Raft format {v} but this build uses {RAFT_FORMAT}",
                        path.display()
                    )),
                ));
            }
        }
        let c = Connection::open(path)?;
        c.pragma_update(None, "journal_mode", "WAL")?;
        c.pragma_update(None, "synchronous", "NORMAL")?;
        c.pragma_update(None, "wal_autocheckpoint", 0)?;
        c.execute_batch(
            "CREATE TABLE IF NOT EXISTS log (idx INTEGER PRIMARY KEY, entry BLOB NOT NULL);
             CREATE TABLE IF NOT EXISTS state (k TEXT PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;",
        )?;
        if Self::get_state::<u32>(&c, "raft_format")?.is_none() {
            Self::put_state(&c, "raft_format", &RAFT_FORMAT)?;
        }
        let ckpt = crate::checkpoint::start(path, crate::checkpoint::INTERVAL);
        Ok(LogStore {
            db: Arc::new(Mutex::new(c)),
            path: path.to_path_buf(),
            _ckpt: ckpt,
        })
    }

    /// The commit index recorded by the previous run, if any. Entries up
    /// to it are re-applied at startup. After a power loss it may lag.
    pub fn persisted_committed(&self) -> rusqlite::Result<Option<u64>> {
        let c = self.db.lock();
        Ok(Self::get_state::<Option<LogId>>(&c, "committed")?
            .flatten()
            .map(|l| l.index()))
    }

    /// The last entry in the log, if any.
    pub fn last_index(&self) -> rusqlite::Result<Option<u64>> {
        let c = self.db.lock();
        c.query_row("SELECT max(idx) FROM log", [], |r| {
            r.get::<_, Option<i64>>(0)
        })
        .map(|v| v.map(|i| i as u64))
    }

    /// The id of the entry at `index`, if the log holds it.
    pub fn log_id_at(&self, index: u64) -> rusqlite::Result<Option<LogId>> {
        let c = self.db.lock();
        let b: Option<Vec<u8>> = c
            .prepare_cached("SELECT entry FROM log WHERE idx = ?1")?
            .query_row(params![index as i64], |r| r.get(0))
            .optional()?;
        Ok(b.map(|b| dec::<Entry>(&b)).transpose()?.map(|e| e.log_id))
    }

    /// Make every entry appended so far durable (fsync the WAL).
    pub async fn sync(&self) -> io::Result<()> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || crate::checkpoint::sync_wal(&path))
            .await
            .map_err(io::Error::other)?
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> rusqlite::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&mut db.lock()))
            .await
            .map_err(io::Error::other)?
            .map_err(sql)
    }

    fn get_state<T: serde::de::DeserializeOwned>(
        c: &Connection,
        k: &str,
    ) -> rusqlite::Result<Option<T>> {
        let v: Option<Vec<u8>> = c
            .prepare_cached("SELECT v FROM state WHERE k = ?1")?
            .query_row(params![k], |r| r.get(0))
            .optional()?;
        v.map(|b| dec(&b)).transpose()
    }

    fn put_state<T: serde::Serialize>(c: &Connection, k: &str, v: &T) -> rusqlite::Result<()> {
        c.prepare_cached("INSERT OR REPLACE INTO state (k, v) VALUES (?1, ?2)")?
            .execute(params![k, enc(v)])?;
        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<Entry>> {
        let start = match range.start_bound() {
            Bound::Included(s) => *s as i64,
            Bound::Excluded(s) => *s as i64 + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(e) => *e as i64 + 1,
            Bound::Excluded(e) => *e as i64,
            Bound::Unbounded => i64::MAX,
        };
        self.blocking(move |c| {
            let mut st = c.prepare_cached(
                "SELECT entry FROM log WHERE idx >= ?1 AND idx < ?2 ORDER BY idx",
            )?;
            let rows = st.query_map(params![start, end], |r| r.get::<_, Vec<u8>>(0))?;
            rows.map(|b| dec(&b?)).collect()
        })
        .await
    }

    async fn read_vote(&mut self) -> io::Result<Option<Vote>> {
        self.blocking(|c| Self::get_state(c, "vote")).await
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> io::Result<LogState<TypeConfig>> {
        self.blocking(|c| {
            let last_purged: Option<LogId> = Self::get_state(c, "last_purged")?;
            let last: Option<Vec<u8>> = c
                .prepare_cached("SELECT entry FROM log ORDER BY idx DESC LIMIT 1")?
                .query_row([], |r| r.get(0))
                .optional()?;
            let last_log_id = match last {
                Some(b) => Some(dec::<Entry>(&b)?.log_id),
                None => last_purged,
            };
            Ok(LogState {
                last_purged_log_id: last_purged,
                last_log_id,
            })
        })
        .await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> io::Result<()> {
        let v = *vote;
        self.blocking(move |c| {
            // The one durable write: fsync this commit (and everything
            // before it in the WAL).
            c.pragma_update(None, "synchronous", "FULL")?;
            let r = Self::put_state(c, "vote", &v);
            c.pragma_update(None, "synchronous", "NORMAL")?;
            r
        })
        .await
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> io::Result<()> {
        self.blocking(move |c| Self::put_state(c, "committed", &committed))
            .await
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogId>> {
        self.blocking(|c| Ok(Self::get_state::<Option<LogId>>(c, "committed")?.flatten()))
            .await
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> io::Result<()>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let rows: Vec<(i64, Vec<u8>)> = entries
            .into_iter()
            .map(|e| (e.index() as i64, enc(&e)))
            .collect();
        let res = self
            .blocking(move |c| {
                let tx = c.transaction()?;
                {
                    let mut st = tx.prepare_cached(
                        "INSERT OR REPLACE INTO log (idx, entry) VALUES (?1, ?2)",
                    )?;
                    for (i, b) in &rows {
                        st.execute(params![i, b])?;
                    }
                }
                tx.commit()
            })
            .await;
        // Committed to the WAL: readable now, and durable by our policy
        // (the checkpoint thread or a barrier fsyncs it; ADR-026).
        match &res {
            Ok(()) => callback.io_completed(Ok(())),
            Err(e) => callback.io_completed(Err(io::Error::other(e.to_string()))),
        }
        res
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> io::Result<()> {
        let from = last_log_id.map(|l| l.index() as i64 + 1).unwrap_or(0);
        self.blocking(move |c| {
            c.execute("DELETE FROM log WHERE idx >= ?1", params![from])
                .map(|_| ())
        })
        .await
    }

    async fn purge(&mut self, log_id: LogId) -> io::Result<()> {
        self.blocking(move |c| {
            let tx = c.transaction()?;
            Self::put_state(&tx, "last_purged", &log_id)?;
            tx.execute(
                "DELETE FROM log WHERE idx <= ?1",
                params![log_id.index() as i64],
            )?;
            tx.commit()
        })
        .await
    }
}
