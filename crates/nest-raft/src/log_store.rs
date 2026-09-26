//! Raft log in `raft.sqlite`.

use crate::{LogId, StorageError, TypeConfig};
use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{Entry, RaftLogId, StorageIOError, Vote};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params};
use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct LogStore {
    db: Arc<Mutex<Connection>>,
}

fn io<E: std::error::Error + 'static>(
    f: fn(openraft::AnyError) -> StorageIOError<u64>,
) -> impl Fn(E) -> StorageError {
    move |e| f(openraft::AnyError::new(&e)).into()
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

impl LogStore {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let c = Connection::open(path)?;
        c.pragma_update(None, "journal_mode", "WAL")?;
        c.pragma_update(None, "synchronous", "FULL")?;
        c.execute_batch(
            "CREATE TABLE IF NOT EXISTS log (idx INTEGER PRIMARY KEY, entry BLOB NOT NULL);
             CREATE TABLE IF NOT EXISTS state (k TEXT PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;",
        )?;
        let found: Option<u32> = Self::get_state(&c, "format")?;
        let has_state: bool = c.query_row(
            "SELECT EXISTS (SELECT 1 FROM log) OR EXISTS (SELECT 1 FROM state)",
            [],
            |r| r.get(0),
        )?;
        match found {
            Some(v) if v == nest_meta::FORMAT_VERSION => {}
            None if !has_state => Self::put_state(&c, "format", &nest_meta::FORMAT_VERSION)?,
            other => {
                return Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISMATCH),
                    Some(format!(
                        "{} holds on-disk format {} but this build uses {}: run a matching version, or wipe this node's state and let it rejoin",
                        path.display(),
                        other
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "0 (unversioned)".into()),
                        nest_meta::FORMAT_VERSION
                    )),
                ));
            }
        }
        Ok(LogStore {
            db: Arc::new(Mutex::new(c)),
        })
    }

    /// The commit index persisted by the previous run, if any. Entries up
    /// to it are re-applied at startup.
    pub fn persisted_committed(&self) -> rusqlite::Result<Option<u64>> {
        let c = self.db.lock();
        Ok(Self::get_state::<Option<LogId>>(&c, "committed")?
            .flatten()
            .map(|l| l.index))
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> rusqlite::Result<T> + Send + 'static,
        err: fn(openraft::AnyError) -> StorageIOError<u64>,
    ) -> Result<T, StorageError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&mut db.lock()))
            .await
            .expect("log store task panicked")
            .map_err(io(err))
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
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError> {
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
        self.blocking(
            move |c| {
                let mut st = c.prepare_cached(
                    "SELECT entry FROM log WHERE idx >= ?1 AND idx < ?2 ORDER BY idx",
                )?;
                let rows = st.query_map(params![start, end], |r| r.get::<_, Vec<u8>>(0))?;
                rows.map(|b| dec(&b?)).collect()
            },
            StorageIOError::read_logs,
        )
        .await
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        self.blocking(
            |c| {
                let last_purged: Option<LogId> = Self::get_state(c, "last_purged")?;
                let last: Option<Vec<u8>> = c
                    .prepare_cached("SELECT entry FROM log ORDER BY idx DESC LIMIT 1")?
                    .query_row([], |r| r.get(0))
                    .optional()?;
                let last_log_id = match last {
                    Some(b) => Some(*dec::<Entry<TypeConfig>>(&b)?.get_log_id()),
                    None => last_purged,
                };
                Ok(LogState {
                    last_purged_log_id: last_purged,
                    last_log_id,
                })
            },
            StorageIOError::read_logs,
        )
        .await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError> {
        let v = *vote;
        self.blocking(
            move |c| Self::put_state(c, "vote", &v),
            StorageIOError::write_vote,
        )
        .await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError> {
        self.blocking(|c| Self::get_state(c, "vote"), StorageIOError::read_vote)
            .await
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        self.blocking(
            move |c| Self::put_state(c, "committed", &committed),
            StorageIOError::write,
        )
        .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        self.blocking(
            |c| Ok(Self::get_state::<Option<LogId>>(c, "committed")?.flatten()),
            StorageIOError::read,
        )
        .await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let rows: Vec<(i64, Vec<u8>)> = entries
            .into_iter()
            .map(|e| (e.get_log_id().index as i64, enc(&e)))
            .collect();
        let res = self
            .blocking(
                move |c| {
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
                },
                StorageIOError::write_logs,
            )
            .await;
        match &res {
            Ok(()) => callback.log_io_completed(Ok(())),
            Err(e) => callback.log_io_completed(Err(std::io::Error::other(e.to_string()))),
        }
        res
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let idx = log_id.index as i64;
        self.blocking(
            move |c| {
                c.execute("DELETE FROM log WHERE idx >= ?1", params![idx])
                    .map(|_| ())
            },
            StorageIOError::write_logs,
        )
        .await
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        self.blocking(
            move |c| {
                let tx = c.transaction()?;
                Self::put_state(&tx, "last_purged", &log_id)?;
                tx.execute(
                    "DELETE FROM log WHERE idx <= ?1",
                    params![log_id.index as i64],
                )?;
                tx.commit()
            },
            StorageIOError::write_logs,
        )
        .await
    }
}
