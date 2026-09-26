//! Raft state machine over `meta.sqlite`.

use crate::{LogId, Request, Response, StorageError, TypeConfig};
use nest_meta::{Effect, Reply};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, RaftSnapshotBuilder, SnapshotMeta, StorageIOError,
    StoredMembership,
};
use parking_lot::Mutex;
use rusqlite::{Connection, MAIN_DB, OptionalExtension, params};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// What local services learn from the state machine.
#[derive(Clone, Debug)]
pub enum SmEvent {
    /// Entries up to `index` were applied; `effects` in order.
    Applied { index: u64, effects: Vec<Effect> },
    /// A snapshot replaced the state wholesale. Per-command effects between
    /// the old and new state were skipped: reconcile everything.
    Resync { index: u64 },
}

/// Receives state-machine events synchronously, after the transaction
/// commits and before the entries count as applied. Implementations must
/// not block: record state, enqueue work, return.
pub trait EffectHandler: Send + Sync + 'static {
    fn on_event(&self, ev: &SmEvent);
}

impl<F: Fn(&SmEvent) + Send + Sync + 'static> EffectHandler for F {
    fn on_event(&self, ev: &SmEvent) {
        self(ev)
    }
}

/// Requests remembered per client for deduplication are pruned once this
/// many log entries old. A retry older than this is a bug in the proposer.
const DEDUP_WINDOW: i64 = 200_000;

const SM_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS sm_state (k TEXT PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS sm_dedup (
    client INTEGER NOT NULL, seq INTEGER NOT NULL, idx INTEGER NOT NULL, response BLOB NOT NULL,
    PRIMARY KEY (client, seq)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS sm_dedup_idx ON sm_dedup(idx);
";

fn enc<T: serde::Serialize>(v: &T) -> Vec<u8> {
    postcard::to_stdvec(v).expect("state machine types always encode")
}

fn sm_err(e: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::write_state_machine(openraft::AnyError::new(&e)).into()
}

fn snap_err(e: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::write_snapshot(None, openraft::AnyError::new(&e)).into()
}

#[derive(Clone)]
struct CurrentSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    path: PathBuf,
}

pub struct StateMachine {
    db: Arc<Mutex<Connection>>,
    meta_path: PathBuf,
    snap_dir: PathBuf,
    handler: Arc<dyn EffectHandler>,
    current: Arc<Mutex<Option<CurrentSnapshot>>>,
    receiving: Option<PathBuf>,
}

type Applied = (Option<LogId>, StoredMembership<u64, BasicNode>);

fn read_applied(c: &Connection) -> rusqlite::Result<Applied> {
    let get = |k: &str| -> rusqlite::Result<Option<Vec<u8>>> {
        c.prepare_cached("SELECT v FROM sm_state WHERE k = ?1")?
            .query_row(params![k], |r| r.get(0))
            .optional()
    };
    let last = get("last_applied")?
        .and_then(|b| postcard::from_bytes(&b).ok())
        .flatten();
    let mem = get("membership")?
        .and_then(|b| postcard::from_bytes(&b).ok())
        .unwrap_or_default();
    Ok((last, mem))
}

impl StateMachine {
    pub fn open(dir: &Path, handler: Arc<dyn EffectHandler>) -> rusqlite::Result<Self> {
        let meta_path = dir.join("meta.sqlite");
        let snap_dir = dir.join("snapshots");
        std::fs::create_dir_all(&snap_dir).map_err(|e| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
                Some(e.to_string()),
            )
        })?;
        let c = nest_meta::open_write(&meta_path)?;
        c.execute_batch(SM_SCHEMA)?;
        // Pick up the newest local snapshot so it can be served to lagging peers.
        let mut current = None;
        if let Ok(rd) = std::fs::read_dir(&snap_dir) {
            let mut snaps: Vec<PathBuf> = rd
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    p.file_name()
                        .is_some_and(|n| n.to_string_lossy().starts_with("snapshot-"))
                })
                .collect();
            snaps.sort();
            for p in &snaps {
                if let Ok(Some(meta)) = snapshot_meta_of(p) {
                    current = Some(CurrentSnapshot {
                        meta,
                        path: p.clone(),
                    });
                }
            }
            for p in rd_leftovers(&snap_dir) {
                let _ = std::fs::remove_file(p);
            }
        }
        Ok(StateMachine {
            db: Arc::new(Mutex::new(c)),
            meta_path,
            snap_dir,
            handler,
            current: Arc::new(Mutex::new(current)),
            receiving: None,
        })
    }

    pub fn meta_path(&self) -> &Path {
        &self.meta_path
    }
}

/// Incomplete incoming snapshots from a previous run.
fn rd_leftovers(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    p.file_name()
                        .is_some_and(|n| n.to_string_lossy().starts_with("incoming-"))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Read the snapshot meta embedded in a snapshot database file.
fn snapshot_meta_of(path: &Path) -> rusqlite::Result<Option<SnapshotMeta<u64, BasicNode>>> {
    let c = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let id: Option<String> = c
        .query_row("SELECT v FROM sm_state WHERE k = 'snapshot_id'", [], |r| {
            r.get::<_, Vec<u8>>(0)
        })
        .optional()?
        .map(|b| String::from_utf8_lossy(&b).into_owned());
    let Some(snapshot_id) = id else {
        return Ok(None);
    };
    let (last_log_id, last_membership) = read_applied(&c)?;
    Ok(Some(SnapshotMeta {
        last_log_id,
        last_membership,
        snapshot_id,
    }))
}

fn apply_entries(
    c: &mut Connection,
    entries: Vec<Entry<TypeConfig>>,
) -> rusqlite::Result<(Vec<Response>, Vec<Effect>, u64)> {
    let tx = c.transaction()?;
    let mut responses = Vec::with_capacity(entries.len());
    let mut effects = Vec::new();
    let mut last = None;
    for e in entries {
        let idx = e.log_id.index as i64;
        last = Some(e.log_id);
        match e.payload {
            EntryPayload::Blank => responses.push(Response(Ok(Reply::Done))),
            EntryPayload::Membership(m) => {
                let sm = StoredMembership::new(Some(e.log_id), m);
                tx.prepare_cached(
                    "INSERT OR REPLACE INTO sm_state (k, v) VALUES ('membership', ?1)",
                )?
                .execute(params![enc(&sm)])?;
                responses.push(Response(Ok(Reply::Done)));
            }
            EntryPayload::Normal(Request { client, seq, cmd }) => {
                let seen: Option<Vec<u8>> = tx
                    .prepare_cached("SELECT response FROM sm_dedup WHERE client = ?1 AND seq = ?2")?
                    .query_row(params![client as i64, seq as i64], |r| r.get(0))
                    .optional()?;
                if let Some(b) = seen {
                    let r = postcard::from_bytes(&b).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Blob,
                            Box::new(e),
                        )
                    })?;
                    responses.push(r);
                    continue;
                }
                let (result, fx) = nest_meta::apply(&tx, &cmd)?;
                let resp = Response(result);
                tx.prepare_cached(
                    "INSERT INTO sm_dedup (client, seq, idx, response) VALUES (?1, ?2, ?3, ?4)",
                )?
                .execute(params![client as i64, seq as i64, idx, enc(&resp)])?;
                effects.extend(fx);
                responses.push(resp);
            }
        }
        if idx % 1024 == 0 {
            tx.prepare_cached("DELETE FROM sm_dedup WHERE idx < ?1")?
                .execute(params![idx - DEDUP_WINDOW])?;
        }
    }
    tx.prepare_cached("INSERT OR REPLACE INTO sm_state (k, v) VALUES ('last_applied', ?1)")?
        .execute(params![enc(&last)])?;
    tx.commit()?;
    Ok((responses, effects, last.map(|l| l.index).unwrap_or(0)))
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> Result<Applied, StorageError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || read_applied(&db.lock()))
            .await
            .expect("state machine task panicked")
            .map_err(sm_err)
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let db = self.db.clone();
        let (responses, effects, index) =
            tokio::task::spawn_blocking(move || apply_entries(&mut db.lock(), entries))
                .await
                .expect("state machine task panicked")
                .map_err(sm_err)?;
        self.handler.on_event(&SmEvent::Applied { index, effects });
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            meta_path: self.meta_path.clone(),
            snap_dir: self.snap_dir.clone(),
            current: self.current.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<tokio::fs::File>, StorageError> {
        let path = self
            .snap_dir
            .join(format!("incoming-{:016x}.sqlite", rand::random::<u64>()));
        let f = tokio::fs::File::create(&path).await.map_err(snap_err)?;
        self.receiving = Some(path);
        Ok(Box::new(f))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StorageError> {
        use tokio::io::AsyncWriteExt;
        let mut f = *snapshot;
        f.flush().await.map_err(snap_err)?;
        f.sync_all().await.map_err(snap_err)?;
        drop(f);
        let incoming = self.receiving.take().ok_or_else(|| {
            snap_err(std::io::Error::other(
                "install_snapshot without begin_receiving_snapshot",
            ))
        })?;
        let final_path = self
            .snap_dir
            .join(format!("snapshot-{}.sqlite", meta.snapshot_id));
        let db = self.db.clone();
        let meta2 = meta.clone();
        let fp = final_path.clone();
        tokio::task::spawn_blocking(move || -> rusqlite::Result<()> {
            let embedded = snapshot_meta_of(&incoming)?;
            if embedded.as_ref().map(|m| &m.snapshot_id) != Some(&meta2.snapshot_id) {
                return Err(rusqlite::Error::InvalidQuery);
            }
            let mut c = db.lock();
            c.restore(MAIN_DB, &incoming, None::<fn(rusqlite::backup::Progress)>)?;
            c.execute_batch(SM_SCHEMA)?;
            std::fs::rename(&incoming, &fp).map_err(|e| {
                rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                    Some(e.to_string()),
                )
            })?;
            Ok(())
        })
        .await
        .expect("state machine task panicked")
        .map_err(snap_err)?;
        let old = self.current.lock().replace(CurrentSnapshot {
            meta: meta.clone(),
            path: final_path.clone(),
        });
        if let Some(old) = old
            && old.path != final_path
        {
            let _ = std::fs::remove_file(old.path);
        }
        let index = meta.last_log_id.map(|l| l.index).unwrap_or(0);
        self.handler.on_event(&SmEvent::Resync { index });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError> {
        let cur = self.current.lock().clone();
        let Some(cur) = cur else { return Ok(None) };
        let f = tokio::fs::File::open(&cur.path).await.map_err(snap_err)?;
        Ok(Some(Snapshot {
            meta: cur.meta,
            snapshot: Box::new(f),
        }))
    }
}

pub struct SnapshotBuilder {
    meta_path: PathBuf,
    snap_dir: PathBuf,
    current: Arc<Mutex<Option<CurrentSnapshot>>>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError> {
        let meta_path = self.meta_path.clone();
        let snap_dir = self.snap_dir.clone();
        let (meta, path) = tokio::task::spawn_blocking(move || -> rusqlite::Result<_> {
            let tmp = snap_dir.join(format!(
                "incoming-build-{:016x}.sqlite",
                rand::random::<u64>()
            ));
            // VACUUM INTO takes its own consistent read snapshot, so the
            // copy and the last_applied recorded inside it agree.
            let src = nest_meta::open_read(&meta_path)?;
            src.execute("VACUUM INTO ?1", params![tmp.to_string_lossy()])?;
            drop(src);
            let c = Connection::open(&tmp)?;
            let (last, _) = read_applied(&c)?;
            let id = format!(
                "{}-{}-{:08x}",
                last.map(|l| l.leader_id.term).unwrap_or(0),
                last.map(|l| l.index).unwrap_or(0),
                rand::random::<u32>()
            );
            c.execute(
                "INSERT OR REPLACE INTO sm_state (k, v) VALUES ('snapshot_id', ?1)",
                params![id.as_bytes()],
            )?;
            // Persist WAL mode in the header so a restore keeps the live
            // database in WAL mode.
            c.pragma_update(None, "journal_mode", "WAL")?;
            drop(c);
            let final_path = snap_dir.join(format!("snapshot-{id}.sqlite"));
            std::fs::rename(&tmp, &final_path).map_err(|e| {
                rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                    Some(e.to_string()),
                )
            })?;
            let meta = snapshot_meta_of(&final_path)?.expect("snapshot id was just written");
            Ok((meta, final_path))
        })
        .await
        .expect("snapshot task panicked")
        .map_err(snap_err)?;
        let old = self.current.lock().replace(CurrentSnapshot {
            meta: meta.clone(),
            path: path.clone(),
        });
        if let Some(old) = old {
            let _ = std::fs::remove_file(old.path);
        }
        let f = tokio::fs::File::open(&path).await.map_err(snap_err)?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(f),
        })
    }
}
