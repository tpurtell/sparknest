//! Raft state machine over `meta.sqlite`.

use crate::{Entry, LogId, RAFT_FORMAT, Request, Response, TypeConfig};
use futures::{Stream, TryStreamExt};
use nest_meta::{Effect, Reply};
use openraft::alias::{SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};
use parking_lot::Mutex;
use rusqlite::{Connection, MAIN_DB, OptionalExtension, params};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A snapshot is a complete `meta.sqlite` copy on disk. The network moves
/// the file itself in chunks (`network::full_snapshot`).
#[derive(Clone, Debug)]
pub struct SnapshotFile(pub PathBuf);

type Meta = SnapshotMetaOf<TypeConfig>;
type Snapshot = SnapshotOf<TypeConfig, SnapshotFile>;
type Membership = StoredMembershipOf<TypeConfig>;

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

fn sm_err(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}

#[derive(Clone)]
struct CurrentSnapshot {
    meta: Meta,
    path: PathBuf,
}

pub struct StateMachine {
    db: Arc<Mutex<Connection>>,
    meta_path: PathBuf,
    snap_dir: PathBuf,
    handler: Arc<dyn EffectHandler>,
    current: Arc<Mutex<Option<CurrentSnapshot>>>,
    _ckpt: crate::checkpoint::Checkpointer,
}

type Applied = (Option<LogId>, Membership);

fn read_applied(c: &Connection) -> rusqlite::Result<Applied> {
    let get = |k: &str| -> rusqlite::Result<Option<Vec<u8>>> {
        c.prepare_cached("SELECT v FROM sm_state WHERE k = ?1")?
            .query_row(params![k], |r| r.get(0))
            .optional()
    };
    fn dec<T: serde::de::DeserializeOwned>(b: Vec<u8>) -> rusqlite::Result<T> {
        postcard::from_bytes(&b).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(e))
        })
    }
    let last: Option<LogId> = get("last_applied")?.map(dec).transpose()?.flatten();
    let mem: Membership = get("membership")?.map(dec).transpose()?.unwrap_or_default();
    Ok((last, mem))
}

/// The Raft format of the openraft values in `sm_state`, if recorded.
/// A database with applied state but no record was written by format 1.
pub fn sm_format(c: &Connection) -> rusqlite::Result<Option<u32>> {
    let v: Option<Vec<u8>> = c
        .query_row("SELECT v FROM sm_state WHERE k = 'raft_format'", [], |r| {
            r.get(0)
        })
        .optional()?;
    if let Some(b) = v {
        return Ok(postcard::from_bytes(&b).ok());
    }
    let applied: bool = c.query_row(
        "SELECT EXISTS (SELECT 1 FROM sm_state WHERE k = 'last_applied')",
        [],
        |r| r.get(0),
    )?;
    Ok(if applied { Some(1) } else { None })
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
        c.pragma_update(None, "wal_autocheckpoint", 0)?;
        c.execute_batch(SM_SCHEMA)?;
        match sm_format(&c)? {
            None => {
                c.execute(
                    "INSERT OR REPLACE INTO sm_state (k, v) VALUES ('raft_format', ?1)",
                    params![enc(&RAFT_FORMAT)],
                )?;
            }
            Some(v) if v == RAFT_FORMAT => {}
            Some(v) => {
                return Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISMATCH),
                    Some(format!(
                        "{} holds Raft format {v} state but this build uses {RAFT_FORMAT}",
                        meta_path.display()
                    )),
                ));
            }
        }
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
                if let Ok(Some((meta, _))) = snapshot_meta_of(p) {
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
        let ckpt = crate::checkpoint::start(&meta_path, crate::checkpoint::INTERVAL);
        Ok(StateMachine {
            db: Arc::new(Mutex::new(c)),
            meta_path,
            snap_dir,
            handler,
            current: Arc::new(Mutex::new(current)),
            _ckpt: ckpt,
        })
    }

    pub fn meta_path(&self) -> &Path {
        &self.meta_path
    }

    /// Where incoming snapshots are written before installation.
    pub fn snap_dir(&self) -> &Path {
        &self.snap_dir
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

/// Read the snapshot meta and id embedded in a snapshot database file.
fn snapshot_meta_of(path: &Path) -> rusqlite::Result<Option<(Meta, String)>> {
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
    Ok(Some((
        Meta {
            last_log_id,
            last_membership,
        },
        snapshot_id,
    )))
}

fn apply_entries(
    c: &mut Connection,
    entries: Vec<Entry>,
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
                let sm = Membership::new(Some(e.log_id), m);
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
    Ok((responses, effects, last.map(|l| l.index()).unwrap_or(0)))
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotData = SnapshotFile;
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> io::Result<Applied> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || read_applied(&db.lock()))
            .await
            .map_err(io::Error::other)?
            .map_err(sm_err)
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let mut batch = Vec::new();
        let mut responders = Vec::new();
        while let Some((entry, responder)) = entries.try_next().await? {
            batch.push(entry);
            responders.push(responder);
        }
        if batch.is_empty() {
            return Ok(());
        }
        let db = self.db.clone();
        let (responses, effects, index) =
            tokio::task::spawn_blocking(move || apply_entries(&mut db.lock(), batch))
                .await
                .map_err(io::Error::other)?
                .map_err(sm_err)?;
        self.handler.on_event(&SmEvent::Applied { index, effects });
        for (responder, resp) in responders.into_iter().zip(responses) {
            if let Some(r) = responder {
                r.send(resp);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            meta_path: self.meta_path.clone(),
            snap_dir: self.snap_dir.clone(),
            current: self.current.clone(),
        }
    }

    async fn install_snapshot(&mut self, meta: &Meta, snapshot: SnapshotFile) -> io::Result<()> {
        let incoming = snapshot.0;
        let db = self.db.clone();
        let snap_dir = self.snap_dir.clone();
        let want = meta.last_log_id;
        let final_path = tokio::task::spawn_blocking(move || -> rusqlite::Result<PathBuf> {
            let Some((embedded, id)) = snapshot_meta_of(&incoming)? else {
                return Err(rusqlite::Error::InvalidQuery);
            };
            if embedded.last_log_id != want {
                return Err(rusqlite::Error::InvalidQuery);
            }
            let mut c = db.lock();
            c.restore(MAIN_DB, &incoming, None::<fn(rusqlite::backup::Progress)>)?;
            c.execute_batch(SM_SCHEMA)?;
            let fp = snap_dir.join(format!("snapshot-{id}.sqlite"));
            std::fs::rename(&incoming, &fp).map_err(|e| {
                rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                    Some(e.to_string()),
                )
            })?;
            Ok(fp)
        })
        .await
        .map_err(io::Error::other)?
        .map_err(sm_err)?;
        let old = self.current.lock().replace(CurrentSnapshot {
            meta: meta.clone(),
            path: final_path.clone(),
        });
        if let Some(old) = old
            && old.path != final_path
        {
            let _ = std::fs::remove_file(old.path);
        }
        let index = meta.last_log_id.map(|l| l.index()).unwrap_or(0);
        self.handler.on_event(&SmEvent::Resync { index });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> io::Result<Option<Snapshot>> {
        let cur = self.current.lock().clone();
        Ok(cur.map(|c| Snapshot {
            meta: c.meta,
            snapshot: SnapshotFile(c.path),
        }))
    }
}

pub struct SnapshotBuilder {
    meta_path: PathBuf,
    snap_dir: PathBuf,
    current: Arc<Mutex<Option<CurrentSnapshot>>>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    type SnapshotData = SnapshotFile;

    async fn build_snapshot(&mut self) -> io::Result<Snapshot> {
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
                last.map(|l| l.committed_leader_id().term).unwrap_or(0),
                last.map(|l| l.index()).unwrap_or(0),
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
            let (meta, _) = snapshot_meta_of(&final_path)?.expect("snapshot id was just written");
            Ok((meta, final_path))
        })
        .await
        .map_err(io::Error::other)?
        .map_err(sm_err)?;
        let old = self.current.lock().replace(CurrentSnapshot {
            meta: meta.clone(),
            path: path.clone(),
        });
        if let Some(old) = old {
            let _ = std::fs::remove_file(old.path);
        }
        Ok(Snapshot {
            meta,
            snapshot: SnapshotFile(path),
        })
    }
}
