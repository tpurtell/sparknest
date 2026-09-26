//! Backups (ADR-021): retained, versioned recovery records of a selection,
//! kept in an archive store and independent of live replicas.
//!
//! Layout inside the archive store's root:
//! ```text
//! backups/objects/<xx>/<file>.<gen>   content, shared by every backup that
//!                                     captured that exact generation
//! backups/manifests/<id>.sqlite       one per backup: paths, kinds, ids,
//!                                     generations, sizes, modes, targets
//! meta/meta-<unix>-<index>.sqlite     metadata snapshots
//! ```
//! A generation's bytes never change, so sharing objects across backups by
//! (file, generation) is exact without hashing (ADR-008). Later writes and
//! deletes in the live namespace never touch any of this.

use nest_data::Vfs;
use nest_meta::{Command, Reply, query};
use nest_store::{ObjectKey, ObjectStore};
use nest_types::{
    FileAttr, FileId, FileKind, GenState, Generation, NestError, NestResult, StoreId, Timestamp,
};
use parking_lot::Mutex;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::admin::JobProgress;
use crate::selector::resolve_path;

fn sql(e: rusqlite::Error) -> NestError {
    NestError::Io(format!("metadata: {e}"))
}

fn io(e: std::io::Error) -> NestError {
    NestError::from_io(&e)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub path: String,
    pub kind: FileKind,
    pub file: FileId,
    pub generation: Generation,
    pub size: u64,
    pub perm: u32,
    pub mtime: Timestamp,
    pub sealed: bool,
    pub stable: bool,
    pub target: Option<Vec<u8>>,
}

/// Every directory, symlink and regular file under `root`, plus whatever
/// its symlinks reach elsewhere in the namespace (recorded at their own
/// paths), each once.
pub fn walk_structure(c: &Connection, root: &str) -> NestResult<Vec<Entry>> {
    let (id, _) = resolve_path(c, root)?;
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut stack = vec![(id, root.trim_end_matches('/').to_string())];
    let entry = |a: &FileAttr, path: String, target: Option<Vec<u8>>| Entry {
        path,
        kind: a.kind,
        file: a.id,
        generation: a.generation,
        size: a.size,
        perm: a.perm,
        mtime: a.mtime,
        sealed: a.sealed,
        stable: a.gen_state == GenState::Stable,
        target,
    };
    while let Some((id, path)) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let Some(a) = query::getattr(c, id).map_err(sql)? else {
            continue;
        };
        match a.kind {
            FileKind::Regular => out.push(entry(&a, path, None)),
            FileKind::Directory => {
                out.push(entry(&a, path.clone(), None));
                let mut after = 0;
                loop {
                    let batch = query::readdir(c, id, after, 1024).map_err(sql)?;
                    if batch.is_empty() {
                        break;
                    }
                    for (cookie, e) in batch {
                        after = cookie;
                        stack.push((e.id, format!("{path}/{}", String::from_utf8_lossy(&e.name))));
                    }
                }
            }
            FileKind::Symlink => {
                let t = query::readlink(c, id).map_err(sql)?.unwrap_or_default();
                out.push(entry(&a, path.clone(), Some(t.clone())));
                let t = String::from_utf8_lossy(&t).into_owned();
                let dir = path.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
                let target = if t.starts_with('/') {
                    t
                } else {
                    format!("{dir}/{t}")
                };
                if let Ok((tid, _)) = resolve_path(c, &target)
                    && let Some(tp) = query::path_of(c, tid).map_err(sql)?
                {
                    stack.push((tid, String::from_utf8_lossy(&tp).into_owned()));
                }
            }
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// The deepest directory containing every entry.
fn common_base(entries: &[Entry]) -> String {
    let mut base: Option<Vec<&str>> = None;
    for e in entries {
        let dir: Vec<&str> = e
            .path
            .rsplit_once('/')
            .map(|(d, _)| d)
            .unwrap_or("")
            .split('/')
            .collect();
        base = Some(match base {
            None => dir,
            Some(b) => b
                .iter()
                .zip(&dir)
                .take_while(|(x, y)| x == y)
                .map(|(x, _)| *x)
                .collect(),
        });
    }
    let b = base.unwrap_or_default().join("/");
    if b.is_empty() { "/".into() } else { b }
}

pub struct BackupArea {
    pub root: PathBuf,
    pub objects: Arc<ObjectStore>,
}

impl BackupArea {
    pub fn open(archive_root: &Path, node: nest_types::NodeId) -> NestResult<BackupArea> {
        let root = archive_root.join("backups");
        std::fs::create_dir_all(root.join("manifests")).map_err(io)?;
        let objects = Arc::new(
            ObjectStore::open_with_staging(&root, &format!("staging-{node}")).map_err(io)?,
        );
        Ok(BackupArea { root, objects })
    }

    fn manifest(&self, id: u64) -> PathBuf {
        self.root.join("manifests").join(format!("{id}.sqlite"))
    }
}

const MANIFEST_SCHEMA: &str = "
CREATE TABLE info (k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE entries (path TEXT PRIMARY KEY, kind INTEGER, file INTEGER, gen INTEGER, size INTEGER,
    perm INTEGER, mtime INTEGER, sealed INTEGER, target BLOB);
";

/// Capture `root` into the backup area of archive store `store`.
pub async fn create(
    vfs: Arc<Vfs>,
    area: Arc<BackupArea>,
    store: StoreId,
    name: String,
    root: String,
    progress: Arc<Mutex<JobProgress>>,
) -> NestResult<u64> {
    use futures::StreamExt;
    let entries = vfs
        .data()
        .with_reader(|c| Ok(walk_structure(c, &root)))
        .map_err(sql)??;
    let files: Vec<Entry> = entries
        .iter()
        .filter(|e| e.kind == FileKind::Regular)
        .cloned()
        .collect();
    {
        let mut p = progress.lock();
        p.total_files = files.len() as u64;
        p.total_bytes = files.iter().map(|e| e.size).sum();
    }
    let skipped: Vec<String> = files
        .iter()
        .filter(|e| !e.stable)
        .map(|e| e.path.clone())
        .collect();
    futures::stream::iter(files.into_iter().filter(|e| e.stable))
        .map(|e| {
            let (vfs, area) = (vfs.clone(), area.clone());
            async move {
                let key = ObjectKey::new(e.file, e.generation);
                let r = if area.objects.exists(key) {
                    Ok(()) // an earlier backup already holds this generation
                } else {
                    match vfs.data().with_reader(|c| query::getattr(c, e.file)) {
                        Ok(Some(a)) if a.generation == e.generation => {
                            vfs.fetch_into(&a, &area.objects).await
                        }
                        Ok(_) => Err(NestError::Stale),
                        Err(err) => Err(sql(err)),
                    }
                };
                (e, r)
            }
        })
        .buffer_unordered(8)
        .for_each(|(e, r)| {
            let mut p = progress.lock();
            p.done_files += 1;
            p.done_bytes += e.size;
            if let Err(err) = r {
                p.failed.push((e.file, format!("{}: {err}", e.path)));
            }
            futures::future::ready(())
        })
        .await;
    {
        let mut p = progress.lock();
        for s in &skipped {
            p.failed
                .push((FileId(0), format!("{s}: being written; not captured")));
        }
        if !p.failed.is_empty() {
            return Err(NestError::Io(format!(
                "{} entries could not be captured; no backup recorded",
                p.failed.len()
            )));
        }
    }
    // Manifest: written under a temporary name, cataloged, then named by id.
    let base = common_base(&entries);
    let tmp = area
        .root
        .join("manifests")
        .join(format!("tmp-{:016x}.sqlite", rand::random::<u64>()));
    let bytes: u64 = entries
        .iter()
        .filter(|e| e.kind == FileKind::Regular)
        .map(|e| e.size)
        .sum();
    let nfiles = entries
        .iter()
        .filter(|e| e.kind == FileKind::Regular)
        .count() as u64;
    {
        let (tmp, entries, name, root, base) = (
            tmp.clone(),
            entries.clone(),
            name.clone(),
            root.clone(),
            base.clone(),
        );
        tokio::task::spawn_blocking(move || -> rusqlite::Result<()> {
            let mut c = Connection::open(&tmp)?;
            c.execute_batch(MANIFEST_SCHEMA)?;
            let tx = c.transaction()?;
            for (k, v) in [
                ("name", name),
                ("selector", root),
                ("base", base),
                ("created", Timestamp::now().0.to_string()),
                ("format", "1".into()),
            ] {
                tx.execute("INSERT INTO info (k, v) VALUES (?1, ?2)", params![k, v])?;
            }
            {
                let mut st =
                    tx.prepare("INSERT INTO entries VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)")?;
                for e in &entries {
                    st.execute(params![
                        e.path,
                        e.kind.as_i64(),
                        e.file.0 as i64,
                        e.generation.0 as i64,
                        e.size as i64,
                        e.perm as i64,
                        e.mtime.0,
                        e.sealed,
                        e.target
                    ])?;
                }
            }
            tx.commit()?;
            c.execute_batch("PRAGMA journal_mode = DELETE")?;
            Ok(())
        })
        .await
        .expect("blocking task")
        .map_err(sql)?;
    }
    let id = match vfs
        .data()
        .meta()
        .propose(Command::RecordBackup {
            name,
            store,
            selector: root,
            files: nfiles,
            bytes,
            now: Timestamp::now(),
        })
        .await?
    {
        Reply::Backup(id) => id,
        other => return Err(NestError::Io(format!("unexpected {other:?}"))),
    };
    std::fs::rename(&tmp, area.manifest(id)).map_err(io)?;
    progress.lock().finished = true;
    Ok(id)
}

/// Recreate backup `id` under namespace directory `dst` as new files (the
/// live namespace is not otherwise touched). Paths are kept relative to the
/// deepest directory containing every captured entry, so the selection
/// keeps its own name under `dst` (`/repo` restores as `dst/repo`), and
/// files it links to outside itself (an HF repo's shared blobs) land beside
/// it where its relative symlinks still resolve.
pub async fn restore(
    vfs: Arc<Vfs>,
    area: Arc<BackupArea>,
    id: u64,
    dst: String,
    progress: Arc<Mutex<JobProgress>>,
) -> NestResult<()> {
    let path = area.manifest(id);
    let (base, entries) = tokio::task::spawn_blocking(move || -> rusqlite::Result<(String, Vec<Entry>)> {
        let c = Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let base: String = c.query_row("SELECT v FROM info WHERE k = 'base'", [], |r| r.get(0))?;
        let mut st = c.prepare("SELECT path, kind, file, gen, size, perm, mtime, sealed, target FROM entries ORDER BY path")?;
        let rows = st.query_map([], |r| {
            Ok(Entry {
                path: r.get(0)?,
                kind: FileKind::from_i64(r.get(1)?).unwrap_or(FileKind::Regular),
                file: FileId(r.get::<_, i64>(2)? as u64),
                generation: Generation(r.get::<_, i64>(3)? as u64),
                size: r.get::<_, i64>(4)? as u64,
                perm: r.get::<_, i64>(5)? as u32,
                mtime: Timestamp(r.get(6)?),
                sealed: r.get(7)?,
                stable: true,
                target: r.get(8)?,
            })
        })?;
        Ok((base, rows.collect::<rusqlite::Result<_>>()?))
    })
    .await
    .expect("blocking task")
    .map_err(|e| NestError::NotFound.max_io(e))?;
    {
        let mut p = progress.lock();
        p.total_files = entries
            .iter()
            .filter(|e| e.kind == FileKind::Regular)
            .count() as u64;
        p.total_bytes = entries
            .iter()
            .filter(|e| e.kind == FileKind::Regular)
            .map(|e| e.size)
            .sum();
    }
    let meta = vfs.data().meta().clone();
    let me = vfs.data().id();
    let rel = |p: &str| -> String {
        let r = p
            .strip_prefix(base.trim_end_matches('/'))
            .unwrap_or(p)
            .trim_start_matches('/');
        format!("{}/{r}", dst.trim_end_matches('/'))
    };
    let mut dirs: std::collections::HashMap<String, FileId> = std::collections::HashMap::new();
    let ensure = |path: String, dirs: &mut std::collections::HashMap<String, FileId>| {
        let vfs = vfs.clone();
        let mut known: Vec<(String, FileId)> = dirs.iter().map(|(k, v)| (k.clone(), *v)).collect();
        known.sort();
        async move {
            let mut cur = FileId::ROOT;
            let mut sofar = String::new();
            let mut made = Vec::new();
            for comp in path.split('/').filter(|c| !c.is_empty()) {
                sofar = format!("{sofar}/{comp}");
                if let Some((_, id)) = known.iter().find(|(k, _)| *k == sofar) {
                    cur = *id;
                    continue;
                }
                let existing = vfs
                    .data()
                    .with_reader(|c| query::lookup(c, cur, comp.as_bytes()))
                    .map_err(sql)?;
                cur = match existing {
                    Some(id) => id,
                    None => match vfs.mkdir(cur, comp.as_bytes(), 0o755).await {
                        Ok(a) => a.id,
                        Err(NestError::Exists) => vfs
                            .data()
                            .with_reader(|c| query::lookup(c, cur, comp.as_bytes()))
                            .map_err(sql)?
                            .ok_or(NestError::Exists)?,
                        Err(e) => return Err(e),
                    },
                };
                made.push((sofar.clone(), cur));
            }
            Ok::<_, NestError>((cur, made))
        }
    };
    for e in &entries {
        let target = rel(&e.path);
        let (parent_path, name) = target.rsplit_once('/').unwrap_or(("", &target));
        let (parent, made) = ensure(parent_path.to_string(), &mut dirs).await?;
        dirs.extend(made);
        match e.kind {
            FileKind::Directory => {
                let (_, made) = ensure(target.clone(), &mut dirs).await?;
                dirs.extend(made);
            }
            FileKind::Symlink => match vfs
                .symlink(
                    parent,
                    name.as_bytes(),
                    e.target.as_deref().unwrap_or_default(),
                )
                .await
            {
                Ok(_) | Err(NestError::Exists) => {}
                Err(err) => progress
                    .lock()
                    .failed
                    .push((e.file, format!("{}: {err}", e.path))),
            },
            FileKind::Regular => {
                let first = match meta.propose(Command::ReserveFileIds { count: 1 }).await? {
                    Reply::FileIds(f) => f,
                    other => return Err(NestError::Io(format!("unexpected {other:?}"))),
                };
                let (from, to) = (
                    ObjectKey::new(e.file, e.generation),
                    ObjectKey::new(first, Generation(1)),
                );
                let (src, dstore) = (area.objects.clone(), vfs.data().store().clone());
                let copied = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                    let r = src.open_read(from)?;
                    let st = dstore.begin_staging(to)?;
                    std::io::copy(&mut &r, &mut st.file())?;
                    dstore.commit_staging(st).map(|_| ())
                })
                .await
                .expect("blocking task");
                let r = match copied {
                    Ok(()) => meta
                        .propose(Command::Import {
                            parent,
                            name: name.as_bytes().to_vec(),
                            file: first,
                            perm: e.perm,
                            size: e.size,
                            mtime: e.mtime,
                            node: me,
                            sealed: e.sealed,
                            now: Timestamp::now(),
                        })
                        .await
                        .map(|_| ()),
                    Err(err) => Err(io(err)),
                };
                let mut p = progress.lock();
                p.done_files += 1;
                p.done_bytes += e.size;
                if let Err(err) = r {
                    let _ = vfs.data().store().delete(to);
                    p.failed.push((e.file, format!("{}: {err}", e.path)));
                }
            }
        }
    }
    progress.lock().finished = true;
    Ok(())
}

/// Drop backup `id` and every object no other backup in this area needs.
pub async fn delete(vfs: Arc<Vfs>, area: Arc<BackupArea>, id: u64) -> NestResult<u64> {
    vfs.data()
        .meta()
        .propose(Command::DeleteBackup { id })
        .await?;
    let manifest = area.manifest(id);
    let a2 = area.clone();
    tokio::task::spawn_blocking(move || -> NestResult<u64> {
        let _ = std::fs::remove_file(&manifest);
        let mut keep = HashSet::new();
        for m in std::fs::read_dir(a2.root.join("manifests"))
            .map_err(io)?
            .flatten()
        {
            if !m.file_name().to_string_lossy().ends_with(".sqlite") {
                continue;
            }
            let c =
                Connection::open_with_flags(m.path(), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                    .map_err(sql)?;
            let mut st = c
                .prepare("SELECT file, gen FROM entries WHERE kind = 1")
                .map_err(sql)?;
            let rows = st
                .query_map([], |r| {
                    Ok(ObjectKey::new(
                        FileId(r.get::<_, i64>(0)? as u64),
                        Generation(r.get::<_, i64>(1)? as u64),
                    ))
                })
                .map_err(sql)?;
            for k in rows {
                keep.insert(k.map_err(sql)?);
            }
        }
        let mut removed = 0;
        for o in a2.objects.scan().map_err(io)? {
            if !keep.contains(&o.key) {
                a2.objects.delete(o.key).map_err(io)?;
                removed += 1;
            }
        }
        Ok(removed)
    })
    .await
    .expect("blocking task")
}

/// Write a metadata snapshot into `<archive root>/meta/`, keeping `keep`.
pub fn meta_snapshot(vfs: &Vfs, archive_root: &Path, keep: usize) -> NestResult<PathBuf> {
    let dir = archive_root.join("meta");
    std::fs::create_dir_all(&dir).map_err(io)?;
    let tmp = dir.join(format!("tmp-{:016x}.sqlite", rand::random::<u64>()));
    let index = vfs.data().meta().snapshot_to(&tmp).map_err(sql)?;
    let secs = Timestamp::now().0 / 1_000_000_000;
    let fin = dir.join(format!("meta-{secs}-{index}.sqlite"));
    std::fs::rename(&tmp, &fin).map_err(io)?;
    let mut snaps: Vec<PathBuf> = std::fs::read_dir(&dir)
        .map_err(io)?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("meta-"))
        })
        .collect();
    snaps.sort();
    while snaps.len() > keep {
        let _ = std::fs::remove_file(snaps.remove(0));
    }
    Ok(fin)
}

trait MaxIo {
    fn max_io(self, e: rusqlite::Error) -> NestError;
}

impl MaxIo for NestError {
    fn max_io(self, e: rusqlite::Error) -> NestError {
        match e {
            rusqlite::Error::SqliteFailure(f, _) if f.code == rusqlite::ErrorCode::CannotOpen => {
                self
            }
            other => sql(other),
        }
    }
}
