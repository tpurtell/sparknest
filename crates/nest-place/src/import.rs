//! Import a local directory tree into the namespace without copying data
//! (runs on the node that holds the source).
//!
//! Per batch of regular files: reserve file ids, hard-link each source into
//! the object store under its final id, commit the entries (STABLE,
//! generation 1, one live copy here), then with `move` unlink the sources.
//! A crash leaves at most an extra link (deleted by reconciliation) or a
//! source that was already imported (re-running skips it). Directories and
//! symlinks (targets verbatim, so Hugging Face's relative links keep
//! working) are recreated; other file types are skipped.
//!
//! When a source is on another filesystem than the store and `copy` is set,
//! the file is copied straight into the store instead of linked (one pass,
//! no FUSE round trips); smaller batches keep the metadata close behind.
//! `import_entries` imports an explicit list of files to namespace paths
//! (used by `nest hf import` to put blobs where huggingface_hub looks).

use nest_data::Vfs;
use nest_meta::{Command, Reply, SealPolicy, query};
use nest_store::ObjectKey;
use nest_types::{FileId, NestError, NestResult, Timestamp};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SealMode {
    None,
    All,
    /// Seal files below a directory named `blobs` (Hugging Face blobs;
    /// `refs/` and `trees/` stay writable, as huggingface_hub rewrites them).
    Blobs,
    /// Follow the destination's seal policy: `blobs` under
    /// rename-from-incomplete, `all` under on-finalize, else none.
    Auto,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImportOptions {
    pub src: PathBuf,
    /// Namespace directory to import into (created if missing).
    pub dst: String,
    #[serde(default)]
    pub r#move: bool,
    #[serde(default = "auto")]
    pub seal: SealMode,
    /// Copy files that cannot be hard-linked (another filesystem).
    #[serde(default)]
    pub copy: bool,
}

fn auto() -> SealMode {
    SealMode::Auto
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ImportProgress {
    pub files: u64,
    pub bytes: u64,
    /// Blobs the namespace already had: this node's file became a copy of
    /// them (hard link, nothing copied).
    #[serde(default)]
    pub adopted: u64,
    #[serde(default)]
    pub adopted_bytes: u64,
    pub dirs: u64,
    pub symlinks: u64,
    pub skipped: u64,
    pub errors: Vec<String>,
    pub finished: bool,
    /// Regular files and bytes found under the source before starting.
    #[serde(default)]
    pub total_files: u64,
    #[serde(default)]
    pub total_bytes: u64,
    /// Stop at the next file; what was imported stays.
    #[serde(default)]
    pub cancelled: bool,
    /// Bytes copied (not linked) so far, counted as they are written.
    #[serde(default)]
    pub copied_bytes: u64,
    /// Spread imports: files and bytes handed to other hosts (their copy
    /// live, this host's staging copy gone), and those kept here.
    #[serde(default)]
    pub spread_files: u64,
    #[serde(default)]
    pub spread_bytes: u64,
    #[serde(default)]
    pub kept_files: u64,
}

impl ImportProgress {
    /// Files and bytes dealt with so far (imported, adopted or skipped).
    pub fn done(&self) -> (u64, u64) {
        (
            self.files + self.adopted + self.skipped,
            self.bytes + self.adopted_bytes + self.copied_bytes,
        )
    }
}

/// Count regular files and bytes under `dir` (for progress).
fn scan(dir: &std::path::Path) -> (u64, u64) {
    let (mut files, mut bytes) = (0, 0);
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let Ok(md) = e.path().symlink_metadata() else {
                continue;
            };
            if md.is_dir() {
                stack.push(e.path());
            } else if md.is_file() {
                files += 1;
                bytes += md.len();
            }
        }
    }
    (files, bytes)
}

const BATCH: usize = 256;
/// Flush a batch of copies once this many bytes are waiting.
const COPY_BATCH_BYTES: u64 = 4 << 30;

/// Files under `blobs/` that are not content: the shared-blob layout's
/// `.refs` hints and marker, which huggingface_hub rewrites in place.
fn mutable_blob_sidecar(name: &[u8]) -> bool {
    name.starts_with(b".") || name.ends_with(b".refs")
}

struct Pending {
    src: PathBuf,
    parent: FileId,
    name: Vec<u8>,
    perm: u32,
    size: u64,
    mtime: Timestamp,
    seal: bool,
}

fn sql(e: rusqlite::Error) -> NestError {
    NestError::Io(format!("metadata: {e}"))
}

async fn ensure_dir(vfs: &Vfs, parent: FileId, name: &[u8]) -> NestResult<(FileId, bool)> {
    let existing = vfs
        .data()
        .with_reader(|c| query::lookup(c, parent, name))
        .map_err(sql)?;
    if let Some(id) = existing {
        return Ok((id, false));
    }
    match vfs.mkdir(parent, name, 0o755).await {
        Ok(a) => Ok((a.id, true)),
        Err(NestError::Exists) => {
            let id = vfs
                .data()
                .with_reader(|c| query::lookup(c, parent, name))
                .map_err(sql)?;
            id.map(|i| (i, false)).ok_or(NestError::Exists)
        }
        Err(e) => Err(e),
    }
}

pub async fn run(
    vfs: Arc<Vfs>,
    opts: ImportOptions,
    progress: Arc<Mutex<ImportProgress>>,
) -> NestResult<()> {
    let src_md = std::fs::metadata(&opts.src)
        .map_err(|e| NestError::Invalid(format!("{}: {e}", opts.src.display())))?;
    if !src_md.is_dir() {
        return Err(NestError::Invalid(
            "import source must be a directory".into(),
        ));
    }
    let src = opts.src.clone();
    let (tf, tb) = tokio::task::spawn_blocking(move || scan(&src))
        .await
        .unwrap_or((0, 0));
    {
        let mut p = progress.lock();
        p.total_files = tf;
        p.total_bytes = tb;
    }
    // Destination: create along the path.
    let mut dst = FileId::ROOT;
    for comp in opts.dst.split('/').filter(|c| !c.is_empty()) {
        dst = ensure_dir(&vfs, dst, comp.as_bytes()).await?.0;
    }
    let policy = vfs
        .data()
        .with_reader(|c| query::effective_seal_policy(c, dst))
        .map_err(sql)?;
    let seal_mode = match opts.seal {
        SealMode::Auto => match policy {
            SealPolicy::RenameFromIncomplete => SealMode::Blobs,
            SealPolicy::OnFinalize => SealMode::All,
            _ => SealMode::None,
        },
        m => m,
    };
    let mut stack: Vec<(PathBuf, FileId, bool)> = vec![(opts.src.clone(), dst, false)];
    let mut pending: Vec<Pending> = Vec::new();
    let mut dirs_seen: Vec<PathBuf> = Vec::new();
    while let Some((dir, id, in_blobs)) = stack.pop() {
        if progress.lock().cancelled {
            break;
        }
        dirs_seen.push(dir.clone());
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) => {
                progress
                    .lock()
                    .errors
                    .push(format!("{}: {e}", dir.display()));
                continue;
            }
        };
        for e in entries.flatten() {
            if progress.lock().cancelled {
                break;
            }
            let path = e.path();
            let name = e.file_name().as_bytes().to_vec();
            let Ok(md) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            let ft = md.file_type();
            if ft.is_dir() {
                match ensure_dir(&vfs, id, &name).await {
                    Ok((child, created)) => {
                        if created {
                            progress.lock().dirs += 1;
                        }
                        stack.push((path, child, in_blobs || name == b"blobs"));
                    }
                    Err(err) => progress
                        .lock()
                        .errors
                        .push(format!("{}: {err}", path.display())),
                }
            } else if ft.is_symlink() {
                let target = std::fs::read_link(&path).map(|t| t.as_os_str().as_bytes().to_vec());
                let r = match target {
                    Ok(t) => vfs.symlink(id, &name, &t).await,
                    Err(err) => Err(NestError::Io(err.to_string())),
                };
                match r {
                    Ok(_) => {
                        progress.lock().symlinks += 1;
                        if opts.r#move {
                            let _ = std::fs::remove_file(&path);
                        }
                    }
                    Err(NestError::Exists) => progress.lock().skipped += 1,
                    Err(err) => progress
                        .lock()
                        .errors
                        .push(format!("{}: {err}", path.display())),
                }
            } else if ft.is_file() {
                if name.ends_with(b".incomplete") || name.ends_with(b".lock") {
                    progress.lock().skipped += 1; // interrupted downloads, stale locks
                    continue;
                }
                // Content-addressed blobs (named by their hash) that the
                // namespace already has: adopt this file as a local copy.
                let blob = in_blobs && !mutable_blob_sidecar(&name);
                if blob {
                    let existing = vfs
                        .data()
                        .with_reader(|c| query::lookup(c, id, &name))
                        .map_err(sql)?;
                    if let Some(file) = existing {
                        match vfs.adopt_local(file, &path, md.len()).await {
                            Ok(adopted) => {
                                let mut p = progress.lock();
                                if adopted {
                                    p.adopted += 1;
                                    p.adopted_bytes += md.len();
                                } else {
                                    p.skipped += 1;
                                }
                                drop(p);
                                if opts.r#move {
                                    let _ = std::fs::remove_file(&path);
                                }
                            }
                            Err(err) => progress
                                .lock()
                                .errors
                                .push(format!("{}: {err}", path.display())),
                        }
                        continue;
                    }
                }
                pending.push(Pending {
                    src: path,
                    parent: id,
                    name,
                    perm: md.mode() & 0o7777,
                    size: md.len(),
                    mtime: Timestamp(md.mtime() * 1_000_000_000 + md.mtime_nsec()),
                    seal: match seal_mode {
                        SealMode::All => true,
                        SealMode::Blobs => blob,
                        _ => false,
                    },
                });
                if full(&pending) {
                    flush(&vfs, &mut pending, opts.r#move, opts.copy, &progress).await?;
                }
            } else {
                progress.lock().skipped += 1;
            }
        }
    }
    flush(&vfs, &mut pending, opts.r#move, opts.copy, &progress).await?;
    if progress.lock().cancelled {
        return Err(NestError::Io(
            "cancelled; what was imported stays, the rest of the source is untouched".into(),
        ));
    }
    if opts.r#move {
        // Remove now-empty source directories, deepest first.
        dirs_seen.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
        for d in dirs_seen {
            let _ = std::fs::remove_dir(&d);
        }
    }
    Ok(())
}

fn full(pending: &[Pending]) -> bool {
    pending.len() >= BATCH || pending.iter().map(|p| p.size).sum::<u64>() >= COPY_BATCH_BYTES
}

/// One file into the store under `key`: a hard link, or with `copy` (and
/// only when linking is impossible across filesystems) a copy. Returns
/// whether it was copied.
fn place(
    store: &nest_store::ObjectStore,
    src: &std::path::Path,
    key: ObjectKey,
    copy: bool,
    progress: &Mutex<ImportProgress>,
) -> Result<bool, String> {
    match store.link_from(src, key) {
        Ok(_) => Ok(false),
        Err(e) if e.raw_os_error() == Some(18) && copy => {
            let run = || -> std::io::Result<()> {
                let mut from = std::fs::File::open(src)?;
                store.reserve_room(from.metadata()?.len())?;
                let st = store.begin_staging(key)?;
                let mut buf = vec![0u8; 8 << 20];
                let mut to = st.file();
                loop {
                    let n = std::io::Read::read(&mut from, &mut buf)?;
                    if n == 0 {
                        break;
                    }
                    std::io::Write::write_all(&mut to, &buf[..n])?;
                    let mut p = progress.lock();
                    p.copied_bytes += n as u64;
                    if p.cancelled {
                        return Err(std::io::Error::other("cancelled"));
                    }
                }
                store.commit_staging(st)?;
                Ok(())
            };
            run()
                .map(|_| true)
                .map_err(|e| format!("{}: copying: {e}", src.display()))
        }
        Err(e) => Err(match e.raw_os_error() {
            Some(18) => format!(
                "{}: not on the same filesystem as the sparknest store (import links; pass --copy to copy)",
                src.display()
            ),
            _ => format!("{}: {e}", src.display()),
        }),
    }
}

async fn flush(
    vfs: &Vfs,
    pending: &mut Vec<Pending>,
    mv: bool,
    copy: bool,
    progress: &Arc<Mutex<ImportProgress>>,
) -> NestResult<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch: Vec<Pending> = std::mem::take(pending);
    let meta = vfs.data().meta().clone();
    let first = match meta
        .propose(Command::ReserveFileIds {
            count: batch.len() as u64,
        })
        .await?
    {
        Reply::FileIds(f) => f,
        other => return Err(NestError::Io(format!("unexpected {other:?}"))),
    };
    let store = vfs.data().store().clone();
    let me = vfs.data().id();
    let srcs: Vec<PathBuf> = batch.iter().map(|p| p.src.clone()).collect();
    let pr = progress.clone();
    let linked: Vec<Result<bool, String>> = tokio::task::spawn_blocking(move || {
        srcs.iter()
            .enumerate()
            .map(|(i, s)| {
                let key = ObjectKey::new(FileId(first.0 + i as u64), nest_types::Generation(1));
                place(&store, s, key, copy, &pr)
            })
            .collect()
    })
    .await
    .expect("blocking task");
    let now = Timestamp::now();
    let mut cmds = Vec::new();
    let mut idx = Vec::new();
    for (i, (p, l)) in batch.iter().zip(&linked).enumerate() {
        match l {
            Ok(_) => {
                cmds.push(Command::Import {
                    parent: p.parent,
                    name: p.name.clone(),
                    file: FileId(first.0 + i as u64),
                    perm: p.perm,
                    size: p.size,
                    mtime: p.mtime,
                    node: me,
                    sealed: p.seal,
                    now,
                });
                idx.push(i);
            }
            Err(e) => progress.lock().errors.push(e.clone()),
        }
    }
    if cmds.is_empty() {
        return Ok(());
    }
    let results = match meta.propose(Command::Batch(cmds)).await? {
        Reply::Batch(r) => r,
        other => return Err(NestError::Io(format!("unexpected {other:?}"))),
    };
    let store = vfs.data().store().clone();
    let mut to_unlink = Vec::new();
    let mut to_drop = Vec::new();
    {
        let mut pr = progress.lock();
        for (r, i) in results.into_iter().zip(idx) {
            let p = &batch[i];
            let key = ObjectKey::new(FileId(first.0 + i as u64), nest_types::Generation(1));
            let copied = matches!(linked[i], Ok(true));
            match r {
                Ok(_) => {
                    pr.files += 1;
                    // Copies were counted as they were written.
                    if !copied {
                        pr.bytes += p.size;
                    }
                    if mv {
                        to_unlink.push(p.src.clone());
                    }
                }
                Err(NestError::Exists) => {
                    pr.skipped += 1;
                    to_drop.push(key);
                }
                Err(e) => {
                    pr.errors.push(format!("{}: {e}", p.src.display()));
                    to_drop.push(key);
                }
            }
        }
    }
    tokio::task::spawn_blocking(move || {
        for k in to_drop {
            let _ = store.delete(k);
        }
        for s in to_unlink {
            let _ = std::fs::remove_file(s);
        }
    })
    .await
    .expect("blocking task");
    Ok(())
}

/// One regular file to import to an exact namespace path.
#[derive(Clone, Debug)]
pub struct Entry {
    /// A regular file, or a symlink to one (followed).
    pub src: PathBuf,
    /// Absolute namespace path.
    pub dst: String,
    pub seal: bool,
}

/// Import `entries` to their namespace paths, creating directories. A
/// destination that already exists is left alone (counted as skipped).
pub async fn import_entries(
    vfs: &Arc<Vfs>,
    entries: Vec<Entry>,
    mv: bool,
    copy: bool,
    progress: &Arc<Mutex<ImportProgress>>,
) -> NestResult<()> {
    let mut dirs: std::collections::HashMap<String, FileId> = std::collections::HashMap::new();
    let mut pending: Vec<Pending> = Vec::new();
    for e in entries {
        if progress.lock().cancelled {
            break;
        }
        let Some((parent_path, name)) = e.dst.rsplit_once('/') else {
            continue;
        };
        let parent = match dirs.get(parent_path) {
            Some(id) => *id,
            None => {
                let mut id = FileId::ROOT;
                let mut created = 0;
                for comp in parent_path.split('/').filter(|c| !c.is_empty()) {
                    let (i, c) = ensure_dir(vfs, id, comp.as_bytes()).await?;
                    id = i;
                    created += c as u64;
                }
                progress.lock().dirs += created;
                dirs.insert(parent_path.to_string(), id);
                id
            }
        };
        let existing = vfs
            .data()
            .with_reader(|c| query::lookup(c, parent, name.as_bytes()))
            .map_err(sql)?;
        let real = match std::fs::canonicalize(&e.src) {
            Ok(r) => r,
            Err(err) => {
                progress
                    .lock()
                    .errors
                    .push(format!("{}: {err}", e.src.display()));
                continue;
            }
        };
        let Ok(md) = std::fs::metadata(&real) else {
            continue;
        };
        if let Some(file) = existing {
            // Content-addressed blobs the cluster already has: this host's
            // file becomes its copy (a hard link, nothing transferred).
            let r = if e.seal {
                vfs.adopt_local(file, &real, md.len()).await
            } else {
                Ok(false)
            };
            let mut p = progress.lock();
            match r {
                Ok(true) => p.adopted += 1,
                // Same name, other size: not the same blob; a person decides.
                Err(NestError::Invalid(m)) => p.errors.push(format!("{}: {m}", e.src.display())),
                // Already a copy here, or another filesystem: nothing to do.
                _ => p.skipped += 1,
            }
            p.adopted_bytes += md.len();
            continue;
        }
        pending.push(Pending {
            src: real,
            parent,
            name: name.as_bytes().to_vec(),
            perm: md.mode() & 0o7777,
            size: md.len(),
            mtime: Timestamp(md.mtime() * 1_000_000_000 + md.mtime_nsec()),
            seal: e.seal,
        });
        if full(&pending) {
            flush(vfs, &mut pending, mv, copy, progress).await?;
        }
    }
    flush(vfs, &mut pending, mv, copy, progress).await
}
