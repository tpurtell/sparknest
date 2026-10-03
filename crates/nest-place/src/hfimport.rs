//! `nest hf import`: bring a Hugging Face cache (the classic per-repo layout
//! or hf 2.0's shared blob store) into sparknest's hub, in the form
//! huggingface_hub itself would have written, without re-implementing its
//! layout rules.
//!
//! Per repo:
//! 1. **Seed blobs.** Every blob a source snapshot uses is placed at
//!    `<hub>/<repo>/blobs/<etag>` as a regular file: hard-linked when the
//!    source shares the store's filesystem, else copied into the store. The
//!    source names blobs by their etag in both layouts (hf 2.0's repo entry
//!    is a link into `blobs/xx/<xet>`, followed here).
//! 2. **Finalize with hf.** `hf download <repo> <the files that snapshot
//!    had> --revision <commit>` runs against the mount. It finds each blob
//!    where it looks first and only writes the snapshot entries; files the
//!    source never finished are downloaded. When hf cannot reach the Hub
//!    (offline, gated, deleted repos), the source's snapshot links are
//!    mirrored exactly instead. Refs are copied from the source unless the
//!    destination already has them.
//! 3. **Verify.** Every file of every source snapshot must resolve in
//!    sparknest to a settled file of the same size.
//! 4. **Move** (only when asked): verified repos leave the source. If every
//!    repo verified, the whole cache directory becomes a symlink to
//!    sparknest's hub, so tools still pointed at it keep working; otherwise
//!    each verified repo directory is replaced by such a link.

use crate::import::{Entry, ImportProgress, import_entries};
use crate::selector;
use nest_data::Vfs;
use nest_types::{FileId, FileKind, GenState, NestError, NestResult};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HfImportOptions {
    /// A hub cache directory (holds `models--*`), an `HF_HOME` (holds
    /// `hub/`), or one repo directory.
    pub src: PathBuf,
    /// Namespace hub directory, e.g. `/hub`.
    pub hub: String,
    /// The same hub on disk through this node's mount (for hf, and for the
    /// symlinks a move leaves behind).
    pub mount_hub: Option<PathBuf>,
    /// The `hf` program; `None` mirrors snapshots without asking the Hub.
    pub hf: Option<PathBuf>,
    #[serde(default)]
    pub r#move: bool,
    /// Copy blobs that are on another filesystem (else they are errors).
    #[serde(default = "yes")]
    pub copy: bool,
    /// Spread the blobs over the cluster: each goes to the host with the
    /// most free space (for caches larger than this host's disk). Blobs
    /// are copied here in batches and handed out while the next batch
    /// copies; the cluster's existing blobs are skipped.
    #[serde(default)]
    pub spread: bool,
}

/// A spread import copies about this much here before handing it out.
const SPREAD_BATCH: u64 = 64 << 30;

fn yes() -> bool {
    true
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RepoOutcome {
    pub repo: String,
    /// `done`, `mirrored` (offline) or `failed`.
    pub status: String,
    pub note: String,
    pub moved: bool,
}

#[derive(Clone, Debug)]
enum SnapFile {
    /// A link to `blobs/<etag>`.
    Blob(String),
    /// A plain file in the snapshot (caches made without symlinks).
    Plain(PathBuf),
}

#[derive(Clone, Debug)]
struct SrcRepo {
    dir: PathBuf,
    /// `models--org--name`
    name: String,
    repo_id: String,
    repo_type: &'static str,
    /// etag → (real file, size)
    blobs: std::collections::BTreeMap<String, (PathBuf, u64)>,
    /// commit → [(path in snapshot, file)]
    snapshots: Vec<(String, Vec<(String, SnapFile)>)>,
    refs: Vec<(String, String)>,
}

fn repo_meta(name: &str) -> Option<(String, &'static str)> {
    let (t, rest) = [
        ("model", "models--"),
        ("dataset", "datasets--"),
        ("space", "spaces--"),
    ]
    .into_iter()
    .find_map(|(t, p)| name.strip_prefix(p).map(|r| (t, r)))?;
    Some((rest.replacen("--", "/", 1), t))
}

/// `(cache, repos, single)`: `single` when `src` is one repo directory.
fn find_repos(src: &Path) -> std::io::Result<(PathBuf, Vec<PathBuf>, bool)> {
    let name = src.file_name().and_then(|n| n.to_str()).unwrap_or("");
    if repo_meta(name).is_some() {
        let cache = src.parent().unwrap_or(src).to_path_buf();
        return Ok((cache, vec![src.to_path_buf()], true));
    }
    let cache = if src.join("hub").is_dir() && !has_repos(src) {
        src.join("hub")
    } else {
        src.to_path_buf()
    };
    let mut repos: Vec<PathBuf> = std::fs::read_dir(&cache)?
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter(|e| repo_meta(&e.file_name().to_string_lossy()).is_some())
        .map(|e| e.path())
        .collect();
    repos.sort();
    Ok((cache, repos, false))
}

fn has_repos(d: &Path) -> bool {
    std::fs::read_dir(d).is_ok_and(|rd| {
        rd.flatten()
            .any(|e| repo_meta(&e.file_name().to_string_lossy()).is_some())
    })
}

fn scan_repo(dir: &Path) -> std::io::Result<SrcRepo> {
    let name = dir
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    let (repo_id, repo_type) = repo_meta(&name).expect("filtered");
    let mut blobs = std::collections::BTreeMap::new();
    if let Ok(rd) = std::fs::read_dir(dir.join("blobs")) {
        for e in rd.flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            if n.starts_with('.') || n.ends_with(".incomplete") || n.ends_with(".lock") {
                continue;
            }
            // hf 2.0: a link into the shared store; follow it.
            if let Ok(real) = std::fs::canonicalize(e.path())
                && let Ok(md) = std::fs::metadata(&real)
                && md.is_file()
            {
                blobs.insert(n, (real, md.len()));
            }
        }
    }
    let mut snapshots = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir.join("snapshots")) {
        for e in rd.flatten() {
            let commit = e.file_name().to_string_lossy().into_owned();
            let mut files = Vec::new();
            let mut stack = vec![(e.path(), String::new())];
            while let Some((d, prefix)) = stack.pop() {
                let Ok(rd) = std::fs::read_dir(&d) else {
                    continue;
                };
                for f in rd.flatten() {
                    let n = f.file_name().to_string_lossy().into_owned();
                    let rel = if prefix.is_empty() {
                        n.clone()
                    } else {
                        format!("{prefix}/{n}")
                    };
                    let Ok(md) = f.path().symlink_metadata() else {
                        continue;
                    };
                    if md.is_dir() {
                        stack.push((f.path(), rel));
                    } else if md.file_type().is_symlink() {
                        let t = std::fs::read_link(f.path()).unwrap_or_default();
                        let etag = t
                            .file_name()
                            .map(|x| x.to_string_lossy().into_owned())
                            .unwrap_or_default();
                        if blobs.contains_key(&etag) {
                            files.push((rel, SnapFile::Blob(etag)));
                        }
                    } else if md.is_file() {
                        files.push((rel, SnapFile::Plain(f.path())));
                    }
                }
            }
            files.sort_by(|a, b| a.0.cmp(&b.0));
            snapshots.push((commit, files));
        }
    }
    snapshots.sort_by(|a, b| a.0.cmp(&b.0));
    let mut refs = Vec::new();
    let refs_dir = dir.join("refs");
    let mut stack = vec![(refs_dir.clone(), String::new())];
    while let Some((d, prefix)) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for f in rd.flatten() {
            let n = f.file_name().to_string_lossy().into_owned();
            let rel = if prefix.is_empty() {
                n
            } else {
                format!("{prefix}/{n}")
            };
            if f.path().is_dir() {
                stack.push((f.path(), rel));
            } else if let Ok(c) = std::fs::read_to_string(f.path()) {
                refs.push((rel, c.trim().to_string()));
            }
        }
    }
    Ok(SrcRepo {
        dir: dir.to_path_buf(),
        name,
        repo_id,
        repo_type,
        blobs,
        snapshots,
        refs,
    })
}

fn sql(e: rusqlite::Error) -> NestError {
    NestError::Io(format!("metadata: {e}"))
}

/// Resolve a namespace path to a settled regular file, following links.
fn settled_size(vfs: &Vfs, path: &str) -> NestResult<Option<u64>> {
    vfs.data()
        .with_reader(|c| {
            let mut p = path.to_string();
            for _ in 0..8 {
                let Ok((id, dir)) = selector::resolve_path(c, &p) else {
                    return Ok(None);
                };
                let Some(a) = nest_meta::query::getattr(c, id)? else {
                    return Ok(None);
                };
                match a.kind {
                    FileKind::Regular => {
                        return Ok((a.gen_state == GenState::Stable).then_some(a.size));
                    }
                    FileKind::Symlink => {
                        let t = nest_meta::query::readlink(c, id)?.unwrap_or_default();
                        let t = String::from_utf8_lossy(&t).into_owned();
                        p = if t.starts_with('/') {
                            t
                        } else {
                            let base = nest_meta::query::path_of(c, dir)?
                                .map(|b| String::from_utf8_lossy(&b).into_owned())
                                .unwrap_or_default();
                            format!("{}/{t}", base.trim_end_matches('/'))
                        };
                    }
                    FileKind::Directory => return Ok(None),
                }
            }
            Ok(None)
        })
        .map_err(sql)
}

async fn mkdirs(vfs: &Vfs, path: &str) -> NestResult<FileId> {
    let mut id = FileId::ROOT;
    for comp in path.split('/').filter(|c| !c.is_empty()) {
        let existing = vfs
            .data()
            .with_reader(|c| nest_meta::query::lookup(c, id, comp.as_bytes()))
            .map_err(sql)?;
        id = match existing {
            Some(i) => i,
            None => match vfs.mkdir(id, comp.as_bytes(), 0o755).await {
                Ok(a) => a.id,
                Err(NestError::Exists) => vfs
                    .data()
                    .with_reader(|c| nest_meta::query::lookup(c, id, comp.as_bytes()))
                    .map_err(sql)?
                    .ok_or(NestError::Exists)?,
                Err(e) => return Err(e),
            },
        };
    }
    Ok(id)
}

/// The source's snapshot links, exactly (offline finalization).
async fn mirror_snapshot(
    vfs: &Vfs,
    base: &str,
    commit: &str,
    files: &[(String, SnapFile)],
) -> NestResult<()> {
    for (rel, f) in files {
        let SnapFile::Blob(etag) = f else { continue };
        let (dir_rel, name) = rel
            .rsplit_once('/')
            .map_or(("", rel.as_str()), |(d, n)| (d, n));
        let dir = format!(
            "{base}/snapshots/{commit}{}{dir_rel}",
            if dir_rel.is_empty() { "" } else { "/" }
        );
        let parent = mkdirs(vfs, &dir).await?;
        let depth = 2 + rel.matches('/').count();
        let target = format!("{}blobs/{etag}", "../".repeat(depth));
        match vfs
            .symlink(parent, name.as_bytes(), target.as_bytes())
            .await
        {
            Ok(_) | Err(NestError::Exists) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

async fn write_small(vfs: &Vfs, path: &str, body: &[u8]) -> NestResult<bool> {
    let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
    let parent = mkdirs(vfs, dir).await?;
    match vfs
        .create(
            parent,
            name.as_bytes(),
            0o644,
            // Exclusive: another host may have just written this ref (hosts
            // import the same repos at once); never take over its file.
            nest_data::vfs::oflags::WRONLY | nest_data::vfs::oflags::EXCL,
        )
        .await
    {
        Ok((_, fh, _)) => {
            let r = vfs.write(fh, 0, body.to_vec()).await;
            vfs.release(fh, None).await;
            r.map(|_| true)
        }
        Err(NestError::Exists) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Let hf finish one revision against the mount; `Err` carries why not.
async fn hf_finalize(
    hf: &Path,
    mount_hub: &Path,
    repo: &SrcRepo,
    commit: &str,
    files: &[(String, SnapFile)],
) -> Result<(), String> {
    let names: Vec<&str> = files
        .iter()
        .filter(|(_, f)| matches!(f, SnapFile::Blob(_)))
        .map(|(r, _)| r.as_str())
        .collect();
    for chunk in names.chunks(200) {
        let out = tokio::process::Command::new(hf)
            .arg("download")
            .arg(&repo.repo_id)
            .args(chunk)
            .args(["--revision", commit, "--repo-type", repo.repo_type])
            .env("HF_HUB_CACHE", mount_hub)
            .env("HF_HUB_DISABLE_PROGRESS_BARS", "1")
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|e| format!("running {}: {e}", hf.display()))?;
        if !out.status.success() {
            let err = String::from_utf8_lossy(&out.stderr);
            let last = err
                .lines()
                .rev()
                .find(|l| !l.trim().is_empty())
                .unwrap_or("failed");
            return Err(last.trim().chars().take(240).collect());
        }
    }
    Ok(())
}

pub async fn run(
    vfs: Arc<Vfs>,
    opts: HfImportOptions,
    progress: Arc<Mutex<ImportProgress>>,
    outcomes: Arc<Mutex<Vec<RepoOutcome>>>,
    spread: Option<Arc<crate::placer::Placer>>,
) -> NestResult<()> {
    // A spread import's handing-out of the previous batch, running while
    // the next batch copies.
    let mut handing: Option<tokio::task::JoinHandle<NestResult<()>>> = None;
    if opts.r#move && opts.mount_hub.is_none() {
        return Err(NestError::Invalid(
            "--move leaves a link to sparknest's hub behind, so this node must have its mount"
                .into(),
        ));
    }
    let src = opts.src.clone();
    let (cache, repos, single) = tokio::task::spawn_blocking(move || -> std::io::Result<_> {
        let (cache, dirs, single) = find_repos(&src)?;
        let repos: Vec<SrcRepo> = dirs.iter().filter_map(|d| scan_repo(d).ok()).collect();
        Ok((cache, repos, single))
    })
    .await
    .expect("blocking task")
    .map_err(|e| NestError::Invalid(format!("{}: {e}", opts.src.display())))?;
    if repos.is_empty() {
        return Err(NestError::Invalid(format!(
            "no models--*, datasets--* or spaces--* directories in {}",
            opts.src.display()
        )));
    }
    {
        let mut p = progress.lock();
        for r in &repos {
            let used = used_blobs(r);
            p.total_files += used.len() as u64 + plain_count(r);
            p.total_bytes += used.iter().map(|e| r.blobs[*e].1).sum::<u64>();
        }
    }
    let hub = opts.hub.trim_end_matches('/').to_string();
    let mut all_verified = true;
    let mut verified_dirs = Vec::new();
    for r in &repos {
        if progress.lock().cancelled {
            all_verified = false;
            break;
        }
        let base = format!("{hub}/{}", r.name);
        let mut out = RepoOutcome {
            repo: r.repo_id.clone(),
            status: "done".into(),
            ..Default::default()
        };
        // 1. Blobs (and plain snapshot files) into place.
        let mut entries: Vec<Entry> = used_blobs(r)
            .into_iter()
            .map(|etag| Entry {
                src: r.blobs[etag].0.clone(),
                dst: format!("{base}/blobs/{etag}"),
                seal: true,
            })
            .collect();
        for (commit, files) in &r.snapshots {
            for (rel, f) in files {
                if let SnapFile::Plain(p) = f {
                    entries.push(Entry {
                        src: p.clone(),
                        dst: format!("{base}/snapshots/{commit}/{rel}"),
                        seal: true,
                    });
                }
            }
        }
        let errs_before = progress.lock().errors.len();
        // A repo that fails is reported and the import moves on.
        let placed = match &spread {
            None => import_entries(&vfs, entries, false, opts.copy, &progress).await,
            Some(p) => place_spread(&vfs, p, entries, opts.copy, &progress, &mut handing).await,
        };
        if let Err(e) = placed {
            out.status = "failed".into();
            out.note = format!("placing blobs: {e}");
            all_verified = false;
            outcomes.lock().push(out);
            continue;
        }
        if progress.lock().errors.len() > errs_before {
            out.status = "failed".into();
            out.note = "some blobs could not be placed (see errors)".into();
            all_verified = false;
            outcomes.lock().push(out);
            continue;
        }
        // 2. Snapshots: hf where possible, else the source's links.
        for (commit, files) in &r.snapshots {
            let via_hf = match (&opts.hf, &opts.mount_hub) {
                (Some(hf), Some(mh)) => hf_finalize(hf, mh, r, commit, files).await,
                _ => Err("offline".into()),
            };
            if let Err(why) = via_hf {
                if why != "offline" {
                    tracing::info!(repo = %r.repo_id, %commit, %why, "hf could not finalize; mirroring the snapshot");
                    out.note = format!("hf: {why}; snapshot links mirrored");
                }
                out.status = "mirrored".into();
                if let Err(e) = mirror_snapshot(&vfs, &base, commit, files).await {
                    out.status = "failed".into();
                    out.note = format!("mirroring snapshot {commit}: {e}");
                }
            }
        }
        for (name, commit) in &r.refs {
            if let Err(e) =
                write_small(&vfs, &format!("{base}/refs/{name}"), commit.as_bytes()).await
            {
                out.status = "failed".into();
                out.note = format!("writing refs/{name}: {e}");
            }
        }
        // 3. Verify every source snapshot file.
        let mut bad = Vec::new();
        for (commit, files) in &r.snapshots {
            for (rel, f) in files {
                let want = match f {
                    SnapFile::Blob(e) => r.blobs[e].1,
                    SnapFile::Plain(p) => std::fs::metadata(p).map(|m| m.len()).unwrap_or(0),
                };
                if settled_size(&vfs, &format!("{base}/snapshots/{commit}/{rel}"))
                    .ok()
                    .flatten()
                    != Some(want)
                {
                    bad.push(rel.clone());
                }
            }
        }
        if bad.is_empty() && out.status != "failed" {
            // This host has the weights; give it its own copy of the repo's
            // small files other hosts wrote (refs, trees/*.json, configs), so
            // it counts as holding the whole model.
            fill_small_files(&vfs, &base).await;
            verified_dirs.push(r.dir.clone());
        } else {
            all_verified = false;
            out.status = "failed".into();
            // A step before failed (its note says why): keep that note.
            if !bad.is_empty() {
                out.note = format!(
                    "{} file(s) did not verify, e.g. {}",
                    bad.len(),
                    bad.first().cloned().unwrap_or_default()
                );
            }
        }
        outcomes.lock().push(out);
    }
    if let Some(h) = handing.take()
        && let Ok(Err(e)) = h.await
    {
        progress
            .lock()
            .errors
            .push(format!("handing out blobs: {e}"));
    }
    // 4. Move: swap in links to sparknest's hub.
    if opts.r#move
        && let Some(mh) = &opts.mount_hub
    {
        let whole = all_verified && !single;
        let mh = mh.clone();
        let cache2 = cache.clone();
        let moved =
            tokio::task::spawn_blocking(move || swap_in_links(&cache2, &verified_dirs, &mh, whole))
                .await
                .expect("blocking task");
        match moved {
            Ok(dirs) => {
                let mut o = outcomes.lock();
                for d in dirs {
                    let name = d
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned();
                    if let Some((id, _)) = repo_meta(&name) {
                        for x in o.iter_mut().filter(|x| x.repo == id) {
                            x.moved = true;
                        }
                    }
                }
            }
            Err(e) => progress.lock().errors.push(format!("moving: {e}")),
        }
    }
    if progress.lock().cancelled {
        return Err(NestError::Io(
            "cancelled; repos already imported stay, sources untouched".into(),
        ));
    }
    Ok(())
}

fn used_blobs(r: &SrcRepo) -> Vec<&String> {
    let mut v: Vec<&String> = r
        .snapshots
        .iter()
        .flat_map(|(_, f)| f.iter())
        .filter_map(|(_, f)| match f {
            SnapFile::Blob(e) => Some(e),
            _ => None,
        })
        .collect();
    v.sort();
    v.dedup();
    v
}

fn plain_count(r: &SrcRepo) -> u64 {
    r.snapshots
        .iter()
        .flat_map(|(_, f)| f.iter())
        .filter(|(_, f)| matches!(f, SnapFile::Plain(_)))
        .count() as u64
}

/// Replace verified sources with links into sparknest's hub: the whole
/// cache when `whole`, else each repo. The old tree is renamed aside first
/// and removed only after the link is in place. Returns the repo dirs moved.
fn swap_in_links(
    cache: &Path,
    repos: &[PathBuf],
    mount_hub: &Path,
    whole: bool,
) -> std::io::Result<Vec<PathBuf>> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let swap = |dir: &Path, target: &Path| -> std::io::Result<()> {
        let aside = dir.with_file_name(format!(
            ".{}.sparknest-moved-{stamp}",
            dir.file_name().unwrap_or_default().to_string_lossy()
        ));
        std::fs::rename(dir, &aside)?;
        if let Err(e) = std::os::unix::fs::symlink(target, dir) {
            // Put it back rather than leave nothing behind.
            let _ = std::fs::rename(&aside, dir);
            return Err(e);
        }
        std::fs::remove_dir_all(&aside)
    };
    if whole {
        swap(cache, mount_hub)?;
        return Ok(repos.to_vec());
    }
    let mut done = Vec::new();
    for d in repos {
        let name = d.file_name().unwrap_or_default();
        swap(d, &mount_hub.join(name))?;
        done.push(d.clone());
    }
    Ok(done)
}

/// Copy onto this host the files under `base` it lacks that are small
/// (repo metadata another host wrote); large ones stay where they are.
async fn fill_small_files(vfs: &Arc<Vfs>, base: &str) {
    const SMALL: u64 = 16 << 20;
    let m = match vfs
        .data()
        .with_reader(|c| Ok(crate::selector::resolve_tree(c, base)))
    {
        Ok(Ok(m)) => m,
        _ => return,
    };
    let me = vfs.data().id().live_store();
    for e in m.entries.iter().filter(|e| e.stable && e.size <= SMALL) {
        let have = vfs
            .data()
            .with_reader(|c| nest_meta::query::has_live_replica(c, e.file, e.generation, me))
            .unwrap_or(true);
        if !have && let Err(err) = vfs.replicate_here(e.file).await {
            tracing::debug!(path = %e.path, error = %err, "could not copy a small file here");
        }
    }
}

/// Place `entries` in batches of about `SPREAD_BATCH`: each batch is copied
/// here, then handed to other hosts (`Placer::spread`) while the next one
/// copies. At most two batches are staged here at once.
async fn place_spread(
    vfs: &Arc<Vfs>,
    placer: &Arc<crate::placer::Placer>,
    entries: Vec<Entry>,
    copy: bool,
    progress: &Arc<Mutex<ImportProgress>>,
    handing: &mut Option<tokio::task::JoinHandle<NestResult<()>>>,
) -> NestResult<()> {
    let mut batch: Vec<Entry> = Vec::new();
    let mut bytes = 0u64;
    let mut rest = entries.into_iter().peekable();
    while let Some(e) = rest.next() {
        bytes += std::fs::metadata(&e.src).map(|m| m.len()).unwrap_or(0);
        batch.push(e);
        if bytes < SPREAD_BATCH && rest.peek().is_some() {
            continue;
        }
        let this = std::mem::take(&mut batch);
        bytes = 0;
        import_entries(vfs, this.clone(), false, copy, progress).await?;
        // The files this batch left on this host (new, or adopted here).
        let me = vfs.data().id().live_store();
        let files: Vec<(FileId, u64)> = vfs
            .data()
            .with_reader(|c| {
                Ok(this
                    .iter()
                    .filter_map(|e| {
                        let (id, _) = crate::selector::resolve_path(c, &e.dst).ok()?;
                        let a = nest_meta::query::getattr(c, id).ok()??;
                        nest_meta::query::has_live_replica(c, id, a.generation, me)
                            .ok()?
                            .then_some((id, a.size))
                    })
                    .collect())
            })
            .map_err(sql)?;
        // Wait for the previous batch's hand-out before starting this one.
        if let Some(h) = handing.take()
            && let Ok(Err(e)) = h.await
        {
            progress
                .lock()
                .errors
                .push(format!("handing out blobs: {e}"));
        }
        if progress.lock().cancelled {
            break;
        }
        let (p, prog) = (placer.clone(), progress.clone());
        *handing = Some(tokio::spawn(async move { p.spread(files, &prog).await }));
    }
    Ok(())
}

/// The `hf` program: on PATH, else where Homebrew or pip put it (the
/// daemon's system unit has a minimal PATH).
pub fn find_hf() -> Option<PathBuf> {
    let mut dirs: Vec<PathBuf> = std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).collect())
        .unwrap_or_default();
    dirs.push("/home/linuxbrew/.linuxbrew/bin".into());
    if let Some(home) = std::env::var_os("HOME") {
        dirs.push(PathBuf::from(home).join(".local/bin"));
    }
    dirs.into_iter().map(|d| d.join("hf")).find(|p| p.is_file())
}

/// A repo target for `hf cache rm`: `model/org/name`, `dataset/org/name`
/// (also accepts the selectors `hf:org/name` and `hf-dataset:org/name`,
/// `org/name`, `datasets/org/name`; a `@revision` is dropped: the repo
/// goes).
pub fn rm_target(t: &str) -> String {
    let t = t.trim();
    let t = t.split_once('@').map_or(t, |(r, _)| r);
    if let Some(rest) = t.strip_prefix("hf-dataset:") {
        return format!("dataset/{rest}");
    }
    let t = t.trim_start_matches("hf:");
    for (plural, single) in [
        ("models/", "model/"),
        ("datasets/", "dataset/"),
        ("spaces/", "space/"),
    ] {
        if let Some(rest) = t.strip_prefix(plural) {
            return format!("{single}{rest}");
        }
    }
    if ["model/", "dataset/", "space/"]
        .iter()
        .any(|p| t.starts_with(p))
    {
        t.to_string()
    } else {
        format!("model/{t}")
    }
}

/// What every hf run gets: the hub (through this node's mount) as its
/// cache and HF_HOME, and the user's token where hf found it before HF_HOME
/// moved into sparknest.
fn hf_env(cmd: &mut tokio::process::Command, mount_hub: &Path) {
    if let Some(home) = mount_hub.parent() {
        cmd.env("HF_HOME", home);
    }
    cmd.env("HF_HUB_CACHE", mount_hub)
        .env("HF_HUB_DISABLE_TELEMETRY", "1")
        .env("HF_HUB_DISABLE_PROGRESS_BARS", "1");
    if std::env::var_os("HF_TOKEN_PATH").is_none()
        && let Some(home) = std::env::var_os("HOME")
    {
        let t = PathBuf::from(home).join(".cache/huggingface/token");
        if t.is_file() {
            cmd.env("HF_TOKEN_PATH", t);
        }
    }
}

/// Run hf; its last JSON line, or the error it printed.
async fn hf_json(mut cmd: tokio::process::Command, what: &str) -> NestResult<serde_json::Value> {
    let out = cmd
        .output()
        .await
        .map_err(|e| NestError::Io(format!("running hf: {e}")))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let msg = err
            .lines()
            .chain(stdout.lines())
            .rfind(|l| !l.trim().is_empty())
            .unwrap_or("failed");
        return Err(NestError::Invalid(format!("{what}: {}", msg.trim())));
    }
    Ok(stdout
        .lines()
        .rev()
        .find_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .unwrap_or(serde_json::Value::Null))
}

/// `model` or `dataset` for hf's --repo-type.
fn repo_type(kind: &str) -> &'static str {
    if kind.starts_with("dataset") {
        "dataset"
    } else {
        "model"
    }
}

/// Search the Hub (most downloaded first).
pub async fn search(hf: &Path, kind: &str, q: &str, limit: u32) -> NestResult<serde_json::Value> {
    let mut cmd = tokio::process::Command::new(hf);
    cmd.arg(if repo_type(kind) == "dataset" {
        "datasets"
    } else {
        "models"
    })
    .args([
        "ls",
        "--search",
        q,
        "--sort",
        "downloads",
        "--json",
        "--limit",
    ])
    .arg(limit.to_string())
    .env("HF_HUB_DISABLE_TELEMETRY", "1");
    hf_json(cmd, "hf search").await
}

/// "548.1M" (hf's human sizes, decimal) in bytes.
fn parse_size(s: &str) -> u64 {
    let s = s.trim();
    let (num, mult) = match s.chars().last() {
        Some('K') => (&s[..s.len() - 1], 1e3),
        Some('M') => (&s[..s.len() - 1], 1e6),
        Some('G') => (&s[..s.len() - 1], 1e9),
        Some('T') => (&s[..s.len() - 1], 1e12),
        Some('P') => (&s[..s.len() - 1], 1e15),
        _ => (s, 1.0),
    };
    (num.parse::<f64>().unwrap_or(0.0) * mult) as u64
}

/// Files and bytes a download of `repo` would fetch in all (hf's dry run).
/// A repo on the Hub against what the hub cache holds (hf's dry run).
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct DownloadSize {
    /// Files in the repo (at the revision).
    pub files: u64,
    /// Files and bytes hf would download: not in the cache anywhere in the
    /// cluster.
    pub missing_files: u64,
    pub missing_bytes: u64,
}

/// The revision the hub cache holds of `repo` (its `refs/main`), so that
/// finishing a download fetches the files of that snapshot, not a newer one.
/// The hub directory of a repo: `models--org--name` or `datasets--org--name`.
pub fn repo_dir(kind: &str, repo: &str) -> String {
    format!(
        "{}s--{}",
        repo_type(kind),
        repo.split('@').next().unwrap_or(repo).replace('/', "--")
    )
}

pub fn cached_revision(mount_hub: &Path, repo: &str, kind: &str) -> Option<String> {
    let dir = repo_dir(kind, repo);
    let r = std::fs::read_to_string(mount_hub.join(dir).join("refs/main")).ok()?;
    let r = r.trim();
    (r.len() == 40 && r.bytes().all(|b| b.is_ascii_hexdigit())).then(|| r.to_string())
}

/// `revision`, or the one the cache holds, or none (the latest).
fn resolve_revision(
    mount_hub: &Path,
    repo: &str,
    kind: &str,
    revision: Option<&str>,
) -> Option<String> {
    revision
        .map(str::to_string)
        .or_else(|| cached_revision(mount_hub, repo, kind))
}

pub async fn download_size(
    hf: &Path,
    mount_hub: &Path,
    repo: &str,
    kind: &str,
    revision: Option<&str>,
) -> NestResult<DownloadSize> {
    let revision = resolve_revision(mount_hub, repo, kind, revision);
    let mut cmd = tokio::process::Command::new(hf);
    cmd.args([
        "download",
        repo,
        "--repo-type",
        repo_type(kind),
        "--dry-run",
        "--json",
    ])
    .arg("--cache-dir")
    .arg(mount_hub);
    if let Some(r) = &revision {
        cmd.args(["--revision", r]);
    }
    hf_env(&mut cmd, mount_hub);
    let v = hf_json(cmd, "hf download --dry-run").await?;
    let files = v.as_array().cloned().unwrap_or_default();
    let mut s = DownloadSize {
        files: files.len() as u64,
        ..Default::default()
    };
    // A file the cache holds is listed with size "-".
    for f in &files {
        let size = f["size"].as_str().unwrap_or("0").trim();
        if size != "-" {
            s.missing_files += 1;
            s.missing_bytes += parse_size(size);
        }
    }
    Ok(s)
}

/// Run `hf download` into the hub through this node's mount (so the files
/// are written here first); `stop` is polled to cancel.
pub async fn download(
    hf: &Path,
    mount_hub: &Path,
    repo: &str,
    kind: &str,
    revision: Option<&str>,
    stop: impl Fn() -> bool,
) -> NestResult<()> {
    let revision = resolve_revision(mount_hub, repo, kind, revision);
    let mut cmd = tokio::process::Command::new(hf);
    cmd.args(["download", repo, "--repo-type", repo_type(kind)])
        .arg("--cache-dir")
        .arg(mount_hub)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    if let Some(r) = &revision {
        cmd.args(["--revision", r]);
    }
    hf_env(&mut cmd, mount_hub);
    let mut child = cmd
        .spawn()
        .map_err(|e| NestError::Io(format!("running hf: {e}")))?;
    let mut stderr = child.stderr.take();
    let tail = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(e) = stderr.as_mut() {
            use tokio::io::AsyncReadExt;
            let _ = e.read_to_end(&mut buf).await;
        }
        buf
    });
    let status = loop {
        tokio::select! {
            s = child.wait() => break s.map_err(|e| NestError::Io(e.to_string()))?,
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                if stop() {
                    let _ = child.kill().await;
                    return Err(NestError::Io("cancelled; what finished stays".into()));
                }
            }
        }
    };
    let err = tail.await.unwrap_or_default();
    if status.success() {
        return Ok(());
    }
    let err = String::from_utf8_lossy(&err);
    let msg = err
        .lines()
        .rfind(|l| !l.trim().is_empty())
        .unwrap_or("failed");
    Err(NestError::Invalid(format!("hf download: {}", msg.trim())))
}

/// Run `hf cache rm` on sparknest's hub (through this node's mount, so the
/// files go on every host and archive), with HF_HOME pointing at it: hf
/// knows which of the hub's shared blobs other repos still use. Returns
/// hf's JSON summary.
pub async fn remove(
    hf: &Path,
    mount_hub: &Path,
    targets: &[String],
    dry_run: bool,
) -> NestResult<serde_json::Value> {
    let mut cmd = tokio::process::Command::new(hf);
    cmd.arg("cache")
        .arg("rm")
        .args(targets.iter().map(|t| rm_target(t)))
        .arg("--cache-dir")
        .arg(mount_hub)
        .arg("--json")
        .arg(if dry_run { "--dry-run" } else { "--yes" });
    hf_env(&mut cmd, mount_hub);
    let out = cmd
        .output()
        .await
        .map_err(|e| NestError::Io(format!("running {}: {e}", hf.display())))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let summary = stdout
        .lines()
        .rev()
        .find_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok());
    match (out.status.success(), summary) {
        (true, Some(v)) => Ok(v),
        (true, None) => Ok(serde_json::json!({ "output": stdout.trim() })),
        (false, _) => {
            let err = String::from_utf8_lossy(&out.stderr);
            let msg = err
                .lines()
                .chain(stdout.lines())
                .rfind(|l| !l.trim().is_empty())
                .unwrap_or("failed");
            Err(NestError::Invalid(format!("hf cache rm: {}", msg.trim())))
        }
    }
}

#[cfg(test)]
mod rm_tests {
    /// With hf installed: a dry run reports, a delete removes the repo.
    #[tokio::test]
    async fn hf_removes_a_repo_from_a_hub() {
        let Some(hf) = super::find_hf() else { return };
        let hub = std::env::temp_dir().join(format!("nest-hf-rm-{}/hub", std::process::id()));
        let r = hub.join("models--org--tiny");
        std::fs::create_dir_all(r.join("blobs")).unwrap();
        std::fs::create_dir_all(r.join("snapshots/abc")).unwrap();
        std::fs::create_dir_all(r.join("refs")).unwrap();
        std::fs::write(r.join("blobs/e1"), b"weights").unwrap();
        std::os::unix::fs::symlink("../../blobs/e1", r.join("snapshots/abc/model.bin")).unwrap();
        std::fs::write(r.join("refs/main"), b"abc").unwrap();
        let t = vec!["hf:org/tiny".to_string()];
        let d = super::remove(&hf, &hub, &t, true).await.unwrap();
        assert_eq!(d["repos"], 1, "{d}");
        assert!(r.exists());
        let v = super::remove(&hf, &hub, &t, false).await.unwrap();
        assert_eq!(v["repos_deleted"], 1, "{v}");
        assert!(!r.exists());
        let _ = std::fs::remove_dir_all(hub.parent().unwrap());
    }

    #[test]
    fn hf_sizes_parse() {
        assert_eq!(super::parse_size("548.1M"), 548_100_000);
        assert_eq!(super::parse_size("445.0"), 445);
        assert_eq!(super::parse_size("8.1K"), 8_100);
        assert_eq!(super::parse_size("1.5T"), 1_500_000_000_000);
    }

    #[test]
    fn targets_are_what_hf_expects() {
        use super::rm_target;
        assert_eq!(rm_target("hf:Qwen/Qwen3-8B"), "model/Qwen/Qwen3-8B");
        assert_eq!(rm_target("Qwen/Qwen3-8B"), "model/Qwen/Qwen3-8B");
        assert_eq!(rm_target("gpt2"), "model/gpt2");
        assert_eq!(rm_target("datasets/cais/mmlu"), "dataset/cais/mmlu");
        assert_eq!(rm_target("dataset/cais/mmlu"), "dataset/cais/mmlu");
        assert_eq!(rm_target("hf-dataset:cais/mmlu"), "dataset/cais/mmlu");
        assert_eq!(rm_target("hf:org/m@main"), "model/org/m");
    }
}

// ------------------------------------------------------------ fetch driver
//
// Downloads go through `sparknest-hf-fetch` (tools/hf-fetch), a small
// Python program run with the interpreter of the `hf` command, so it uses
// the same huggingface_hub and hf_xet. It plans a repo (its files and sizes
// at one commit) and fetches a list of files one after another, reporting
// bytes as they arrive. The host that runs it writes the files through its
// mount, so that is where their first copy lands.

/// The Python interpreter of the `hf` command (from its `#!` line).
pub fn hf_python(hf: &Path) -> Option<PathBuf> {
    let head = std::fs::read(hf).ok()?;
    let line = head.split(|b| *b == b'\n').next()?;
    let line = std::str::from_utf8(line).ok()?.strip_prefix("#!")?.trim();
    // `#!/usr/bin/env python3` names the interpreter after env.
    let mut words = line.split_whitespace();
    let first = words.next()?;
    let py = if first.ends_with("/env") {
        words.next()?
    } else {
        first
    };
    let p = PathBuf::from(py);
    if p.is_absolute() {
        p.is_file().then_some(p)
    } else {
        std::env::var_os("PATH").and_then(|path| {
            std::env::split_paths(&path)
                .map(|d| d.join(py))
                .find(|c| c.is_file())
        })
    }
}

/// The driver: `$SPARKNEST_HF_FETCH`, beside this executable, or in
/// `../libexec/sparknest/` (the Homebrew layout).
pub fn fetch_driver() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("SPARKNEST_HF_FETCH") {
        let p = PathBuf::from(p);
        return p.is_file().then_some(p);
    }
    let exe = std::env::current_exe().ok()?;
    let dir = exe.parent()?;
    [
        dir.join("sparknest-hf-fetch"),
        dir.join("../libexec/sparknest/sparknest-hf-fetch"),
    ]
    .into_iter()
    .find(|p| p.is_file())
}

/// The interpreter and driver to run, or why there are none.
pub fn driver(hf: Option<&Path>) -> NestResult<(PathBuf, PathBuf)> {
    let hf = hf
        .map(Path::to_path_buf)
        .filter(|p| p.is_file())
        .or_else(find_hf)
        .ok_or_else(|| NestError::Invalid("hf is not installed on this host".into()))?;
    let py = hf_python(&hf).ok_or_else(|| {
        NestError::Invalid(format!("cannot tell which Python runs {}", hf.display()))
    })?;
    let drv = fetch_driver().ok_or_else(|| {
        NestError::Invalid("sparknest-hf-fetch is not installed beside sparknestd".into())
    })?;
    Ok((py, drv))
}

/// A repo at one commit: its files and their sizes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepoPlan {
    pub sha: String,
    pub files: Vec<PlanFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanFile {
    pub name: String,
    pub size: u64,
}

/// List `repo` at `revision` (default: its main branch) on the Hub.
pub async fn repo_plan(
    hf: Option<&Path>,
    mount_hub: &Path,
    repo: &str,
    kind: &str,
    revision: Option<&str>,
) -> NestResult<RepoPlan> {
    let (py, drv) = driver(hf)?;
    let mut cmd = tokio::process::Command::new(py);
    cmd.arg(drv)
        .args(["plan", repo, "--repo-type", repo_type(kind)]);
    if let Some(r) = revision {
        cmd.args(["--revision", r]);
    }
    hf_env(&mut cmd, mount_hub);
    let v = hf_json(cmd, "sparknest-hf-fetch plan").await?;
    serde_json::from_value(v).map_err(|e| NestError::Io(format!("unexpected plan: {e}")))
}

/// One host's share of a download, as it goes.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FetchProgress {
    pub files: Vec<FetchFile>,
    pub finished: bool,
    pub cancelled: bool,
    /// The driver itself failed (not one file).
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FetchFile {
    pub name: String,
    pub size: u64,
    pub done: u64,
    pub state: FetchState,
    pub error: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FetchState {
    #[default]
    Queued,
    Running,
    Done,
    Failed,
}

impl FetchProgress {
    pub fn new(files: &[(String, u64)]) -> FetchProgress {
        FetchProgress {
            files: files
                .iter()
                .map(|(name, size)| FetchFile {
                    name: name.clone(),
                    size: *size,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Apply one line the driver printed; false if it was not one of its
    /// reports.
    pub fn apply(&mut self, line: &str) -> bool {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            return false;
        };
        let Some(name) = v["file"].as_str() else {
            return false;
        };
        let Some(f) = self.files.iter_mut().find(|f| f.name == name) else {
            return false;
        };
        if let Some(e) = v["error"].as_str() {
            f.state = FetchState::Failed;
            f.error = e.to_string();
        } else if v["ok"].as_bool() == Some(true) {
            f.state = FetchState::Done;
            f.done = f.size.max(f.done);
        } else if let Some(d) = v["done"].as_u64() {
            f.state = FetchState::Running;
            f.done = d;
            if let Some(t) = v["total"].as_u64().filter(|t| *t > 0) {
                f.size = t;
            }
        }
        true
    }

    pub fn done_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.done.min(f.size)).sum()
    }
}

/// Fetch `progress`'s files of `repo` at commit `sha` into the hub through
/// this host's mount, one after another, until done or cancelled.
pub async fn fetch_files(
    hf: Option<&Path>,
    mount_hub: &Path,
    repo: &str,
    kind: &str,
    sha: &str,
    progress: Arc<Mutex<FetchProgress>>,
) -> NestResult<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let (py, drv) = driver(hf)?;
    let mut cmd = tokio::process::Command::new(py);
    cmd.arg(drv)
        .args([
            "fetch",
            repo,
            "--repo-type",
            repo_type(kind),
            "--revision",
            sha,
        ])
        .arg("--cache-dir")
        .arg(mount_hub)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    hf_env(&mut cmd, mount_hub);
    let mut child = cmd
        .spawn()
        .map_err(|e| NestError::Io(format!("running sparknest-hf-fetch: {e}")))?;
    let names: String = progress
        .lock()
        .files
        .iter()
        .map(|f| format!("{}\n", f.name))
        .collect();
    let mut stdin = child.stdin.take().expect("piped");
    stdin
        .write_all(names.as_bytes())
        .await
        .map_err(|e| NestError::Io(e.to_string()))?;
    drop(stdin);
    let mut stderr = child.stderr.take().expect("piped");
    let tail = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let _ = stderr.read_to_end(&mut buf).await;
        buf
    });
    let mut lines = tokio::io::BufReader::new(child.stdout.take().expect("piped")).lines();
    loop {
        tokio::select! {
            l = lines.next_line() => match l {
                Ok(Some(l)) => { progress.lock().apply(&l); }
                _ => break,
            },
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
        }
        if progress.lock().cancelled {
            let _ = child.kill().await;
            return Err(NestError::Io("cancelled".into()));
        }
    }
    let status = child
        .wait()
        .await
        .map_err(|e| NestError::Io(e.to_string()))?;
    if !status.success() {
        let err = tail.await.unwrap_or_default();
        let err = String::from_utf8_lossy(&err);
        let msg = err
            .lines()
            .rfind(|l| !l.trim().is_empty())
            .unwrap_or("failed")
            .to_string();
        return Err(NestError::Io(format!("sparknest-hf-fetch: {msg}")));
    }
    Ok(())
}

/// Hosts to fetch files on, each with room to fill (free space above its
/// margin) and what it was given so far.
#[derive(Clone, Debug)]
pub struct FetchHost {
    pub name: String,
    pub room: u64,
}

/// Assign files to hosts: largest first, each to the host with the least
/// assigned so far that has room for it. Returns host indexes per file (in
/// the order given), or None for a file no host has room for.
pub fn assign_files(files: &[(String, u64)], hosts: &[FetchHost]) -> Vec<Option<usize>> {
    let mut order: Vec<usize> = (0..files.len()).collect();
    order.sort_by(|&a, &b| {
        files[b]
            .1
            .cmp(&files[a].1)
            .then(files[a].0.cmp(&files[b].0))
    });
    let mut given = vec![0u64; hosts.len()];
    let mut out = vec![None; files.len()];
    for i in order {
        let size = files[i].1;
        let best = (0..hosts.len())
            .filter(|&h| hosts[h].room.saturating_sub(given[h]) >= size)
            .min_by_key(|&h| (given[h], h));
        if let Some(h) = best {
            given[h] += size;
            out[i] = Some(h);
        }
    }
    out
}

/// Which host fetches which file next: batches per host (a big file
/// alone, a host's small files together), retries, and hosts that keep
/// failing. A failed file is retried once on its host, then on another; a
/// host that fails twice in a row (it cannot reach the Hub, say) gets no
/// more files and its queued ones move to the others at once.
pub struct Schedule {
    pub hosts: Vec<FetchHost>,
    /// Bytes and files each host is to hold (assigned, plus moved in,
    /// minus moved out).
    pub given: Vec<u64>,
    pub given_files: Vec<u64>,
    pub bad: Vec<bool>,
    strikes: Vec<u32>,
    queue: std::collections::VecDeque<(usize, Vec<(String, u64)>)>,
    attempts: std::collections::HashMap<String, u32>,
    /// Files given up on.
    pub failed: Vec<String>,
    /// What people should know (hosts dropped).
    pub notes: Vec<String>,
}

/// Files below this go in one batch per host.
const SMALL_FILE: u64 = 64 << 20;
/// Failures in a row that drop a host.
const STRIKES: u32 = 2;

impl Schedule {
    pub fn new(
        hosts: Vec<FetchHost>,
        files: &[(String, u64)],
        assigned: &[Option<usize>],
    ) -> Schedule {
        let n = hosts.len();
        let mut given = vec![0u64; n];
        let mut given_files = vec![0u64; n];
        let mut small: std::collections::BTreeMap<usize, Vec<(String, u64)>> = Default::default();
        let mut big: Vec<(usize, (String, u64))> = Vec::new();
        for (f, a) in files.iter().zip(assigned) {
            let Some(h) = *a else { continue };
            given[h] += f.1;
            given_files[h] += 1;
            if f.1 >= SMALL_FILE {
                big.push((h, f.clone()));
            } else {
                small.entry(h).or_default().push(f.clone());
            }
        }
        big.sort_by_key(|b| std::cmp::Reverse(b.1.1));
        let mut queue: std::collections::VecDeque<_> = small.into_iter().collect();
        queue.extend(big.into_iter().map(|(h, f)| (h, vec![f])));
        Schedule {
            hosts,
            given,
            given_files,
            bad: vec![false; n],
            strikes: vec![0; n],
            queue,
            attempts: Default::default(),
            failed: Vec::new(),
            notes: Vec::new(),
        }
    }

    /// The next batch to start, never on a dropped host.
    pub fn next_batch(&mut self) -> Option<(usize, Vec<(String, u64)>)> {
        while let Some((h, files)) = self.queue.pop_front() {
            if !self.bad[h] {
                return Some((h, files));
            }
            for f in files {
                self.move_away(h, f);
            }
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn done(&mut self, host: usize) {
        self.strikes[host] = 0;
    }

    /// `file` failed on `host` (or the host could not be asked).
    pub fn failed(&mut self, host: usize, file: (String, u64), why: &str) {
        let n = {
            let a = self.attempts.entry(file.0.clone()).or_default();
            *a += 1;
            *a
        };
        self.strikes[host] += 1;
        if self.strikes[host] >= STRIKES && !self.bad[host] {
            self.bad[host] = true;
            self.notes.push(format!(
                "{}: {} fetches failed in a row ({why}); its files go to other hosts",
                self.hosts[host].name, self.strikes[host]
            ));
            // Its queued files move now, not when their turn comes.
            let mine: Vec<_> = self
                .queue
                .iter()
                .filter(|(h, _)| *h == host)
                .cloned()
                .collect();
            self.queue.retain(|(h, _)| *h != host);
            for (_, fs) in mine {
                for f in fs {
                    self.move_away(host, f);
                }
            }
        }
        if n >= 3 {
            self.failed.push(file.0);
        } else if n == 1 && !self.bad[host] {
            self.queue.push_back((host, vec![file]));
        } else {
            self.move_away(host, file);
        }
    }

    /// Give `file` to the least loaded other good host with room.
    fn move_away(&mut self, from: usize, file: (String, u64)) {
        let to = (0..self.hosts.len())
            .filter(|&h| h != from && !self.bad[h])
            .filter(|&h| self.hosts[h].room.saturating_sub(self.given[h]) >= file.1)
            .min_by_key(|&h| self.given[h]);
        match to {
            Some(to) => {
                self.given[from] = self.given[from].saturating_sub(file.1);
                self.given[to] += file.1;
                self.given_files[from] = self.given_files[from].saturating_sub(1);
                self.given_files[to] += 1;
                self.queue.push_back((to, vec![file]));
            }
            None => self.failed.push(file.0),
        }
    }
}

#[cfg(test)]
mod fetch_tests {
    use super::*;

    #[test]
    fn files_are_spread_evenly_largest_first_within_room() {
        let gb = 1u64 << 30;
        let files: Vec<(String, u64)> = (0..6)
            .map(|i| (format!("model-{i}.safetensors"), 5 * gb))
            .chain([("config.json".to_string(), 1000)])
            .collect();
        let hosts = vec![
            FetchHost {
                name: "a".into(),
                room: 100 * gb,
            },
            FetchHost {
                name: "b".into(),
                room: 100 * gb,
            },
            FetchHost {
                name: "c".into(),
                room: 7 * gb,
            },
        ];
        let got = assign_files(&files, &hosts);
        let per = |h| got.iter().filter(|g| **g == Some(h)).count();
        // c has room for one shard; a and b share the rest evenly.
        assert_eq!(per(2), 2, "{got:?}"); // one shard and the config
        assert_eq!(per(0) + per(1), 5);
        assert!(per(0).abs_diff(per(1)) <= 1);
        // Nobody has room: unplaced.
        let none = assign_files(&[("huge".into(), 500 * gb)], &hosts);
        assert_eq!(none, vec![None]);
    }

    #[test]
    fn driver_lines_update_progress() {
        let mut p = FetchProgress::new(&[("a".into(), 100), ("b".into(), 50)]);
        assert!(p.apply(r#"{"file": "a", "done": 40, "total": 100}"#));
        assert_eq!(p.files[0].state, FetchState::Running);
        assert!(p.apply(r#"{"file": "a", "ok": true}"#));
        assert!(p.apply(r#"{"file": "b", "error": "HTTPError: 404"}"#));
        assert!(!p.apply("warning: something"));
        assert!(!p.apply(r#"{"file": "zzz", "ok": true}"#));
        assert_eq!(p.done_bytes(), 100);
        assert_eq!(p.files[1].state, FetchState::Failed);
        assert_eq!(p.files[1].error, "HTTPError: 404");
    }

    fn hosts(n: usize) -> Vec<FetchHost> {
        (0..n)
            .map(|i| FetchHost {
                name: format!("h{i}"),
                room: 1 << 40,
            })
            .collect()
    }

    #[test]
    fn a_host_that_keeps_failing_is_dropped_and_its_files_move() {
        let gb = 1u64 << 30;
        let files: Vec<(String, u64)> = (0..6).map(|i| (format!("f{i}"), gb)).collect();
        let assigned: Vec<Option<usize>> = (0..6).map(|i| Some(i % 2)).collect();
        let mut s = Schedule::new(hosts(2), &files, &assigned);
        // Host 1 cannot reach the Hub: its first file fails twice.
        let mut on = std::collections::HashMap::new();
        while let Some((h, fs)) = s.next_batch() {
            for f in fs {
                *on.entry(h).or_insert(0) += 1;
                if h == 1 {
                    s.failed(h, f, "LocalEntryNotFoundError");
                } else {
                    s.done(h);
                }
            }
        }
        assert!(s.bad[1] && !s.bad[0]);
        assert!(
            s.failed.is_empty(),
            "everything ended up on host 0: {:?}",
            s.failed
        );
        assert_eq!(on[&0], 6, "{on:?}");
        assert_eq!(on[&1], 2, "host 1 was tried twice, then dropped");
        assert_eq!(s.given, vec![6 * gb, 0]);
        assert_eq!(s.given_files, vec![6, 0]);
        assert!(s.notes[0].starts_with("h1: 2 fetches failed"));
    }

    #[test]
    fn one_failure_retries_on_the_same_host_and_all_bad_gives_up() {
        let mut s = Schedule::new(hosts(2), &[("a".into(), 100 << 20)], &[Some(0)]);
        let (h, fs) = s.next_batch().unwrap();
        s.failed(h, fs[0].clone(), "timeout");
        assert_eq!(
            s.next_batch().unwrap().0,
            0,
            "a single failure stays on its host"
        );
        // Both hosts fail: given up after three tries, not lost silently.
        let mut s = Schedule::new(hosts(2), &[("a".into(), 100 << 20)], &[Some(0)]);
        while let Some((h, fs)) = s.next_batch() {
            for f in fs {
                s.failed(h, f, "down");
            }
        }
        assert_eq!(s.failed, vec!["a".to_string()]);
    }
}
