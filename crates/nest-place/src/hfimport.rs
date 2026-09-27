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
}

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
            nest_data::vfs::oflags::WRONLY,
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
) -> NestResult<()> {
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
        import_entries(&vfs, entries, false, opts.copy, &progress).await?;
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
                mirror_snapshot(&vfs, &base, commit, files).await?;
            }
        }
        for (name, commit) in &r.refs {
            write_small(&vfs, &format!("{base}/refs/{name}"), commit.as_bytes()).await?;
        }
        // 3. Verify every source snapshot file.
        let mut bad = Vec::new();
        for (commit, files) in &r.snapshots {
            for (rel, f) in files {
                let want = match f {
                    SnapFile::Blob(e) => r.blobs[e].1,
                    SnapFile::Plain(p) => std::fs::metadata(p).map(|m| m.len()).unwrap_or(0),
                };
                if settled_size(&vfs, &format!("{base}/snapshots/{commit}/{rel}"))? != Some(want) {
                    bad.push(rel.clone());
                }
            }
        }
        if bad.is_empty() {
            verified_dirs.push(r.dir.clone());
        } else {
            all_verified = false;
            out.status = "failed".into();
            out.note = format!(
                "{} file(s) did not verify, e.g. {}",
                bad.len(),
                bad.first().cloned().unwrap_or_default()
            );
        }
        outcomes.lock().push(out);
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
