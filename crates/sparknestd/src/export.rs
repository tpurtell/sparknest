//! `sparknestd export`: rebuild the namespace as an ordinary directory tree
//! from a metadata snapshot and object directories, with no cluster
//! (PROPOSAL §8 disaster recovery; ADR-005's promise that the object layout
//! stays recoverable).

use anyhow::{Context, Result, bail};
use nest_types::FileKind;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

pub struct ExportArgs {
    pub meta: PathBuf,
    pub objects: Vec<PathBuf>,
    pub out: PathBuf,
    pub path: String,
    pub link: bool,
}

#[derive(Default, Debug)]
pub struct ExportReport {
    pub dirs: u64,
    pub files: u64,
    pub bytes: u64,
    pub symlinks: u64,
    pub missing: Vec<String>,
}

fn object_path(roots: &[PathBuf], file: u64, generation: u64) -> Option<PathBuf> {
    let name = format!("{:016x}.{:x}", file, generation);
    let sub = format!("{:02x}", file & 0xff);
    for r in roots {
        for base in [r.join("objects"), r.join("backups").join("objects")] {
            let p = base.join(&sub).join(&name);
            if p.is_file() {
                return Some(p);
            }
        }
    }
    None
}

pub fn run(a: &ExportArgs) -> Result<ExportReport> {
    let c =
        nest_meta::open_read(&a.meta).with_context(|| format!("opening {}", a.meta.display()))?;
    let entries =
        nest_place::backup::walk_structure(&c, &a.path).map_err(|e| anyhow::anyhow!("{e}"))?;
    if entries.is_empty() {
        bail!("{} not found in the snapshot", a.path);
    }
    let base = a.path.trim_end_matches('/');
    let dest = |p: &str| -> PathBuf {
        let rel = p.strip_prefix(base).unwrap_or(p).trim_start_matches('/');
        // Entries reached through symlinks outside `path` keep their full path.
        let rel = if p.starts_with(base) {
            rel.to_string()
        } else {
            format!("_outside{p}")
        };
        a.out.join(rel)
    };
    let mut r = ExportReport::default();
    std::fs::create_dir_all(&a.out)?;
    for e in &entries {
        let d = dest(&e.path);
        match e.kind {
            FileKind::Directory => {
                std::fs::create_dir_all(&d)?;
                r.dirs += 1;
            }
            FileKind::Symlink => {
                if let Some(parent) = d.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let t =
                    String::from_utf8_lossy(e.target.as_deref().unwrap_or_default()).into_owned();
                let _ = std::fs::remove_file(&d);
                std::os::unix::fs::symlink(&t, &d)?;
                r.symlinks += 1;
            }
            FileKind::Regular => {
                if let Some(parent) = d.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let Some(src) = object_path(&a.objects, e.file.0, e.generation.0) else {
                    r.missing.push(e.path.clone());
                    continue;
                };
                let _ = std::fs::remove_file(&d);
                let linked = a.link && std::fs::hard_link(&src, &d).is_ok();
                if !linked {
                    std::fs::copy(&src, &d)
                        .with_context(|| format!("copying {}", src.display()))?;
                }
                if !linked {
                    std::fs::set_permissions(&d, std::fs::Permissions::from_mode(e.perm & 0o7777))?;
                    let f = std::fs::File::options().write(true).open(&d)?;
                    f.set_times(std::fs::FileTimes::new().set_modified(e.mtime.as_system_time()))?;
                }
                r.files += 1;
                r.bytes += e.size;
            }
        }
    }
    Ok(r)
}
