//! Resolving selectors to manifests.

use crate::spec::Selector;
use nest_meta::query;
use nest_types::{FileId, FileKind, GenState, Generation, NestError, NestResult};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// One regular file a selection needs, at its current generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub file: FileId,
    pub generation: Generation,
    pub size: u64,
    pub stable: bool,
    pub path: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub entries: Vec<Entry>,
    /// Symlinks whose targets do not resolve inside the namespace.
    pub dangling: Vec<String>,
}

impl Manifest {
    pub fn bytes(&self) -> u64 {
        self.entries.iter().map(|e| e.size).sum()
    }
}

fn nf(what: &str) -> NestError {
    NestError::Invalid(format!("{what} not found"))
}

fn sql(e: rusqlite::Error) -> NestError {
    NestError::Io(format!("metadata: {e}"))
}

/// Resolve `path` (absolute, may traverse symlinks in intermediate
/// components) to a file id and the directory containing it.
pub fn resolve_path(c: &Connection, path: &str) -> NestResult<(FileId, FileId)> {
    let mut stack: Vec<Vec<u8>> = path
        .split('/')
        .filter(|p| !p.is_empty())
        .map(|p| p.as_bytes().to_vec())
        .collect();
    stack.reverse();
    let mut dir = FileId::ROOT;
    let mut cur = FileId::ROOT;
    let mut hops = 0;
    while let Some(comp) = stack.pop() {
        if comp == b"." {
            continue;
        }
        if comp == b".." {
            dir = query::dir_parent(c, dir)
                .map_err(sql)?
                .unwrap_or(FileId::ROOT);
            cur = dir;
            continue;
        }
        let id = query::lookup(c, dir, &comp)
            .map_err(sql)?
            .ok_or_else(|| nf(path))?;
        let a = query::getattr(c, id)
            .map_err(sql)?
            .ok_or_else(|| nf(path))?;
        if a.kind == FileKind::Symlink && !stack.is_empty() {
            hops += 1;
            if hops > 40 {
                return Err(NestError::Invalid("too many symlinks".into()));
            }
            let t = query::readlink(c, id).map_err(sql)?.unwrap_or_default();
            if t.starts_with(b"/") {
                dir = FileId::ROOT;
            }
            let mut parts: Vec<Vec<u8>> = t
                .split(|b| *b == b'/')
                .filter(|p| !p.is_empty())
                .map(|p| p.to_vec())
                .collect();
            parts.reverse();
            stack.extend(parts);
            continue;
        }
        if !stack.is_empty() {
            if a.kind != FileKind::Directory {
                return Err(NestError::NotDir);
            }
            dir = id;
        }
        cur = id;
    }
    Ok((cur, dir))
}

struct Walker<'a> {
    c: &'a Connection,
    seen: HashSet<FileId>,
    out: Manifest,
}

impl Walker<'_> {
    fn add(&mut self, id: FileId, dir: FileId, path: String) -> NestResult<()> {
        if !self.seen.insert(id) {
            return Ok(());
        }
        let Some(a) = query::getattr(self.c, id).map_err(sql)? else {
            return Ok(());
        };
        match a.kind {
            FileKind::Regular => self.out.entries.push(Entry {
                file: id,
                generation: a.generation,
                size: a.size,
                stable: a.gen_state == GenState::Stable,
                path,
            }),
            FileKind::Directory => {
                let mut after = 0;
                loop {
                    let batch = query::readdir(self.c, id, after, 1024).map_err(sql)?;
                    if batch.is_empty() {
                        break;
                    }
                    for (cookie, e) in batch {
                        after = cookie;
                        let child = format!("{path}/{}", String::from_utf8_lossy(&e.name));
                        self.add(e.id, id, child)?;
                    }
                }
            }
            FileKind::Symlink => {
                let t = query::readlink(self.c, id)
                    .map_err(sql)?
                    .unwrap_or_default();
                let t = String::from_utf8_lossy(&t).into_owned();
                let target = if t.starts_with('/') {
                    t.clone()
                } else {
                    let base = query::path_of(self.c, dir)
                        .map_err(sql)?
                        .unwrap_or_else(|| b"/".to_vec());
                    format!(
                        "{}/{t}",
                        String::from_utf8_lossy(&base).trim_end_matches('/')
                    )
                };
                match resolve_path(self.c, &target) {
                    Ok((tid, tdir)) => {
                        let tpath = query::path_of(self.c, tid)
                            .map_err(sql)?
                            .map(|p| String::from_utf8_lossy(&p).into_owned())
                            .unwrap_or(target);
                        self.add(tid, tdir, tpath)?;
                    }
                    Err(_) => self.out.dangling.push(path),
                }
            }
        }
        Ok(())
    }
}

/// Everything `root` needs: every regular file in it and every file its
/// symlinks reach, each once.
pub fn resolve_tree(c: &Connection, root: &str) -> NestResult<Manifest> {
    let (id, dir) = resolve_path(c, root)?;
    let mut w = Walker {
        c,
        seen: HashSet::new(),
        out: Manifest::default(),
    };
    w.add(id, dir, root.trim_end_matches('/').to_string())?;
    Ok(w.out)
}

/// Resolve a selector. `read_small` reads a small file's content (for HF
/// `refs/<branch>`), which may live on another node.
pub async fn resolve<F, Fut>(
    c_open: impl Fn() -> NestResult<Connection>,
    sel: &Selector,
    read_small: F,
) -> NestResult<Manifest>
where
    F: Fn(FileId) -> Fut,
    Fut: std::future::Future<Output = NestResult<Vec<u8>>>,
{
    match sel {
        Selector::Path { path } => resolve_tree(&c_open()?, path),
        Selector::Hf {
            hub,
            repo,
            revision,
            repo_type,
        } => {
            let prefix = if repo_type == "dataset" {
                "datasets"
            } else {
                "models"
            };
            let dir = format!(
                "{}/{prefix}--{}",
                hub.trim_end_matches('/'),
                repo.replace('/', "--")
            );
            let Some(rev) = revision else {
                return resolve_tree(&c_open()?, &dir);
            };
            let commit = if rev.len() == 40 && rev.bytes().all(|b| b.is_ascii_hexdigit()) {
                rev.clone()
            } else {
                let (rid, _) = resolve_path(&c_open()?, &format!("{dir}/refs/{rev}"))?;
                let bytes = read_small(rid).await?;
                String::from_utf8_lossy(&bytes).trim().to_string()
            };
            let c = c_open()?;
            let mut m = resolve_tree(&c, &format!("{dir}/snapshots/{commit}"))?;
            for extra in [format!("{dir}/refs"), format!("{dir}/trees/{commit}.json")] {
                if let Ok(x) = resolve_tree(&c, &extra) {
                    let have: HashSet<FileId> = m.entries.iter().map(|e| e.file).collect();
                    m.entries
                        .extend(x.entries.into_iter().filter(|e| !have.contains(&e.file)));
                }
            }
            Ok(m)
        }
    }
}
