//! Where storage goes: a size-weighted tree for treemaps, for one host, one
//! archive store, or the whole cluster.
//!
//! Two shapes of the same bytes:
//! - **path**: the namespace as it is, directories summing their files.
//! - **models**: Hugging Face repos first. Each repo holds the files its
//!   snapshots point at, named as the snapshot names them
//!   (`model-00001-of-00004.safetensors`), wherever the blob physically
//!   lives (a repo's own `blobs/` or the shared `/hub/blobs`); a file two
//!   repos share counts once, for the first. Everything else follows under
//!   "other files" in path shape.
//!
//! Weights: a file's logical size, or its size times its copies (what it
//! costs the cluster). Scoped to one store, a file weighs its size there.

use crate::selector;
use nest_meta::query;
use nest_types::{FileId, FileKind, NestError, NestResult, StoreId};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Weight {
    Logical,
    Copies,
}

#[derive(Clone, Debug)]
pub struct TreeReq {
    /// `None`: every store; else one live or archive store.
    pub store: Option<StoreId>,
    pub weight: Weight,
    pub models: bool,
    /// Path to start from (path shape only).
    pub root: String,
    pub depth: u32,
    /// Children per node beyond which the smallest fold into one "more".
    pub max_children: usize,
    pub hub: String,
    /// Store id → display name, for leaves' `hosts`.
    pub store_names: HashMap<StoreId, String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TreeNode {
    pub name: String,
    /// Namespace path (directories and files) — what actions select.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// `dir`, `file`, `repo`, `revision`, `group` or `more`.
    pub kind: String,
    /// Selector for actions on the whole node (path or hf:org/name).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    pub bytes: u64,
    pub files: u64,
    /// Stores holding a leaf.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<String>,
    /// A leaf's file id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<FileId>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<TreeNode>,
    /// Children exist below the depth that was asked for.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

fn sql(e: rusqlite::Error) -> NestError {
    NestError::Io(format!("metadata: {e}"))
}

struct Leaf {
    size: u64,
    stores: Vec<StoreId>,
}

struct Ns {
    /// child → (parent, name), first link wins.
    up: HashMap<FileId, (FileId, String)>,
    down: HashMap<FileId, Vec<(String, FileId)>>,
    kind: HashMap<FileId, FileKind>,
    leaves: HashMap<FileId, Leaf>,
}

impl Ns {
    fn load(c: &Connection, store: Option<StoreId>) -> NestResult<Ns> {
        let mut up = HashMap::new();
        let mut down: HashMap<FileId, Vec<(String, FileId)>> = HashMap::new();
        let mut st = c
            .prepare("SELECT parent, name, child FROM dentries")
            .map_err(sql)?;
        let rows = st
            .query_map([], |r| {
                Ok((
                    FileId(r.get::<_, i64>(0)? as u64),
                    r.get::<_, Vec<u8>>(1)?,
                    FileId(r.get::<_, i64>(2)? as u64),
                ))
            })
            .map_err(sql)?;
        for row in rows {
            let (p, name, ch) = row.map_err(sql)?;
            let name = String::from_utf8_lossy(&name).into_owned();
            up.entry(ch).or_insert((p, name.clone()));
            down.entry(p).or_default().push((name, ch));
        }
        let mut kind = HashMap::new();
        let mut st = c.prepare("SELECT id, kind FROM files").map_err(sql)?;
        let rows = st
            .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
            .map_err(sql)?;
        for row in rows {
            let (id, k) = row.map_err(sql)?;
            if let Some(k) = FileKind::from_i64(k) {
                kind.insert(FileId(id as u64), k);
            }
        }
        let mut leaves: HashMap<FileId, Leaf> = HashMap::new();
        let mut st = c
            .prepare(
                "SELECT r.file, f.size, r.store FROM replicas r
                 JOIN files f ON f.id = r.file AND f.gen = r.gen WHERE r.state = 1",
            )
            .map_err(sql)?;
        let rows = st
            .query_map([], |r| {
                Ok((
                    FileId(r.get::<_, i64>(0)? as u64),
                    r.get::<_, i64>(1)? as u64,
                    StoreId(r.get::<_, i64>(2)? as u64),
                ))
            })
            .map_err(sql)?;
        for row in rows {
            let (f, size, s) = row.map_err(sql)?;
            leaves
                .entry(f)
                .or_insert(Leaf {
                    size,
                    stores: Vec::new(),
                })
                .stores
                .push(s);
        }
        if let Some(s) = store {
            leaves.retain(|_, l| l.stores.contains(&s));
        }
        Ok(Ns {
            up,
            down,
            kind,
            leaves,
        })
    }

    fn path(&self, mut id: FileId) -> String {
        let mut parts = Vec::new();
        while id != FileId::ROOT {
            let Some((p, n)) = self.up.get(&id) else {
                break;
            };
            parts.push(n.as_str());
            id = *p;
        }
        parts.reverse();
        format!("/{}", parts.join("/"))
    }
}

struct Builder<'a> {
    ns: &'a Ns,
    req: &'a TreeReq,
    /// Leaves already credited (models shape), or to leave out.
    taken: HashSet<FileId>,
    /// Per directory: (weight, files), for leaves not taken.
    agg: HashMap<FileId, (u64, u64)>,
}

impl<'a> Builder<'a> {
    fn weight(&self, l: &Leaf) -> u64 {
        match (self.req.weight, self.req.store) {
            (Weight::Copies, None) => l.size * l.stores.len() as u64,
            _ => l.size,
        }
    }

    fn hosts(&self, l: &Leaf) -> Vec<String> {
        let mut v: Vec<String> = l
            .stores
            .iter()
            .map(|s| {
                self.req
                    .store_names
                    .get(s)
                    .cloned()
                    .unwrap_or_else(|| format!("store{}", s.0))
            })
            .collect();
        v.sort();
        v
    }

    /// Sum every untaken leaf into its ancestors.
    fn aggregate(&mut self) {
        self.agg.clear();
        for (id, l) in &self.ns.leaves {
            if self.taken.contains(id) {
                continue;
            }
            let w = self.weight(l);
            let mut cur = *id;
            while let Some((p, _)) = self.ns.up.get(&cur) {
                let a = self.agg.entry(*p).or_default();
                a.0 += w;
                a.1 += 1;
                if *p == FileId::ROOT {
                    break;
                }
                cur = *p;
            }
        }
    }

    fn leaf_node(&self, id: FileId, name: String) -> Option<TreeNode> {
        let l = self.ns.leaves.get(&id)?;
        let path = self.ns.path(id);
        Some(TreeNode {
            name,
            selector: Some(path.clone()),
            path: Some(path),
            kind: "file".into(),
            bytes: self.weight(l),
            files: 1,
            hosts: self.hosts(l),
            file: Some(id),
            ..Default::default()
        })
    }

    /// Path-shaped subtree under directory `id`.
    fn dir_node(&self, id: FileId, name: String, depth: u32) -> TreeNode {
        let (bytes, files) = self.agg.get(&id).copied().unwrap_or_default();
        let path = self.ns.path(id);
        let mut n = TreeNode {
            name,
            selector: Some(path.clone()),
            path: Some(path),
            kind: "dir".into(),
            bytes,
            files,
            ..Default::default()
        };
        if bytes == 0 {
            return n;
        }
        if depth == 0 {
            n.truncated = true;
            return n;
        }
        let mut kids = Vec::new();
        for (cname, ch) in self.ns.down.get(&id).into_iter().flatten() {
            match self.ns.kind.get(ch) {
                Some(FileKind::Directory) => {
                    if self.agg.get(ch).is_some_and(|a| a.0 > 0) {
                        kids.push(self.dir_node(*ch, cname.clone(), depth - 1));
                    }
                }
                Some(FileKind::Regular) if !self.taken.contains(ch) => {
                    if let Some(l) = self.leaf_node(*ch, cname.clone()) {
                        kids.push(l);
                    }
                }
                _ => {}
            }
        }
        n.children = fold(kids, self.req.max_children);
        n
    }
}

/// Largest first; past `max`, the rest become one "more" node.
fn fold(mut kids: Vec<TreeNode>, max: usize) -> Vec<TreeNode> {
    kids.sort_by(|a, b| b.bytes.cmp(&a.bytes).then(a.name.cmp(&b.name)));
    if kids.len() > max.max(1) {
        let rest = kids.split_off(max.max(1) - 1);
        kids.push(TreeNode {
            name: format!("{} more", rest.len()),
            kind: "more".into(),
            bytes: rest.iter().map(|k| k.bytes).sum(),
            files: rest.iter().map(|k| k.files).sum(),
            ..Default::default()
        });
    }
    kids
}

/// Insert `leaf` at `rel` ("a/b/c.bin") under `into`, creating groups.
fn insert(into: &mut TreeNode, rel: &str, leaf: TreeNode) {
    match rel.split_once('/') {
        None => into.children.push(TreeNode {
            name: rel.to_string(),
            ..leaf
        }),
        Some((head, rest)) => {
            let pos = into
                .children
                .iter()
                .position(|c| c.kind == "group" && c.name == head);
            let i = match pos {
                Some(i) => i,
                None => {
                    into.children.push(TreeNode {
                        name: head.to_string(),
                        kind: "group".into(),
                        ..Default::default()
                    });
                    into.children.len() - 1
                }
            };
            insert(&mut into.children[i], rest, leaf);
        }
    }
}

/// Fill `bytes`/`files` of groups from their children and sort.
fn settle(n: &mut TreeNode, max: usize) {
    if n.children.is_empty() {
        return;
    }
    for c in &mut n.children {
        settle(c, max);
    }
    n.bytes = n.children.iter().map(|c| c.bytes).sum();
    n.files = n.children.iter().map(|c| c.files).sum();
    n.children = fold(std::mem::take(&mut n.children), max);
}

/// Resolve `abs` to a regular file, following a final symlink too (hf 2.0:
/// snapshot → repo `blobs/<sha>` link → shared `/hub/blobs/xx/<sha>`).
fn follow(c: &Connection, ns: &Ns, abs: &str) -> NestResult<Option<FileId>> {
    let mut path = abs.to_string();
    for _ in 0..8 {
        let Ok((id, dir)) = selector::resolve_path(c, &path) else {
            return Ok(None);
        };
        if ns.kind.get(&id) != Some(&FileKind::Symlink) {
            return Ok(Some(id));
        }
        let t = query::readlink(c, id).map_err(sql)?.unwrap_or_default();
        let t = String::from_utf8_lossy(&t);
        path = if t.starts_with('/') {
            t.into_owned()
        } else {
            format!("{}/{t}", ns.path(dir))
        };
    }
    Ok(None)
}

/// Every file Hugging Face snapshots under `hub` point at: its repo
/// (org/name) and the name the snapshot gives it (the first snapshot's
/// name when several name it; a file two repos share goes to the first).
pub fn hf_names(c: &Connection, hub: &str) -> NestResult<HashMap<FileId, (String, String)>> {
    let ns = Ns::load(c, None)?;
    let mut out = HashMap::new();
    let hub = hub.trim_end_matches('/');
    let Ok((hub_id, _)) = selector::resolve_path(c, hub) else {
        return Ok(out);
    };
    let mut repos: Vec<(String, FileId)> = ns
        .down
        .get(&hub_id)
        .into_iter()
        .flatten()
        .filter(|(n, _)| n.starts_with("models--") || n.starts_with("datasets--"))
        .map(|(n, id)| (n.clone(), *id))
        .collect();
    repos.sort();
    for (dname, rid) in repos {
        let rest = dname
            .strip_prefix("models--")
            .or_else(|| dname.strip_prefix("datasets--"))
            .expect("filtered");
        let repo = rest.replacen("--", "/", 1);
        let snaps = ns
            .down
            .get(&rid)
            .into_iter()
            .flatten()
            .find(|(n, _)| n == "snapshots")
            .map(|(_, id)| *id);
        let revs: Vec<(String, FileId)> = snaps
            .and_then(|s| ns.down.get(&s))
            .cloned()
            .unwrap_or_default();
        for (_, rev_id) in revs {
            let mut stack = vec![(rev_id, String::new())];
            while let Some((dir, prefix)) = stack.pop() {
                for (n, ch) in ns.down.get(&dir).into_iter().flatten() {
                    let rel = if prefix.is_empty() {
                        n.clone()
                    } else {
                        format!("{prefix}/{n}")
                    };
                    let target = match ns.kind.get(ch) {
                        Some(FileKind::Directory) => {
                            stack.push((*ch, rel));
                            continue;
                        }
                        Some(FileKind::Regular) => Some(*ch),
                        Some(FileKind::Symlink) => {
                            let t = query::readlink(c, *ch).map_err(sql)?.unwrap_or_default();
                            let t = String::from_utf8_lossy(&t);
                            let abs = if t.starts_with('/') {
                                t.into_owned()
                            } else {
                                format!("{}/{t}", ns.path(dir))
                            };
                            follow(c, &ns, &abs)?
                        }
                        None => None,
                    };
                    if let Some(t) = target {
                        out.entry(t).or_insert_with(|| (repo.clone(), rel));
                    }
                }
            }
        }
    }
    Ok(out)
}

pub fn tree(c: &Connection, req: &TreeReq) -> NestResult<TreeNode> {
    let ns = Ns::load(c, req.store)?;
    let mut b = Builder {
        ns: &ns,
        req,
        taken: HashSet::new(),
        agg: HashMap::new(),
    };
    if !req.models {
        b.aggregate();
        let (id, _) = selector::resolve_path(c, &req.root)?;
        if ns.kind.get(&id) != Some(&FileKind::Directory) {
            return b.leaf_node(id, req.root.clone()).ok_or(NestError::NotFound);
        }
        let name = req.root.rsplit('/').find(|s| !s.is_empty()).unwrap_or("/");
        return Ok(b.dir_node(id, name.to_string(), req.depth));
    }

    // Models shape.
    let mut root = TreeNode {
        name: "everything".into(),
        kind: "group".into(),
        ..Default::default()
    };
    let hub = req.hub.trim_end_matches('/');
    if let Ok((hub_id, _)) = selector::resolve_path(c, hub) {
        let mut repos: Vec<(String, FileId)> = ns
            .down
            .get(&hub_id)
            .into_iter()
            .flatten()
            .filter(|(n, _)| n.starts_with("models--") || n.starts_with("datasets--"))
            .map(|(n, id)| (n.clone(), *id))
            .collect();
        repos.sort();
        for (dname, rid) in repos {
            let (kind, rest) = dname
                .strip_prefix("models--")
                .map(|r| ("model", r))
                .or_else(|| dname.strip_prefix("datasets--").map(|r| ("dataset", r)))
                .expect("filtered");
            let repo = rest.replacen("--", "/", 1);
            let sel = if kind == "dataset" {
                format!("hf-dataset:{repo}")
            } else {
                format!("hf:{repo}")
            };
            let mut node = TreeNode {
                name: repo,
                path: Some(format!("{hub}/{dname}")),
                kind: "repo".into(),
                selector: Some(sel),
                ..Default::default()
            };
            // Snapshot entries, resolved through their links.
            let snaps = ns
                .down
                .get(&rid)
                .into_iter()
                .flatten()
                .find(|(n, _)| n == "snapshots")
                .map(|(_, id)| *id);
            let revs: Vec<(String, FileId)> = snaps
                .and_then(|s| ns.down.get(&s))
                .cloned()
                .unwrap_or_default();
            let many = revs.len() > 1;
            for (rev, rev_id) in revs {
                let mut rnode = TreeNode {
                    name: rev.chars().take(10).collect(),
                    kind: "revision".into(),
                    ..Default::default()
                };
                let mut stack = vec![(rev_id, String::new())];
                while let Some((dir, prefix)) = stack.pop() {
                    for (n, ch) in ns.down.get(&dir).into_iter().flatten() {
                        let rel = if prefix.is_empty() {
                            n.clone()
                        } else {
                            format!("{prefix}/{n}")
                        };
                        let target = match ns.kind.get(ch) {
                            Some(FileKind::Directory) => {
                                stack.push((*ch, rel));
                                continue;
                            }
                            Some(FileKind::Regular) => Some(*ch),
                            Some(FileKind::Symlink) => {
                                let t = query::readlink(c, *ch).map_err(sql)?.unwrap_or_default();
                                let t = String::from_utf8_lossy(&t);
                                let abs = if t.starts_with('/') {
                                    t.into_owned()
                                } else {
                                    format!("{}/{t}", ns.path(dir))
                                };
                                follow(c, &ns, &abs)?
                            }
                            None => None,
                        };
                        let Some(t) = target else { continue };
                        if b.taken.contains(&t) {
                            continue;
                        }
                        if let Some(leaf) = b.leaf_node(t, rel.clone()) {
                            b.taken.insert(t);
                            insert(if many { &mut rnode } else { &mut node }, &rel, leaf);
                        }
                    }
                }
                if many && !rnode.children.is_empty() {
                    node.children.push(rnode);
                }
            }
            // The repo's own files no snapshot points at (old revisions,
            // refs, downloads in progress).
            let mut stray = TreeNode {
                name: "unreferenced".into(),
                kind: "group".into(),
                ..Default::default()
            };
            let mut stack = vec![rid];
            while let Some(dir) = stack.pop() {
                for (n, ch) in ns.down.get(&dir).into_iter().flatten() {
                    match ns.kind.get(ch) {
                        Some(FileKind::Directory) => stack.push(*ch),
                        Some(FileKind::Regular) if !b.taken.contains(ch) => {
                            if let Some(leaf) = b.leaf_node(*ch, n.clone()) {
                                b.taken.insert(*ch);
                                stray.children.push(leaf);
                            }
                        }
                        _ => {}
                    }
                }
            }
            if !stray.children.is_empty() {
                node.children.push(stray);
            }
            settle(&mut node, req.max_children);
            if node.bytes > 0 {
                root.children.push(node);
            }
        }
    }
    // Everything no repo claimed, in path shape.
    b.aggregate();
    let mut other = b.dir_node(FileId::ROOT, "other files".into(), req.depth.max(1));
    if other.bytes > 0 {
        other.kind = "dir".into();
        root.children.push(other);
    }
    // Every repo stays visible at the top; deeper levels are already folded.
    let top = req.max_children.max(root.children.len());
    settle(&mut root, top);
    Ok(root)
}
