//! Free-space planning (PROPOSAL §7): given desired free space per host,
//! propose the smallest disruption that reaches it, and apply it on
//! approval.
//!
//! Order of preference on each host that is short:
//! 1. Evict copies that are redundant (another live copy exists elsewhere,
//!    including an archive) and that no rule requires on that host; files
//!    with the most copies first, then largest first.
//! 2. If an archive store has room, offload files that are only here and
//!    not rule-required (copy into the archive, then evict).
//! 3. Otherwise report what blocks it: bytes required there by rules, and
//!    only copies with nowhere to go.
//!
//! Plans only ever remove copies the placement engine could re-create; the
//! last live copy is never removed, and every step names the exact
//! generation it planned for, so a file that changed since is left alone.

use crate::placer::Placer;
use crate::spec::Selector;
use nest_meta::query;
use nest_types::{FileId, Generation, NestError, NestResult, NodeId, StoreId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Copy {
    pub file: FileId,
    pub generation: Generation,
    pub size: u64,
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Step {
    Evict {
        host: String,
        node: NodeId,
        bytes: u64,
        copies: Vec<Copy>,
    },
    Offload {
        host: String,
        node: NodeId,
        store: String,
        bytes: u64,
        copies: Vec<Copy>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostTarget {
    pub host: String,
    pub free_now: u64,
    pub target: u64,
    pub projected_free: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Plan {
    pub id: u64,
    pub hosts: Vec<HostTarget>,
    pub steps: Vec<Step>,
    pub blocked: Vec<String>,
    pub feasible: bool,
}

fn sql(e: rusqlite::Error) -> NestError {
    NestError::Io(format!("metadata: {e}"))
}

/// Which copies the current rules require, and the first rule requiring
/// each. Archive-store targets are not host requirements.
pub async fn requirements(placer: &Placer) -> NestResult<HashMap<(FileId, NodeId), String>> {
    let stores: Vec<String> = placer
        .archive_stores()?
        .into_iter()
        .map(|(_, n, _)| n)
        .collect();
    let mut required_by = HashMap::new();
    for (rname, spec, _) in placer.rules()? {
        let hosts: Vec<String> = spec
            .hosts
            .iter()
            .filter(|x| !stores.contains(x))
            .cloned()
            .collect();
        let m = placer.manifest(&spec.selector).await?;
        for h in placer.resolve_hosts(&hosts)? {
            for e in &m.entries {
                required_by
                    .entry((e.file, h.node))
                    .or_insert_with(|| rname.clone());
            }
        }
    }
    Ok(required_by)
}

/// Build a plan. `free` maps host names or @groups to desired free bytes.
pub async fn make(placer: &Placer, free: &[(String, u64)]) -> NestResult<Plan> {
    let status = placer.status().await?;
    let nodes = placer.nodes()?;
    // Desired free space per node. A host named directly beats a group it
    // belongs to, whatever the order; otherwise a later entry wins.
    let mut want: BTreeMap<NodeId, u64> = BTreeMap::new();
    let (groups, hosts): (Vec<_>, Vec<_>) = free.iter().partition(|(n, _)| n.starts_with('@'));
    for (name, bytes) in groups.into_iter().chain(hosts) {
        for h in placer.resolve_hosts(std::slice::from_ref(name))? {
            want.insert(h.node, *bytes);
        }
    }
    let free_now: HashMap<NodeId, u64> = status
        .iter()
        .filter_map(|s| s.info.as_ref().map(|i| (s.node, i.free_bytes)))
        .collect();
    let name_of: HashMap<NodeId, String> = nodes.iter().map(|h| (h.node, h.name.clone())).collect();

    // What rules require where.
    let required_by = requirements(placer).await?;
    let required: HashSet<(FileId, NodeId)> = required_by.keys().copied().collect();

    // Every live copy on every node, with sizes and copy counts.
    let (mut copies_on, mut count) = collect_copies(placer)?;

    // An archive with room, for offloading only-copies.
    let mut archive: Option<(String, u64)> = None;
    for s in placer.stores().await.unwrap_or_default() {
        if let Some((_, h)) = s.gateways.iter().find(|(_, h)| h.healthy) {
            let room = h.free_bytes;
            if archive.as_ref().is_none_or(|(_, r)| room > *r) {
                archive = Some((s.name.clone(), room));
            }
        }
    }

    let mut steps = Vec::new();
    let mut blocked = Vec::new();
    let mut hosts = Vec::new();
    for (node, target) in &want {
        let host = name_of
            .get(node)
            .cloned()
            .unwrap_or_else(|| format!("node{node}"));
        let now = free_now.get(node).copied().unwrap_or(0);
        let mut deficit = target.saturating_sub(now);
        let mut projected = now;
        let mut here = copies_on.remove(node).unwrap_or_default();
        // Redundant copies first: most copies, then largest.
        here.sort_by(|a, b| {
            let ca = count[&(a.file, a.generation)];
            let cb = count[&(b.file, b.generation)];
            cb.cmp(&ca).then(b.size.cmp(&a.size))
        });
        let mut evict = Vec::new();
        let mut only = Vec::new();
        let mut req_bytes: HashMap<String, u64> = HashMap::new();
        for cp in here {
            if deficit == 0 {
                break;
            }
            if required.contains(&(cp.file, *node)) {
                *req_bytes
                    .entry(required_by[&(cp.file, *node)].clone())
                    .or_default() += cp.size;
                continue;
            }
            let n = count.get_mut(&(cp.file, cp.generation)).expect("counted");
            if *n >= 2 {
                *n -= 1;
                deficit = deficit.saturating_sub(cp.size);
                projected += cp.size;
                evict.push(cp);
            } else {
                only.push(cp);
            }
        }
        if !evict.is_empty() {
            steps.push(Step::Evict {
                host: host.clone(),
                node: *node,
                bytes: evict.iter().map(|c| c.size).sum(),
                copies: evict,
            });
        }
        if deficit > 0
            && !only.is_empty()
            && let Some((store, room)) = archive.as_mut()
        {
            let mut off = Vec::new();
            let mut left = Vec::new();
            for cp in only.drain(..) {
                if deficit == 0 || cp.size > *room {
                    left.push(cp);
                    continue;
                }
                *room -= cp.size;
                deficit = deficit.saturating_sub(cp.size);
                projected += cp.size;
                off.push(cp);
            }
            only = left;
            if !off.is_empty() {
                steps.push(Step::Offload {
                    host: host.clone(),
                    node: *node,
                    store: store.clone(),
                    bytes: off.iter().map(|c| c.size).sum(),
                    copies: off,
                });
            }
        }
        if deficit > 0 {
            let mut why = format!("{host}: still {} short of the target", human(deficit));
            for (rule, b) in &req_bytes {
                why += &format!("; rule {rule:?} requires {} here", human(*b));
            }
            let stuck: u64 = only.iter().map(|c| c.size).sum();
            if stuck > 0 {
                let archive_why = if archive.is_some() {
                    "no archive store has room for them"
                } else {
                    "no archive store is available"
                };
                why += &format!("; {} here are only copies and {archive_why}", human(stuck));
            }
            if req_bytes.is_empty() && stuck == 0 {
                why += "; nothing else sparknest holds here can move (the rest is other data)";
            }
            blocked.push(why);
        }
        hosts.push(HostTarget {
            host,
            free_now: now,
            target: *target,
            projected_free: projected,
        });
    }
    let feasible = blocked.is_empty();
    Ok(Plan {
        id: rand::random::<u32>() as u64,
        hosts,
        steps,
        blocked,
        feasible,
    })
}

type CopyMaps = (
    HashMap<NodeId, Vec<Copy>>,
    HashMap<(FileId, Generation), usize>,
);

/// All live copies (live stores only in the per-node map; archives count
/// toward copy totals). Synchronous: no connection crosses an await.
fn collect_copies(placer: &Placer) -> NestResult<CopyMaps> {
    let c = placer.vfs().data().meta().open_reader().map_err(sql)?;
    let mut copies_on: HashMap<NodeId, Vec<Copy>> = HashMap::new();
    let mut count: HashMap<(FileId, Generation), usize> = HashMap::new();
    let mut st = c
        .prepare("SELECT r.file, r.gen, r.store, f.size FROM replicas r JOIN files f ON f.id = r.file AND f.gen = r.gen WHERE r.state = 1")
        .map_err(sql)?;
    let rows: Vec<(u64, u64, u64, u64)> = st
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)? as u64,
                r.get::<_, i64>(1)? as u64,
                r.get::<_, i64>(2)? as u64,
                r.get::<_, i64>(3)? as u64,
            ))
        })
        .map_err(sql)?
        .collect::<rusqlite::Result<_>>()
        .map_err(sql)?;
    for (f, g, store, size) in rows {
        let (file, generation) = (FileId(f), Generation(g));
        *count.entry((file, generation)).or_default() += 1;
        if !nest_data::is_archive(StoreId(store)) {
            let path = query::path_of(&c, file)
                .map_err(sql)?
                .map(|p| String::from_utf8_lossy(&p).into_owned())
                .unwrap_or_else(|| format!("<file {f}>"));
            copies_on.entry(NodeId(store)).or_default().push(Copy {
                file,
                generation,
                size,
                path,
            });
        }
    }
    Ok((copies_on, count))
}

fn human(b: u64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < units.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{v:.1} {}", units[i])
}

/// Selector naming exactly one file (for offload steps).
pub fn file_selector(path: &str) -> Selector {
    Selector::Path {
        path: path.to_string(),
    }
}
