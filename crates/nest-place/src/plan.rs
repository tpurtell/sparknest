//! Goal-oriented planning (PROPOSAL §7, ADR-028): propose changes to where
//! copies live, show why each one is proposed, and apply them on approval.
//!
//! Goals:
//! - **Free**: reach desired free space on hosts. Redundant copies go first
//!   (another copy exists elsewhere, including an archive, and no rule needs
//!   it here), least recently opened on that host first; then sole copies
//!   are offloaded into the archive stores the caller chose, filled in the
//!   order given. With no archive chosen only redundant copies are removed.
//! - **Tidy**: remove copies not opened on their host within a window (and
//!   not written within it), oldest use first, down to one copy anywhere;
//!   sole copies move to a chosen archive or stay.
//! - **Speedup**: add copies on hosts that read a file over the network (or
//!   from an archive) at least a threshold within a window, most-read first,
//!   while the host keeps its free-space floor.
//!
//! Plans only remove copies the placement engine could re-create; the last
//! copy is never removed, rule-required copies are never removed, and every
//! step names the exact generation it planned for, so a file that changed
//! since is left alone. Plans never happen by themselves: reading never
//! creates a replica (invariant 2), a person applies a plan.

use crate::placer::Placer;
use crate::spec::Selector;
use nest_data::usage::FileUsage;
use nest_meta::query;
use nest_types::{FileId, GenState, Generation, NestError, NestResult, NodeId, StoreId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

const DAY_MS: u64 = 86_400_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Copy {
    pub file: FileId,
    pub generation: Generation,
    pub size: u64,
    pub path: String,
    /// Why this copy is in the plan, for people.
    #[serde(default)]
    pub why: String,
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
    /// Copy onto `host` (speedup).
    Replicate {
        host: String,
        node: NodeId,
        bytes: u64,
        copies: Vec<Copy>,
    },
}

impl Step {
    pub fn copies(&self) -> &[Copy] {
        match self {
            Step::Evict { copies, .. }
            | Step::Offload { copies, .. }
            | Step::Replicate { copies, .. } => copies,
        }
    }
}

/// What a plan wants.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "goal", rename_all = "snake_case")]
pub enum Goal {
    Free {
        /// (host or @group, desired free bytes)
        free: Vec<(String, u64)>,
        #[serde(default)]
        archives: Vec<String>,
    },
    Tidy {
        /// Not opened within this many days (at most `usage::KEEP_DAYS`).
        days: u64,
        /// Hosts or @groups to tidy; every host when empty.
        #[serde(default)]
        hosts: Vec<String>,
        #[serde(default)]
        archives: Vec<String>,
    },
    Speedup {
        days: u64,
        #[serde(default)]
        hosts: Vec<String>,
        /// Least network bytes read within the window that earns a copy.
        #[serde(default = "default_min_remote")]
        min_remote_bytes: u64,
        /// Free space each host keeps; default a tenth of its disk.
        #[serde(default)]
        keep_free: Option<u64>,
    },
}

fn default_min_remote() -> u64 {
    1 << 30
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostTarget {
    pub host: String,
    pub free_now: u64,
    /// Desired free bytes (Free goal), else 0.
    pub target: u64,
    pub projected_free: u64,
    #[serde(default)]
    pub total: u64,
}

/// An archive store the plan may offload into, and what it would take.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArchiveTarget {
    pub store: String,
    /// Checked through a healthy gateway; `false` means it takes nothing.
    pub reachable: bool,
    pub free_now: u64,
    pub total: u64,
    /// Bytes this plan would write into it.
    pub adds: u64,
    pub projected_free: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Plan {
    pub id: u64,
    pub goal: Goal,
    pub hosts: Vec<HostTarget>,
    #[serde(default)]
    pub archives: Vec<ArchiveTarget>,
    pub steps: Vec<Step>,
    /// What keeps the goal from being met.
    pub blocked: Vec<String>,
    /// Worth knowing, not a failure (e.g. idle sole copies left alone).
    #[serde(default)]
    pub notes: Vec<String>,
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

/// Everything a goal needs to know about the cluster, gathered once.
struct World {
    now_ms: u64,
    name_of: HashMap<NodeId, String>,
    free_now: HashMap<NodeId, u64>,
    total: HashMap<NodeId, u64>,
    required_by: HashMap<(FileId, NodeId), String>,
    /// Live copies per node.
    copies_on: HashMap<NodeId, Vec<Copy>>,
    /// Copies of each generation anywhere (archives included).
    count: HashMap<(FileId, Generation), usize>,
    created_ms: HashMap<FileId, u64>,
    usage: HashMap<NodeId, HashMap<FileId, FileUsage>>,
    /// Growing plan state.
    projected: HashMap<NodeId, u64>,
    dest: Vec<ArchiveTarget>,
    blocked: Vec<String>,
    notes: Vec<String>,
}

impl World {
    async fn gather(placer: &Placer, archives: &[String], usage_days: u64) -> NestResult<World> {
        let now_ms = nest_data::usage::now_ms();
        let status = placer.status().await?;
        let nodes = placer.nodes()?;
        let free_now: HashMap<NodeId, u64> = status
            .iter()
            .filter_map(|s| s.info.as_ref().map(|i| (s.node, i.free_bytes)))
            .collect();
        let total = status
            .iter()
            .filter_map(|s| s.info.as_ref().map(|i| (s.node, i.total_bytes)))
            .collect();
        let required_by = requirements(placer).await?;
        let (copies_on, count, created_ms) = collect_copies(placer)?;
        let usage = if usage_days > 0 {
            placer
                .usage(now_ms.saturating_sub(usage_days * DAY_MS))
                .await?
        } else {
            HashMap::new()
        };
        let mut w = World {
            now_ms,
            name_of: nodes.iter().map(|h| (h.node, h.name.clone())).collect(),
            projected: free_now.clone(),
            free_now,
            total,
            required_by,
            copies_on,
            count,
            created_ms,
            usage,
            dest: Vec::new(),
            blocked: Vec::new(),
            notes: Vec::new(),
        };
        w.archives(placer, archives).await?;
        Ok(w)
    }

    async fn archives(&mut self, placer: &Placer, archives: &[String]) -> NestResult<()> {
        let known = placer.archive_stores()?;
        for a in archives {
            if !known.iter().any(|(_, n, _)| n == a) {
                return Err(NestError::Invalid(format!("{a} is not an archive store")));
            }
        }
        if archives.is_empty() {
            return Ok(());
        }
        let health = placer.stores().await.unwrap_or_default();
        for a in archives {
            if self.dest.iter().any(|d| &d.store == a) {
                continue;
            }
            let h = health
                .iter()
                .filter(|s| &s.name == a)
                .flat_map(|s| s.gateways.iter())
                .find(|(_, h)| h.healthy)
                .map(|(_, h)| h.clone());
            self.dest.push(ArchiveTarget {
                store: a.clone(),
                reachable: h.is_some(),
                free_now: h.as_ref().map_or(0, |h| h.free_bytes),
                total: h.as_ref().map_or(0, |h| h.total_bytes),
                adds: 0,
                projected_free: h.as_ref().map_or(0, |h| h.free_bytes),
            });
        }
        for d in self.dest.iter().filter(|d| !d.reachable) {
            self.blocked.push(format!(
                "archive store {}: no gateway can reach it",
                d.store
            ));
        }
        Ok(())
    }

    fn host(&self, n: NodeId) -> String {
        self.name_of
            .get(&n)
            .cloned()
            .unwrap_or_else(|| format!("node{n}"))
    }

    fn last_open(&self, n: NodeId, f: FileId) -> u64 {
        self.usage
            .get(&n)
            .and_then(|u| u.get(&f))
            .map_or(0, |u| u.last_open_ms)
    }

    fn idle_words(&self, n: NodeId, f: FileId) -> String {
        match self.last_open(n, f) {
            0 => format!(
                "not opened on {} in {} days",
                self.host(n),
                nest_data::usage::KEEP_DAYS
            ),
            t => format!(
                "last opened on {} {}",
                self.host(n),
                ago(self.now_ms.saturating_sub(t))
            ),
        }
    }

    fn required(&self, f: FileId, n: NodeId) -> Option<&String> {
        self.required_by.get(&(f, n))
    }

    /// Move `only` (sole copies on `node`) into the chosen archives while
    /// `deficit` lasts (`None`: move all that fit). Returns what did not fit.
    fn offload(
        &mut self,
        node: NodeId,
        mut only: Vec<Copy>,
        mut deficit: Option<u64>,
        steps: &mut Vec<Step>,
    ) -> Vec<Copy> {
        let host = self.host(node);
        for d in self.dest.iter_mut().filter(|d| d.reachable) {
            if deficit == Some(0) || only.is_empty() {
                break;
            }
            let mut off = Vec::new();
            let mut left = Vec::new();
            for mut cp in only.drain(..) {
                if deficit == Some(0) || cp.size > d.projected_free {
                    left.push(cp);
                    continue;
                }
                d.projected_free -= cp.size;
                d.adds += cp.size;
                deficit = deficit.map(|x| x.saturating_sub(cp.size));
                *self.projected.entry(node).or_default() += cp.size;
                cp.why = format!("only copy; {}", cp.why);
                off.push(cp);
            }
            only = left;
            if !off.is_empty() {
                steps.push(Step::Offload {
                    host: host.clone(),
                    node,
                    store: d.store.clone(),
                    bytes: off.iter().map(|c| c.size).sum(),
                    copies: off,
                });
            }
        }
        only
    }

    fn host_rows(&self, nodes: impl Iterator<Item = (NodeId, u64)>) -> Vec<HostTarget> {
        nodes
            .map(|(n, target)| HostTarget {
                host: self.host(n),
                free_now: self.free_now.get(&n).copied().unwrap_or(0),
                target,
                projected_free: self.projected.get(&n).copied().unwrap_or(0),
                total: self.total.get(&n).copied().unwrap_or(0),
            })
            .collect()
    }

    fn finish(self, goal: Goal, hosts: Vec<HostTarget>, steps: Vec<Step>) -> Plan {
        Plan {
            id: rand::random::<u32>() as u64,
            goal,
            hosts,
            archives: self.dest,
            steps,
            feasible: self.blocked.is_empty(),
            blocked: self.blocked,
            notes: self.notes,
        }
    }
}

fn ago(ms: u64) -> String {
    let h = ms / 3_600_000;
    match h {
        0 => "within the hour".into(),
        1..=47 => format!("{h} hours ago"),
        _ => format!("{} days ago", h / 24),
    }
}

/// Resolve hosts/@groups to nodes; every node when `names` is empty.
fn scope(placer: &Placer, names: &[String]) -> NestResult<Vec<NodeId>> {
    Ok(if names.is_empty() {
        placer.nodes()?.into_iter().map(|h| h.node).collect()
    } else {
        placer
            .resolve_hosts(names)?
            .into_iter()
            .map(|h| h.node)
            .collect()
    })
}

/// Build a plan for `goal`.
pub async fn make(placer: &Placer, goal: Goal) -> NestResult<Plan> {
    match &goal {
        Goal::Free { free, archives } => {
            let w = World::gather(placer, archives, nest_data::usage::KEEP_DAYS).await?;
            let free = free.clone();
            make_free(placer, w, goal, &free)
        }
        Goal::Tidy {
            days,
            hosts,
            archives,
        } => {
            let days = (*days).clamp(1, nest_data::usage::KEEP_DAYS);
            let nodes = scope(placer, hosts)?;
            let w = World::gather(placer, archives, days).await?;
            Ok(make_tidy(w, goal.clone(), days, &nodes))
        }
        Goal::Speedup {
            days,
            hosts,
            min_remote_bytes,
            keep_free,
        } => {
            let days = (*days).clamp(1, nest_data::usage::KEEP_DAYS);
            let nodes = scope(placer, hosts)?;
            let w = World::gather(placer, &[], days).await?;
            let (min, keep) = (*min_remote_bytes, *keep_free);
            make_speedup(placer, w, goal, days, &nodes, min, keep)
        }
    }
}

fn make_free(
    placer: &Placer,
    mut w: World,
    goal: Goal,
    free: &[(String, u64)],
) -> NestResult<Plan> {
    // Desired free space per node. A host named directly beats a group it
    // belongs to, whatever the order; otherwise a later entry wins.
    let mut want: BTreeMap<NodeId, u64> = BTreeMap::new();
    let (groups, hosts): (Vec<_>, Vec<_>) = free.iter().partition(|(n, _)| n.starts_with('@'));
    for (name, bytes) in groups.into_iter().chain(hosts) {
        for h in placer.resolve_hosts(std::slice::from_ref(name))? {
            want.insert(h.node, *bytes);
        }
    }
    let mut steps = Vec::new();
    for (&node, &target) in &want {
        let host = w.host(node);
        let now = w.free_now.get(&node).copied().unwrap_or(0);
        let mut deficit = target.saturating_sub(now);
        let mut here = w.copies_on.remove(&node).unwrap_or_default();
        // Least recently opened here first; then most copies, largest.
        here.sort_by(|a, b| {
            let (la, lb) = (w.last_open(node, a.file), w.last_open(node, b.file));
            let ca = w.count[&(a.file, a.generation)];
            let cb = w.count[&(b.file, b.generation)];
            la.cmp(&lb).then(cb.cmp(&ca)).then(b.size.cmp(&a.size))
        });
        let mut evict = Vec::new();
        let mut only = Vec::new();
        let mut req_bytes: HashMap<String, u64> = HashMap::new();
        for mut cp in here {
            if deficit == 0 {
                break;
            }
            if let Some(rule) = w.required(cp.file, node) {
                *req_bytes.entry(rule.clone()).or_default() += cp.size;
                continue;
            }
            cp.why = w.idle_words(node, cp.file);
            let n = w.count.get_mut(&(cp.file, cp.generation)).expect("counted");
            if *n >= 2 {
                *n -= 1;
                cp.why = format!(
                    "{}; {} other {}",
                    cp.why,
                    *n,
                    if *n == 1 { "copy" } else { "copies" }
                );
                deficit = deficit.saturating_sub(cp.size);
                *w.projected.entry(node).or_default() += cp.size;
                evict.push(cp);
            } else {
                only.push(cp);
            }
        }
        if !evict.is_empty() {
            steps.push(Step::Evict {
                host: host.clone(),
                node,
                bytes: evict.iter().map(|c| c.size).sum(),
                copies: evict,
            });
        }
        if deficit > 0 && !only.is_empty() {
            let before: u64 = only.iter().map(|c| c.size).sum();
            only = w.offload(node, only, Some(deficit), &mut steps);
            let after: u64 = only.iter().map(|c| c.size).sum();
            deficit = deficit.saturating_sub(before - after);
        }
        if deficit > 0 {
            let mut why = format!("{host}: still {} short of the target", human(deficit));
            for (rule, b) in &req_bytes {
                why += &format!("; rule {rule:?} requires {} here", human(*b));
            }
            let stuck: u64 = only.iter().map(|c| c.size).sum();
            if stuck > 0 {
                let archive_why = if w.dest.iter().any(|d| d.reachable) {
                    "the chosen archive stores have no room for them"
                } else {
                    "no archive store was chosen to offload them into"
                };
                why += &format!("; {} here are only copies and {archive_why}", human(stuck));
            }
            if req_bytes.is_empty() && stuck == 0 {
                why += "; nothing else sparknest holds here can move (the rest is other data)";
            }
            w.blocked.push(why);
        }
    }
    let rows = w.host_rows(want.clone().into_iter());
    Ok(w.finish(goal, rows, steps))
}

fn make_tidy(mut w: World, goal: Goal, days: u64, nodes: &[NodeId]) -> Plan {
    let cutoff = w.now_ms.saturating_sub(days * DAY_MS);
    let mut cands: Vec<(NodeId, Copy, u64)> = Vec::new();
    let mut kept_required: HashMap<NodeId, u64> = HashMap::new();
    // Not opened, but written within the window (just downloaded): kept,
    // and said so.
    let mut kept_fresh: HashMap<NodeId, u64> = HashMap::new();
    for &node in nodes {
        if !w.usage.contains_key(&node) {
            let h = w.host(node);
            w.blocked
                .push(format!("{h}: usage unknown (did not answer); left alone"));
            continue;
        }
        for cp in w.copies_on.get(&node).cloned().unwrap_or_default() {
            let last = w.last_open(node, cp.file);
            let created = w.created_ms.get(&cp.file).copied().unwrap_or(0);
            if last >= cutoff {
                continue;
            }
            if created >= cutoff {
                *kept_fresh.entry(node).or_default() += cp.size;
                continue;
            }
            if w.required(cp.file, node).is_some() {
                *kept_required.entry(node).or_default() += cp.size;
                continue;
            }
            cands.push((node, cp, last));
        }
    }
    // Oldest use first, so the host that used a file last keeps it when
    // only one copy may stay.
    cands.sort_by(|a, b| a.2.cmp(&b.2).then(b.1.size.cmp(&a.1.size)));
    let mut evict: BTreeMap<NodeId, Vec<Copy>> = BTreeMap::new();
    let mut only: BTreeMap<NodeId, Vec<Copy>> = BTreeMap::new();
    for (node, mut cp, _) in cands {
        cp.why = w.idle_words(node, cp.file);
        let n = w.count.get_mut(&(cp.file, cp.generation)).expect("counted");
        if *n >= 2 {
            *n -= 1;
            *w.projected.entry(node).or_default() += cp.size;
            evict.entry(node).or_default().push(cp);
        } else {
            only.entry(node).or_default().push(cp);
        }
    }
    let mut steps = Vec::new();
    for (node, copies) in evict {
        steps.push(Step::Evict {
            host: w.host(node),
            node,
            bytes: copies.iter().map(|c| c.size).sum(),
            copies,
        });
    }
    for (node, copies) in only {
        let left = w.offload(node, copies, None, &mut steps);
        let b: u64 = left.iter().map(|c| c.size).sum();
        if b > 0 {
            let h = w.host(node);
            w.notes.push(format!(
                "{h}: {} idle but the only copies{}",
                human(b),
                if w.dest.is_empty() {
                    "; choose an archive to move them"
                } else {
                    "; no room in the chosen archives"
                }
            ));
        }
    }
    for (node, b) in kept_fresh {
        let h = w.host(node);
        w.notes.push(format!(
            "{h}: {} not opened, but written within the last {days} day{} (new downloads stay); kept",
            human(b),
            if days == 1 { "" } else { "s" }
        ));
    }
    for (node, b) in kept_required {
        let h = w.host(node);
        w.notes.push(format!(
            "{h}: {} idle but required by rules; kept",
            human(b)
        ));
    }
    let rows = w.host_rows(nodes.iter().map(|n| (*n, 0)));
    w.finish(goal, rows, steps)
}

fn make_speedup(
    placer: &Placer,
    mut w: World,
    goal: Goal,
    days: u64,
    nodes: &[NodeId],
    min_remote: u64,
    keep_free: Option<u64>,
) -> NestResult<Plan> {
    let c = placer.vfs().data().meta().open_reader().map_err(sql)?;
    let mut steps = Vec::new();
    for &node in nodes {
        let host = w.host(node);
        let Some(used) = w.usage.get(&node) else {
            w.blocked
                .push(format!("{host}: usage unknown (did not answer)"));
            continue;
        };
        let have: HashSet<(FileId, Generation)> = w
            .copies_on
            .get(&node)
            .map(|v| v.iter().map(|c| (c.file, c.generation)).collect())
            .unwrap_or_default();
        let mut wanted: Vec<(u64, Copy)> = Vec::new();
        for u in used.values() {
            let net = u.remote_bytes + u.archive_bytes;
            if net < min_remote {
                continue;
            }
            let Some(a) = query::getattr(&c, u.file).map_err(sql)? else {
                continue;
            };
            if a.gen_state != GenState::Stable || have.contains(&(a.id, a.generation)) {
                continue;
            }
            let path = query::path_of(&c, a.id)
                .map_err(sql)?
                .map(|p| String::from_utf8_lossy(&p).into_owned())
                .unwrap_or_default();
            let mut why = format!(
                "{host} read {} of it over the network in the last {days} days",
                human(u.remote_bytes)
            );
            if u.archive_bytes > 0 {
                why = format!(
                    "{host} read {} over the network and {} from an archive in the last {days} days",
                    human(u.remote_bytes),
                    human(u.archive_bytes)
                );
            }
            wanted.push((
                net,
                Copy {
                    file: a.id,
                    generation: a.generation,
                    size: a.size,
                    path,
                    why,
                },
            ));
        }
        wanted.sort_by_key(|w| std::cmp::Reverse(w.0));
        let floor = keep_free.unwrap_or(w.total.get(&node).copied().unwrap_or(0) / 10);
        let mut room = w
            .free_now
            .get(&node)
            .copied()
            .unwrap_or(0)
            .saturating_sub(floor);
        let mut add = Vec::new();
        let mut skipped = 0u64;
        for (_, cp) in wanted {
            if cp.size > room {
                skipped += cp.size;
                continue;
            }
            room -= cp.size;
            let p = w.projected.entry(node).or_default();
            *p = p.saturating_sub(cp.size);
            add.push(cp);
        }
        if skipped > 0 {
            w.notes.push(format!(
                "{host}: {} more would help but would leave less than {} free",
                human(skipped),
                human(floor)
            ));
        }
        if !add.is_empty() {
            steps.push(Step::Replicate {
                host,
                node,
                bytes: add.iter().map(|c| c.size).sum(),
                copies: add,
            });
        }
    }
    let rows = w.host_rows(nodes.iter().map(|n| (*n, 0)));
    Ok(w.finish(goal, rows, steps))
}

type CopyMaps = (
    HashMap<NodeId, Vec<Copy>>,
    HashMap<(FileId, Generation), usize>,
    HashMap<FileId, u64>,
);

/// All live copies (live stores only in the per-node map; archives count
/// toward copy totals), and when each file was created. Synchronous: no
/// connection crosses an await.
fn collect_copies(placer: &Placer) -> NestResult<CopyMaps> {
    let c = placer.vfs().data().meta().open_reader().map_err(sql)?;
    let mut copies_on: HashMap<NodeId, Vec<Copy>> = HashMap::new();
    let mut count: HashMap<(FileId, Generation), usize> = HashMap::new();
    let mut created: HashMap<FileId, u64> = HashMap::new();
    let mut st = c
        .prepare("SELECT r.file, r.gen, r.store, f.size, f.mtime FROM replicas r JOIN files f ON f.id = r.file AND f.gen = r.gen WHERE r.state = 1")
        .map_err(sql)?;
    let rows: Vec<(u64, u64, u64, u64, i64)> = st
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)? as u64,
                r.get::<_, i64>(1)? as u64,
                r.get::<_, i64>(2)? as u64,
                r.get::<_, i64>(3)? as u64,
                r.get::<_, i64>(4)?,
            ))
        })
        .map_err(sql)?
        .collect::<rusqlite::Result<_>>()
        .map_err(sql)?;
    for (f, g, store, size, mtime_ns) in rows {
        let (file, generation) = (FileId(f), Generation(g));
        *count.entry((file, generation)).or_default() += 1;
        created.insert(file, (mtime_ns.max(0) as u64) / 1_000_000);
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
                why: String::new(),
            });
        }
    }
    Ok((copies_on, count, created))
}

pub fn human(b: u64) -> String {
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
