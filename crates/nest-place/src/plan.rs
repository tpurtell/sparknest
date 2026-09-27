//! Goal-oriented planning (PROPOSAL §7, ADR-028): propose changes to where
//! copies live, show why each one is proposed, and apply them on approval.
//!
//! Goals:
//! - **Free**: a target free space per host. Above what a host has now, it
//!   sheds: redundant copies first (another copy exists elsewhere, including
//!   an archive, and no rule needs it here), least recently opened there
//!   first; then its sole copies move to hosts whose target is *below* what
//!   they have now (room the user granted), most room first; then into the
//!   archive stores the caller chose, in order. A moved copy is removed
//!   from its source only once the receiver holds it (checked when
//!   applying).
//! - **Tidy**: remove copies not opened on their host within a window (and
//!   not written within it), oldest use first, down to one copy anywhere;
//!   sole copies move to a chosen archive or stay.
//! - **Speedup**: add copies on hosts that read a file over the network (or
//!   from an archive) at least a threshold within a window, most-read first,
//!   while the host keeps its free-space floor.
//! - **Consolidate**: one selection onto one host (by default the host that
//!   used it most in the last 30 days): copy what it lacks, then remove the
//!   other hosts' copies. A copy elsewhere is removed only once the chosen
//!   host holds that file (checked when applying); archive copies and
//!   copies rules require stay.
//!
//! - **Place**: one selection over chosen hosts with a replica factor: each
//!   file ends on `round(factor × hosts)` of them (at least one), the split
//!   kept even (every host near its share of the bytes), and among the hosts
//!   with room in their share, those that read that file most come first,
//!   then those already holding it. Copies outside the assignment are
//!   removed once a host it assigns holds the file (checked when applying).
//!   Factor 1 copies everything everywhere; 1/hosts spreads it; one host
//!   gathers it there.
//!
//! A plan can be edited before it is applied: any copy can be skipped (and
//! taken back), by host, by model or file (`select`). Skipping a copy a
//! removal depends on only keeps the removal from happening.
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
    /// Remove this copy only once this node holds the file (place).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keeper: Option<NodeId>,
    /// Left out of the plan by the person editing it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub skip: bool,
    /// The model (org/name) or directory the file belongs to, and its name
    /// there, for people (`label`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
}

impl Copy {
    /// How people know the file: group/name when labeled, else its path.
    pub fn label(&self) -> String {
        if self.name.is_empty() {
            self.path.clone()
        } else {
            format!("{}/{}", self.group, self.name)
        }
    }

    fn new(file: FileId, generation: Generation, size: u64, path: String, why: String) -> Copy {
        Copy {
            file,
            generation,
            size,
            path,
            why,
            keeper: None,
            skip: false,
            group: String::new(),
            name: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Step {
    Evict {
        host: String,
        node: NodeId,
        bytes: u64,
        copies: Vec<Copy>,
        /// Remove a copy only if this node then holds the file (consolidate).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        requires: Option<NodeId>,
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

    fn parts(&mut self) -> (&str, &mut Vec<Copy>, &mut u64) {
        match self {
            Step::Evict {
                host,
                copies,
                bytes,
                ..
            }
            | Step::Offload {
                host,
                copies,
                bytes,
                ..
            }
            | Step::Replicate {
                host,
                copies,
                bytes,
                ..
            } => (host, copies, bytes),
        }
    }

    /// The step without the copies skipped; none if nothing is left.
    pub fn selected(&self) -> Option<Step> {
        let mut s = self.clone();
        let (_, copies, bytes) = s.parts();
        copies.retain(|c| !c.skip);
        *bytes = copies.iter().map(|c| c.size).sum();
        (!copies.is_empty()).then_some(s)
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
    Consolidate {
        /// A path, or hf:org/name.
        selector: String,
        /// Where to; by default the host that used it most.
        #[serde(default)]
        host: Option<String>,
        /// Namespace hub for hf selectors (filled by the API).
        #[serde(default)]
        hub: String,
    },
    Place {
        /// A path, or hf:org/name.
        selector: String,
        /// Hosts or @groups it goes on; every host when empty.
        #[serde(default)]
        hosts: Vec<String>,
        /// Share of those hosts each file is on: 1/hosts (spread) to 1
        /// (everywhere).
        #[serde(default = "default_factor")]
        factor: f64,
        #[serde(default)]
        hub: String,
    },
}

fn default_factor() -> f64 {
    1.0
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
        Goal::Consolidate {
            selector,
            host,
            hub,
        } => {
            let sel = Selector::parse(selector, hub).map_err(NestError::Invalid)?;
            let m = placer.manifest(&sel).await?;
            let w = World::gather(placer, &[], nest_data::usage::KEEP_DAYS).await?;
            let host = host.clone();
            make_consolidate(placer, w, goal.clone(), &m, host.as_deref())
        }
        Goal::Place {
            selector,
            hosts,
            factor,
            hub,
        } => {
            let sel = Selector::parse(selector, hub).map_err(NestError::Invalid)?;
            let m = placer.manifest(&sel).await?;
            let w = World::gather(placer, &[], nest_data::usage::KEEP_DAYS).await?;
            let targets = scope(placer, hosts)?;
            make_place(w, goal.clone(), &m, targets, *factor)
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
    // Room granted: hosts asked to keep less free than they have.
    let mut rooms: BTreeMap<NodeId, u64> = want
        .iter()
        .filter_map(|(n, t)| {
            let now = w.free_now.get(n).copied().unwrap_or(0);
            (*t < now).then(|| (*n, now - *t))
        })
        .collect();
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
                requires: None,
            });
        }
        // Sole copies move to hosts with room granted, most room first.
        if deficit > 0 && !only.is_empty() {
            let mut moved: BTreeMap<NodeId, Vec<Copy>> = BTreeMap::new();
            let mut left = Vec::new();
            for mut cp in only.drain(..) {
                if deficit == 0 {
                    left.push(cp);
                    continue;
                }
                let to = rooms
                    .iter()
                    .filter(|(r, room)| **r != node && **room >= cp.size)
                    .max_by_key(|(_, room)| **room)
                    .map(|(r, _)| *r);
                match to {
                    Some(r) => {
                        *rooms.get_mut(&r).expect("present") -= cp.size;
                        deficit = deficit.saturating_sub(cp.size);
                        *w.projected.entry(node).or_default() += cp.size;
                        let pr = w.projected.entry(r).or_default();
                        *pr = pr.saturating_sub(cp.size);
                        cp.why = format!(
                            "only copy; {}; moved to {} (room granted there)",
                            cp.why,
                            w.host(r)
                        );
                        moved.entry(r).or_default().push(cp);
                    }
                    None => left.push(cp),
                }
            }
            only = left;
            for (r, copies) in moved {
                let bytes = copies.iter().map(|c| c.size).sum();
                steps.push(Step::Replicate {
                    host: w.host(r),
                    node: r,
                    bytes,
                    copies: copies.clone(),
                });
                steps.push(Step::Evict {
                    host: host.clone(),
                    node,
                    bytes,
                    copies,
                    requires: Some(r),
                });
            }
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
                    "no host was granted room for them and the chosen archive stores have none"
                } else {
                    "no host was granted room for them (drag another host's handle toward less free) and no archive store was chosen"
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
            requires: None,
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
            wanted.push((net, Copy::new(a.id, a.generation, a.size, path, why)));
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

fn make_consolidate(
    placer: &Placer,
    mut w: World,
    goal: Goal,
    m: &crate::selector::Manifest,
    host: Option<&str>,
) -> NestResult<Plan> {
    let files: HashSet<FileId> = m
        .entries
        .iter()
        .filter(|e| e.stable)
        .map(|e| e.file)
        .collect();
    let nodes: Vec<NodeId> = placer.nodes()?.into_iter().map(|h| h.node).collect();
    let held = |w: &World, n: NodeId| -> u64 {
        w.copies_on
            .get(&n)
            .map(|v| {
                v.iter()
                    .filter(|c| files.contains(&c.file))
                    .map(|c| c.size)
                    .sum()
            })
            .unwrap_or(0)
    };
    // The target: named, else most opens, then most bytes read, then most
    // already held.
    let target = match host {
        Some(h) => placer
            .resolve_hosts(&[h.to_string()])?
            .first()
            .map(|x| x.node)
            .ok_or_else(|| NestError::Invalid(format!("unknown host {h}")))?,
        None => {
            let score = |n: NodeId| {
                let (mut opens, mut bytes) = (0u64, 0u64);
                if let Some(u) = w.usage.get(&n) {
                    for f in &files {
                        if let Some(x) = u.get(f) {
                            opens += x.opens;
                            bytes += x.local_bytes + x.remote_bytes + x.archive_bytes;
                        }
                    }
                }
                (opens, bytes, held(&w, n))
            };
            let best = nodes
                .iter()
                .copied()
                .max_by_key(|n| score(*n))
                .ok_or(NestError::NotFound)?;
            let (opens, bytes, have) = score(best);
            let why = if opens > 0 {
                format!(
                    "used most there: opened {opens}× and {} read in 30 days",
                    human(bytes)
                )
            } else if have > 0 {
                format!(
                    "no host opened it in 30 days; it already holds the most ({})",
                    human(have)
                )
            } else {
                "no host holds or used it".into()
            };
            w.notes.push(format!("{}: {why}", w.host(best)));
            best
        }
    };
    let host_name = w.host(target);
    let mut steps = Vec::new();
    // 1. What the target lacks.
    let have: HashSet<(FileId, Generation)> = w
        .copies_on
        .get(&target)
        .map(|v| v.iter().map(|c| (c.file, c.generation)).collect())
        .unwrap_or_default();
    let add: Vec<Copy> = m
        .entries
        .iter()
        .filter(|e| e.stable && !have.contains(&(e.file, e.generation)))
        .map(|e| {
            Copy::new(
                e.file,
                e.generation,
                e.size,
                e.path.clone(),
                format!("gathering it on {host_name}"),
            )
        })
        .collect();
    let need: u64 = add.iter().map(|c| c.size).sum();
    if need > w.free_now.get(&target).copied().unwrap_or(0) {
        w.blocked.push(format!(
            "{host_name}: needs {} more but has {} free",
            human(need),
            human(w.free_now.get(&target).copied().unwrap_or(0))
        ));
    }
    if !add.is_empty() {
        let p = w.projected.entry(target).or_default();
        *p = p.saturating_sub(need);
        steps.push(Step::Replicate {
            host: host_name.clone(),
            node: target,
            bytes: need,
            copies: add,
        });
    }
    // 2. Everyone else's copies, once the target holds them.
    for &n in &nodes {
        if n == target {
            continue;
        }
        let mut drop = Vec::new();
        let mut req = 0u64;
        let mut rules: Vec<String> = Vec::new();
        for c in w.copies_on.get(&n).cloned().unwrap_or_default() {
            if !files.contains(&c.file) {
                continue;
            }
            if let Some(r) = w.required(c.file, n) {
                req += c.size;
                if !rules.contains(r) {
                    rules.push(r.clone());
                }
                continue;
            }
            drop.push(Copy {
                why: format!("kept on {host_name} instead"),
                ..c
            });
        }
        if req > 0 {
            let h = w.host(n);
            let quoted: Vec<String> = rules.iter().map(|r| format!("{r:?}")).collect();
            w.notes.push(format!(
                "{h}: {} stays, rule {} keeps it there",
                human(req),
                quoted.join(", ")
            ));
        }
        if !drop.is_empty() {
            let b: u64 = drop.iter().map(|c| c.size).sum();
            *w.projected.entry(n).or_default() += b;
            steps.push(Step::Evict {
                host: w.host(n),
                node: n,
                bytes: b,
                copies: drop,
                requires: Some(target),
            });
        }
    }
    let rows = w.host_rows(nodes.iter().map(|n| (*n, 0)));
    Ok(w.finish(goal, rows, steps))
}

fn make_place(
    mut w: World,
    goal: Goal,
    m: &crate::selector::Manifest,
    mut targets: Vec<NodeId>,
    factor: f64,
) -> NestResult<Plan> {
    targets.sort();
    targets.dedup();
    if targets.is_empty() {
        return Err(NestError::Invalid("no hosts to place it on".into()));
    }
    let h = targets.len();
    let k = ((factor.clamp(0.0, 1.0) * h as f64).round() as usize).clamp(1, h);
    // One entry per file (a file two snapshots share is placed once).
    let mut seen = HashSet::new();
    let mut entries: Vec<&crate::selector::Entry> = m
        .entries
        .iter()
        .filter(|e| e.stable && seen.insert(e.file))
        .collect();
    let skipped = m.entries.iter().filter(|e| !e.stable).count();
    if skipped > 0 {
        w.notes.push(format!(
            "{skipped} files are being written and are left as they are"
        ));
    }
    entries.sort_by(|a, b| b.size.cmp(&a.size).then(a.file.cmp(&b.file)));
    let total: u64 = entries.iter().map(|e| e.size).sum();
    // Each host's even share of the bytes to hold.
    let share = ((total as u128 * k as u128) / h as u128) as u64;
    // Who holds which generation now (live stores).
    let mut holders: HashMap<(FileId, Generation), Vec<NodeId>> = HashMap::new();
    for (n, v) in &w.copies_on {
        for c in v {
            holders.entry((c.file, c.generation)).or_default().push(*n);
        }
    }
    let read = |w: &World, n: NodeId, f: FileId| -> u64 {
        w.usage
            .get(&n)
            .and_then(|u| u.get(&f))
            .map_or(0, |u| u.local_bytes + u.remote_bytes + u.archive_bytes)
    };
    let margin = |w: &World, n: NodeId| -> u64 {
        (64u64 << 30).max(w.total.get(&n).copied().unwrap_or(0) / 20)
    };
    let mut assigned: HashMap<NodeId, u64> = HashMap::new();
    let mut add: BTreeMap<NodeId, Vec<Copy>> = BTreeMap::new();
    let mut drop: BTreeMap<NodeId, Vec<Copy>> = BTreeMap::new();
    let mut kept: HashMap<String, u64> = HashMap::new();
    let (mut short_files, mut short_bytes) = (0u64, 0u64);
    for e in &entries {
        let held = holders
            .get(&(e.file, e.generation))
            .cloned()
            .unwrap_or_default();
        let mut cands = targets.clone();
        // Hosts still below their share first; among them, those that read
        // this file most, then those holding it, then the emptiest.
        cands.sort_by_key(|n| {
            let a = assigned.get(n).copied().unwrap_or(0);
            (
                a >= share,
                std::cmp::Reverse(read(&w, *n, e.file)),
                !held.contains(n),
                a,
                *n,
            )
        });
        let mut picked: Vec<NodeId> = Vec::new();
        for n in cands {
            if picked.len() == k {
                break;
            }
            if !held.contains(&n) {
                let room = w.projected.get(&n).copied().unwrap_or(0);
                if room < e.size + margin(&w, n) {
                    continue;
                }
            }
            picked.push(n);
        }
        if picked.len() < k {
            short_files += 1;
            short_bytes += e.size;
        }
        for &n in &picked {
            *assigned.entry(n).or_default() += e.size;
            if !held.contains(&n) {
                let p = w.projected.entry(n).or_default();
                *p = p.saturating_sub(e.size);
                add.entry(n).or_default().push(Copy::new(
                    e.file,
                    e.generation,
                    e.size,
                    e.path.clone(),
                    if read(&w, n, e.file) > 0 {
                        format!("read there ({})", human(read(&w, n, e.file)))
                    } else {
                        format!("{k} of {h} hosts hold each file")
                    },
                ));
            }
        }
        let Some(&keeper) = picked.iter().find(|n| held.contains(n)).or(picked.first()) else {
            continue;
        };
        for &n in &held {
            if picked.contains(&n) {
                continue;
            }
            if let Some(rule) = w.required(e.file, n) {
                *kept
                    .entry(format!("{}: rule {rule:?}", w.host(n)))
                    .or_default() += e.size;
                continue;
            }
            let mut c = Copy::new(
                e.file,
                e.generation,
                e.size,
                e.path.clone(),
                format!("kept on {} instead", w.host(keeper)),
            );
            c.keeper = Some(keeper);
            drop.entry(n).or_default().push(c);
        }
    }
    let names: Vec<String> = targets.iter().map(|n| w.host(*n)).collect();
    w.notes.push(if k == h && h > 1 {
        format!("every file on each of {}", names.join(", "))
    } else if k == 1 && h > 1 {
        format!(
            "one copy of each file, spread over {}: about {} each",
            names.join(", "),
            human(share)
        )
    } else if h == 1 {
        format!("everything on {}", names[0])
    } else {
        format!(
            "{k} copies of each file over {}: about {} each",
            names.join(", "),
            human(share)
        )
    });
    if short_files > 0 {
        w.blocked.push(format!(
            "{short_files} files ({}) get fewer than {k} copies: the hosts lack room",
            human(short_bytes)
        ));
    }
    let mut kept: Vec<_> = kept.into_iter().collect();
    kept.sort();
    for (who, b) in kept {
        let (host, rule) = who.split_once(": ").unwrap_or((&who, ""));
        w.notes
            .push(format!("{host}: {} stays, {rule} keeps it there", human(b)));
    }
    let mut steps = Vec::new();
    for (n, copies) in add {
        steps.push(Step::Replicate {
            host: w.host(n),
            node: n,
            bytes: copies.iter().map(|c| c.size).sum(),
            copies,
        });
    }
    for (n, copies) in drop {
        let b: u64 = copies.iter().map(|c| c.size).sum();
        *w.projected.entry(n).or_default() += b;
        steps.push(Step::Evict {
            host: w.host(n),
            node: n,
            bytes: b,
            copies,
            requires: None,
        });
    }
    let mut rows: Vec<NodeId> = w.name_of.keys().copied().collect();
    rows.sort();
    let rows = w.host_rows(rows.into_iter().map(|n| (n, 0)));
    Ok(w.finish(goal, rows, steps))
}

/// Name every copy for people: a Hugging Face file by its repo and the name
/// its snapshot gives it, anything else by its directory and file name.
pub fn label(c: &rusqlite::Connection, hub: &str, plan: &mut Plan) -> NestResult<()> {
    let names = if hub.is_empty() {
        HashMap::new()
    } else {
        crate::space::hf_names(c, hub)?
    };
    for s in &mut plan.steps {
        let (_, copies, _) = s.parts();
        for cp in copies.iter_mut() {
            (cp.group, cp.name) = match names.get(&cp.file) {
                Some((repo, name)) => (repo.clone(), name.clone()),
                None => match cp.path.rsplit_once('/') {
                    Some((d, f)) => (
                        if d.is_empty() {
                            "/".into()
                        } else {
                            d.to_string()
                        },
                        f.to_string(),
                    ),
                    None => (String::new(), cp.path.clone()),
                },
            };
        }
    }
    Ok(())
}

/// Skip (`on` false) or take back copies of a plan: on `host` (every host
/// when none), matching any of `matches` — a model (org/name, hf:org/name),
/// a directory or path prefix, or one file (group/name, its path, or #id).
/// No `matches` means every copy there. Returns how many copies changed.
pub fn select(plan: &mut Plan, host: Option<&str>, matches: &[String], on: bool) -> usize {
    let hit = |c: &Copy| {
        matches.is_empty()
            || matches.iter().any(|m| {
                let m = m
                    .strip_prefix("hf:")
                    .or_else(|| m.strip_prefix("hf-dataset:"))
                    .unwrap_or(m);
                let m = m.trim_end_matches('/');
                if let Some(id) = m.strip_prefix('#') {
                    return id.parse::<u64>().is_ok_and(|id| c.file.0 == id);
                }
                c.group == m
                    || format!("{}/{}", c.group, c.name) == m
                    || c.path == m
                    || c.path.starts_with(&format!("{m}/"))
                    || c.group.starts_with(&format!("{m}/"))
            })
    };
    let mut n = 0;
    for s in &mut plan.steps {
        let (h, copies, _) = s.parts();
        if host.is_some_and(|x| x != h) {
            continue;
        }
        for c in copies.iter_mut() {
            if c.skip == on && hit(c) {
                c.skip = !on;
                n += 1;
            }
        }
    }
    n
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
            copies_on.entry(NodeId(store)).or_default().push(Copy::new(
                file,
                generation,
                size,
                path,
                String::new(),
            ));
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
