//! Offline checks, with no daemon running (`sparknestd fsck`).

use crate::config::Config;
use anyhow::Context;
use nest_meta::query;
use nest_store::{ObjectKey, ObjectStore};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct OfflineReport {
    pub objects: usize,
    pub listed: usize,
    /// Copies the metadata lists here that the disk lacks.
    pub missing: Vec<String>,
    /// Stable copies whose size differs from the metadata.
    pub damaged: Vec<String>,
    /// Objects the metadata does not know.
    pub unknown: Vec<String>,
}

/// Compare this host's object store with its local `meta.sqlite` (which may
/// lag the cluster, so this only reports).
pub fn fsck(cfg: &Config) -> anyhow::Result<OfflineReport> {
    let dir = &cfg.node.state_dir;
    let meta = dir.join("meta.sqlite");
    let c =
        rusqlite::Connection::open_with_flags(&meta, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("opening {}", meta.display()))?;
    let live = cfg.node.id.live_store();
    let listed: HashSet<ObjectKey> = query::store_inventory(&c, live)?
        .into_iter()
        .map(|(f, g)| ObjectKey::new(f, g))
        .collect();
    let owned: HashSet<ObjectKey> = query::owned_by(&c, cfg.node.id)?
        .into_iter()
        .map(|a| ObjectKey::new(a.id, a.generation))
        .collect();
    let store = ObjectStore::open_readonly(dir).context("opening the object store")?;
    let disk: HashMap<ObjectKey, u64> =
        store.scan()?.into_iter().map(|o| (o.key, o.size)).collect();
    let path = |k: &ObjectKey| {
        query::path_of(&c, k.file)
            .ok()
            .flatten()
            .map(|p| String::from_utf8_lossy(&p).into_owned())
            .unwrap_or_else(|| format!("<file {}>", k.file.0))
    };
    let mut r = OfflineReport {
        objects: disk.len(),
        listed: listed.len(),
        ..Default::default()
    };
    for k in &listed {
        match disk.get(k) {
            None => r.missing.push(path(k)),
            Some(&size) => {
                if let Ok(Some(a)) = query::getattr(&c, k.file)
                    && a.generation == k.generation
                    && a.gen_state == nest_types::GenState::Stable
                    && a.size != size
                {
                    r.damaged
                        .push(format!("{} ({size} bytes, expected {})", path(k), a.size));
                }
            }
        }
    }
    for k in disk.keys() {
        if !listed.contains(k) && !owned.contains(k) {
            r.unknown
                .push(format!("{:016x}.{:x}", k.file.0, k.generation.0));
        }
    }
    r.missing.sort();
    r.damaged.sort();
    r.unknown.sort();
    Ok(r)
}
