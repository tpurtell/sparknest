//! fsck: compare this host's object store with the metadata and settle the
//! differences (ADR-026). Used by the daemon's pre-mount recovery phase and
//! by `nest fsck`.
//!
//! The metadata is authoritative. Every resolution keeps data rather than
//! destroy it when the right answer is not certain:
//!
//! - an object no metadata refers to is deleted when the metadata is known
//!   complete (a crashed host that has caught up with a healthy quorum),
//!   and otherwise moved into `/.lost+found/<run>/<host>/<its path>` (after
//!   a re-found the metadata may have rolled back past it); its path comes
//!   from earlier metadata the host set aside, if any knows it;
//! - a copy the metadata lists but the disk lacks is retired; a file left
//!   with no copy anywhere is reported lost;
//! - a stable copy whose size (or, deep, HF blob hash) is wrong is retired
//!   when another copy exists; if it was the only one its bytes go to
//!   lost+found and the file is reported lost;
//! - a file this host was writing when the previous run ended is finalized
//!   with the size found on disk (startup only).

use crate::vfs::Vfs;
use nest_meta::{Command, Reply, query};
use nest_store::ObjectKey;
use nest_types::{
    FileId, FileKind, GenState, Generation, NestError, NestResult, ReplicaState, Timestamp,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Orphans {
    Delete,
    Quarantine,
    Report,
}

#[derive(Clone, Debug)]
pub struct FsckOptions {
    /// Apply resolutions (otherwise only report).
    pub repair: bool,
    pub orphans: Orphans,
    /// Verify Hugging Face blobs against the SHA-256 in their names.
    pub deep: bool,
    /// Settle files this host owned when the previous run ended. Startup
    /// only: while running, owned files have live writers.
    pub settle_owned: bool,
    /// Leave objects younger than this alone (a copy that is still being
    /// published looks like an orphan for a moment).
    pub min_age: Duration,
    /// Name used for this host's lost+found directory.
    pub host: String,
    /// This run's directory under `/.lost+found` (a UTC date-time).
    pub stamp: String,
}

/// A lost+found run name: the current UTC time, `2026-09-27T03-14-09Z`.
pub fn stamp_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}-{:02}-{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Earlier metadata this host set aside (re-found, join), newest first.
fn previous_metadata(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<(std::time::SystemTime, std::path::PathBuf)> = std::fs::read_dir(root)
        .map(|rd| {
            rd.flatten()
                .filter(|e| e.file_name().to_string_lossy().starts_with("pre-"))
                .map(|e| e.path().join("meta.sqlite"))
                .filter(|p| p.exists())
                .filter_map(|p| Some((std::fs::metadata(&p).ok()?.modified().ok()?, p)))
                .collect()
        })
        .unwrap_or_default();
    v.sort_by_key(|x| std::cmp::Reverse(x.0));
    v.into_iter().map(|(_, p)| p).collect()
}

/// The path of `file` in earlier metadata, if any knows it.
fn old_path(previous: &[std::path::PathBuf], file: FileId) -> Option<String> {
    previous.iter().find_map(|p| {
        let c =
            rusqlite::Connection::open_with_flags(p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .ok()?;
        query::path_of(&c, file)
            .ok()
            .flatten()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "issue", rename_all = "snake_case")]
pub enum Issue {
    /// An object no metadata refers to.
    Orphan {
        file: u64,
        generation: u64,
        size: u64,
        note: Option<String>,
    },
    /// The metadata lists a copy here that is not on disk.
    Missing {
        file: u64,
        generation: u64,
        path: String,
        other_copies: usize,
    },
    /// A stable copy whose size differs from the metadata.
    Damaged {
        file: u64,
        generation: u64,
        path: String,
        expected: u64,
        found: u64,
        other_copies: usize,
    },
    /// Deep check: an HF blob whose bytes do not match its name.
    Corrupt {
        file: u64,
        generation: u64,
        path: String,
        other_copies: usize,
    },
    /// A file this host was writing when the previous run ended.
    Unfinished {
        file: u64,
        generation: u64,
        path: String,
        size: u64,
        content_lost: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    Reported,
    Deleted,
    Quarantined {
        to: String,
    },
    Retired,
    /// No copy left: the file's data is gone; any bytes kept are in `to`.
    Lost {
        to: Option<String>,
    },
    Finalized {
        size: u64,
    },
    Failed {
        error: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Finding {
    #[serde(flatten)]
    pub issue: Issue,
    #[serde(flatten)]
    pub action: Action,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FsckReport {
    pub objects: usize,
    pub findings: Vec<Finding>,
}

impl FsckReport {
    /// Files whose data no longer exists anywhere.
    pub fn lost(&self) -> usize {
        self.findings
            .iter()
            .filter(|f| matches!(f.action, Action::Lost { .. }))
            .count()
    }
}

fn io(e: std::io::Error) -> NestError {
    NestError::from_io(&e)
}

/// A Hugging Face blob name: 64 lowercase hex characters (SHA-256).
fn hf_sha256_name(path: &str) -> Option<&str> {
    let name = path.rsplit('/').next()?;
    (path.contains("/blobs/") && name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit()))
        .then_some(name)
}

impl Vfs {
    /// Check (and with `repair`, settle) this host's live object store.
    pub async fn fsck(&self, opts: &FsckOptions) -> NestResult<FsckReport> {
        let d = self.data().clone();
        let store = d.store().clone();
        let me = self.me();
        let live = me.live_store();
        let (expected, owned) = self.q(|c| {
            let e: HashSet<ObjectKey> = query::store_inventory(c, live)?
                .into_iter()
                .map(|(f, g)| ObjectKey::new(f, g))
                .collect();
            Ok((e, query::owned_by(c, me)?))
        })?;
        let st = store.clone();
        let disk: HashMap<ObjectKey, u64> = tokio::task::spawn_blocking(move || st.scan())
            .await
            .map_err(|e| NestError::Io(e.to_string()))?
            .map_err(io)?
            .into_iter()
            .map(|o| (o.key, o.size))
            .collect();
        let mut report = FsckReport {
            objects: disk.len(),
            ..Default::default()
        };
        let previous = previous_metadata(store.root());
        let path_of = |f: FileId| -> String {
            self.q(|c| query::path_of(c, f))
                .ok()
                .flatten()
                .map(|p| String::from_utf8_lossy(&p).into_owned())
                .unwrap_or_else(|| format!("<file {}>", f.0))
        };
        let others = |f: FileId, g: Generation| -> usize {
            self.q(|c| query::replicas(c, f))
                .map(|rs| {
                    rs.iter()
                        .filter(|r| {
                            r.generation == g && r.state == ReplicaState::Live && r.store != live
                        })
                        .count()
                })
                .unwrap_or(0)
        };

        // Files this host was writing.
        let mut working: HashSet<ObjectKey> = HashSet::new();
        for a in &owned {
            let key = ObjectKey::new(a.id, a.generation);
            working.insert(key);
            if !opts.settle_owned {
                continue;
            }
            let (size, lost) = match disk.get(&key) {
                Some(s) => (*s, false),
                None => (0, a.size > 0),
            };
            let issue = Issue::Unfinished {
                file: a.id.0,
                generation: a.generation.0,
                path: path_of(a.id),
                size,
                content_lost: lost,
            };
            let action = if !opts.repair {
                Action::Reported
            } else {
                match self.settle_owned(a, key, disk.contains_key(&key)).await {
                    Ok(size) => Action::Finalized { size },
                    Err(e) => Action::Failed {
                        error: e.to_string(),
                    },
                }
            };
            report.findings.push(Finding { issue, action });
        }

        // Objects on disk that nothing refers to.
        let now = Timestamp::now();
        for (&key, &size) in &disk {
            if expected.contains(&key) || working.contains(&key) {
                continue;
            }
            let fresh = store
                .stat(key)
                .map(|(_, m)| now.0.saturating_sub(m.0) < opts.min_age.as_nanos() as i64)
                .unwrap_or(true);
            if fresh {
                continue;
            }
            // Re-check: a copy may have been published since we looked.
            if self
                .q(|c| query::has_live_replica(c, key.file, key.generation, live))
                .unwrap_or(true)
            {
                continue;
            }
            let note = self
                .q(|c| query::getattr(c, key.file))
                .ok()
                .flatten()
                .filter(|a| a.generation < key.generation)
                .map(|a| {
                    format!(
                        "newer than the cluster's version of {} (generation {})",
                        path_of(a.id),
                        a.generation.0
                    )
                });
            let issue = Issue::Orphan {
                file: key.file.0,
                generation: key.generation.0,
                size,
                note,
            };
            let action = match (opts.repair, opts.orphans) {
                (false, _) | (_, Orphans::Report) => Action::Reported,
                (true, Orphans::Delete) => match store.delete(key) {
                    Ok(_) => Action::Deleted,
                    Err(e) => Action::Failed {
                        error: e.to_string(),
                    },
                },
                (true, Orphans::Quarantine) => match self
                    .quarantine(key, old_path(&previous, key.file), opts)
                    .await
                {
                    Ok(to) => Action::Quarantined { to },
                    Err(e) => Action::Failed {
                        error: e.to_string(),
                    },
                },
            };
            report.findings.push(Finding { issue, action });
        }

        // Copies the metadata lists here.
        for &key in &expected {
            let Some(&found) = disk.get(&key) else {
                let other = others(key.file, key.generation);
                let issue = Issue::Missing {
                    file: key.file.0,
                    generation: key.generation.0,
                    path: path_of(key.file),
                    other_copies: other,
                };
                let action = if !opts.repair {
                    Action::Reported
                } else {
                    match self.retire(key, true).await {
                        Ok(()) if other == 0 => Action::Lost { to: None },
                        Ok(()) => Action::Retired,
                        Err(e) => Action::Failed {
                            error: e.to_string(),
                        },
                    }
                };
                report.findings.push(Finding { issue, action });
                continue;
            };
            let Some(a) = self.q(|c| query::getattr(c, key.file))? else {
                continue;
            };
            if a.kind != FileKind::Regular
                || a.generation != key.generation
                || a.gen_state != GenState::Stable
            {
                continue;
            }
            let path = path_of(key.file);
            let issue = if found != a.size {
                Some(Issue::Damaged {
                    file: key.file.0,
                    generation: key.generation.0,
                    path: path.clone(),
                    expected: a.size,
                    found,
                    other_copies: others(key.file, key.generation),
                })
            } else if opts.deep
                && let Some(want) = hf_sha256_name(&path)
                && !self.sha256_matches(key, want).await
            {
                Some(Issue::Corrupt {
                    file: key.file.0,
                    generation: key.generation.0,
                    path: path.clone(),
                    other_copies: others(key.file, key.generation),
                })
            } else {
                None
            };
            let Some(issue) = issue else { continue };
            let other = match &issue {
                Issue::Damaged { other_copies, .. } | Issue::Corrupt { other_copies, .. } => {
                    *other_copies
                }
                _ => 0,
            };
            let action = if !opts.repair {
                Action::Reported
            } else if other > 0 {
                match self.retire(key, false).await {
                    Ok(()) => {
                        let _ = store.delete(key);
                        Action::Retired
                    }
                    Err(e) => Action::Failed {
                        error: e.to_string(),
                    },
                }
            } else {
                // The only copy: keep any bytes, stop claiming a good copy.
                match self.retire(key, true).await {
                    Ok(()) if found == 0 => {
                        let _ = store.delete(key);
                        Action::Lost { to: None }
                    }
                    Ok(()) => match self.quarantine(key, Some(path.clone()), opts).await {
                        Ok(to) => Action::Lost { to: Some(to) },
                        Err(e) => {
                            tracing::warn!(?key, error = %e, "fsck: could not keep damaged bytes");
                            Action::Lost { to: None }
                        }
                    },
                    Err(e) => Action::Failed {
                        error: e.to_string(),
                    },
                }
            };
            report.findings.push(Finding { issue, action });
        }
        for f in &report.findings {
            match &f.action {
                Action::Lost { .. } => tracing::error!(finding = ?f, "fsck: data lost"),
                Action::Reported => tracing::warn!(finding = ?f, "fsck"),
                _ => tracing::info!(finding = ?f, "fsck"),
            }
        }
        Ok(report)
    }

    async fn retire(&self, key: ObjectKey, allow_last: bool) -> NestResult<()> {
        self.propose(Command::RetireReplica {
            file: key.file,
            generation: key.generation,
            store: self.me().live_store(),
            allow_last,
        })
        .await
        .map(|_| ())
    }

    /// Finalize a file left owned by the previous run, with the size on
    /// disk (recreating an empty working object if it vanished).
    async fn settle_owned(
        &self,
        a: &nest_types::FileAttr,
        key: ObjectKey,
        present: bool,
    ) -> NestResult<u64> {
        let store = self.data().store().clone();
        let (size, mtime) = if present {
            store.stat(key).map_err(io)?
        } else {
            store.create(key).map_err(io)?;
            (0, a.mtime)
        };
        self.propose(Command::Finalize {
            file: a.id,
            epoch: a.epoch,
            size,
            mtime,
            now: Timestamp::now(),
        })
        .await?;
        Ok(size)
    }

    /// Move an object into `/.lost+found/<stamp>/<host>/<path>` (or
    /// `.../unknown/<file>.<generation>` when no metadata knows its path) as
    /// a new file; returns where it went.
    async fn quarantine(
        &self,
        key: ObjectKey,
        path: Option<String>,
        opts: &FsckOptions,
    ) -> NestResult<String> {
        let tag = format!("{:016x}.{:x}", key.file.0, key.generation.0);
        let mut parts: Vec<Vec<u8>> = vec![
            b".lost+found".to_vec(),
            opts.stamp.clone().into_bytes(),
            opts.host.clone().into_bytes(),
        ];
        let name: Vec<u8> = match &path {
            Some(p) if !p.starts_with("/.lost+found/") => {
                let comps: Vec<&str> = p.split('/').filter(|c| !c.is_empty()).collect();
                match comps.split_last() {
                    Some((last, dirs)) => {
                        parts.extend(dirs.iter().map(|d| d.as_bytes().to_vec()));
                        last.as_bytes().to_vec()
                    }
                    None => tag.clone().into_bytes(),
                }
            }
            _ => {
                parts.push(b"unknown".to_vec());
                tag.clone().into_bytes()
            }
        };
        let refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
        let dir = self.ensure_dir_path(&refs).await?;
        let name = if self.q(|c| query::lookup(c, dir, &name))?.is_some() {
            [name, b".".to_vec(), tag.into_bytes()].concat()
        } else {
            name
        };
        let store = self.data().store().clone();
        let (size, mtime) = store.stat(key).map_err(io)?;
        let first = match self.propose(Command::ReserveFileIds { count: 1 }).await? {
            Reply::FileIds(f) => f,
            other => return Err(NestError::Io(format!("unexpected {other:?}"))),
        };
        let to = ObjectKey::new(first, Generation(1));
        store.move_object(key, to).map_err(io)?;
        let r = self
            .propose(Command::Import {
                parent: dir,
                name: name.clone(),
                file: first,
                perm: 0o600,
                size,
                mtime,
                node: self.me(),
                sealed: false,
                now: Timestamp::now(),
            })
            .await;
        if let Err(e) = r {
            let _ = store.move_object(to, key);
            return Err(e);
        }
        let shown: Vec<String> = parts
            .iter()
            .chain(std::iter::once(&name))
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect();
        Ok(format!("/{}", shown.join("/")))
    }

    async fn ensure_dir_path(&self, parts: &[&[u8]]) -> NestResult<FileId> {
        let mut dir = FileId::ROOT;
        for p in parts {
            dir = match self.q(|c| query::lookup(c, dir, p))? {
                Some(id) => id,
                None => match self.mkdir(dir, p, 0o700).await {
                    Ok(a) => a.id,
                    Err(NestError::Exists) => self
                        .q(|c| query::lookup(c, dir, p))?
                        .ok_or(NestError::NotFound)?,
                    Err(e) => return Err(e),
                },
            };
        }
        Ok(dir)
    }

    async fn sha256_matches(&self, key: ObjectKey, want: &str) -> bool {
        let store = self.data().store().clone();
        let want = want.to_ascii_lowercase();
        tokio::task::spawn_blocking(move || -> std::io::Result<bool> {
            use sha2::{Digest, Sha256};
            use std::io::Read;
            let mut f = store.open_read(key)?;
            let mut h = Sha256::new();
            let mut buf = vec![0u8; 1 << 20];
            loop {
                let n = f.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                h.update(&buf[..n]);
            }
            let got: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
            Ok(got == want)
        })
        .await
        .ok()
        .and_then(|r| r.ok())
        .unwrap_or(true)
    }
}
