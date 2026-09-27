//! Did the previous run end cleanly, or did the host crash under it?
//! (ADR-026). A marker in the state directory records the kernel boot id
//! while the daemon runs and is set clean at orderly shutdown.
//!
//! - clean shutdown: nothing to worry about;
//! - daemon crashed but the host kept running: everything written reached
//!   the page cache, which survived; not dirty;
//! - the host went down while the daemon ran (boot id changed): anything
//!   not yet synced may be gone. This host is **dirty**.

use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Prior {
    /// No previous run in this state directory.
    Fresh,
    Clean,
    /// The daemon stopped abnormally but the host did not reboot.
    DaemonCrashed,
    /// The host rebooted while the daemon ran: unsynced writes may be lost.
    HostCrashed,
}

impl Prior {
    pub fn dirty(self) -> bool {
        self == Prior::HostCrashed
    }
}

#[derive(Serialize, Deserialize)]
struct Marker {
    boot_id: String,
    clean: bool,
    /// The last re-found this host has reconciled its objects against.
    #[serde(default)]
    refound: Option<String>,
}

pub struct RunState {
    path: PathBuf,
    boot_id: String,
    refound: Option<String>,
}

/// The kernel's id for this boot.
pub fn boot_id() -> String {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn write_synced(path: &Path, m: &Marker) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(serde_json::to_string(m)?.as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    if let Some(d) = path.parent() {
        std::fs::File::open(d)?.sync_all()?;
    }
    Ok(())
}

impl RunState {
    /// Read how the previous run ended and mark this one as running.
    pub fn begin(state_dir: &Path, boot_id: &str) -> std::io::Result<(RunState, Prior)> {
        let path = state_dir.join("run.json");
        let old: Option<Marker> = std::fs::read(&path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok());
        let prior = match &old {
            Some(m) if m.clean => Prior::Clean,
            Some(m) if m.boot_id == boot_id => Prior::DaemonCrashed,
            Some(_) => Prior::HostCrashed,
            // Builds before this marker synced every commit: not dirty.
            None if state_dir.join("raft.sqlite").exists() => Prior::DaemonCrashed,
            None => Prior::Fresh,
        };
        let refound = old.and_then(|m| m.refound);
        let rs = RunState {
            path,
            boot_id: boot_id.to_string(),
            refound,
        };
        rs.write(false)?;
        Ok((rs, prior))
    }

    fn write(&self, clean: bool) -> std::io::Result<()> {
        write_synced(
            &self.path,
            &Marker {
                boot_id: self.boot_id.clone(),
                clean,
                refound: self.refound.clone(),
            },
        )
    }

    /// The re-found this host last reconciled against.
    pub fn refound(&self) -> Option<&str> {
        self.refound.as_deref()
    }

    pub fn set_refound(&mut self, id: Option<String>) -> std::io::Result<()> {
        self.refound = id;
        self.write(false)
    }

    /// Orderly shutdown: the next start is clean.
    pub fn end_clean(&self) -> std::io::Result<()> {
        self.write(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_previous_runs() {
        let d = tempfile::tempdir().unwrap();
        let (rs, p) = RunState::begin(d.path(), "boot-a").unwrap();
        assert_eq!(p, Prior::Fresh);
        drop(rs);
        // Killed, same boot.
        let (rs, p) = RunState::begin(d.path(), "boot-a").unwrap();
        assert_eq!(p, Prior::DaemonCrashed);
        rs.end_clean().unwrap();
        let (rs, p) = RunState::begin(d.path(), "boot-b").unwrap();
        assert_eq!(p, Prior::Clean, "a clean stop survives a reboot");
        drop(rs);
        // Running when the host went down.
        let (_, p) = RunState::begin(d.path(), "boot-c").unwrap();
        assert!(p.dirty());
    }
}
