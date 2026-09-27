//! Files written into the bare mountpoint while sparknest was not mounted
//! (after a daemon crash, or before it started). They would be hidden under
//! the mount, so they are moved into the state directory first and, once
//! mounted, imported into `/.lost+found/<run>/<host>/unmounted/` (ADR-027).
//! Past a size cap nothing is moved: that looks like a real directory chosen
//! as mountpoint by mistake, and needs a person.

use anyhow::Context;
use std::path::{Path, PathBuf};

/// Move no more than this automatically.
pub const MAX_BYTES: u64 = 16 << 30;
pub const MAX_ENTRIES: u64 = 200_000;

fn tally(p: &Path, entries: &mut u64, bytes: &mut u64) -> std::io::Result<()> {
    for e in std::fs::read_dir(p)? {
        let e = e?;
        let md = std::fs::symlink_metadata(e.path())?;
        *entries += 1;
        if md.is_dir() {
            tally(&e.path(), entries, bytes)?;
        } else {
            *bytes += md.len();
        }
        if *entries > MAX_ENTRIES || *bytes > MAX_BYTES {
            return Ok(());
        }
    }
    Ok(())
}

fn copy_tree(from: &Path, to: &Path) -> std::io::Result<()> {
    let md = std::fs::symlink_metadata(from)?;
    if md.file_type().is_symlink() {
        std::os::unix::fs::symlink(std::fs::read_link(from)?, to)
    } else if md.is_dir() {
        std::fs::create_dir_all(to)?;
        for e in std::fs::read_dir(from)? {
            let e = e?;
            copy_tree(&e.path(), &to.join(e.file_name()))?;
        }
        Ok(())
    } else {
        std::fs::copy(from, to).map(|_| ())
    }
}

/// Empty the mountpoint into `<state_dir>/unmounted-<stamp>`; `None` if it
/// was already empty.
pub fn rescue(mountpoint: &Path, state_dir: &Path, stamp: &str) -> anyhow::Result<Option<PathBuf>> {
    let names: Vec<_> = match std::fs::read_dir(mountpoint) {
        Ok(rd) => rd.flatten().collect(),
        Err(_) => return Ok(None),
    };
    if names.is_empty() {
        return Ok(None);
    }
    let (mut entries, mut bytes) = (0u64, 0u64);
    tally(mountpoint, &mut entries, &mut bytes)?;
    anyhow::ensure!(
        entries <= MAX_ENTRIES && bytes <= MAX_BYTES,
        "mountpoint {} holds more than {MAX_ENTRIES} entries or {} GiB: too much to move \
         aside automatically (was it a real directory?). Move it away, or set \
         fuse.allow_nonempty",
        mountpoint.display(),
        MAX_BYTES >> 30
    );
    let dest = state_dir.join(format!("unmounted-{stamp}"));
    std::fs::create_dir_all(&dest).with_context(|| format!("creating {}", dest.display()))?;
    for e in names {
        let to = dest.join(e.file_name());
        match std::fs::rename(e.path(), &to) {
            Ok(()) => {}
            Err(err) if err.raw_os_error() == Some(libc::EXDEV) => {
                copy_tree(&e.path(), &to)?;
                let p = e.path();
                if std::fs::symlink_metadata(&p)?.is_dir() {
                    std::fs::remove_dir_all(&p)?;
                } else {
                    std::fs::remove_file(&p)?;
                }
            }
            Err(err) => return Err(err).with_context(|| format!("moving {}", e.path().display())),
        }
    }
    tracing::warn!(
        entries,
        bytes,
        to = %dest.display(),
        "moved files written into the bare mountpoint aside; they go to /.lost+found once mounted"
    );
    Ok(Some(dest))
}
