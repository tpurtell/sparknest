//! Re-founding a cluster from metadata (ADR-026).
//!
//! The Raft log is disposable: after a full outage under relaxed
//! durability, or when an upgrade changes the Raft format, the cluster
//! starts a new Raft group whose state machine begins as a *seed*: one
//! host's `meta.sqlite` with the openraft bookkeeping removed. Every
//! participant installs the same seed (verified by SHA-256), so all state
//! machines start byte-identical.

use rusqlite::{Connection, OptionalExtension, params};
use std::path::{Path, PathBuf};

/// The applied position recorded in a `meta.sqlite` of any Raft format:
/// `(term, index)`. Formats 1 and 2 encode the log id identically
/// (leader term, leader node, index).
pub fn applied_of(meta: &Path) -> Option<(u64, u64)> {
    let c = Connection::open_with_flags(meta, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
    let b: Vec<u8> = c
        .query_row("SELECT v FROM sm_state WHERE k = 'last_applied'", [], |r| {
            r.get(0)
        })
        .optional()
        .ok()??;
    let v: Option<(u64, u64, u64)> = postcard::from_bytes(&b).ok()?;
    v.map(|(term, _node, index)| (term, index))
}

/// The re-found this metadata descends from, if any.
pub fn refound_of(c: &Connection) -> Option<String> {
    c.query_row("SELECT v FROM sm_state WHERE k = 'refound_id'", [], |r| {
        r.get::<_, Vec<u8>>(0)
    })
    .optional()
    .ok()
    .flatten()
    .map(|b| String::from_utf8_lossy(&b).into_owned())
}

/// Build a seed from `meta` into `out`: a consistent copy with the Raft
/// bookkeeping cleared, marked with this build's Raft format and `refound`.
/// Returns its SHA-256 (hex).
pub fn build(meta: &Path, out: &Path, refound: &str) -> anyhow::Result<String> {
    let _ = std::fs::remove_file(out);
    {
        let src = Connection::open_with_flags(meta, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        src.execute("VACUUM INTO ?1", params![out.to_string_lossy()])?;
    }
    {
        let c = Connection::open(out)?;
        c.execute_batch(
            "CREATE TABLE IF NOT EXISTS sm_state (k TEXT PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;
             DELETE FROM sm_state WHERE k IN ('last_applied', 'membership', 'snapshot_id', 'raft_format', 'refound_id');
             DROP TABLE IF EXISTS sm_dedup;",
        )?;
        c.execute(
            "INSERT INTO sm_state (k, v) VALUES ('raft_format', ?1)",
            params![postcard::to_stdvec(&crate::RAFT_FORMAT)?],
        )?;
        c.execute(
            "INSERT INTO sm_state (k, v) VALUES ('refound_id', ?1)",
            params![refound.as_bytes()],
        )?;
        // Rollback journal while we hold it: one self-contained file.
        c.pragma_update(None, "journal_mode", "DELETE")?;
    }
    sha256_file(out)
}

pub fn sha256_file(p: &Path) -> anyhow::Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut f = std::fs::File::open(p)?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(h.finalize().iter().map(|b| format!("{b:02x}")).collect())
}

/// Move this host's Raft and metadata state aside (kept for inspection) and
/// install `seed` as its metadata. Objects are untouched.
pub fn install(dir: &Path, seed: &Path, refound: &str) -> anyhow::Result<PathBuf> {
    let aside = dir.join(format!("pre-refound-{refound}"));
    std::fs::create_dir_all(&aside)?;
    for name in [
        "raft.sqlite",
        "raft.sqlite-wal",
        "raft.sqlite-shm",
        "meta.sqlite",
        "meta.sqlite-wal",
        "meta.sqlite-shm",
        "snapshots",
    ] {
        let p = dir.join(name);
        if p.exists() {
            std::fs::rename(&p, aside.join(name))?;
        }
    }
    std::fs::copy(seed, dir.join("meta.sqlite"))?;
    std::fs::File::open(dir.join("meta.sqlite"))?.sync_all()?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(aside)
}

/// Move this host's Raft and metadata state aside so it can join an
/// existing cluster empty (the leader sends a snapshot).
pub fn discard(dir: &Path, why: &str) -> anyhow::Result<PathBuf> {
    let aside = dir.join(format!("pre-{why}-{}", std::process::id()));
    std::fs::create_dir_all(&aside)?;
    for name in [
        "raft.sqlite",
        "raft.sqlite-wal",
        "raft.sqlite-shm",
        "meta.sqlite",
        "meta.sqlite-wal",
        "meta.sqlite-shm",
        "snapshots",
    ] {
        let p = dir.join(name);
        if p.exists() {
            std::fs::rename(&p, aside.join(name))?;
        }
    }
    Ok(aside)
}
