//! Read-only queries over replicated metadata. Used by apply (inside its
//! transaction) and by FUSE/API readers on their own connections.

use nest_types::{
    DirEntry, Epoch, FileAttr, FileId, FileKind, GenState, Generation, NodeId, ReplicaState,
    SessionId, StoreId, Timestamp,
};
use rusqlite::{Connection, OptionalExtension, Row, params};

pub(crate) const ATTR_COLS: &str = "id, kind, perm, size, nlink, atime, mtime, ctime, crtime, gen, gen_state, owner, epoch, sealed";

pub(crate) fn attr_from_row(r: &Row<'_>) -> rusqlite::Result<FileAttr> {
    let kind: i64 = r.get(1)?;
    let gs: i64 = r.get(10)?;
    Ok(FileAttr {
        id: FileId(r.get::<_, i64>(0)? as u64),
        kind: FileKind::from_i64(kind).ok_or(rusqlite::Error::IntegralValueOutOfRange(1, kind))?,
        perm: r.get::<_, i64>(2)? as u32,
        size: r.get::<_, i64>(3)? as u64,
        nlink: r.get::<_, i64>(4)? as u32,
        atime: Timestamp(r.get(5)?),
        mtime: Timestamp(r.get(6)?),
        ctime: Timestamp(r.get(7)?),
        crtime: Timestamp(r.get(8)?),
        generation: Generation(r.get::<_, i64>(9)? as u64),
        gen_state: GenState::from_i64(gs)
            .ok_or(rusqlite::Error::IntegralValueOutOfRange(10, gs))?,
        owner: r.get::<_, Option<i64>>(11)?.map(|v| NodeId(v as u64)),
        epoch: Epoch(r.get::<_, i64>(12)? as u64),
        sealed: r.get::<_, i64>(13)? != 0,
    })
}

pub fn getattr(c: &Connection, id: FileId) -> rusqlite::Result<Option<FileAttr>> {
    c.prepare_cached(&format!("SELECT {ATTR_COLS} FROM files WHERE id = ?1"))?
        .query_row(params![id.0 as i64], attr_from_row)
        .optional()
}

pub fn lookup(c: &Connection, parent: FileId, name: &[u8]) -> rusqlite::Result<Option<FileId>> {
    c.prepare_cached("SELECT child FROM dentries WHERE parent = ?1 AND name = ?2")?
        .query_row(params![parent.0 as i64, name], |r| r.get::<_, i64>(0))
        .optional()
        .map(|o| o.map(|v| FileId(v as u64)))
}

pub fn lookup_attr(
    c: &Connection,
    parent: FileId,
    name: &[u8],
) -> rusqlite::Result<Option<FileAttr>> {
    c.prepare_cached(&format!(
        "SELECT {} FROM dentries d JOIN files f ON f.id = d.child WHERE d.parent = ?1 AND d.name = ?2",
        ATTR_COLS.split(", ").map(|c| format!("f.{c}")).collect::<Vec<_>>().join(", ")
    ))?
    .query_row(params![parent.0 as i64, name], attr_from_row)
    .optional()
}

/// Entries of `dir` with cookie greater than `after`, in cookie order.
/// Cookies are stable across concurrent inserts and removals.
pub fn readdir(
    c: &Connection,
    dir: FileId,
    after: u64,
    limit: usize,
) -> rusqlite::Result<Vec<(u64, DirEntry)>> {
    let mut st = c.prepare_cached(
        "SELECT d.cookie, d.name, d.child, f.kind FROM dentries d JOIN files f ON f.id = d.child \
         WHERE d.parent = ?1 AND d.cookie > ?2 ORDER BY d.cookie LIMIT ?3",
    )?;
    let rows = st.query_map(params![dir.0 as i64, after as i64, limit as i64], |r| {
        let kind: i64 = r.get(3)?;
        Ok((
            r.get::<_, i64>(0)? as u64,
            DirEntry {
                name: r.get(1)?,
                id: FileId(r.get::<_, i64>(2)? as u64),
                kind: FileKind::from_i64(kind)
                    .ok_or(rusqlite::Error::IntegralValueOutOfRange(3, kind))?,
            },
        ))
    })?;
    rows.collect()
}

pub fn dir_is_empty(c: &Connection, dir: FileId) -> rusqlite::Result<bool> {
    Ok(
        c.prepare_cached("SELECT 1 FROM dentries WHERE parent = ?1 LIMIT 1")?
            .query_row(params![dir.0 as i64], |_| Ok(()))
            .optional()?
            .is_none(),
    )
}

/// Parent of a directory (root is its own parent).
pub fn dir_parent(c: &Connection, dir: FileId) -> rusqlite::Result<Option<FileId>> {
    c.prepare_cached("SELECT parent FROM files WHERE id = ?1 AND kind = 2")?
        .query_row(params![dir.0 as i64], |r| r.get::<_, Option<i64>>(0))
        .optional()
        .map(|o| o.flatten().map(|v| FileId(v as u64)))
}

pub fn readlink(c: &Connection, id: FileId) -> rusqlite::Result<Option<Vec<u8>>> {
    c.prepare_cached("SELECT target FROM files WHERE id = ?1 AND kind = 3")?
        .query_row(params![id.0 as i64], |r| r.get::<_, Option<Vec<u8>>>(0))
        .optional()
        .map(|o| o.flatten())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaRow {
    pub generation: Generation,
    pub store: StoreId,
    pub state: ReplicaState,
}

pub fn replicas(c: &Connection, file: FileId) -> rusqlite::Result<Vec<ReplicaRow>> {
    let mut st = c.prepare_cached(
        "SELECT gen, store, state FROM replicas WHERE file = ?1 ORDER BY gen, store",
    )?;
    let rows = st.query_map(params![file.0 as i64], |r| {
        let s: i64 = r.get(2)?;
        Ok(ReplicaRow {
            generation: Generation(r.get::<_, i64>(0)? as u64),
            store: StoreId(r.get::<_, i64>(1)? as u64),
            state: ReplicaState::from_i64(s)
                .ok_or(rusqlite::Error::IntegralValueOutOfRange(2, s))?,
        })
    })?;
    rows.collect()
}

pub fn has_live_replica(
    c: &Connection,
    file: FileId,
    generation: Generation,
    store: StoreId,
) -> rusqlite::Result<bool> {
    Ok(c.prepare_cached(
        "SELECT 1 FROM replicas WHERE file = ?1 AND gen = ?2 AND store = ?3 AND state = 1",
    )?
    .query_row(
        params![file.0 as i64, generation.0 as i64, store.0 as i64],
        |_| Ok(()),
    )
    .optional()?
    .is_some())
}

/// All (file, generation) copies held in a store: the inventory a node
/// reconciles its object directory against at startup.
pub fn store_inventory(
    c: &Connection,
    store: StoreId,
) -> rusqlite::Result<Vec<(FileId, Generation)>> {
    let mut st =
        c.prepare_cached("SELECT file, gen FROM replicas WHERE store = ?1 ORDER BY file")?;
    let rows = st.query_map(params![store.0 as i64], |r| {
        Ok((
            FileId(r.get::<_, i64>(0)? as u64),
            Generation(r.get::<_, i64>(1)? as u64),
        ))
    })?;
    rows.collect()
}

/// Files currently OWNED by `node`: the working objects it must hold.
pub fn owned_by(c: &Connection, node: NodeId) -> rusqlite::Result<Vec<FileAttr>> {
    let mut st = c.prepare_cached(&format!(
        "SELECT {ATTR_COLS} FROM files WHERE owner = ?1 ORDER BY id"
    ))?;
    let rows = st.query_map(params![node.0 as i64], attr_from_row)?;
    rows.collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRow {
    pub id: SessionId,
    pub node: NodeId,
    pub renewed: Timestamp,
}

pub fn sessions(c: &Connection) -> rusqlite::Result<Vec<SessionRow>> {
    let mut st = c.prepare_cached("SELECT id, node, renewed FROM sessions ORDER BY id")?;
    let rows = st.query_map([], |r| {
        Ok(SessionRow {
            id: SessionId(r.get::<_, i64>(0)? as u64),
            node: NodeId(r.get::<_, i64>(1)? as u64),
            renewed: Timestamp(r.get(2)?),
        })
    })?;
    rows.collect()
}

/// Orphaned files a session still has to release.
pub fn orphans_of(c: &Connection, session: SessionId) -> rusqlite::Result<Vec<FileId>> {
    let mut st = c.prepare_cached("SELECT file FROM orphans WHERE session = ?1 ORDER BY file")?;
    let rows = st.query_map(params![session.0 as i64], |r| {
        Ok(FileId(r.get::<_, i64>(0)? as u64))
    })?;
    rows.collect()
}

/// Resolve a slash-separated path from the root without following symlinks.
pub fn resolve(c: &Connection, path: &[u8]) -> rusqlite::Result<Option<FileId>> {
    let mut cur = FileId::ROOT;
    for comp in path
        .split(|b| *b == b'/')
        .filter(|p| !p.is_empty() && *p != b".")
    {
        if comp == b".." {
            cur = dir_parent(c, cur)?.unwrap_or(FileId::ROOT);
            continue;
        }
        match lookup(c, cur, comp)? {
            Some(id) => cur = id,
            None => return Ok(None),
        }
    }
    Ok(Some(cur))
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Totals {
    pub files: u64,
    pub dirs: u64,
    pub logical_bytes: u64,
}

pub fn totals(c: &Connection) -> rusqlite::Result<Totals> {
    c.query_row(
        "SELECT COALESCE(SUM(kind = 1), 0), COALESCE(SUM(kind = 2), 0), COALESCE(SUM(CASE WHEN kind = 1 THEN size ELSE 0 END), 0) FROM files",
        [],
        |r| {
            Ok(Totals {
                files: r.get::<_, i64>(0)? as u64,
                dirs: r.get::<_, i64>(1)? as u64,
                logical_bytes: r.get::<_, i64>(2)? as u64,
            })
        },
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockRow {
    pub session: SessionId,
    pub owner: u64,
    pub start: u64,
    pub end: u64,
    pub write: bool,
    pub pid: u32,
}

/// The first lock held by someone other than `(session, owner)` that
/// conflicts with a request for `[start, end]` (write conflicts with
/// anything; read conflicts with write).
pub fn lock_conflict(
    c: &Connection,
    file: FileId,
    session: SessionId,
    owner: u64,
    start: u64,
    end: u64,
    write: bool,
) -> rusqlite::Result<Option<LockRow>> {
    c.prepare_cached(
        "SELECT session, owner, start, end_, kind, pid FROM locks WHERE file = ?1 \
         AND NOT (session = ?2 AND owner = ?3) AND start <= ?5 AND end_ >= ?4 AND (kind = 1 OR ?6) \
         ORDER BY start LIMIT 1",
    )?
    .query_row(
        params![
            file.0 as i64,
            session.0 as i64,
            owner as i64,
            start as i64,
            end as i64,
            write
        ],
        |r| {
            Ok(LockRow {
                session: SessionId(r.get::<_, i64>(0)? as u64),
                owner: r.get::<_, i64>(1)? as u64,
                start: r.get::<_, i64>(2)? as u64,
                end: r.get::<_, i64>(3)? as u64,
                write: r.get::<_, i64>(4)? == 1,
                pid: r.get::<_, i64>(5)? as u32,
            })
        },
    )
    .optional()
}

pub fn locks_of(c: &Connection, file: FileId) -> rusqlite::Result<Vec<LockRow>> {
    let mut st = c.prepare_cached(
        "SELECT session, owner, start, end_, kind, pid FROM locks WHERE file = ?1 ORDER BY session, owner, start",
    )?;
    let rows = st.query_map(params![file.0 as i64], |r| {
        Ok(LockRow {
            session: SessionId(r.get::<_, i64>(0)? as u64),
            owner: r.get::<_, i64>(1)? as u64,
            start: r.get::<_, i64>(2)? as u64,
            end: r.get::<_, i64>(3)? as u64,
            write: r.get::<_, i64>(4)? == 1,
            pid: r.get::<_, i64>(5)? as u32,
        })
    })?;
    rows.collect()
}

/// A directory's own sealing policy (bits 8-9 of `files.flags`).
pub fn seal_policy(c: &Connection, dir: FileId) -> rusqlite::Result<crate::SealPolicy> {
    let flags: Option<i64> = c
        .prepare_cached("SELECT flags FROM files WHERE id = ?1 AND kind = 2")?
        .query_row(params![dir.0 as i64], |r| r.get(0))
        .optional()?;
    Ok(crate::SealPolicy::from_bits(flags.unwrap_or(0) >> 8))
}

/// The policy in force for files placed in `dir`: the nearest explicit
/// setting on the way to the root; `Off` if none.
pub fn effective_seal_policy(c: &Connection, dir: FileId) -> rusqlite::Result<crate::SealPolicy> {
    let mut cur = dir;
    for _ in 0..4096 {
        let p = seal_policy(c, cur)?;
        if p != crate::SealPolicy::Inherit {
            return Ok(p);
        }
        match dir_parent(c, cur)? {
            Some(parent) if parent != cur => cur = parent,
            _ => break,
        }
    }
    Ok(crate::SealPolicy::Off)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuleRow {
    pub name: String,
    pub spec: String,
    pub revision: u64,
}

pub fn rules(c: &Connection) -> rusqlite::Result<Vec<RuleRow>> {
    let mut st = c.prepare_cached("SELECT name, spec, revision FROM rules ORDER BY name")?;
    let rows = st.query_map([], |r| {
        Ok(RuleRow {
            name: r.get(0)?,
            spec: r.get(1)?,
            revision: r.get::<_, i64>(2)? as u64,
        })
    })?;
    rows.collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreRow {
    pub id: StoreId,
    pub name: String,
    pub class: i64,
    pub node: Option<NodeId>,
    pub config: String,
}

pub fn stores(c: &Connection) -> rusqlite::Result<Vec<StoreRow>> {
    let mut st =
        c.prepare_cached("SELECT id, name, class, node, config FROM stores ORDER BY id")?;
    let rows = st.query_map([], |r| {
        Ok(StoreRow {
            id: StoreId(r.get::<_, i64>(0)? as u64),
            name: r.get(1)?,
            class: r.get(2)?,
            node: r.get::<_, Option<i64>>(3)?.map(|n| NodeId(n as u64)),
            config: r.get(4)?,
        })
    })?;
    rows.collect()
}

/// Every name (with its parent directory) linking to `file`.
pub fn names_of(c: &Connection, file: FileId) -> rusqlite::Result<Vec<(FileId, Vec<u8>)>> {
    let mut st = c.prepare_cached(
        "SELECT parent, name FROM dentries WHERE child = ?1 ORDER BY parent, name",
    )?;
    let rows = st.query_map(params![file.0 as i64], |r| {
        Ok((FileId(r.get::<_, i64>(0)? as u64), r.get(1)?))
    })?;
    rows.collect()
}

/// One absolute path of `file` (any of its names), or None if unlinked.
pub fn path_of(c: &Connection, file: FileId) -> rusqlite::Result<Option<Vec<u8>>> {
    let mut parts: Vec<Vec<u8>> = Vec::new();
    let mut cur = file;
    for _ in 0..4096 {
        if cur == FileId::ROOT {
            parts.reverse();
            let mut out = Vec::new();
            for p in parts {
                out.push(b'/');
                out.extend_from_slice(&p);
            }
            if out.is_empty() {
                out.push(b'/');
            }
            return Ok(Some(out));
        }
        let Some((parent, name)) = names_of(c, cur)?.into_iter().next() else {
            return Ok(None);
        };
        parts.push(name);
        cur = parent;
    }
    Ok(None)
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct BackupRow {
    pub id: u64,
    pub name: String,
    pub store: StoreId,
    pub created: Timestamp,
    pub selector: String,
    pub files: u64,
    pub bytes: u64,
}

pub fn backups(c: &Connection) -> rusqlite::Result<Vec<BackupRow>> {
    let mut st = c.prepare_cached(
        "SELECT id, name, store, created, selector, files, bytes FROM backups ORDER BY id",
    )?;
    let rows = st.query_map([], |r| {
        Ok(BackupRow {
            id: r.get::<_, i64>(0)? as u64,
            name: r.get(1)?,
            store: StoreId(r.get::<_, i64>(2)? as u64),
            created: Timestamp(r.get(3)?),
            selector: r.get(4)?,
            files: r.get::<_, i64>(5)? as u64,
            bytes: r.get::<_, i64>(6)? as u64,
        })
    })?;
    rows.collect()
}

/// Host groups: name -> member host names.
pub fn groups(c: &Connection) -> rusqlite::Result<Vec<(String, Vec<String>)>> {
    let mut st = c.prepare_cached("SELECT name, members FROM groups ORDER BY name")?;
    let rows = st.query_map([], |r| {
        let members: String = r.get(1)?;
        let list = members
            .trim_matches(|ch| ch == '[' || ch == ']')
            .split(',')
            .map(|m| m.trim().trim_matches('"').to_string())
            .filter(|m| !m.is_empty())
            .collect();
        Ok((r.get(0)?, list))
    })?;
    rows.collect()
}
