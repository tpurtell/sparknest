//! Deterministic application of commands to the metadata database.

use crate::command::{Command, CreateReply, LockKind, RenameFlags, Reply, SealPolicy, StoreClass};
use crate::effect::Effect;
use crate::query::{self, ATTR_COLS, attr_from_row};
use nest_types::{
    Epoch, FileAttr, FileId, FileKind, GenState, Generation, NestError, NodeId, SessionId, StoreId,
    Timestamp, validate_name,
};
use rusqlite::{Connection, OptionalExtension, params};

/// `files.flags` bit: seal when the current write epoch finalizes.
const FLAG_SEAL_PENDING: i64 = 1;

enum Fail {
    Nest(NestError),
    Sql(rusqlite::Error),
}

impl From<NestError> for Fail {
    fn from(e: NestError) -> Self {
        Fail::Nest(e)
    }
}
impl From<rusqlite::Error> for Fail {
    fn from(e: rusqlite::Error) -> Self {
        Fail::Sql(e)
    }
}

type R<T> = Result<T, Fail>;

fn err<T>(e: NestError) -> R<T> {
    Err(Fail::Nest(e))
}

/// Apply one command. Must be called inside a transaction owned by the
/// caller (the state machine commits it together with the applied log
/// position).
///
/// The outer `Result` is a database failure: the state machine cannot make
/// progress and the node must stop. The inner `Result` is the command's
/// outcome; a failed command leaves no trace in the database and produces no
/// effects.
pub fn apply(
    c: &Connection,
    cmd: &Command,
) -> rusqlite::Result<(Result<Reply, NestError>, Vec<Effect>)> {
    let mut effects = Vec::new();
    let result = apply_one(c, cmd, &mut effects)?;
    Ok((result, effects))
}

fn apply_one(
    c: &Connection,
    cmd: &Command,
    fx: &mut Vec<Effect>,
) -> rusqlite::Result<Result<Reply, NestError>> {
    c.execute_batch("SAVEPOINT cmd")?;
    let mark = fx.len();
    match exec(c, cmd, fx) {
        Ok(reply) => {
            c.execute_batch("RELEASE cmd")?;
            Ok(Ok(reply))
        }
        Err(Fail::Nest(e)) => {
            c.execute_batch("ROLLBACK TO cmd; RELEASE cmd")?;
            fx.truncate(mark);
            Ok(Err(e))
        }
        Err(Fail::Sql(e)) => Err(e),
    }
}

fn exec(c: &Connection, cmd: &Command, fx: &mut Vec<Effect>) -> R<Reply> {
    match cmd {
        Command::Mkdir {
            parent,
            name,
            perm,
            now,
        } => mkdir(c, *parent, name, *perm, *now, fx),
        Command::Create {
            parent,
            name,
            perm,
            node,
            exclusive,
            now,
        } => create(c, *parent, name, *perm, *node, *exclusive, *now, fx),
        Command::Symlink {
            parent,
            name,
            target,
            now,
        } => symlink(c, *parent, name, target, *now, fx),
        Command::Link {
            file,
            parent,
            name,
            now,
        } => link(c, *file, *parent, name, *now, fx),
        Command::Unlink { parent, name, now } => unlink(c, *parent, name, *now, fx),
        Command::Rmdir { parent, name, now } => rmdir(c, *parent, name, *now, fx),
        Command::Rename {
            parent,
            name,
            new_parent,
            new_name,
            flags,
            now,
        } => rename(c, *parent, name, *new_parent, new_name, *flags, *now, fx),
        Command::SetAttr {
            file,
            perm,
            atime,
            mtime,
            now,
        } => setattr(c, *file, *perm, *atime, *mtime, *now, fx),
        Command::AcquireOwner {
            file,
            node,
            expect_gen,
            truncate,
            now,
        } => acquire_owner(c, *file, *node, *expect_gen, *truncate, *now, fx),
        Command::SyncOwned {
            file,
            epoch,
            size,
            mtime,
        } => sync_owned(c, *file, *epoch, *size, *mtime, fx),
        Command::Finalize {
            file,
            epoch,
            size,
            mtime,
            now,
        } => finalize(c, *file, *epoch, *size, *mtime, *now, fx),
        Command::Seal { file, sealed, now } => seal(c, *file, *sealed, *now, fx),
        Command::PublishReplica {
            file,
            generation,
            store,
        } => publish_replica(c, *file, *generation, *store),
        Command::RetireReplica {
            file,
            generation,
            store,
            allow_last,
        } => retire_replica(c, *file, *generation, *store, *allow_last, fx),
        Command::OpenSession { node, now } => open_session(c, *node, *now, fx),
        Command::RenewSession { session, now } => {
            let n = c
                .prepare_cached("UPDATE sessions SET renewed = ?2 WHERE id = ?1")?
                .execute(params![session.0 as i64, now.0])?;
            if n == 0 {
                return err(NestError::NotFound);
            }
            Ok(Reply::Done)
        }
        Command::ExpireSession { session } => {
            if !expire_session(c, *session, fx)? {
                return err(NestError::NotFound);
            }
            Ok(Reply::Done)
        }
        Command::ReleaseOrphans { session, files } => {
            for f in files {
                release_orphan(c, *f, *session, fx)?;
            }
            Ok(Reply::Done)
        }
        Command::SetSealPolicy {
            dir: d,
            policy,
            now,
        } => {
            dir(c, *d)?;
            c.prepare_cached(
                "UPDATE files SET flags = (flags & ~768) | (?2 << 8), ctime = ?3 WHERE id = ?1",
            )?
            .execute(params![d.0 as i64, policy.as_bits(), now.0])?;
            fx.push(Effect::AttrChanged { file: *d });
            Ok(Reply::Done)
        }
        Command::ReserveFileIds { count } => {
            if *count == 0 || *count > 1 << 20 {
                return err(NestError::Invalid("bad id count".into()));
            }
            let first: i64 = c
                .prepare_cached(
                    "UPDATE kv SET v = v + ?1 WHERE k = 'next_file_id' RETURNING v - ?1",
                )?
                .query_row(params![*count as i64], |r| r.get(0))?;
            Ok(Reply::FileIds(FileId(first as u64)))
        }
        Command::Import {
            parent,
            name,
            file,
            perm,
            size,
            mtime,
            node,
            sealed,
            now,
        } => {
            validate_name(name)?;
            dir(c, *parent)?;
            let next: i64 = c
                .prepare_cached("SELECT v FROM kv WHERE k = 'next_file_id'")?
                .query_row([], |r| r.get(0))?;
            if file.0 == 0 || file.0 as i64 >= next || query::getattr(c, *file)?.is_some() {
                return err(NestError::Invalid(
                    "import needs an unused reserved file id".into(),
                ));
            }
            if query::lookup(c, *parent, name)?.is_some() {
                return err(NestError::Exists);
            }
            c.prepare_cached(
                "INSERT INTO files (id, kind, perm, size, nlink, atime, mtime, ctime, crtime, gen, gen_state, owner, epoch, sealed, flags, parent, target) \
                 VALUES (?1, 1, ?2, ?3, 1, ?4, ?4, ?5, ?5, 1, 0, NULL, 0, ?6, 0, NULL, NULL)",
            )?
            .execute(params![file.0 as i64, (*perm & 0o7777) as i64, *size as i64, mtime.0, now.0, *sealed])?;
            c.prepare_cached(
                "INSERT INTO replicas (file, gen, store, state) VALUES (?1, 1, ?2, 1)",
            )?
            .execute(params![file.0 as i64, node.live_store().0 as i64])?;
            insert_dentry(c, *parent, name, *file)?;
            touch_dir(c, *parent, *now, fx)?;
            fx.push(Effect::EntryChanged {
                parent: *parent,
                name: name.clone(),
            });
            Ok(Reply::Done)
        }
        Command::RecordBackup {
            name,
            store,
            selector,
            files,
            bytes,
            now,
        } => {
            let id: i64 = c
                .prepare_cached(
                    "INSERT INTO backups (name, store, created, selector, files, bytes) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING id",
                )?
                .query_row(
                    params![name, store.0 as i64, now.0, selector, *files as i64, *bytes as i64],
                    |r| r.get(0),
                )?;
            Ok(Reply::Backup(id as u64))
        }
        Command::SetGroup { name, members } => {
            if name.is_empty() || name.contains(',') || name.starts_with('@') {
                return err(NestError::Invalid(
                    "group names are plain words (used as @name)".into(),
                ));
            }
            let json = format!(
                "[{}]",
                members
                    .iter()
                    .map(|m| format!("{m:?}"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            c.prepare_cached("INSERT OR REPLACE INTO groups (name, members) VALUES (?1, ?2)")?
                .execute(params![name, json])?;
            Ok(Reply::Done)
        }
        Command::DeleteGroup { name } => {
            let n = c
                .prepare_cached("DELETE FROM groups WHERE name = ?1")?
                .execute(params![name])?;
            if n == 0 {
                return err(NestError::NotFound);
            }
            Ok(Reply::Done)
        }
        Command::DeleteBackup { id } => {
            let n = c
                .prepare_cached("DELETE FROM backups WHERE id = ?1")?
                .execute(params![*id as i64])?;
            if n == 0 {
                return err(NestError::NotFound);
            }
            Ok(Reply::Done)
        }
        Command::SetRule {
            name,
            spec,
            expect_revision,
        } => {
            let cur: Option<i64> = c
                .prepare_cached("SELECT revision FROM rules WHERE name = ?1")?
                .query_row(params![name], |r| r.get(0))
                .optional()?;
            if let Some(want) = expect_revision
                && cur.unwrap_or(0) as u64 != *want
            {
                return err(NestError::Stale);
            }
            let rev = cur.unwrap_or(0) + 1;
            c.prepare_cached(
                "INSERT OR REPLACE INTO rules (name, spec, revision) VALUES (?1, ?2, ?3)",
            )?
            .execute(params![name, spec, rev])?;
            Ok(Reply::Revision(rev as u64))
        }
        Command::DeleteRule { name } => {
            let n = c
                .prepare_cached("DELETE FROM rules WHERE name = ?1")?
                .execute(params![name])?;
            if n == 0 {
                return err(NestError::NotFound);
            }
            Ok(Reply::Done)
        }
        Command::SetLock {
            file,
            session,
            owner,
            start,
            end,
            kind,
            pid,
        } => set_lock(c, *file, *session, *owner, *start, *end, *kind, *pid, fx),
        Command::ReleaseLocks {
            file,
            session,
            owner,
        } => {
            let n = c
                .prepare_cached(
                    "DELETE FROM locks WHERE file = ?1 AND session = ?2 AND owner = ?3",
                )?
                .execute(params![file.0 as i64, session.0 as i64, *owner as i64])?;
            if n > 0 {
                fx.push(Effect::LocksReleased { file: *file });
            }
            Ok(Reply::Done)
        }
        Command::RegisterStore {
            name,
            class,
            node,
            config,
        } => register_store(c, name, *class, *node, config),
        Command::Batch(cmds) => {
            let mut out = Vec::with_capacity(cmds.len());
            for sub in cmds {
                out.push(apply_one(c, sub, fx)?);
            }
            Ok(Reply::Batch(out))
        }
    }
}

// ---------------------------------------------------------------- helpers

fn kv_next(c: &Connection, key: &str) -> R<i64> {
    let v: i64 = c
        .prepare_cached("UPDATE kv SET v = v + 1 WHERE k = ?1 RETURNING v - 1")?
        .query_row(params![key], |r| r.get(0))?;
    Ok(v)
}

fn attr(c: &Connection, id: FileId) -> R<FileAttr> {
    match query::getattr(c, id)? {
        Some(a) => Ok(a),
        None => err(NestError::NotFound),
    }
}

fn dir(c: &Connection, id: FileId) -> R<FileAttr> {
    let a = attr(c, id)?;
    if a.kind != FileKind::Directory {
        return err(NestError::NotDir);
    }
    Ok(a)
}

fn flags(c: &Connection, id: FileId) -> R<i64> {
    Ok(c.prepare_cached("SELECT flags FROM files WHERE id = ?1")?
        .query_row(params![id.0 as i64], |r| r.get(0))?)
}

fn insert_dentry(c: &Connection, parent: FileId, name: &[u8], child: FileId) -> R<()> {
    let cookie = kv_next(c, "next_cookie")?;
    c.prepare_cached("INSERT INTO dentries (parent, name, child, cookie) VALUES (?1, ?2, ?3, ?4)")?
        .execute(params![parent.0 as i64, name, child.0 as i64, cookie])?;
    Ok(())
}

fn remove_dentry(c: &Connection, parent: FileId, name: &[u8]) -> R<()> {
    c.prepare_cached("DELETE FROM dentries WHERE parent = ?1 AND name = ?2")?
        .execute(params![parent.0 as i64, name])?;
    Ok(())
}

/// A directory's contents changed: bump mtime/ctime.
fn touch_dir(c: &Connection, d: FileId, now: Timestamp, fx: &mut Vec<Effect>) -> R<()> {
    c.prepare_cached("UPDATE files SET mtime = ?2, ctime = ?2 WHERE id = ?1")?
        .execute(params![d.0 as i64, now.0])?;
    fx.push(Effect::AttrChanged { file: d });
    Ok(())
}

fn touch_ctime(c: &Connection, f: FileId, now: Timestamp, fx: &mut Vec<Effect>) -> R<()> {
    c.prepare_cached("UPDATE files SET ctime = ?2 WHERE id = ?1")?
        .execute(params![f.0 as i64, now.0])?;
    fx.push(Effect::AttrChanged { file: f });
    Ok(())
}

fn add_nlink(c: &Connection, f: FileId, delta: i64) -> R<i64> {
    Ok(
        c.prepare_cached("UPDATE files SET nlink = nlink + ?2 WHERE id = ?1 RETURNING nlink")?
            .query_row(params![f.0 as i64, delta], |r| r.get(0))?,
    )
}

#[allow(clippy::too_many_arguments)]
fn insert_file(
    c: &Connection,
    kind: FileKind,
    perm: u32,
    nlink: i64,
    now: Timestamp,
    size: u64,
    generation: u64,
    owner: Option<NodeId>,
    epoch: u64,
    parent: Option<FileId>,
    target: Option<&[u8]>,
) -> R<FileId> {
    let id = kv_next(c, "next_file_id")?;
    let gen_state = if owner.is_some() {
        GenState::Owned
    } else {
        GenState::Stable
    };
    c.prepare_cached(
        "INSERT INTO files (id, kind, perm, size, nlink, atime, mtime, ctime, crtime, gen, gen_state, owner, epoch, sealed, flags, parent, target) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, ?6, ?6, ?7, ?8, ?9, ?10, 0, 0, ?11, ?12)",
    )?
    .execute(params![
        id,
        kind.as_i64(),
        (perm & 0o7777) as i64,
        size as i64,
        nlink,
        now.0,
        generation as i64,
        gen_state.as_i64(),
        owner.map(|n| n.0 as i64),
        epoch as i64,
        parent.map(|p| p.0 as i64),
        target,
    ])?;
    Ok(FileId(id as u64))
}

/// A name for `f` was removed. Drop its link count and, at zero, orphan or
/// delete the object.
fn drop_link(c: &Connection, f: FileId, now: Timestamp, fx: &mut Vec<Effect>) -> R<()> {
    let n = add_nlink(c, f, -1)?;
    touch_ctime(c, f, now, fx)?;
    if n <= 0 {
        orphan_or_delete(c, f, fx)?;
    }
    Ok(())
}

fn orphan_or_delete(c: &Connection, f: FileId, fx: &mut Vec<Effect>) -> R<()> {
    let a = attr(c, f)?;
    if a.kind == FileKind::Regular {
        let sessions: Vec<SessionId> = query::sessions(c)?.into_iter().map(|s| s.id).collect();
        if !sessions.is_empty() {
            let mut st = c.prepare_cached("INSERT INTO orphans (file, session) VALUES (?1, ?2)")?;
            for s in &sessions {
                st.execute(params![f.0 as i64, s.0 as i64])?;
            }
            fx.push(Effect::Orphaned { file: f, sessions });
            return Ok(());
        }
    }
    delete_file(c, f, fx)
}

fn delete_file(c: &Connection, f: FileId, fx: &mut Vec<Effect>) -> R<()> {
    let a = attr(c, f)?;
    for r in query::replicas(c, f)? {
        fx.push(Effect::ReplicaInvalidated {
            file: f,
            generation: r.generation,
            store: r.store,
        });
    }
    if let (GenState::Owned, Some(owner)) = (a.gen_state, a.owner) {
        fx.push(Effect::WorkingObjectDeleted {
            file: f,
            generation: a.generation,
            owner,
        });
    }
    c.prepare_cached("DELETE FROM replicas WHERE file = ?1")?
        .execute(params![f.0 as i64])?;
    c.prepare_cached("DELETE FROM orphans WHERE file = ?1")?
        .execute(params![f.0 as i64])?;
    c.prepare_cached("DELETE FROM locks WHERE file = ?1")?
        .execute(params![f.0 as i64])?;
    c.prepare_cached("DELETE FROM files WHERE id = ?1")?
        .execute(params![f.0 as i64])?;
    fx.push(Effect::FileDeleted { file: f });
    Ok(())
}

fn release_orphan(c: &Connection, f: FileId, s: SessionId, fx: &mut Vec<Effect>) -> R<()> {
    let n = c
        .prepare_cached("DELETE FROM orphans WHERE file = ?1 AND session = ?2")?
        .execute(params![f.0 as i64, s.0 as i64])?;
    if n == 0 {
        return Ok(());
    }
    let remaining: i64 = c
        .prepare_cached("SELECT COUNT(*) FROM orphans WHERE file = ?1")?
        .query_row(params![f.0 as i64], |r| r.get(0))?;
    if remaining == 0 {
        let nlink: Option<i64> = c
            .prepare_cached("SELECT nlink FROM files WHERE id = ?1")?
            .query_row(params![f.0 as i64], |r| r.get(0))
            .optional()?;
        if nlink == Some(0) {
            delete_file(c, f, fx)?;
        }
    }
    Ok(())
}

/// Returns false if the session did not exist.
fn expire_session(c: &Connection, s: SessionId, fx: &mut Vec<Effect>) -> R<bool> {
    let node: Option<i64> = c
        .prepare_cached("DELETE FROM sessions WHERE id = ?1 RETURNING node")?
        .query_row(params![s.0 as i64], |r| r.get(0))
        .optional()?;
    let Some(node) = node else { return Ok(false) };
    for f in query::orphans_of(c, s)? {
        release_orphan(c, f, s, fx)?;
    }
    let mut st = c.prepare_cached("DELETE FROM locks WHERE session = ?1 RETURNING file")?;
    let files: Vec<i64> = st
        .query_map(params![s.0 as i64], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    drop(st);
    let mut files = files;
    files.sort();
    files.dedup();
    for f in files {
        fx.push(Effect::LocksReleased {
            file: FileId(f as u64),
        });
    }
    fx.push(Effect::SessionExpired {
        session: s,
        node: NodeId(node as u64),
    });
    Ok(true)
}

/// True if `candidate` is `ancestor` or lies beneath it.
fn is_within(c: &Connection, candidate: FileId, ancestor: FileId) -> R<bool> {
    let mut cur = candidate;
    loop {
        if cur == ancestor {
            return Ok(true);
        }
        if cur == FileId::ROOT {
            return Ok(false);
        }
        match query::dir_parent(c, cur)? {
            Some(p) if p != cur => cur = p,
            _ => return Ok(false),
        }
    }
}

// ---------------------------------------------------------------- namespace

fn mkdir(
    c: &Connection,
    parent: FileId,
    name: &[u8],
    perm: u32,
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    validate_name(name)?;
    dir(c, parent)?;
    if query::lookup(c, parent, name)?.is_some() {
        return err(NestError::Exists);
    }
    let id = insert_file(
        c,
        FileKind::Directory,
        perm,
        2,
        now,
        0,
        0,
        None,
        0,
        Some(parent),
        None,
    )?;
    insert_dentry(c, parent, name, id)?;
    add_nlink(c, parent, 1)?;
    touch_dir(c, parent, now, fx)?;
    fx.push(Effect::EntryChanged {
        parent,
        name: name.to_vec(),
    });
    Ok(Reply::Attr(attr(c, id)?))
}

#[allow(clippy::too_many_arguments)]
fn create(
    c: &Connection,
    parent: FileId,
    name: &[u8],
    perm: u32,
    node: NodeId,
    exclusive: bool,
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    validate_name(name)?;
    dir(c, parent)?;
    if let Some(existing) = query::lookup(c, parent, name)? {
        if exclusive {
            return err(NestError::Exists);
        }
        let a = attr(c, existing)?;
        return match a.kind {
            FileKind::Regular => Ok(Reply::Created(CreateReply {
                attr: a,
                created: false,
            })),
            FileKind::Directory => err(NestError::IsDir),
            FileKind::Symlink => err(NestError::Exists),
        };
    }
    let id = insert_file(
        c,
        FileKind::Regular,
        perm,
        1,
        now,
        0,
        1,
        Some(node),
        1,
        None,
        None,
    )?;
    insert_dentry(c, parent, name, id)?;
    touch_dir(c, parent, now, fx)?;
    fx.push(Effect::EntryChanged {
        parent,
        name: name.to_vec(),
    });
    fx.push(Effect::OwnershipGranted {
        file: id,
        owner: node,
        generation: Generation(1),
        epoch: Epoch(1),
        from_gen: None,
    });
    Ok(Reply::Created(CreateReply {
        attr: attr(c, id)?,
        created: true,
    }))
}

fn symlink(
    c: &Connection,
    parent: FileId,
    name: &[u8],
    target: &[u8],
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    validate_name(name)?;
    if target.is_empty() || target.contains(&0) || target.len() > 4096 {
        return err(NestError::Invalid("bad symlink target".into()));
    }
    dir(c, parent)?;
    if query::lookup(c, parent, name)?.is_some() {
        return err(NestError::Exists);
    }
    let id = insert_file(
        c,
        FileKind::Symlink,
        0o777,
        1,
        now,
        target.len() as u64,
        0,
        None,
        0,
        None,
        Some(target),
    )?;
    insert_dentry(c, parent, name, id)?;
    touch_dir(c, parent, now, fx)?;
    fx.push(Effect::EntryChanged {
        parent,
        name: name.to_vec(),
    });
    Ok(Reply::Attr(attr(c, id)?))
}

fn link(
    c: &Connection,
    file: FileId,
    parent: FileId,
    name: &[u8],
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    validate_name(name)?;
    let a = attr(c, file)?;
    if a.kind == FileKind::Directory {
        return err(NestError::NotPermitted("hard link to directory".into()));
    }
    if a.nlink == 0 {
        return err(NestError::NotFound);
    }
    dir(c, parent)?;
    if query::lookup(c, parent, name)?.is_some() {
        return err(NestError::Exists);
    }
    insert_dentry(c, parent, name, file)?;
    add_nlink(c, file, 1)?;
    touch_ctime(c, file, now, fx)?;
    touch_dir(c, parent, now, fx)?;
    fx.push(Effect::EntryChanged {
        parent,
        name: name.to_vec(),
    });
    Ok(Reply::Attr(attr(c, file)?))
}

fn unlink(
    c: &Connection,
    parent: FileId,
    name: &[u8],
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    dir(c, parent)?;
    let Some(child) = query::lookup(c, parent, name)? else {
        return err(NestError::NotFound);
    };
    if attr(c, child)?.kind == FileKind::Directory {
        return err(NestError::IsDir);
    }
    remove_dentry(c, parent, name)?;
    touch_dir(c, parent, now, fx)?;
    fx.push(Effect::EntryChanged {
        parent,
        name: name.to_vec(),
    });
    drop_link(c, child, now, fx)?;
    Ok(Reply::Done)
}

fn rmdir(
    c: &Connection,
    parent: FileId,
    name: &[u8],
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    dir(c, parent)?;
    let Some(child) = query::lookup(c, parent, name)? else {
        return err(NestError::NotFound);
    };
    remove_directory(c, parent, name, child, now, fx)?;
    Ok(Reply::Done)
}

fn remove_directory(
    c: &Connection,
    parent: FileId,
    name: &[u8],
    child: FileId,
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<()> {
    if attr(c, child)?.kind != FileKind::Directory {
        return err(NestError::NotDir);
    }
    if !query::dir_is_empty(c, child)? {
        return err(NestError::NotEmpty);
    }
    remove_dentry(c, parent, name)?;
    add_nlink(c, parent, -1)?;
    touch_dir(c, parent, now, fx)?;
    fx.push(Effect::EntryChanged {
        parent,
        name: name.to_vec(),
    });
    delete_file(c, child, fx)
}

/// Move a directory from `from` to `to` (different parents): fix `..` and
/// link counts.
fn reparent_dir(c: &Connection, d: FileId, from: FileId, to: FileId) -> R<()> {
    c.prepare_cached("UPDATE files SET parent = ?2 WHERE id = ?1")?
        .execute(params![d.0 as i64, to.0 as i64])?;
    add_nlink(c, from, -1)?;
    add_nlink(c, to, 1)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn rename(
    c: &Connection,
    parent: FileId,
    name: &[u8],
    new_parent: FileId,
    new_name: &[u8],
    flags: RenameFlags,
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    if flags.exchange && flags.noreplace {
        return err(NestError::Invalid(
            "RENAME_EXCHANGE with RENAME_NOREPLACE".into(),
        ));
    }
    validate_name(new_name)?;
    dir(c, parent)?;
    dir(c, new_parent)?;
    let Some(src) = query::lookup(c, parent, name)? else {
        return err(NestError::NotFound);
    };
    let src_attr = attr(c, src)?;
    let dst = query::lookup(c, new_parent, new_name)?;

    if flags.exchange {
        let Some(dst) = dst else {
            return err(NestError::NotFound);
        };
        if dst == src {
            return Ok(Reply::Done);
        }
        let dst_attr = attr(c, dst)?;
        if src_attr.kind == FileKind::Directory && is_within(c, new_parent, src)? {
            return err(NestError::Invalid("rename into own subtree".into()));
        }
        if dst_attr.kind == FileKind::Directory && is_within(c, parent, dst)? {
            return err(NestError::Invalid("rename into own subtree".into()));
        }
        c.prepare_cached("UPDATE dentries SET child = ?3 WHERE parent = ?1 AND name = ?2")?
            .execute(params![parent.0 as i64, name, dst.0 as i64])?;
        c.prepare_cached("UPDATE dentries SET child = ?3 WHERE parent = ?1 AND name = ?2")?
            .execute(params![new_parent.0 as i64, new_name, src.0 as i64])?;
        if parent != new_parent {
            if src_attr.kind == FileKind::Directory {
                reparent_dir(c, src, parent, new_parent)?;
            }
            if dst_attr.kind == FileKind::Directory {
                reparent_dir(c, dst, new_parent, parent)?;
            }
        }
        touch_ctime(c, src, now, fx)?;
        touch_ctime(c, dst, now, fx)?;
    } else {
        if dst == Some(src) {
            // Two names for the same object: POSIX says do nothing.
            return Ok(Reply::Done);
        }
        if src_attr.kind == FileKind::Directory && is_within(c, new_parent, src)? {
            return err(NestError::Invalid("rename into own subtree".into()));
        }
        if let Some(dst) = dst {
            if flags.noreplace {
                return err(NestError::Exists);
            }
            let dst_attr = attr(c, dst)?;
            match (
                src_attr.kind == FileKind::Directory,
                dst_attr.kind == FileKind::Directory,
            ) {
                (true, true) => remove_directory(c, new_parent, new_name, dst, now, fx)?,
                (true, false) => return err(NestError::NotDir),
                (false, true) => return err(NestError::IsDir),
                (false, false) => {
                    remove_dentry(c, new_parent, new_name)?;
                    drop_link(c, dst, now, fx)?;
                }
            }
        }
        remove_dentry(c, parent, name)?;
        insert_dentry(c, new_parent, new_name, src)?;
        // huggingface_hub completes a download by renaming `x.incomplete`
        // to `x`: seal it there when the destination tree asks for that.
        if src_attr.kind == FileKind::Regular
            && name.ends_with(b".incomplete")
            && !new_name.ends_with(b".incomplete")
            && query::effective_seal_policy(c, new_parent)? == SealPolicy::RenameFromIncomplete
        {
            seal(c, src, true, now, fx)?;
        }
        if src_attr.kind == FileKind::Directory && parent != new_parent {
            reparent_dir(c, src, parent, new_parent)?;
        }
        touch_ctime(c, src, now, fx)?;
    }
    touch_dir(c, parent, now, fx)?;
    if new_parent != parent {
        touch_dir(c, new_parent, now, fx)?;
    }
    fx.push(Effect::EntryChanged {
        parent,
        name: name.to_vec(),
    });
    fx.push(Effect::EntryChanged {
        parent: new_parent,
        name: new_name.to_vec(),
    });
    Ok(Reply::Done)
}

fn setattr(
    c: &Connection,
    file: FileId,
    perm: Option<u32>,
    atime: Option<Timestamp>,
    mtime: Option<Timestamp>,
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    attr(c, file)?;
    c.prepare_cached(
        "UPDATE files SET perm = COALESCE(?2, perm), atime = COALESCE(?3, atime), mtime = COALESCE(?4, mtime), ctime = ?5 WHERE id = ?1",
    )?
    .execute(params![
        file.0 as i64,
        perm.map(|p| (p & 0o7777) as i64),
        atime.map(|t| t.0),
        mtime.map(|t| t.0),
        now.0
    ])?;
    fx.push(Effect::AttrChanged { file });
    Ok(Reply::Attr(attr(c, file)?))
}

// ---------------------------------------------------------------- lifecycle

fn acquire_owner(
    c: &Connection,
    file: FileId,
    node: NodeId,
    expect_gen: Generation,
    truncate: bool,
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    let a = attr(c, file)?;
    match a.kind {
        FileKind::Regular => {}
        FileKind::Directory => return err(NestError::IsDir),
        FileKind::Symlink => return err(NestError::Invalid("not a regular file".into())),
    }
    if a.sealed {
        return err(NestError::NotPermitted("file is sealed".into()));
    }
    if a.gen_state == GenState::Owned {
        return Ok(Reply::AlreadyOwned {
            owner: a.owner.expect("owned file has an owner"),
            generation: a.generation,
            epoch: a.epoch,
        });
    }
    if a.generation != expect_gen {
        return err(NestError::Stale);
    }
    let own_store = node.live_store();
    let has_local = query::has_live_replica(c, file, a.generation, own_store)?;
    if !truncate && a.size > 0 && !has_local {
        return err(NestError::Invalid(
            "owner must hold a live replica of the current generation".into(),
        ));
    }
    let converts = has_local && !truncate;
    for r in query::replicas(c, file)? {
        if converts && r.store == own_store && r.generation == a.generation {
            continue; // becomes the owner's working object; no deletion
        }
        fx.push(Effect::ReplicaInvalidated {
            file,
            generation: r.generation,
            store: r.store,
        });
    }
    c.prepare_cached("DELETE FROM replicas WHERE file = ?1")?
        .execute(params![file.0 as i64])?;

    let generation = Generation(a.generation.0 + 1);
    let epoch = Epoch(a.epoch.0 + 1);
    c.prepare_cached(
        "UPDATE files SET gen = ?2, gen_state = ?3, owner = ?4, epoch = ?5, \
         size = CASE WHEN ?6 THEN 0 ELSE size END, \
         mtime = CASE WHEN ?6 THEN ?7 ELSE mtime END, \
         ctime = CASE WHEN ?6 THEN ?7 ELSE ctime END \
         WHERE id = ?1",
    )?
    .execute(params![
        file.0 as i64,
        generation.0 as i64,
        GenState::Owned.as_i64(),
        node.0 as i64,
        epoch.0 as i64,
        truncate,
        now.0
    ])?;
    let from_gen = converts.then_some(a.generation);
    fx.push(Effect::OwnershipGranted {
        file,
        owner: node,
        generation,
        epoch,
        from_gen,
    });
    fx.push(Effect::AttrChanged { file });
    Ok(Reply::Acquired {
        attr: attr(c, file)?,
        from_gen,
    })
}

fn owned_with_epoch(c: &Connection, file: FileId, epoch: Epoch) -> R<FileAttr> {
    let a = attr(c, file)?;
    if a.gen_state != GenState::Owned || a.epoch != epoch {
        return err(NestError::Stale);
    }
    Ok(a)
}

fn sync_owned(
    c: &Connection,
    file: FileId,
    epoch: Epoch,
    size: u64,
    mtime: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    owned_with_epoch(c, file, epoch)?;
    c.prepare_cached(
        "UPDATE files SET size = ?2, mtime = ?3, ctime = MAX(ctime, ?3) WHERE id = ?1",
    )?
    .execute(params![file.0 as i64, size as i64, mtime.0])?;
    fx.push(Effect::AttrChanged { file });
    Ok(Reply::Done)
}

fn finalize(
    c: &Connection,
    file: FileId,
    epoch: Epoch,
    size: u64,
    mtime: Timestamp,
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    let a = owned_with_epoch(c, file, epoch)?;
    let owner = a.owner.expect("owned file has an owner");
    let mut seal = flags(c, file)? & FLAG_SEAL_PENDING != 0;
    if !seal && !a.sealed {
        let parents: Vec<i64> = c
            .prepare_cached("SELECT parent FROM dentries WHERE child = ?1")?
            .query_map(params![file.0 as i64], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for p in parents {
            if query::effective_seal_policy(c, FileId(p as u64))? == SealPolicy::OnFinalize {
                seal = true;
                break;
            }
        }
    }
    c.prepare_cached(
        "UPDATE files SET size = ?2, mtime = ?3, ctime = MAX(ctime, ?4), gen_state = ?5, owner = NULL, \
         sealed = CASE WHEN ?6 THEN 1 ELSE sealed END, flags = flags & ~?7 WHERE id = ?1",
    )?
    .execute(params![
        file.0 as i64,
        size as i64,
        mtime.0,
        now.0,
        GenState::Stable.as_i64(),
        seal,
        FLAG_SEAL_PENDING
    ])?;
    c.prepare_cached("INSERT INTO replicas (file, gen, store, state) VALUES (?1, ?2, ?3, 1)")?
        .execute(params![
            file.0 as i64,
            a.generation.0 as i64,
            owner.live_store().0 as i64
        ])?;
    fx.push(Effect::Finalized {
        file,
        generation: a.generation,
        owner,
    });
    fx.push(Effect::AttrChanged { file });
    Ok(Reply::Attr(attr(c, file)?))
}

fn seal(
    c: &Connection,
    file: FileId,
    sealed: bool,
    now: Timestamp,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    let a = attr(c, file)?;
    if a.kind != FileKind::Regular {
        return err(NestError::Invalid(
            "only regular files can be sealed".into(),
        ));
    }
    if sealed && a.gen_state == GenState::Owned {
        c.prepare_cached("UPDATE files SET flags = flags | ?2, ctime = ?3 WHERE id = ?1")?
            .execute(params![file.0 as i64, FLAG_SEAL_PENDING, now.0])?;
    } else {
        c.prepare_cached(
            "UPDATE files SET sealed = ?2, flags = flags & ~?3, ctime = ?4 WHERE id = ?1",
        )?
        .execute(params![file.0 as i64, sealed, FLAG_SEAL_PENDING, now.0])?;
    }
    fx.push(Effect::AttrChanged { file });
    Ok(Reply::Attr(attr(c, file)?))
}

// ---------------------------------------------------------------- replicas

fn publish_replica(
    c: &Connection,
    file: FileId,
    generation: Generation,
    store: StoreId,
) -> R<Reply> {
    let a = attr(c, file)?;
    if a.kind != FileKind::Regular || a.gen_state != GenState::Stable || a.generation != generation
    {
        return err(NestError::Stale);
    }
    c.prepare_cached(
        "INSERT OR IGNORE INTO replicas (file, gen, store, state) VALUES (?1, ?2, ?3, 1)",
    )?
    .execute(params![file.0 as i64, generation.0 as i64, store.0 as i64])?;
    Ok(Reply::Done)
}

fn retire_replica(
    c: &Connection,
    file: FileId,
    generation: Generation,
    store: StoreId,
    allow_last: bool,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    if !query::has_live_replica(c, file, generation, store)? {
        return err(NestError::NotFound);
    }
    let a = attr(c, file)?;
    if !allow_last && a.gen_state == GenState::Stable && a.generation == generation {
        let live: i64 = c
            .prepare_cached(
                "SELECT COUNT(*) FROM replicas WHERE file = ?1 AND gen = ?2 AND state = 1",
            )?
            .query_row(params![file.0 as i64, generation.0 as i64], |r| r.get(0))?;
        if live <= 1 {
            return err(NestError::Busy("last live copy".into()));
        }
    }
    c.prepare_cached("DELETE FROM replicas WHERE file = ?1 AND gen = ?2 AND store = ?3")?
        .execute(params![file.0 as i64, generation.0 as i64, store.0 as i64])?;
    fx.push(Effect::ReplicaInvalidated {
        file,
        generation,
        store,
    });
    Ok(Reply::Done)
}

// ---------------------------------------------------------------- locks

/// Lock ranges are stored inclusive; `u64::MAX` (EOF) is stored as
/// `i64::MAX` so SQLite comparisons stay in signed range.
fn lk(v: u64) -> i64 {
    v.min(i64::MAX as u64) as i64
}

#[allow(clippy::too_many_arguments)]
fn set_lock(
    c: &Connection,
    file: FileId,
    session: SessionId,
    owner: u64,
    start: u64,
    end: u64,
    kind: LockKind,
    pid: u32,
    fx: &mut Vec<Effect>,
) -> R<Reply> {
    if end < start {
        return err(NestError::Invalid("lock range end before start".into()));
    }
    attr(c, file)?;
    let (start, end) = (lk(start), lk(end));
    if kind != LockKind::Unlock {
        let write = kind == LockKind::Write;
        if query::lock_conflict(c, file, session, owner, start as u64, end as u64, write)?.is_some()
        {
            return err(NestError::WouldBlock);
        }
    }
    // Carve [start, end] out of this owner's existing ranges on the file,
    // keeping the parts outside it.
    let mut st = c.prepare_cached(
        "DELETE FROM locks WHERE file = ?1 AND session = ?2 AND owner = ?3 AND start <= ?5 AND end_ >= ?4 \
         RETURNING start, end_, kind, pid",
    )?;
    let removed: Vec<(i64, i64, i64, i64)> = st
        .query_map(
            params![file.0 as i64, session.0 as i64, owner as i64, start, end],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?
        .collect::<rusqlite::Result<_>>()?;
    drop(st);
    let mut ins = c.prepare_cached(
        "INSERT OR REPLACE INTO locks (file, session, owner, start, end_, kind, pid) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;
    for (s0, e0, k0, p0) in &removed {
        if *s0 < start {
            ins.execute(params![
                file.0 as i64,
                session.0 as i64,
                owner as i64,
                s0,
                start - 1,
                k0,
                p0
            ])?;
        }
        if *e0 > end {
            ins.execute(params![
                file.0 as i64,
                session.0 as i64,
                owner as i64,
                end + 1,
                e0,
                k0,
                p0
            ])?;
        }
    }
    match kind {
        LockKind::Unlock => {}
        LockKind::Read | LockKind::Write => {
            let k = i64::from(kind == LockKind::Write);
            ins.execute(params![
                file.0 as i64,
                session.0 as i64,
                owner as i64,
                start,
                end,
                k,
                pid as i64
            ])?;
        }
    }
    // Releasing or downgrading may unblock someone.
    if !removed.is_empty() && kind != LockKind::Write {
        fx.push(Effect::LocksReleased { file });
    }
    Ok(Reply::Done)
}

// ---------------------------------------------------------------- sessions, stores

fn open_session(c: &Connection, node: NodeId, now: Timestamp, fx: &mut Vec<Effect>) -> R<Reply> {
    let old: Vec<SessionId> = query::sessions(c)?
        .into_iter()
        .filter(|s| s.node == node)
        .map(|s| s.id)
        .collect();
    for s in old {
        expire_session(c, s, fx)?;
    }
    let id = SessionId(kv_next(c, "next_session")? as u64);
    c.prepare_cached("INSERT INTO sessions (id, node, renewed) VALUES (?1, ?2, ?3)")?
        .execute(params![id.0 as i64, node.0 as i64, now.0])?;
    fx.push(Effect::SessionOpened { session: id, node });
    Ok(Reply::Session(id))
}

fn register_store(
    c: &Connection,
    name: &str,
    class: StoreClass,
    node: Option<NodeId>,
    config: &str,
) -> R<Reply> {
    let exists: Option<i64> = c
        .prepare_cached("SELECT id FROM stores WHERE name = ?1")?
        .query_row(params![name], |r| r.get(0))
        .optional()?;
    if exists.is_some() {
        return err(NestError::Exists);
    }
    let id = match (class, node) {
        (StoreClass::Live, Some(n)) => n.live_store(),
        (StoreClass::Live, None) => {
            return err(NestError::Invalid("live store needs a node".into()));
        }
        (StoreClass::Archive, _) => StoreId(kv_next(c, "next_store")? as u64),
    };
    c.prepare_cached(
        "INSERT INTO stores (id, name, class, node, config) VALUES (?1, ?2, ?3, ?4, ?5)",
    )?
    .execute(params![
        id.0 as i64,
        name,
        class.as_i64(),
        node.map(|n| n.0 as i64),
        config
    ])?;
    Ok(Reply::Store(id))
}

#[allow(dead_code)]
fn all_attrs(c: &Connection) -> rusqlite::Result<Vec<FileAttr>> {
    let mut st = c.prepare(&format!("SELECT {ATTR_COLS} FROM files ORDER BY id"))?;
    let rows = st.query_map([], attr_from_row)?;
    rows.collect()
}
