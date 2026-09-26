//! Pins the positional encoding of persisted enums (ADR-018). If this test
//! fails you reordered, removed or inserted a variant: append instead, or
//! bump FORMAT_VERSION with a migration plan.

use nest_meta::*;
use nest_types::*;

fn tag<T: serde::Serialize>(v: &T) -> u8 {
    postcard::to_stdvec(v).unwrap()[0]
}

#[test]
fn format_is_pinned() {
    assert_eq!(FORMAT_VERSION, 1);
    let f = FileId(1);
    let n = NodeId(1);
    let t = Timestamp(0);
    let s = SessionId(1);
    let g = Generation(1);
    let e = Epoch(1);
    let cmds: Vec<(Command, &str)> = vec![
        (
            Command::Mkdir {
                parent: f,
                name: vec![],
                perm: 0,
                now: t,
            },
            "Mkdir",
        ),
        (
            Command::Create {
                parent: f,
                name: vec![],
                perm: 0,
                node: n,
                exclusive: false,
                now: t,
            },
            "Create",
        ),
        (
            Command::Symlink {
                parent: f,
                name: vec![],
                target: vec![],
                now: t,
            },
            "Symlink",
        ),
        (
            Command::Link {
                file: f,
                parent: f,
                name: vec![],
                now: t,
            },
            "Link",
        ),
        (
            Command::Unlink {
                parent: f,
                name: vec![],
                now: t,
            },
            "Unlink",
        ),
        (
            Command::Rmdir {
                parent: f,
                name: vec![],
                now: t,
            },
            "Rmdir",
        ),
        (
            Command::Rename {
                parent: f,
                name: vec![],
                new_parent: f,
                new_name: vec![],
                flags: RenameFlags::default(),
                now: t,
            },
            "Rename",
        ),
        (
            Command::SetAttr {
                file: f,
                perm: None,
                atime: None,
                mtime: None,
                now: t,
            },
            "SetAttr",
        ),
        (
            Command::AcquireOwner {
                file: f,
                node: n,
                expect_gen: g,
                truncate: false,
                now: t,
            },
            "AcquireOwner",
        ),
        (
            Command::SyncOwned {
                file: f,
                epoch: e,
                size: 0,
                mtime: t,
            },
            "SyncOwned",
        ),
        (
            Command::Finalize {
                file: f,
                epoch: e,
                size: 0,
                mtime: t,
                now: t,
            },
            "Finalize",
        ),
        (
            Command::Seal {
                file: f,
                sealed: true,
                now: t,
            },
            "Seal",
        ),
        (
            Command::SetSealPolicy {
                dir: f,
                policy: SealPolicy::Off,
                now: t,
            },
            "SetSealPolicy",
        ),
        (
            Command::PublishReplica {
                file: f,
                generation: g,
                store: StoreId(1),
            },
            "PublishReplica",
        ),
        (
            Command::RetireReplica {
                file: f,
                generation: g,
                store: StoreId(1),
                allow_last: false,
            },
            "RetireReplica",
        ),
        (Command::OpenSession { node: n, now: t }, "OpenSession"),
        (Command::RenewSession { session: s, now: t }, "RenewSession"),
        (Command::ExpireSession { session: s }, "ExpireSession"),
        (
            Command::ReleaseOrphans {
                session: s,
                files: vec![],
            },
            "ReleaseOrphans",
        ),
        (
            Command::SetLock {
                file: f,
                session: s,
                owner: 0,
                start: 0,
                end: 0,
                kind: LockKind::Read,
                pid: 0,
            },
            "SetLock",
        ),
        (
            Command::ReleaseLocks {
                file: f,
                session: s,
                owner: 0,
            },
            "ReleaseLocks",
        ),
        (
            Command::RegisterStore {
                name: String::new(),
                class: StoreClass::Live,
                node: None,
                config: String::new(),
            },
            "RegisterStore",
        ),
        (Command::ReserveFileIds { count: 1 }, "ReserveFileIds"),
        (
            Command::Import {
                parent: f,
                name: vec![],
                file: f,
                perm: 0,
                size: 0,
                mtime: t,
                node: n,
                sealed: false,
                now: t,
            },
            "Import",
        ),
        (
            Command::SetRule {
                name: String::new(),
                spec: String::new(),
                expect_revision: None,
            },
            "SetRule",
        ),
        (
            Command::DeleteRule {
                name: String::new(),
            },
            "DeleteRule",
        ),
        (Command::Batch(vec![]), "Batch"),
        (
            Command::RecordBackup {
                name: String::new(),
                store: StoreId(1),
                selector: String::new(),
                files: 0,
                bytes: 0,
                now: t,
            },
            "RecordBackup",
        ),
        (Command::DeleteBackup { id: 0 }, "DeleteBackup"),
        (
            Command::SetGroup {
                name: String::new(),
                members: vec![],
            },
            "SetGroup",
        ),
        (
            Command::DeleteGroup {
                name: String::new(),
            },
            "DeleteGroup",
        ),
    ];
    for (i, (c, name)) in cmds.iter().enumerate() {
        assert_eq!(tag(c) as usize, i, "Command::{name} moved");
    }
    let replies: Vec<(Reply, &str)> = vec![
        (Reply::Done, "Done"),
        (
            Reply::Attr(FileAttr {
                id: f,
                kind: FileKind::Regular,
                perm: 0,
                size: 0,
                nlink: 0,
                atime: t,
                mtime: t,
                ctime: t,
                crtime: t,
                generation: g,
                gen_state: GenState::Stable,
                owner: None,
                epoch: e,
                sealed: false,
            }),
            "Attr",
        ),
    ];
    for (i, (r, name)) in replies.iter().enumerate() {
        assert_eq!(tag(r) as usize, i, "Reply::{name} moved");
    }
    assert_eq!(tag(&Reply::Session(s)), 5);
    assert_eq!(tag(&Reply::Store(StoreId(1))), 6);
    assert_eq!(tag(&Reply::Batch(vec![])), 9);
    assert_eq!(tag(&Reply::Revision(1)), 7);
    assert_eq!(tag(&Reply::FileIds(f)), 8);
    assert_eq!(tag(&Reply::Backup(1)), 10);
    let errs = [
        NestError::NotFound,
        NestError::Exists,
        NestError::NotDir,
        NestError::IsDir,
        NestError::NotEmpty,
        NestError::NameTooLong,
        NestError::Invalid(String::new()),
        NestError::NotPermitted(String::new()),
        NestError::Stale,
        NestError::Busy(String::new()),
        NestError::WouldBlock,
        NestError::NoQuorum,
        NestError::Unavailable(String::new()),
        NestError::NoSpace,
        NestError::CrossDevice,
        NestError::Io(String::new()),
    ];
    for (i, e) in errs.iter().enumerate() {
        assert_eq!(tag(e) as usize, i, "NestError::{e:?} moved");
    }
}
