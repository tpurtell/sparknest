mod common;
use common::Db;
use nest_meta::{Command, Effect, RenameFlags, Reply, query};
use nest_types::*;

const ROOT: FileId = FileId::ROOT;

fn rename(
    db: &mut Db,
    p: FileId,
    n: &str,
    np: FileId,
    nn: &str,
    flags: RenameFlags,
) -> Result<Reply, NestError> {
    let now = db.now();
    db.run(Command::Rename {
        parent: p,
        name: n.into(),
        new_parent: np,
        new_name: nn.into(),
        flags,
        now,
    })
    .0
}

#[test]
fn namespace_basics() {
    let mut db = Db::new();
    let hub = db.mkdir(ROOT, "hub");
    let f = db.create(hub, "config.json", 1);
    assert_eq!(f.gen_state, GenState::Owned);
    assert_eq!(f.owner, Some(NodeId(1)));
    assert_eq!(f.generation, Generation(1));
    assert_eq!(db.lookup(hub, "config.json"), Some(f.id));
    assert_eq!(db.names(hub), vec!["config.json"]);
    assert_eq!(db.attr(ROOT).nlink, 3);

    // Non-exclusive create of an existing file returns it.
    let now = db.now();
    let r = db.ok(Command::Create {
        parent: hub,
        name: "config.json".into(),
        perm: 0o600,
        node: NodeId(2),
        exclusive: false,
        now,
    });
    assert!(matches!(r, Reply::Created(c) if !c.created && c.attr.id == f.id));
    // Exclusive create fails and leaves nothing behind.
    let now = db.now();
    let (r, fx) = db.run(Command::Create {
        parent: hub,
        name: "config.json".into(),
        perm: 0o600,
        node: NodeId(2),
        exclusive: true,
        now,
    });
    assert_eq!(r, Err(NestError::Exists));
    assert!(fx.is_empty());

    assert_eq!(
        db.run(Command::Mkdir {
            parent: f.id,
            name: "x".into(),
            perm: 0o755,
            now
        })
        .0,
        Err(NestError::NotDir)
    );
    assert!(matches!(
        db.run(Command::Mkdir {
            parent: hub,
            name: "a/b".into(),
            perm: 0o755,
            now
        })
        .0,
        Err(NestError::Invalid(_))
    ));
    assert_eq!(
        db.run(Command::Rmdir {
            parent: ROOT,
            name: "hub".into(),
            now
        })
        .0,
        Err(NestError::NotEmpty)
    );
    assert_eq!(
        db.run(Command::Unlink {
            parent: ROOT,
            name: "hub".into(),
            now
        })
        .0,
        Err(NestError::IsDir)
    );
}

#[test]
fn readdir_cookies_are_stable() {
    let mut db = Db::new();
    for n in ["a", "b", "c", "d"] {
        db.create(ROOT, n, 1);
    }
    let first = query::readdir(&db.c, ROOT, 0, 2).unwrap();
    assert_eq!(first.len(), 2);
    let now = db.now();
    db.ok(Command::Unlink {
        parent: ROOT,
        name: "a".into(),
        now,
    });
    db.create(ROOT, "e", 1);
    let rest = query::readdir(&db.c, ROOT, first[1].0, 100).unwrap();
    let names: Vec<_> = rest.iter().map(|(_, e)| e.name.clone()).collect();
    assert_eq!(names, vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);
}

#[test]
fn symlinks_and_hardlinks() {
    let mut db = Db::new();
    let now = db.now();
    let s = match db.ok(Command::Symlink {
        parent: ROOT,
        name: "l".into(),
        target: b"../../blobs/abc".to_vec(),
        now,
    }) {
        Reply::Attr(a) => a,
        r => panic!("{r:?}"),
    };
    assert_eq!(s.kind, FileKind::Symlink);
    assert_eq!(
        query::readlink(&db.c, s.id).unwrap().unwrap(),
        b"../../blobs/abc"
    );
    let f = db.file(ROOT, "f", 1, 10);
    let now = db.now();
    db.ok(Command::Link {
        file: f.id,
        parent: ROOT,
        name: "g".into(),
        now,
    });
    assert_eq!(db.attr(f.id).nlink, 2);
    db.ok(Command::Unlink {
        parent: ROOT,
        name: "f".into(),
        now,
    });
    assert_eq!(db.attr(f.id).nlink, 1);
    let d = db.mkdir(ROOT, "d");
    assert!(matches!(
        db.run(Command::Link {
            file: d,
            parent: ROOT,
            name: "d2".into(),
            now
        })
        .0,
        Err(NestError::NotPermitted(_))
    ));
}

#[test]
fn rename_semantics() {
    let mut db = Db::new();
    let a = db.mkdir(ROOT, "a");
    let b = db.mkdir(ROOT, "b");
    let sub = db.mkdir(a, "sub");
    let f = db.file(a, "f.incomplete", 1, 5);
    let g = db.file(b, "g", 1, 7);

    // Replace an existing file: the replaced file loses its only name.
    rename(&mut db, a, "f.incomplete", b, "g", RenameFlags::default()).unwrap();
    assert_eq!(db.lookup(b, "g"), Some(f.id));
    assert!(query::getattr(&db.c, g.id).unwrap().is_none());

    // Directory into its own subtree.
    assert!(matches!(
        rename(&mut db, ROOT, "a", sub, "a", RenameFlags::default()),
        Err(NestError::Invalid(_))
    ));
    // Directory over non-empty directory, file over directory, dir over file.
    db.file(b, "h", 1, 1);
    assert_eq!(
        rename(&mut db, ROOT, "a", ROOT, "b", RenameFlags::default()),
        Err(NestError::NotEmpty)
    );
    assert_eq!(
        rename(&mut db, b, "h", ROOT, "a", RenameFlags::default()),
        Err(NestError::IsDir)
    );
    assert_eq!(
        rename(&mut db, ROOT, "a", b, "h", RenameFlags::default()),
        Err(NestError::NotDir)
    );
    // Move a directory across parents: link counts follow.
    rename(&mut db, a, "sub", b, "sub2", RenameFlags::default()).unwrap();
    assert_eq!(db.attr(a).nlink, 2);
    assert_eq!(db.attr(b).nlink, 3);
    assert_eq!(query::dir_parent(&db.c, sub).unwrap(), Some(b));
    // Replace an empty directory.
    let e = db.mkdir(ROOT, "empty");
    rename(&mut db, b, "sub2", ROOT, "empty", RenameFlags::default()).unwrap();
    assert!(query::getattr(&db.c, e).unwrap().is_none());
    assert_eq!(db.lookup(ROOT, "empty"), Some(sub));
    // noreplace and exchange.
    let noreplace = RenameFlags {
        noreplace: true,
        exchange: false,
    };
    assert_eq!(
        rename(&mut db, b, "g", b, "h", noreplace),
        Err(NestError::Exists)
    );
    let exchange = RenameFlags {
        noreplace: false,
        exchange: true,
    };
    let h = db.lookup(b, "h").unwrap();
    rename(&mut db, b, "g", ROOT, "empty", exchange).unwrap();
    assert_eq!(db.lookup(b, "g"), Some(sub));
    assert_eq!(db.lookup(ROOT, "empty"), Some(f.id));
    assert_eq!(query::dir_parent(&db.c, sub).unwrap(), Some(b));
    // Same object under two names: no-op.
    let now = db.now();
    db.ok(Command::Link {
        file: h,
        parent: b,
        name: "h2".into(),
        now,
    });
    rename(&mut db, b, "h", b, "h2", RenameFlags::default()).unwrap();
    assert_eq!(db.lookup(b, "h"), Some(h));
}

#[test]
fn ownership_lifecycle() {
    let mut db = Db::new();
    let f = db.file(ROOT, "model.safetensors", 1, 1_000);
    assert_eq!(f.gen_state, GenState::Stable);
    assert_eq!(f.owner, None);
    assert_eq!(db.stores(f.id), vec![1]);

    // Replicate to nodes 2 and 3.
    for s in [2, 3] {
        db.ok(Command::PublishReplica {
            file: f.id,
            generation: f.generation,
            store: StoreId(s),
        });
    }
    assert_eq!(db.stores(f.id), vec![1, 2, 3]);

    // A node without a copy cannot take ownership for an in-place edit...
    let now = db.now();
    assert!(matches!(
        db.run(Command::AcquireOwner {
            file: f.id,
            node: NodeId(4),
            expect_gen: f.generation,
            truncate: false,
            now
        })
        .0,
        Err(NestError::Invalid(_))
    ));
    // ...and a stale expectation is refused.
    assert_eq!(
        db.run(Command::AcquireOwner {
            file: f.id,
            node: NodeId(2),
            expect_gen: Generation(0),
            truncate: false,
            now
        })
        .0,
        Err(NestError::Stale)
    );

    // Node 2 holds a copy: it becomes owner, the others are invalidated.
    let (r, fx) = db.run(Command::AcquireOwner {
        file: f.id,
        node: NodeId(2),
        expect_gen: f.generation,
        truncate: false,
        now,
    });
    let Ok(Reply::Acquired { attr, from_gen }) = r else {
        panic!("{r:?}")
    };
    assert_eq!(from_gen, Some(Generation(1)));
    assert_eq!(attr.generation, Generation(2));
    assert_eq!(attr.epoch, Epoch(2));
    assert_eq!(attr.owner, Some(NodeId(2)));
    let invalidated: Vec<u64> = fx
        .iter()
        .filter_map(|e| match e {
            Effect::ReplicaInvalidated {
                store, generation, ..
            } => {
                assert_eq!(*generation, Generation(1));
                Some(store.0)
            }
            _ => None,
        })
        .collect();
    assert_eq!(invalidated, vec![1, 3]);
    assert!(fx.contains(&Effect::OwnershipGranted {
        file: f.id,
        owner: NodeId(2),
        generation: Generation(2),
        epoch: Epoch(2),
        from_gen: Some(Generation(1))
    }));
    assert!(db.stores(f.id).is_empty());

    // A second writer is routed to the owner.
    let r = db.ok(Command::AcquireOwner {
        file: f.id,
        node: NodeId(3),
        expect_gen: Generation(1),
        truncate: false,
        now,
    });
    assert_eq!(
        r,
        Reply::AlreadyOwned {
            owner: NodeId(2),
            generation: Generation(2),
            epoch: Epoch(2)
        }
    );

    // Owner syncs and finalizes; stale epochs are fenced.
    assert_eq!(
        db.run(Command::SyncOwned {
            file: f.id,
            epoch: Epoch(1),
            size: 5,
            mtime: now
        })
        .0,
        Err(NestError::Stale)
    );
    db.ok(Command::SyncOwned {
        file: f.id,
        epoch: Epoch(2),
        size: 1_500,
        mtime: now,
    });
    assert_eq!(db.attr(f.id).size, 1_500);
    let now = db.now();
    db.ok(Command::Finalize {
        file: f.id,
        epoch: Epoch(2),
        size: 2_000,
        mtime: now,
        now,
    });
    let a = db.attr(f.id);
    assert_eq!(
        (a.gen_state, a.size, a.generation),
        (GenState::Stable, 2_000, Generation(2))
    );
    assert_eq!(db.stores(f.id), vec![2]);
    assert_eq!(
        db.run(Command::Finalize {
            file: f.id,
            epoch: Epoch(2),
            size: 1,
            mtime: now,
            now
        })
        .0,
        Err(NestError::Stale)
    );

    // Publishing an old generation is refused.
    assert_eq!(
        db.run(Command::PublishReplica {
            file: f.id,
            generation: Generation(1),
            store: StoreId(3)
        })
        .0,
        Err(NestError::Stale)
    );

    // Truncation lets any node own it without the old content.
    let now = db.now();
    let r = db.ok(Command::AcquireOwner {
        file: f.id,
        node: NodeId(5),
        expect_gen: Generation(2),
        truncate: true,
        now,
    });
    let Reply::Acquired { attr, from_gen } = r else {
        panic!()
    };
    assert_eq!((attr.size, from_gen), (0, None));
}

#[test]
fn read_pattern_hint() {
    let mut db = Db::new();
    let f = db.file(ROOT, "table", 1, 10);
    assert!(!query::read_scattered(&db.c, f.id).unwrap());
    let before = db.attr(f.id);
    db.ok(Command::SetReadPattern {
        file: f.id,
        scattered: true,
    });
    assert!(query::read_scattered(&db.c, f.id).unwrap());
    assert_eq!(db.attr(f.id), before, "a hint changes no attributes");
    db.ok(Command::SetReadPattern {
        file: f.id,
        scattered: false,
    });
    assert!(!query::read_scattered(&db.c, f.id).unwrap());
    // Regular files only.
    assert!(matches!(
        db.run(Command::SetReadPattern {
            file: ROOT,
            scattered: true
        })
        .0,
        Err(NestError::NotFound)
    ));
}

#[test]
fn sealing() {
    let mut db = Db::new();
    let f = db.file(ROOT, "blob", 1, 10);
    let now = db.now();
    db.ok(Command::Seal {
        file: f.id,
        sealed: true,
        now,
    });
    assert!(db.attr(f.id).sealed);
    assert!(matches!(
        db.run(Command::AcquireOwner {
            file: f.id,
            node: NodeId(1),
            expect_gen: f.generation,
            truncate: true,
            now
        })
        .0,
        Err(NestError::NotPermitted(_))
    ));
    // Rename and unlink still work on sealed files.
    rename_ok(&mut db, "blob", "blob2");
    // Sealing while owned applies at finalize.
    let g = db.create(ROOT, "x.incomplete", 1);
    db.ok(Command::Seal {
        file: g.id,
        sealed: true,
        now,
    });
    assert!(!db.attr(g.id).sealed);
    db.ok(Command::Finalize {
        file: g.id,
        epoch: g.epoch,
        size: 3,
        mtime: now,
        now,
    });
    assert!(db.attr(g.id).sealed);
}

fn rename_ok(db: &mut Db, from: &str, to: &str) {
    let now = db.now();
    db.ok(Command::Rename {
        parent: ROOT,
        name: from.into(),
        new_parent: ROOT,
        new_name: to.into(),
        flags: RenameFlags::default(),
        now,
    });
}

#[test]
fn retire_refuses_last_copy() {
    let mut db = Db::new();
    let f = db.file(ROOT, "f", 1, 10);
    db.ok(Command::PublishReplica {
        file: f.id,
        generation: f.generation,
        store: StoreId(2),
    });
    db.ok(Command::RetireReplica {
        file: f.id,
        generation: f.generation,
        store: StoreId(1),
        allow_last: false,
    });
    assert!(matches!(
        db.run(Command::RetireReplica {
            file: f.id,
            generation: f.generation,
            store: StoreId(2),
            allow_last: false
        })
        .0,
        Err(NestError::Busy(_))
    ));
    let (_, fx) = db.run(Command::RetireReplica {
        file: f.id,
        generation: f.generation,
        store: StoreId(2),
        allow_last: true,
    });
    assert_eq!(
        fx,
        vec![Effect::ReplicaInvalidated {
            file: f.id,
            generation: f.generation,
            store: StoreId(2)
        }]
    );
}

#[test]
fn unlink_open_file_orphans_until_released() {
    let mut db = Db::new();
    let s1 = db.session(1);
    let s2 = db.session(2);
    let f = db.file(ROOT, "f", 1, 10);
    db.ok(Command::PublishReplica {
        file: f.id,
        generation: f.generation,
        store: StoreId(2),
    });
    let now = db.now();
    let (_, fx) = db.run(Command::Unlink {
        parent: ROOT,
        name: "f".into(),
        now,
    });
    assert!(fx.contains(&Effect::Orphaned {
        file: f.id,
        sessions: vec![s1, s2]
    }));
    assert!(db.lookup(ROOT, "f").is_none());
    assert_eq!(db.attr(f.id).nlink, 0);
    // Replicas survive while any session might still read it.
    db.ok(Command::ReleaseOrphans {
        session: s1,
        files: vec![f.id],
    });
    assert_eq!(db.stores(f.id), vec![1, 2]);
    // Node 2 restarts: its old session expires, releasing the orphan.
    let (_, fx) = {
        let now = db.now();
        db.run(Command::OpenSession {
            node: NodeId(2),
            now,
        })
    };
    assert!(query::getattr(&db.c, f.id).unwrap().is_none());
    assert!(fx.contains(&Effect::SessionExpired {
        session: s2,
        node: NodeId(2)
    }));
    assert!(fx.contains(&Effect::ReplicaInvalidated {
        file: f.id,
        generation: f.generation,
        store: StoreId(1)
    }));
    assert!(fx.contains(&Effect::FileDeleted { file: f.id }));
}

#[test]
fn delete_while_owned_drops_working_object() {
    let mut db = Db::new();
    let f = db.create(ROOT, "tmp", 3);
    let now = db.now();
    let (_, fx) = db.run(Command::Unlink {
        parent: ROOT,
        name: "tmp".into(),
        now,
    });
    assert!(fx.contains(&Effect::WorkingObjectDeleted {
        file: f.id,
        generation: Generation(1),
        owner: NodeId(3)
    }));
}

#[test]
fn batch_applies_each_independently() {
    let mut db = Db::new();
    let now = db.now();
    let r = db.ok(Command::Batch(vec![
        Command::Mkdir {
            parent: ROOT,
            name: "a".into(),
            perm: 0o755,
            now,
        },
        Command::Mkdir {
            parent: ROOT,
            name: "a".into(),
            perm: 0o755,
            now,
        },
        Command::Mkdir {
            parent: ROOT,
            name: "b".into(),
            perm: 0o755,
            now,
        },
    ]));
    let Reply::Batch(rs) = r else { panic!() };
    assert!(rs[0].is_ok());
    assert_eq!(rs[1], Err(NestError::Exists));
    assert!(rs[2].is_ok());
    assert_eq!(db.names(ROOT), vec!["a", "b"]);
}

#[test]
fn stores_register() {
    let mut db = Db::new();
    let r = db.ok(Command::RegisterStore {
        name: "raptor".into(),
        class: nest_meta::StoreClass::Live,
        node: Some(NodeId(1)),
        config: "{}".into(),
    });
    assert_eq!(r, Reply::Store(StoreId(1)));
    let r = db.ok(Command::RegisterStore {
        name: "nas".into(),
        class: nest_meta::StoreClass::Archive,
        node: None,
        config: "{}".into(),
    });
    assert_eq!(r, Reply::Store(StoreId(1 << 20)));
    assert_eq!(
        db.run(Command::RegisterStore {
            name: "nas".into(),
            class: nest_meta::StoreClass::Archive,
            node: None,
            config: "{}".into()
        })
        .0,
        Err(NestError::Exists)
    );
}

fn lock(
    db: &mut Db,
    f: FileId,
    s: SessionId,
    owner: u64,
    start: u64,
    end: u64,
    kind: nest_meta::LockKind,
) -> Result<Reply, NestError> {
    db.run(Command::SetLock {
        file: f,
        session: s,
        owner,
        start,
        end,
        kind,
        pid: 1,
    })
    .0
}

#[test]
fn advisory_locks() {
    use nest_meta::LockKind::*;
    let mut db = Db::new();
    let s1 = db.session(1);
    let s2 = db.session(2);
    let f = db.file(ROOT, "hub.lock", 1, 0);
    // flock-style whole-file exclusive lock, as huggingface's filelock uses.
    lock(&mut db, f.id, s1, 10, 0, u64::MAX, Write).unwrap();
    assert_eq!(
        lock(&mut db, f.id, s2, 20, 0, u64::MAX, Write),
        Err(NestError::WouldBlock)
    );
    assert_eq!(
        lock(&mut db, f.id, s2, 20, 5, 5, Read),
        Err(NestError::WouldBlock)
    );
    // Same owner may convert its own lock.
    lock(&mut db, f.id, s1, 10, 0, u64::MAX, Read).unwrap();
    lock(&mut db, f.id, s2, 20, 0, 99, Read).unwrap();
    assert_eq!(
        lock(&mut db, f.id, s2, 21, 50, 60, Write),
        Err(NestError::WouldBlock)
    );
    // Unlocking a middle range splits the owner's lock.
    lock(&mut db, f.id, s1, 10, 100, 199, Unlock).unwrap();
    let rows = query::locks_of(&db.c, f.id).unwrap();
    let mine: Vec<(u64, u64)> = rows
        .iter()
        .filter(|r| r.owner == 10)
        .map(|r| (r.start, r.end))
        .collect();
    assert_eq!(mine, vec![(0, 99), (200, i64::MAX as u64)]);
    lock(&mut db, f.id, s2, 22, 150, 160, Write).unwrap();
    // Close releases everything the owner holds on the file.
    let (_, fx) = db.run(Command::ReleaseLocks {
        file: f.id,
        session: s1,
        owner: 10,
    });
    assert_eq!(fx, vec![Effect::LocksReleased { file: f.id }]);
    // Session expiry releases the rest.
    let (_, fx) = db.run(Command::ExpireSession { session: s2 });
    assert!(fx.contains(&Effect::LocksReleased { file: f.id }));
    assert!(query::locks_of(&db.c, f.id).unwrap().is_empty());
}

#[test]
fn seal_policies() {
    use nest_meta::SealPolicy;
    let mut db = Db::new();
    let hub = db.mkdir(ROOT, "hub");
    let repo = db.mkdir(hub, "models--org--m");
    let blobs = db.mkdir(repo, "blobs");
    let now = db.now();
    db.ok(Command::SetSealPolicy {
        dir: hub,
        policy: SealPolicy::RenameFromIncomplete,
        now,
    });
    assert_eq!(
        query::effective_seal_policy(&db.c, blobs).unwrap(),
        SealPolicy::RenameFromIncomplete
    );

    // huggingface_hub: write x.incomplete, close, os.replace to x. The
    // rename can land before or after the write epoch finalizes.
    let a = db.create(blobs, "aaa.incomplete", 1);
    rename_in(&mut db, blobs, "aaa.incomplete", "aaa");
    assert!(
        !db.attr(a.id).sealed,
        "still owned: seal takes effect at finalize"
    );
    let now = db.now();
    db.ok(Command::Finalize {
        file: a.id,
        epoch: a.epoch,
        size: 3,
        mtime: now,
        now,
    });
    assert!(db.attr(a.id).sealed);
    let b = db.file(blobs, "bbb.incomplete", 1, 3);
    rename_in(&mut db, blobs, "bbb.incomplete", "bbb");
    assert!(db.attr(b.id).sealed);
    // Other renames and names are left alone.
    let c = db.file(blobs, "ccc", 1, 3);
    rename_in(&mut db, blobs, "ccc", "ccc2");
    assert!(!db.attr(c.id).sealed);

    // An explicit Off below overrides; OnFinalize seals at finalize.
    let scratch = db.mkdir(hub, "scratch");
    let now = db.now();
    db.ok(Command::SetSealPolicy {
        dir: scratch,
        policy: SealPolicy::Off,
        now,
    });
    let d = db.file(scratch, "x.incomplete", 1, 1);
    rename_in(&mut db, scratch, "x.incomplete", "x");
    assert!(!db.attr(d.id).sealed);
    let weights = db.mkdir(ROOT, "weights");
    db.ok(Command::SetSealPolicy {
        dir: weights,
        policy: SealPolicy::OnFinalize,
        now,
    });
    let e = db.file(weights, "model.bin", 1, 10);
    assert!(e.sealed);
}

fn rename_in(db: &mut Db, dir: FileId, from: &str, to: &str) {
    let now = db.now();
    db.ok(Command::Rename {
        parent: dir,
        name: from.into(),
        new_parent: dir,
        new_name: to.into(),
        flags: RenameFlags::default(),
        now,
    });
}
