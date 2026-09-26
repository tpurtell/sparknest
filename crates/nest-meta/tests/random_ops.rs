//! Random command sequences: invariants hold after every step (checked by
//! `Db::run` via fsck) and two replicas applying the same log end identical.

mod common;
use common::Db;
use nest_meta::{Command, RenameFlags, Reply, query};
use nest_types::*;
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Op {
    Mkdir(u8, u8),
    Create(u8, u8, u8),
    Symlink(u8, u8),
    Link(u8, u8, u8),
    Unlink(u8, u8),
    Rmdir(u8, u8),
    Rename(u8, u8, u8, u8, u8),
    Acquire(u8, u8, bool),
    Finalize(u8, u16),
    Publish(u8, u8),
    Retire(u8, u8),
    Seal(u8, bool),
    OpenSession(u8),
    Release(u8, u8),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (any::<u8>(), any::<u8>()).prop_map(|(a, b)| Op::Mkdir(a, b)),
        (any::<u8>(), any::<u8>(), any::<u8>()).prop_map(|(a, b, c)| Op::Create(a, b, c)),
        (any::<u8>(), any::<u8>()).prop_map(|(a, b)| Op::Symlink(a, b)),
        (any::<u8>(), any::<u8>(), any::<u8>()).prop_map(|(a, b, c)| Op::Link(a, b, c)),
        (any::<u8>(), any::<u8>()).prop_map(|(a, b)| Op::Unlink(a, b)),
        (any::<u8>(), any::<u8>()).prop_map(|(a, b)| Op::Rmdir(a, b)),
        (
            any::<u8>(),
            any::<u8>(),
            any::<u8>(),
            any::<u8>(),
            any::<u8>()
        )
            .prop_map(|(a, b, c, d, e)| Op::Rename(a, b, c, d, e)),
        (any::<u8>(), any::<u8>(), any::<bool>()).prop_map(|(a, b, c)| Op::Acquire(a, b, c)),
        (any::<u8>(), any::<u16>()).prop_map(|(a, b)| Op::Finalize(a, b)),
        (any::<u8>(), any::<u8>()).prop_map(|(a, b)| Op::Publish(a, b)),
        (any::<u8>(), any::<u8>()).prop_map(|(a, b)| Op::Retire(a, b)),
        (any::<u8>(), any::<bool>()).prop_map(|(a, b)| Op::Seal(a, b)),
        any::<u8>().prop_map(Op::OpenSession),
        (any::<u8>(), any::<u8>()).prop_map(|(a, b)| Op::Release(a, b)),
    ]
}

const NAMES: [&str; 5] = ["a", "b", "c", "d.incomplete", "e"];

fn name(i: u8) -> Vec<u8> {
    NAMES[i as usize % NAMES.len()].into()
}

fn node(i: u8) -> NodeId {
    NodeId(1 + (i as u64 % 3))
}

/// Pick an existing file id (any kind) deterministically from the db.
fn pick_file(db: &Db, i: u8) -> Option<FileAttr> {
    let mut st = db.c.prepare("SELECT id FROM files ORDER BY id").unwrap();
    let ids: Vec<i64> = st
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let id = ids[i as usize % ids.len()];
    query::getattr(&db.c, FileId(id as u64)).unwrap()
}

fn pick_dir(db: &Db, i: u8) -> FileId {
    let mut st =
        db.c.prepare("SELECT id FROM files WHERE kind = 2 ORDER BY id")
            .unwrap();
    let ids: Vec<i64> = st
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    FileId(ids[i as usize % ids.len()] as u64)
}

fn to_command(db: &mut Db, op: &Op) -> Option<Command> {
    let now = db.now();
    Some(match *op {
        Op::Mkdir(d, n) => Command::Mkdir {
            parent: pick_dir(db, d),
            name: name(n),
            perm: 0o755,
            now,
        },
        Op::Create(d, n, x) => Command::Create {
            parent: pick_dir(db, d),
            name: name(n),
            perm: 0o644,
            node: node(x),
            exclusive: x % 2 == 0,
            now,
        },
        Op::Symlink(d, n) => Command::Symlink {
            parent: pick_dir(db, d),
            name: name(n),
            target: b"t".to_vec(),
            now,
        },
        Op::Link(f, d, n) => Command::Link {
            file: pick_file(db, f)?.id,
            parent: pick_dir(db, d),
            name: name(n),
            now,
        },
        Op::Unlink(d, n) => Command::Unlink {
            parent: pick_dir(db, d),
            name: name(n),
            now,
        },
        Op::Rmdir(d, n) => Command::Rmdir {
            parent: pick_dir(db, d),
            name: name(n),
            now,
        },
        Op::Rename(d, n, e, m, fl) => Command::Rename {
            parent: pick_dir(db, d),
            name: name(n),
            new_parent: pick_dir(db, e),
            new_name: name(m),
            flags: RenameFlags {
                noreplace: fl % 4 == 1,
                exchange: fl % 4 == 2,
            },
            now,
        },
        Op::Acquire(f, x, t) => {
            let a = pick_file(db, f)?;
            Command::AcquireOwner {
                file: a.id,
                node: node(x),
                expect_gen: a.generation,
                truncate: t,
                now,
            }
        }
        Op::Finalize(f, size) => {
            let a = pick_file(db, f)?;
            Command::Finalize {
                file: a.id,
                epoch: a.epoch,
                size: size as u64,
                mtime: now,
                now,
            }
        }
        Op::Publish(f, s) => {
            let a = pick_file(db, f)?;
            Command::PublishReplica {
                file: a.id,
                generation: a.generation,
                store: node(s).live_store(),
            }
        }
        Op::Retire(f, s) => {
            let a = pick_file(db, f)?;
            Command::RetireReplica {
                file: a.id,
                generation: a.generation,
                store: node(s).live_store(),
                allow_last: false,
            }
        }
        Op::Seal(f, s) => Command::Seal {
            file: pick_file(db, f)?.id,
            sealed: s,
            now,
        },
        Op::OpenSession(x) => Command::OpenSession { node: node(x), now },
        Op::Release(s, f) => {
            let sessions = query::sessions(&db.c).unwrap();
            if sessions.is_empty() {
                return None;
            }
            let s = sessions[s as usize % sessions.len()].id;
            let orphans = query::orphans_of(&db.c, s).unwrap();
            if orphans.is_empty() {
                return None;
            }
            Command::ReleaseOrphans {
                session: s,
                files: vec![orphans[f as usize % orphans.len()]],
            }
        }
    })
}

fn dump(db: &Db) -> Vec<String> {
    let mut out = Vec::new();
    for table in [
        "files", "dentries", "replicas", "sessions", "orphans", "stores", "kv",
    ] {
        let mut st =
            db.c.prepare(&format!("SELECT * FROM {table} ORDER BY 1, 2"))
                .unwrap();
        let n = st.column_count();
        let rows = st
            .query_map([], |r| {
                let mut s = String::new();
                for i in 0..n {
                    s += &format!("{:?}|", r.get_ref(i)?);
                }
                Ok(s)
            })
            .unwrap();
        for r in rows {
            out.push(format!("{table}:{}", r.unwrap()));
        }
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn invariants_and_determinism(ops in proptest::collection::vec(op(), 1..120)) {
        let mut a = Db::new();
        let mut log = Vec::new();
        for op in &ops {
            if let Some(cmd) = to_command(&mut a, op) {
                let (r, _) = a.run(cmd.clone());
                // Owned files never have replicas; stable regular files keep
                // at least one copy (we never retire with allow_last).
                if let Ok(Reply::Acquired { attr, .. }) = &r {
                    prop_assert!(query::replicas(&a.c, attr.id).unwrap().is_empty());
                }
                log.push(cmd);
            }
        }
        let mut st = a.c.prepare("SELECT id FROM files WHERE kind = 1 AND gen_state = 0").unwrap();
        let stable: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect();
        for id in stable {
            prop_assert!(!query::replicas(&a.c, FileId(id as u64)).unwrap().is_empty(), "stable file {id} lost its last copy");
        }
        drop(st);
        let mut b = Db::new();
        for cmd in &log {
            let _ = b.run(cmd.clone());
        }
        prop_assert_eq!(dump(&a), dump(&b));
    }
}
