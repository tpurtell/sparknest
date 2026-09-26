#![allow(dead_code, clippy::new_without_default)]
use nest_meta::{Command, Effect, Reply, fsck, open_memory, query};
use nest_types::*;
use rusqlite::Connection;

pub struct Db {
    pub c: Connection,
    pub clock: i64,
}

impl Db {
    pub fn new() -> Self {
        Db {
            c: open_memory().unwrap(),
            clock: 1_000,
        }
    }

    pub fn now(&mut self) -> Timestamp {
        self.clock += 1;
        Timestamp(self.clock)
    }

    pub fn run(&mut self, cmd: Command) -> (Result<Reply, NestError>, Vec<Effect>) {
        let tx = self.c.transaction().unwrap();
        let out = nest_meta::apply(&tx, &cmd).unwrap();
        tx.commit().unwrap();
        let problems = fsck::check(&self.c).unwrap();
        assert!(problems.is_empty(), "after {cmd:?}: {problems:#?}");
        out
    }

    pub fn ok(&mut self, cmd: Command) -> Reply {
        match self.run(cmd.clone()).0 {
            Ok(r) => r,
            Err(e) => panic!("{cmd:?} failed: {e}"),
        }
    }

    pub fn mkdir(&mut self, parent: FileId, name: &str) -> FileId {
        let now = self.now();
        match self.ok(Command::Mkdir {
            parent,
            name: name.into(),
            perm: 0o755,
            now,
        }) {
            Reply::Attr(a) => a.id,
            r => panic!("{r:?}"),
        }
    }

    pub fn create(&mut self, parent: FileId, name: &str, node: u64) -> FileAttr {
        let now = self.now();
        match self.ok(Command::Create {
            parent,
            name: name.into(),
            perm: 0o644,
            node: NodeId(node),
            exclusive: true,
            now,
        }) {
            Reply::Created(c) => c.attr,
            r => panic!("{r:?}"),
        }
    }

    /// Create and finalize a file of `size` bytes on `node`.
    pub fn file(&mut self, parent: FileId, name: &str, node: u64, size: u64) -> FileAttr {
        let a = self.create(parent, name, node);
        let now = self.now();
        match self.ok(Command::Finalize {
            file: a.id,
            epoch: a.epoch,
            size,
            mtime: now,
            now,
        }) {
            Reply::Attr(a) => a,
            r => panic!("{r:?}"),
        }
    }

    pub fn attr(&self, id: FileId) -> FileAttr {
        query::getattr(&self.c, id).unwrap().unwrap()
    }

    pub fn lookup(&self, parent: FileId, name: &str) -> Option<FileId> {
        query::lookup(&self.c, parent, name.as_bytes()).unwrap()
    }

    pub fn names(&self, dir: FileId) -> Vec<String> {
        query::readdir(&self.c, dir, 0, 10_000)
            .unwrap()
            .into_iter()
            .map(|(_, e)| String::from_utf8(e.name).unwrap())
            .collect()
    }

    pub fn stores(&self, file: FileId) -> Vec<u64> {
        query::replicas(&self.c, file)
            .unwrap()
            .into_iter()
            .map(|r| r.store.0)
            .collect()
    }

    pub fn session(&mut self, node: u64) -> SessionId {
        let now = self.now();
        match self.ok(Command::OpenSession {
            node: NodeId(node),
            now,
        }) {
            Reply::Session(s) => s,
            r => panic!("{r:?}"),
        }
    }
}
