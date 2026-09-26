//! SQLite schema for `meta.sqlite` and connection setup.

use rusqlite::{Connection, OpenFlags};
use std::path::Path;

/// Bumped whenever the schema changes. Snapshots carry the version so a node
/// never installs a snapshot it cannot read.
pub const SCHEMA_VERSION: i64 = 2;

/// Migrations from version N to N+1, applied in order at open.
const MIGRATIONS: &[(i64, &str)] = &[(
    1,
    r#"
-- Retained, versioned backups of selections (ADR-021). The manifest and
-- objects live in the archive store; this row is the catalog entry.
CREATE TABLE IF NOT EXISTS backups (
    id       INTEGER PRIMARY KEY,
    name     TEXT    NOT NULL,
    store    INTEGER NOT NULL,
    created  INTEGER NOT NULL,
    selector TEXT    NOT NULL,
    files    INTEGER NOT NULL,
    bytes    INTEGER NOT NULL
);
"#,
)];

const SCHEMA: &str = r#"
CREATE TABLE files (
    id        INTEGER PRIMARY KEY,
    kind      INTEGER NOT NULL,
    perm      INTEGER NOT NULL,
    size      INTEGER NOT NULL DEFAULT 0,
    nlink     INTEGER NOT NULL,
    atime     INTEGER NOT NULL,
    mtime     INTEGER NOT NULL,
    ctime     INTEGER NOT NULL,
    crtime    INTEGER NOT NULL,
    gen       INTEGER NOT NULL DEFAULT 0,
    gen_state INTEGER NOT NULL DEFAULT 0,
    owner     INTEGER,
    epoch     INTEGER NOT NULL DEFAULT 0,
    sealed    INTEGER NOT NULL DEFAULT 0,
    flags     INTEGER NOT NULL DEFAULT 0,
    parent    INTEGER,   -- directories only: the single parent (for "..")
    target    BLOB       -- symlinks only
);
CREATE INDEX files_owner ON files(owner) WHERE owner IS NOT NULL;

-- cookie: monotonically assigned per insertion; stable readdir offsets.
CREATE TABLE dentries (
    parent INTEGER NOT NULL,
    name   BLOB    NOT NULL,
    child  INTEGER NOT NULL,
    cookie INTEGER NOT NULL,
    PRIMARY KEY (parent, name)
) WITHOUT ROWID;
CREATE UNIQUE INDEX dentries_cookie ON dentries(parent, cookie);
CREATE INDEX dentries_child ON dentries(child);

-- Complete copies of a STABLE generation. The owner's working object while
-- OWNED is not a replica and is not listed here.
CREATE TABLE replicas (
    file  INTEGER NOT NULL,
    gen   INTEGER NOT NULL,
    store INTEGER NOT NULL,
    state INTEGER NOT NULL,
    PRIMARY KEY (file, gen, store)
) WITHOUT ROWID;
CREATE INDEX replicas_store ON replicas(store, file);

CREATE TABLE sessions (
    id      INTEGER PRIMARY KEY,
    node    INTEGER NOT NULL,
    renewed INTEGER NOT NULL
);
CREATE INDEX sessions_node ON sessions(node);

-- Unlinked files (nlink = 0) kept alive until every session that might hold
-- them open has released them.
CREATE TABLE orphans (
    file    INTEGER NOT NULL,
    session INTEGER NOT NULL,
    PRIMARY KEY (file, session)
) WITHOUT ROWID;
CREATE INDEX orphans_session ON orphans(session);

-- Advisory locks (fcntl and flock), cluster-wide. `owner` is the kernel's
-- lock owner id, unique per node; with `session` it identifies the holder.
-- Ranges are inclusive; `end_` = u64::MAX as i64 means to end of file.
CREATE TABLE locks (
    file    INTEGER NOT NULL,
    session INTEGER NOT NULL,
    owner   INTEGER NOT NULL,
    start   INTEGER NOT NULL,
    end_    INTEGER NOT NULL,
    kind    INTEGER NOT NULL,   -- 0 read (shared), 1 write (exclusive)
    pid     INTEGER NOT NULL,
    PRIMARY KEY (file, session, owner, start)
) WITHOUT ROWID;
CREATE INDEX locks_session ON locks(session);

CREATE TABLE stores (
    id     INTEGER PRIMARY KEY,
    name   TEXT    NOT NULL UNIQUE,
    class  INTEGER NOT NULL,
    node   INTEGER,
    config TEXT    NOT NULL
);

-- Placement rules: durable intent, interpreted by nest-place. `spec` is
-- JSON; `revision` increments on every change (optimistic concurrency).
CREATE TABLE rules (
    name     TEXT PRIMARY KEY,
    spec     TEXT NOT NULL,
    revision INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE kv (
    k TEXT PRIMARY KEY,
    v INTEGER NOT NULL
) WITHOUT ROWID;

INSERT INTO kv (k, v) VALUES ('next_file_id', 2), ('next_cookie', 1), ('next_session', 1), ('next_store', 1048576);
INSERT INTO files (id, kind, perm, nlink, atime, mtime, ctime, crtime, parent)
    VALUES (1, 2, 493, 2, 0, 0, 0, 0, 1);
"#;

fn pragmas(conn: &Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    // The Raft log is the durability source; after a crash the state machine
    // replays from its recorded last_applied position.
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    conn.set_prepared_statement_cache_capacity(128);
    Ok(())
}

/// Open (creating and initializing if needed) the database for the single
/// writer: the state machine.
pub fn open_write(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    pragmas(&conn)?;
    init(&conn)?;
    Ok(conn)
}

/// Open an in-memory database (tests, snapshot staging).
pub fn open_memory() -> rusqlite::Result<Connection> {
    let conn = Connection::open_in_memory()?;
    conn.set_prepared_statement_cache_capacity(128);
    init(&conn)?;
    Ok(conn)
}

/// Open a read-only connection for FUSE/API readers. WAL lets any number of
/// these run concurrently with the writer.
pub fn open_read(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    conn.set_prepared_statement_cache_capacity(128);
    Ok(conn)
}

fn init(conn: &Connection) -> rusqlite::Result<()> {
    let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    match version {
        0 => {
            let all: String = MIGRATIONS.iter().map(|(_, m)| *m).collect();
            conn.execute_batch(&format!(
                "BEGIN; {SCHEMA} {all} PRAGMA user_version = {SCHEMA_VERSION}; COMMIT;"
            ))?;
            Ok(())
        }
        SCHEMA_VERSION => Ok(()),
        v if v > 0 && v < SCHEMA_VERSION => {
            for (from, sql) in MIGRATIONS.iter().filter(|(f, _)| *f >= v) {
                conn.execute_batch(&format!(
                    "BEGIN; {sql} PRAGMA user_version = {}; COMMIT;",
                    from + 1
                ))?;
            }
            Ok(())
        }
        other => Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISMATCH),
            Some(format!(
                "meta schema version {other}, expected {SCHEMA_VERSION}"
            )),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_1_databases_migrate_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("meta.sqlite");
        {
            let c = Connection::open(&p).unwrap();
            c.execute_batch(&format!("BEGIN; {SCHEMA} PRAGMA user_version = 1; COMMIT;"))
                .unwrap();
            c.execute("INSERT INTO kv (k, v) VALUES ('probe', 7)", [])
                .unwrap();
        }
        let c = open_write(&p).unwrap();
        let v: i64 = c
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        c.execute("INSERT INTO backups (name, store, created, selector, files, bytes) VALUES ('b', 1, 0, 's', 0, 0)", []).unwrap();
        let probe: i64 = c
            .query_row("SELECT v FROM kv WHERE k = 'probe'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(probe, 7, "existing data survives");
    }
}
