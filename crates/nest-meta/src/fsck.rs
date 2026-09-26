//! Structural consistency checks over replicated metadata. Used by tests
//! after every step and by `nest fsck`.

use rusqlite::Connection;

/// Returns a list of human-readable problems; empty means consistent.
pub fn check(c: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut problems = Vec::new();
    let mut q = |sql: &str, what: &str| -> rusqlite::Result<()> {
        let mut st = c.prepare(sql)?;
        let rows: Vec<String> = st
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        for r in rows {
            problems.push(format!("{what}: {r}"));
        }
        Ok(())
    };
    q(
        "SELECT printf('%d/%s -> %d', d.parent, hex(d.name), d.child) FROM dentries d LEFT JOIN files f ON f.id = d.child WHERE f.id IS NULL",
        "dangling dentry",
    )?;
    q(
        "SELECT printf('%d/%s', d.parent, hex(d.name)) FROM dentries d JOIN files p ON p.id = d.parent WHERE p.kind != 2",
        "dentry under non-directory",
    )?;
    q(
        "SELECT printf('file %d nlink %d names %d', f.id, f.nlink, (SELECT COUNT(*) FROM dentries d WHERE d.child = f.id)) \
         FROM files f WHERE f.kind != 2 AND f.nlink != (SELECT COUNT(*) FROM dentries d WHERE d.child = f.id)",
        "link count",
    )?;
    q(
        "SELECT printf('dir %d nlink %d expected %d', f.id, f.nlink, 2 + (SELECT COUNT(*) FROM files s WHERE s.kind = 2 AND s.parent = f.id AND s.id != f.id)) \
         FROM files f WHERE f.kind = 2 AND f.nlink != 2 + (SELECT COUNT(*) FROM files s WHERE s.kind = 2 AND s.parent = f.id AND s.id != f.id)",
        "directory link count",
    )?;
    q(
        "SELECT printf('dir %d has %d names', f.id, (SELECT COUNT(*) FROM dentries d WHERE d.child = f.id)) \
         FROM files f WHERE f.kind = 2 AND f.id != 1 AND (SELECT COUNT(*) FROM dentries d WHERE d.child = f.id) != 1",
        "directory name count",
    )?;
    q(
        "SELECT printf('dir %d parent %d but named under %d', f.id, f.parent, d.parent) FROM files f JOIN dentries d ON d.child = f.id WHERE f.kind = 2 AND f.parent != d.parent",
        "directory parent mismatch",
    )?;
    q(
        "SELECT printf('file %d', f.id) FROM files f WHERE f.id != 1 AND f.nlink = 0 AND NOT EXISTS (SELECT 1 FROM orphans o WHERE o.file = f.id)",
        "unreferenced file",
    )?;
    q(
        "SELECT printf('file %d', o.file) FROM orphans o JOIN files f ON f.id = o.file WHERE f.nlink != 0",
        "orphan row for linked file",
    )?;
    q(
        "SELECT printf('file %d session %d', o.file, o.session) FROM orphans o LEFT JOIN sessions s ON s.id = o.session WHERE s.id IS NULL",
        "orphan row for dead session",
    )?;
    q(
        "SELECT printf('file %d session %d', l.file, l.session) FROM locks l LEFT JOIN sessions s ON s.id = l.session LEFT JOIN files f ON f.id = l.file WHERE s.id IS NULL OR f.id IS NULL",
        "lock held by dead session or on missing file",
    )?;
    q(
        "SELECT printf('file %d: %d/%d [%d,%d] vs %d/%d [%d,%d]', a.file, a.session, a.owner, a.start, a.end_, b.session, b.owner, b.start, b.end_) \
         FROM locks a JOIN locks b ON a.file = b.file \
         WHERE (a.session < b.session OR (a.session = b.session AND a.owner < b.owner)) \
         AND a.start <= b.end_ AND b.start <= a.end_ AND (a.kind = 1 OR b.kind = 1)",
        "conflicting locks",
    )?;
    q(
        "SELECT printf('file %d owner %d/%d at %d', a.file, a.session, a.owner, b.start) FROM locks a JOIN locks b \
         ON a.file = b.file AND a.session = b.session AND a.owner = b.owner AND a.start < b.start AND a.end_ >= b.start",
        "overlapping ranges of one owner",
    )?;
    q(
        "SELECT printf('file %d gen %d store %d', r.file, r.gen, r.store) FROM replicas r LEFT JOIN files f ON f.id = r.file WHERE f.id IS NULL OR f.kind != 1",
        "replica of missing file",
    )?;
    q(
        "SELECT printf('file %d gen %d current %d', r.file, r.gen, f.gen) FROM replicas r JOIN files f ON f.id = r.file WHERE r.gen != f.gen",
        "replica of non-current generation",
    )?;
    q(
        "SELECT printf('file %d', f.id) FROM files f WHERE f.gen_state = 1 AND EXISTS (SELECT 1 FROM replicas r WHERE r.file = f.id)",
        "owned file with replicas",
    )?;
    q(
        "SELECT printf('file %d', f.id) FROM files f WHERE (f.gen_state = 1) != (f.owner IS NOT NULL)",
        "owner/state mismatch",
    )?;
    q(
        "SELECT printf('file %d', f.id) FROM files f WHERE f.kind != 1 AND (f.gen_state != 0 OR f.owner IS NOT NULL OR f.sealed != 0)",
        "lifecycle state on non-regular file",
    )?;
    // Directory cycles: walk parents from every directory.
    let dirs: Vec<(i64, i64)> = c
        .prepare("SELECT id, parent FROM files WHERE kind = 2")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let parent: std::collections::HashMap<i64, i64> = dirs.iter().copied().collect();
    for (id, _) in &dirs {
        let mut cur = *id;
        let mut steps = 0;
        while cur != 1 {
            cur = match parent.get(&cur) {
                Some(p) => *p,
                None => {
                    problems.push(format!("directory {id} not connected to root"));
                    break;
                }
            };
            steps += 1;
            if steps > dirs.len() {
                problems.push(format!("directory cycle through {id}"));
                break;
            }
        }
    }
    Ok(problems)
}
