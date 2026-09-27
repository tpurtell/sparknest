//! A node's live object store.
//!
//! Objects are addressed by identity, never by path (ADR-005):
//!
//! ```text
//! <root>/objects/<xx>/<file_id:016x>.<generation:x>   complete copies and working objects
//! <root>/staging/<file_id:016x>.<generation:x>.<nonce> incomplete transfers, never served
//! ```
//!
//! `xx` is the low byte of the file id, spreading objects over 256
//! directories. Renames and hard links in the namespace never touch this
//! tree. Deleting `(file, generation)` can never remove a different
//! generation that later appears for the same file.
//!
//! This crate performs blocking filesystem I/O; async callers run it on a
//! blocking pool. Consistency with metadata is restored at startup by the
//! node's reconciliation pass (see `nest-data`), which is why most
//! operations need no separate intent journal.

use nest_types::{FileId, Generation};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey {
    pub file: FileId,
    pub generation: Generation,
}

impl ObjectKey {
    pub fn new(file: FileId, generation: Generation) -> Self {
        ObjectKey { file, generation }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: ObjectKey,
    pub size: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capacity {
    pub total: u64,
    pub free: u64,
}

#[derive(Debug)]
pub struct ObjectStore {
    root: PathBuf,
    objects: PathBuf,
    staging: PathBuf,
    /// Bytes data writes must leave free (0 = no limit); see `reserve_room`.
    reserve: std::sync::atomic::AtomicU64,
    /// Last statvfs sample: (when, free bytes minus writes since).
    room: std::sync::Mutex<(std::time::Instant, u64)>,
}

fn object_name(k: ObjectKey) -> String {
    format!("{:016x}.{:x}", k.file.0, k.generation.0)
}

fn parse_name(name: &str) -> Option<ObjectKey> {
    let (f, g) = name.split_once('.')?;
    if f.len() != 16 || g.is_empty() || g.contains('.') {
        return None;
    }
    Some(ObjectKey::new(
        FileId(u64::from_str_radix(f, 16).ok()?),
        Generation(u64::from_str_radix(g, 16).ok()?),
    ))
}

impl ObjectStore {
    /// Open (creating directories as needed) the store rooted at `root`.
    /// Incomplete staging files from a previous run are discarded:
    /// interrupted transfers restart from scratch.
    pub fn open(root: &Path) -> io::Result<Self> {
        Self::open_with_staging(root, "staging")
    }

    /// Open for inspection only: nothing is created or cleaned up.
    pub fn open_readonly(root: &Path) -> io::Result<Self> {
        let objects = root.join("objects");
        if !objects.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} has no objects directory", root.display()),
            ));
        }
        Ok(ObjectStore {
            root: root.to_path_buf(),
            objects,
            staging: root.join("staging"),
            reserve: std::sync::atomic::AtomicU64::new(0),
            room: std::sync::Mutex::new((std::time::Instant::now(), 0)),
        })
    }

    /// Open with a named staging directory (archive stores shared by several
    /// gateways give each gateway its own, so none discards another's
    /// in-flight transfers).
    pub fn open_with_staging(root: &Path, staging_dir: &str) -> io::Result<Self> {
        let objects = root.join("objects");
        let staging = root.join(staging_dir);
        fs::create_dir_all(&objects)?;
        fs::create_dir_all(&staging)?;
        for i in 0..=255u8 {
            fs::create_dir_all(objects.join(format!("{i:02x}")))?;
        }
        for e in fs::read_dir(&staging)? {
            let p = e?.path();
            tracing::debug!(path = %p.display(), "discarding incomplete staging file");
            let _ = fs::remove_file(p);
        }
        Ok(ObjectStore {
            root: root.to_path_buf(),
            objects,
            staging,
            reserve: std::sync::atomic::AtomicU64::new(0),
            room: std::sync::Mutex::new((std::time::Instant::now(), 0)),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path(&self, k: ObjectKey) -> PathBuf {
        self.objects
            .join(format!("{:02x}", k.file.0 & 0xff))
            .join(object_name(k))
    }

    pub fn exists(&self, k: ObjectKey) -> bool {
        self.path(k).exists()
    }

    pub fn size(&self, k: ObjectKey) -> io::Result<u64> {
        Ok(fs::metadata(self.path(k))?.len())
    }

    /// Size and modification time of an object.
    pub fn stat(&self, k: ObjectKey) -> io::Result<(u64, nest_types::Timestamp)> {
        let md = fs::metadata(self.path(k))?;
        Ok((
            md.len(),
            nest_types::Timestamp::from_system_time(md.modified()?),
        ))
    }

    /// Create a new, empty object. Fails if it already exists.
    pub fn create(&self, k: ObjectKey) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.path(k))
    }

    pub fn open_read(&self, k: ObjectKey) -> io::Result<File> {
        File::open(self.path(k))
    }

    pub fn open_write(&self, k: ObjectKey) -> io::Result<File> {
        OpenOptions::new().read(true).write(true).open(self.path(k))
    }

    /// Re-key an object for a new generation (ownership conversion: the
    /// owner's copy of `from` becomes the working object of `to`). Fails if
    /// the destination exists.
    pub fn rekey(&self, from: ObjectKey, to: ObjectKey) -> io::Result<()> {
        debug_assert_eq!(from.file, to.file);
        rename_noreplace(&self.path(from), &self.path(to))
    }

    /// Remove an object. Missing objects are not an error: deletion is
    /// idempotent. Readers holding descriptors keep reading until they close.
    /// Give an object a different key, file id included (fsck moving an
    /// orphan into lost+found). Refuses to overwrite.
    pub fn move_object(&self, from: ObjectKey, to: ObjectKey) -> io::Result<()> {
        if let Some(d) = self.path(to).parent() {
            fs::create_dir_all(d)?;
        }
        rename_noreplace(&self.path(from), &self.path(to))
    }

    pub fn delete(&self, k: ObjectKey) -> io::Result<bool> {
        match fs::remove_file(self.path(k)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Start an incoming whole-file transfer.
    pub fn begin_staging(&self, k: ObjectKey) -> io::Result<Staging> {
        let path = self
            .staging
            .join(format!("{}.{:016x}", object_name(k), rand::random::<u64>()));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        Ok(Staging {
            key: k,
            path,
            file: Some(file),
        })
    }

    /// Move a completely written staging file into place. The data is made
    /// durable first; after this returns the object survives a crash.
    pub fn commit_staging(&self, mut s: Staging) -> io::Result<ObjectKey> {
        let f = s.file.take().expect("staging file present until commit");
        f.sync_all()?;
        drop(f);
        rename_noreplace(&s.path, &self.path(s.key))?;
        sync_dir(&self.path(s.key))?;
        Ok(s.key)
    }

    /// Adopt an existing file on the same filesystem as this store by
    /// renaming it into place: no data is copied. Fails with EXDEV if it
    /// lives on another filesystem, and with EEXIST if the object exists.
    pub fn adopt(&self, src: &Path, k: ObjectKey) -> io::Result<u64> {
        let md = fs::symlink_metadata(src)?;
        if !md.file_type().is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not a regular file",
            ));
        }
        rename_noreplace(src, &self.path(k))?;
        Ok(md.len())
    }

    /// Adopt an existing file by hard-linking it into place: no data moves
    /// and the source stays until the caller removes it (import commits the
    /// metadata in between, so a crash never loses the source). Fails with
    /// EXDEV across filesystems and EEXIST if the object exists.
    pub fn link_from(&self, src: &Path, k: ObjectKey) -> io::Result<u64> {
        let md = fs::symlink_metadata(src)?;
        if !md.file_type().is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not a regular file",
            ));
        }
        fs::hard_link(src, self.path(k))?;
        Ok(md.len())
    }

    /// Every object present, with its size. Unrecognized names are skipped.
    pub fn scan(&self) -> io::Result<Vec<ObjectInfo>> {
        let mut out = Vec::new();
        for i in 0..=255u8 {
            let dir = self.objects.join(format!("{i:02x}"));
            let rd = match fs::read_dir(&dir) {
                Ok(rd) => rd,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            for e in rd {
                let e = e?;
                let name = e.file_name();
                let Some(key) = parse_name(&name.to_string_lossy()) else {
                    tracing::warn!(path = %e.path().display(), "unrecognized file in object store");
                    continue;
                };
                out.push(ObjectInfo {
                    key,
                    size: e.metadata()?.len(),
                });
            }
        }
        out.sort_by_key(|o| o.key);
        Ok(out)
    }

    /// Keep `bytes` free on this store's filesystem for everything that is
    /// not object data: the metadata database and Raft log share it, and a
    /// full disk there would stall the cluster's metadata.
    pub fn set_reserve(&self, bytes: u64) {
        self.reserve
            .store(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Account for a data write of `bytes`: `ENOSPC` if it would eat into
    /// the reserve. statvfs is sampled at most every 250 ms; writes in
    /// between are subtracted from the sample (overwrites count too, which
    /// errs on the safe side until the next sample).
    pub fn reserve_room(&self, bytes: u64) -> io::Result<()> {
        let reserve = self.reserve.load(std::sync::atomic::Ordering::Relaxed);
        if reserve == 0 {
            return Ok(());
        }
        let mut g = self.room.lock().unwrap_or_else(|e| e.into_inner());
        let stale = g.0.elapsed() >= std::time::Duration::from_millis(250);
        if stale || g.1 < reserve.saturating_add(bytes) {
            *g = (std::time::Instant::now(), self.capacity()?.free);
        }
        if g.1 < reserve.saturating_add(bytes) {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        g.1 -= bytes;
        Ok(())
    }

    pub fn capacity(&self) -> io::Result<Capacity> {
        let s = nix::sys::statvfs::statvfs(&self.root).map_err(io::Error::from)?;
        let frag = s.fragment_size() as u64;
        Ok(Capacity {
            total: s.blocks() as u64 * frag,
            free: s.blocks_available() as u64 * frag,
        })
    }
}

/// An incomplete transfer. Dropped without commit, it is removed.
pub struct Staging {
    key: ObjectKey,
    path: PathBuf,
    file: Option<File>,
}

impl Staging {
    pub fn key(&self) -> ObjectKey {
        self.key
    }
    pub fn file(&self) -> &File {
        self.file
            .as_ref()
            .expect("staging file present until commit")
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    match nix::fcntl::renameat2(
        nix::fcntl::AT_FDCWD,
        from,
        nix::fcntl::AT_FDCWD,
        to,
        nix::fcntl::RenameFlags::RENAME_NOREPLACE,
    ) {
        Ok(()) => Ok(()),
        // Some filesystems (CIFS, ntfs3) reject the flag. Object names are
        // unique per (file, generation) and this node is their only writer,
        // so check-then-rename is safe here.
        Err(
            nix::errno::Errno::EINVAL | nix::errno::Errno::ENOSYS | nix::errno::Errno::EOPNOTSUPP,
        ) => {
            if to.exists() {
                return Err(io::Error::from_raw_os_error(libc::EEXIST));
            }
            fs::rename(from, to)
        }
        Err(e) => Err(e.into()),
    }
}

/// Name of the marker file identifying a store's root.
pub const MARKER: &str = ".sparknest-store";

/// Write (or verify) the marker identifying `root` as `identity`. Refuses a
/// root that holds a different store's marker, or files but no marker.
pub fn init_marker(root: &Path, identity: &str) -> io::Result<()> {
    let m = root.join(MARKER);
    match fs::read_to_string(&m) {
        Ok(existing) if existing.trim() == identity.trim() => return Ok(()),
        Ok(existing) => {
            return Err(io::Error::other(format!(
                "{} belongs to another store ({})",
                root.display(),
                existing.trim()
            )));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    if !root.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} is not a directory", root.display()),
        ));
    }
    fs::write(&m, format!("{}\n", identity.trim()))?;
    File::open(root)?.sync_all()
}

/// True if `root` carries the marker for `identity`: an unmounted share
/// shows an empty mountpoint without it and is never written to.
pub fn marker_ok(root: &Path, identity: &str) -> bool {
    fs::read_to_string(root.join(MARKER)).is_ok_and(|s| s.trim() == identity.trim())
}

fn sync_dir(child: &Path) -> io::Result<()> {
    if let Some(d) = child.parent() {
        File::open(d)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn k(f: u64, g: u64) -> ObjectKey {
        ObjectKey::new(FileId(f), Generation(g))
    }

    #[test]
    fn names_roundtrip() {
        let key = k(0x1234, 0xab);
        assert_eq!(parse_name(&object_name(key)), Some(key));
        assert_eq!(parse_name("junk"), None);
        assert_eq!(parse_name("0000000000001234.ab.tmp"), None);
    }

    #[test]
    fn lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let s = ObjectStore::open(dir.path()).unwrap();
        let mut f = s.create(k(7, 1)).unwrap();
        f.write_all(b"hello").unwrap();
        assert!(s.create(k(7, 1)).is_err());
        assert_eq!(s.size(k(7, 1)).unwrap(), 5);

        // Ownership conversion keeps the bytes under the new key.
        s.rekey(k(7, 1), k(7, 2)).unwrap();
        assert!(!s.exists(k(7, 1)));
        let mut buf = String::new();
        s.open_read(k(7, 2))
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        assert_eq!(buf, "hello");

        // Deleting an old generation never touches the new one.
        assert!(!s.delete(k(7, 1)).unwrap());
        assert!(s.exists(k(7, 2)));

        // Staging: invisible until committed; dropped staging disappears.
        let st = s.begin_staging(k(9, 3)).unwrap();
        st.file().write_all_at(b"abc", 0).unwrap();
        assert!(s.scan().unwrap().iter().all(|o| o.key != k(9, 3)));
        s.commit_staging(st).unwrap();
        let dropped = s.begin_staging(k(10, 1)).unwrap();
        drop(dropped);
        assert_eq!(fs::read_dir(dir.path().join("staging")).unwrap().count(), 0);

        let scan = s.scan().unwrap();
        assert_eq!(
            scan,
            vec![
                ObjectInfo {
                    key: k(7, 2),
                    size: 5
                },
                ObjectInfo {
                    key: k(9, 3),
                    size: 3
                },
            ]
        );

        // Adoption renames without copying.
        let src = dir.path().join("hf-blob");
        fs::write(&src, b"model bytes").unwrap();
        assert_eq!(s.adopt(&src, k(11, 1)).unwrap(), 11);
        assert!(!src.exists());
        assert!(s.capacity().unwrap().total > 0);
    }

    #[test]
    fn leftover_staging_is_discarded_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let s = ObjectStore::open(dir.path()).unwrap();
        let st = s.begin_staging(k(1, 1)).unwrap();
        std::mem::forget(st);
        drop(s);
        ObjectStore::open(dir.path()).unwrap();
        assert_eq!(fs::read_dir(dir.path().join("staging")).unwrap().count(), 0);
    }

    use std::os::unix::fs::FileExt;
}
