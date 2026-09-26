//! The filesystem through a real kernel FUSE mount on this machine.

use nest_testkit::TestCluster;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.unwrap()
}

async fn cluster_with_mount(n: u64) -> Option<(TestCluster, PathBuf)> {
    let c = TestCluster::start(n).await;
    c.eventually("caught up", std::time::Duration::from_secs(5), |c| {
        c.node(1).data.caught_up()
    })
    .await;
    let mp = c.mount(1)?;
    Some((c, mp))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn posix_basics_through_the_kernel() {
    let Some((c, mp)) = cluster_with_mount(3).await else {
        return;
    };
    blocking(move || {
        let d = mp.join("hub");
        fs::create_dir(&d).unwrap();
        assert!(fs::create_dir(&d).is_err());

        // Write, read back, stat.
        fs::write(d.join("config.json"), b"{\"a\": 1}").unwrap();
        assert_eq!(fs::read(d.join("config.json")).unwrap(), b"{\"a\": 1}");
        assert_eq!(fs::metadata(d.join("config.json")).unwrap().len(), 8);

        // Append, then overwrite in the middle.
        let mut f = OpenOptions::new()
            .append(true)
            .open(d.join("config.json"))
            .unwrap();
        f.write_all(b"\n").unwrap();
        drop(f);
        let f = OpenOptions::new()
            .write(true)
            .open(d.join("config.json"))
            .unwrap();
        f.write_all_at(b"b", 2).unwrap();
        drop(f);
        assert_eq!(fs::read(d.join("config.json")).unwrap(), b"{\"b\": 1}\n");

        // Truncate via set_len and via O_TRUNC.
        let f = OpenOptions::new()
            .write(true)
            .open(d.join("config.json"))
            .unwrap();
        f.set_len(3).unwrap();
        drop(f);
        assert_eq!(fs::read(d.join("config.json")).unwrap(), b"{\"b");
        fs::write(d.join("config.json"), b"new").unwrap();
        assert_eq!(fs::read(d.join("config.json")).unwrap(), b"new");

        // O_EXCL.
        assert!(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(d.join("config.json"))
                .is_err()
        );

        // Rename over an existing file, hard links, symlinks.
        fs::write(d.join("a.incomplete"), b"blob").unwrap();
        fs::write(d.join("a"), b"old").unwrap();
        fs::rename(d.join("a.incomplete"), d.join("a")).unwrap();
        assert_eq!(fs::read(d.join("a")).unwrap(), b"blob");
        fs::hard_link(d.join("a"), d.join("a2")).unwrap();
        assert_eq!(fs::metadata(d.join("a")).unwrap().nlink(), 2);
        assert_eq!(
            fs::metadata(d.join("a")).unwrap().ino(),
            fs::metadata(d.join("a2")).unwrap().ino()
        );
        std::os::unix::fs::symlink("../hub/a", d.join("link")).unwrap();
        assert_eq!(
            fs::read_link(d.join("link")).unwrap(),
            PathBuf::from("../hub/a")
        );
        assert_eq!(fs::read(d.join("link")).unwrap(), b"blob");

        // Directory listing.
        let mut names: Vec<String> = fs::read_dir(&d)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, vec!["a", "a2", "config.json", "link"]);

        // Permissions.
        fs::set_permissions(d.join("a"), fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            fs::metadata(d.join("a")).unwrap().permissions().mode() & 0o777,
            0o600
        );

        // Unlink while open: the open descriptor keeps working.
        let mut f = File::open(d.join("a2")).unwrap();
        fs::remove_file(d.join("a")).unwrap();
        fs::remove_file(d.join("a2")).unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        assert_eq!(s, "blob");
        drop(f);

        // Directory removal.
        assert!(fs::remove_dir(&d).is_err());
        fs::remove_file(d.join("config.json")).unwrap();
        fs::remove_file(d.join("link")).unwrap();
        fs::remove_dir(&d).unwrap();
        assert!(fs::read_dir(&mp).unwrap().next().is_none());
    })
    .await;
    c.node(1).unmount();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn large_file_roundtrip_and_sparse_seek() {
    let Some((c, mp)) = cluster_with_mount(1).await else {
        return;
    };
    blocking(move || {
        let p = mp.join("shard.safetensors");
        let chunk: Vec<u8> = (0..(1 << 20)).map(|i| (i * 7 % 251) as u8).collect();
        let mut f = File::create(&p).unwrap();
        for _ in 0..64 {
            f.write_all(&chunk).unwrap();
        }
        f.sync_all().unwrap();
        drop(f);
        assert_eq!(fs::metadata(&p).unwrap().len(), 64 << 20);
        let mut f = File::open(&p).unwrap();
        let mut buf = vec![0u8; 1 << 20];
        for _ in 0..64 {
            f.read_exact(&mut buf).unwrap();
            assert!(buf == chunk);
        }
        // Positional reads across chunk boundaries.
        f.seek(SeekFrom::Start((5 << 20) - 3)).unwrap();
        let mut small = [0u8; 6];
        f.read_exact(&mut small).unwrap();
        let base = (5usize << 20) - 3;
        let want: Vec<u8> = (0..6)
            .map(|i| (((base + i) % (1 << 20)) * 7 % 251) as u8)
            .collect();
        assert_eq!(&small[..], &want[..]);
        // Writing past EOF leaves a hole that reads as zeros.
        let g = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(mp.join("sparse"))
            .unwrap();
        g.write_all_at(b"end", 10 << 20).unwrap();
        drop(g);
        let data = fs::read(mp.join("sparse")).unwrap();
        assert_eq!(data.len(), (10 << 20) + 3);
        assert!(data[..10 << 20].iter().all(|b| *b == 0));
    })
    .await;
    c.node(1).unmount();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn flock_is_exclusive_across_descriptors() {
    let Some((c, mp)) = cluster_with_mount(3).await else {
        return;
    };
    c.eventually("session", std::time::Duration::from_secs(5), |c| {
        c.node(1).data.session().is_some()
    })
    .await;
    blocking(move || {
        let p = mp.join("model.lock");
        let a = File::create(&p).unwrap();
        let b = File::open(&p).unwrap();
        unsafe {
            assert_eq!(libc::flock(a.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB), 0);
            assert_eq!(
                libc::flock(b.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB),
                -1
            );
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::EWOULDBLOCK)
            );
        }
        // Closing releases the flock, but FUSE delivers that unlock with
        // the asynchronous RELEASE request, after close() has returned. Poll
        // the way huggingface's filelock does.
        drop(a);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if unsafe { libc::flock(b.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "flock never released");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        unsafe {
            assert_eq!(libc::flock(b.as_raw_fd(), libc::LOCK_UN), 0);
        }
    })
    .await;
    c.node(1).unmount();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sealed_files_are_immutable_and_mmap_readable() {
    let Some((c, mp)) = cluster_with_mount(1).await else {
        return;
    };
    let p = mp.join("blob");
    let p2 = p.clone();
    blocking(move || fs::write(&p2, vec![42u8; 3 << 20]).unwrap()).await;
    let id = c.lookup(1, nest_types::FileId::ROOT, "blob").unwrap();
    c.eventually("finalized", std::time::Duration::from_secs(3), |c| {
        c.attr(1, id).unwrap().gen_state == nest_types::GenState::Stable
    })
    .await;
    c.node(1).vfs.seal(id, true).await.unwrap();
    blocking(move || {
        // Writes are refused; reads (including mmap) see the content.
        let e = OpenOptions::new().write(true).open(&p).unwrap_err();
        assert_eq!(e.raw_os_error(), Some(libc::EPERM));
        let f = File::open(&p).unwrap();
        let len = 3 << 20;
        unsafe {
            let m = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                f.as_raw_fd(),
                0,
            );
            assert_ne!(m, libc::MAP_FAILED);
            let s = std::slice::from_raw_parts(m as *const u8, len);
            assert!(s.iter().all(|b| *b == 42));
            libc::munmap(m, len);
        }
        // Rename and unlink still work on sealed files.
        fs::rename(&p, p.with_file_name("blob2")).unwrap();
        fs::remove_file(p.with_file_name("blob2")).unwrap();
    })
    .await;
    c.node(1).unmount();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn explicit_times_survive_ownership_and_finalize() {
    let Some((c, mp)) = cluster_with_mount(1).await else {
        return;
    };
    let when = std::time::UNIX_EPOCH + std::time::Duration::new(1_577_808_000, 123_456_789);
    let p = mp.join("stamped");
    let p2 = p.clone();
    blocking(move || {
        // Freshly created (still owned by this node) and then stamped, as
        // `touch -d`, tar and rsync -t do.
        let f = File::create(&p2).unwrap();
        f.set_times(std::fs::FileTimes::new().set_modified(when))
            .unwrap();
        drop(f);
        assert_eq!(fs::metadata(&p2).unwrap().modified().unwrap(), when);
    })
    .await;
    let id = c.lookup(1, nest_types::FileId::ROOT, "stamped").unwrap();
    c.eventually("finalized", std::time::Duration::from_secs(3), |c| {
        c.attr(1, id).unwrap().gen_state == nest_types::GenState::Stable
    })
    .await;
    blocking(move || assert_eq!(fs::metadata(&p).unwrap().modified().unwrap(), when)).await;
    c.node(1).unmount();
}

/// hf_xet's download pattern: several threads pwrite disjoint ranges of one
/// `.incomplete` file in arbitrary order, then it is renamed into place.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_random_offset_writers() {
    let Some((c, mp)) = cluster_with_mount(1).await else {
        return;
    };
    blocking(move || {
        let tmp = mp.join("blob.incomplete");
        let blocks = 96usize;
        let bs = 1usize << 20;
        let f = std::sync::Arc::new(File::create(&tmp).unwrap());
        // A fixed shuffle of block order.
        let mut order: Vec<usize> = (0..blocks).collect();
        let mut x = 0x2545F4914F6CDD1Du64;
        for i in (1..blocks).rev() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            order.swap(i, (x % (i as u64 + 1)) as usize);
        }
        let order = std::sync::Arc::new(order);
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let (f, order) = (f.clone(), order.clone());
                std::thread::spawn(move || {
                    for &b in order.iter().skip(t).step_by(8) {
                        let data = vec![(b % 251) as u8; bs];
                        f.write_all_at(&data, (b * bs) as u64).unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        drop(f);
        fs::rename(&tmp, mp.join("blob")).unwrap();
        let data = fs::read(mp.join("blob")).unwrap();
        assert_eq!(data.len(), blocks * bs);
        for b in 0..blocks {
            assert!(
                data[b * bs..(b + 1) * bs]
                    .iter()
                    .all(|v| *v == (b % 251) as u8),
                "block {b}"
            );
        }
    })
    .await;
    c.node(1).unmount();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn seal_through_an_extended_attribute() {
    let Some((c, mp)) = cluster_with_mount(1).await else {
        return;
    };
    let p = mp.join("weights.safetensors");
    let p2 = p.clone();
    blocking(move || fs::write(&p2, b"tensor").unwrap()).await;
    let id = c
        .lookup(1, nest_types::FileId::ROOT, "weights.safetensors")
        .unwrap();
    c.eventually("stable", std::time::Duration::from_secs(3), |c| {
        c.attr(1, id).unwrap().gen_state == nest_types::GenState::Stable
    })
    .await;
    blocking(move || {
        use std::ffi::CString;
        let path = CString::new(p.as_os_str().as_encoded_bytes()).unwrap();
        let name = CString::new("user.sparknest.sealed").unwrap();
        let get = || {
            let mut buf = [0u8; 8];
            let n = unsafe {
                libc::getxattr(
                    path.as_ptr(),
                    name.as_ptr(),
                    buf.as_mut_ptr() as *mut _,
                    buf.len(),
                )
            };
            assert!(n > 0, "getxattr: {}", std::io::Error::last_os_error());
            buf[..n as usize].to_vec()
        };
        assert_eq!(get(), b"0");
        let r = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                b"1".as_ptr() as *const _,
                1,
                0,
            )
        };
        assert_eq!(r, 0, "setxattr: {}", std::io::Error::last_os_error());
        assert_eq!(get(), b"1");
        assert_eq!(
            OpenOptions::new()
                .write(true)
                .open(&p)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EPERM)
        );
        assert_eq!(fs::read(&p).unwrap(), b"tensor");
    })
    .await;
    c.node(1).unmount();
}
