//! Hub refs are read once and then only when a host changes them: the web
//! UI's model views read every repo's refs, and each read through the mount
//! is a round trip to whichever host holds that small file.

use nest_data::vfs::oflags;
use nest_testkit::TestCluster;
use nest_types::*;
use std::time::Duration;

async fn mkdirs(c: &TestCluster, path: &str) -> FileId {
    let v = &c.node(1).vfs;
    let mut at = FileId::ROOT;
    for part in path.split('/').filter(|p| !p.is_empty()) {
        at = match c.lookup(1, at, part) {
            Some(id) => id,
            None => v.mkdir(at, part.as_bytes(), 0o755).await.unwrap().id,
        };
    }
    at
}

/// Write `body` as `dir/name` from node `on`, replacing what is there.
async fn put(c: &TestCluster, on: u64, dir: FileId, name: &str, body: &[u8]) {
    let v = &c.node(on).vfs;
    let fh = match v.lookup(dir, name.as_bytes()).await {
        Ok(a) => {
            v.open(a.id, oflags::WRONLY | oflags::TRUNC)
                .await
                .unwrap()
                .0
        }
        Err(_) => {
            v.create(dir, name.as_bytes(), 0o644, oflags::WRONLY | oflags::EXCL)
                .await
                .unwrap()
                .1
        }
    };
    v.write(fh, 0, body.to_vec()).await.unwrap();
    v.release(fh, None).await;
}

/// Wait until no file is owned by a writer: a ref still lingering after
/// its write is read on every call, as it may still change.
async fn settled(c: &TestCluster) {
    c.converge().await;
    c.eventually("writes finalized", Duration::from_secs(8), |c| {
        (1..=2).all(|i| {
            nest_meta::query::owned_by(
                &c.node(1).vfs.data().meta().open_reader().unwrap(),
                NodeId(i),
            )
            .unwrap()
            .is_empty()
        })
    })
    .await;
}

fn api(c: &TestCluster, id: u64) -> nest_api::Api {
    let n = c.node(id);
    nest_api::Api {
        vfs: n.vfs.clone(),
        push: None,
        placer: n.placer.clone(),
        mountpoint: None,
        hub: "/hub".into(),
        web_token: String::new(),
        web_addr: None,
        host: format!("node{id}"),
        events: Default::default(),
        refs_cache: Default::default(),
    }
}

fn named(
    refs: &std::collections::HashMap<String, std::collections::HashMap<String, Vec<String>>>,
    repo: &str,
    commit: &str,
) -> Vec<String> {
    refs.get(repo)
        .and_then(|m| m.get(commit))
        .cloned()
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn hub_refs_are_read_again_only_when_changed() {
    let c = TestCluster::start(2).await;
    c.eventually("serving", Duration::from_secs(8), |c| {
        (1..=2).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    let a = mkdirs(&c, "/hub/models--org--a/refs").await;
    let a_pr = mkdirs(&c, "/hub/models--org--a/refs/pr").await;
    let b = mkdirs(&c, "/hub/models--org--b/refs").await;
    put(&c, 1, a, "main", b"c1\n").await;
    put(&c, 1, a, "int4", b"c2").await;
    put(&c, 2, a_pr, "1", b"c2").await;
    put(&c, 2, b, "main", b"d1").await;
    settled(&c).await;

    let api = api(&c, 1);
    let refs = nest_api::hub_refs(&api).await;
    assert_eq!(named(&refs, "models--org--a", "c1"), ["main"]);
    assert_eq!(named(&refs, "models--org--a", "c2"), ["int4", "pr/1"]);
    assert_eq!(named(&refs, "models--org--b", "d1"), ["main"]);
    assert_eq!(api.refs_cache.reads(), 4);

    // Nothing changed: nothing is read.
    assert_eq!(nest_api::hub_refs(&api).await, refs);
    assert_eq!(api.refs_cache.reads(), 4);

    // Another host moves a branch: that ref alone is read again.
    put(&c, 2, a, "main", b"c3").await;
    settled(&c).await;
    let refs = nest_api::hub_refs(&api).await;
    assert_eq!(named(&refs, "models--org--a", "c3"), ["main"]);
    assert!(named(&refs, "models--org--a", "c1").is_empty());
    assert_eq!(api.refs_cache.reads(), 5);

    // A ref removed is gone, and nothing else is read.
    c.node(2).vfs.unlink(b, b"main").await.unwrap();
    settled(&c).await;
    let refs = nest_api::hub_refs(&api).await;
    assert!(!refs.contains_key("models--org--b"));
    assert_eq!(api.refs_cache.reads(), 5);
}
