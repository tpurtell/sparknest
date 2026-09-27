//! Per-host usage statistics and the plans they drive (ADR-028).

use nest_data::vfs::oflags;
use nest_place::Selector;
use nest_place::plan::{Goal, Step};
use nest_testkit::TestCluster;
use nest_types::*;
use std::time::Duration;

const MIB: u64 = 1 << 20;

async fn ready(n: u64) -> TestCluster {
    let c = TestCluster::start(n).await;
    c.eventually("serving", Duration::from_secs(8), |c| {
        (1..=n).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    c.eventually("names", Duration::from_secs(8), |c| {
        c.node(1)
            .placer
            .nodes()
            .map(|n| n.iter().all(|h| !h.name.starts_with("node")))
            .unwrap_or(false)
    })
    .await;
    c
}

async fn write(c: &TestCluster, name: &str, size: u64) -> FileId {
    let v = c.node(1).vfs.clone();
    let (a, fh, _) = v
        .create(FileId::ROOT, name.as_bytes(), 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v.write(fh, 0, vec![7u8; size as usize]).await.unwrap();
    v.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(1, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    a.id
}

async fn read_all(c: &TestCluster, node: u64, f: FileId, size: u64) {
    let v = c.node(node).vfs.clone();
    let (fh, _) = v.open(f, 0).await.unwrap();
    let mut off = 0;
    while off < size {
        let b = v.read(fh, off, MIB as u32).await.unwrap();
        assert!(!b.is_empty());
        off += b.len() as u64;
    }
    v.release(fh, None).await;
}

async fn wait_job(c: &TestCluster, id: u64) -> nest_place::placer::ClusterJob {
    c.eventually("job finished", Duration::from_secs(20), |c| {
        c.node(1).placer.job(id).is_some_and(|j| j.finished)
    })
    .await;
    c.node(1).placer.job(id).unwrap()
}

fn holders(c: &TestCluster, f: FileId) -> Vec<u64> {
    let r = c.node(1).meta.open_reader().unwrap();
    let mut v: Vec<u64> = nest_meta::query::replicas(&r, f)
        .unwrap()
        .iter()
        .map(|r| r.store.0)
        .collect();
    v.sort();
    v
}

fn evicted(plan: &nest_place::plan::Plan) -> Vec<(String, FileId)> {
    let mut v: Vec<_> = plan
        .steps
        .iter()
        .filter_map(|s| match s {
            Step::Evict { host, copies, .. } => Some(
                copies
                    .iter()
                    .map(|c| (host.clone(), c.file))
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .flatten()
        .collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn usage_is_counted_per_host_and_drives_speedup_and_tidy() {
    let c = ready(3).await;
    let p = c.node(1).placer.clone();
    let hot = write(&c, "hot", 8 * MIB).await;
    let cold = write(&c, "cold", 4 * MIB).await;
    for f in ["/hot", "/cold"] {
        let j = p
            .replicate(Selector::parse(f, "/hub").unwrap(), vec!["n2".into()], 2)
            .await
            .unwrap();
        assert!(wait_job(&c, j).await.error.is_none());
    }
    c.converge().await;

    // n3 has no copy: its reads come over the network. n1 reads locally.
    read_all(&c, 3, hot, 8 * MIB).await;
    read_all(&c, 3, hot, 8 * MIB).await;
    read_all(&c, 1, hot, 8 * MIB).await;
    let u = p.usage(0).await.unwrap();
    let on3 = &u[&NodeId(3)][&hot];
    assert_eq!(on3.opens, 2);
    assert!(
        on3.remote_bytes >= 16 * MIB && on3.local_bytes == 0,
        "{on3:?}"
    );
    let on1 = &u[&NodeId(1)][&hot];
    assert!(
        on1.local_bytes >= 8 * MIB && on1.remote_bytes == 0,
        "{on1:?}"
    );
    assert!(!u[&NodeId(2)].contains_key(&hot), "n2 never opened it");
    assert!(u.values().all(|m| !m.contains_key(&cold)));

    // Speed up: only n3 earns a copy, of the file it read.
    let plan = p
        .plan(Goal::Speedup {
            days: 7,
            hosts: vec![],
            min_remote_bytes: MIB,
            keep_free: Some(0),
        })
        .await
        .unwrap();
    assert_eq!(plan.steps.len(), 1, "{plan:?}");
    let Step::Replicate { host, copies, .. } = &plan.steps[0] else {
        panic!("{plan:?}")
    };
    assert_eq!(host, "n3");
    assert_eq!(copies.iter().map(|c| c.file).collect::<Vec<_>>(), [hot]);
    assert!(
        copies[0].why.contains("over the network"),
        "{:?}",
        copies[0].why
    );
    let job = wait_job(&c, p.apply_plan(plan.id).await.unwrap()).await;
    assert!(job.error.is_none(), "{job:?}");
    c.converge().await;
    assert_eq!(holders(&c, hot), [1, 2, 3]);

    // Tidy: nothing written recently counts as idle...
    let week = Goal::Tidy {
        days: 7,
        hosts: vec![],
        archives: vec![],
    };
    let plan = p.plan(week.clone()).await.unwrap();
    assert!(plan.steps.is_empty(), "just written: {plan:?}");
    // ...but once last written ten days ago, copies not opened on their host
    // within the week go: n2's `hot` (n1 and n3 opened theirs) and one of the
    // two `cold` copies. The other `cold` copy is the last one and stays.
    let old = Timestamp(Timestamp::now().0 - 10 * 86_400 * 1_000_000_000);
    for f in [hot, cold] {
        c.node(1)
            .vfs
            .setattr(f, None, None, None, Some(old), None)
            .await
            .unwrap();
    }
    c.converge().await;
    let plan = p.plan(week).await.unwrap();
    let ev = evicted(&plan);
    assert_eq!(ev.len(), 2, "{plan:?}");
    assert!(ev.contains(&("n2".into(), hot)), "{ev:?}");
    assert_eq!(ev.iter().filter(|(_, f)| *f == cold).count(), 1, "{ev:?}");
    assert!(
        plan.notes.iter().any(|n| n.contains("only copies")),
        "{:?}",
        plan.notes
    );
    let job = wait_job(&c, p.apply_plan(plan.id).await.unwrap()).await;
    assert!(job.error.is_none(), "{job:?}");
    c.converge().await;
    assert_eq!(holders(&c, hot), [1, 3]);
    assert_eq!(holders(&c, cold).len(), 1);
}

/// The space tree credits shared blobs to the repo whose snapshot names
/// them, and weighs by copies when asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn space_tree_groups_by_model_and_weighs_copies() {
    use nest_place::space::{TreeReq, Weight, tree};
    let c = ready(2).await;
    let v = c.node(1).vfs.clone();
    let mk = |parent: FileId, name: &'static str| {
        let v = v.clone();
        async move { v.mkdir(parent, name.as_bytes(), 0o755).await.unwrap().id }
    };
    let file = |parent: FileId, name: &'static str, size: usize| {
        let v = v.clone();
        async move {
            let (a, fh, _) = v
                .create(parent, name.as_bytes(), 0o644, oflags::WRONLY)
                .await
                .unwrap();
            v.write(fh, 0, vec![1u8; size]).await.unwrap();
            v.release(fh, None).await;
            a.id
        }
    };
    // /hub/blobs/ab/abc (shared, 3 MiB), /hub/models--org--m/{blobs/cfg,
    // snapshots/r1/{model.bin -> shared, config.json -> ../../blobs/cfg}},
    // /data/x (1 MiB).
    let hub = mk(FileId::ROOT, "hub").await;
    let blobs = mk(hub, "blobs").await;
    let ab = mk(blobs, "ab").await;
    let shared = file(ab, "abc", 3 << 20).await;
    let repo = mk(hub, "models--org--m").await;
    let rb = mk(repo, "blobs").await;
    let cfg = file(rb, "cfg", 1000).await;
    let snaps = mk(repo, "snapshots").await;
    let r1 = mk(snaps, "r1").await;
    // hf 2.0: the snapshot names the repo's blob link, which points at the
    // shared store.
    v.symlink(rb, b"sha-abc", b"../../blobs/ab/abc")
        .await
        .unwrap();
    v.symlink(r1, b"model.bin", b"../../blobs/sha-abc")
        .await
        .unwrap();
    v.symlink(r1, b"config.json", b"../../blobs/cfg")
        .await
        .unwrap();
    let data = mk(FileId::ROOT, "data").await;
    let x = file(data, "x", 1 << 20).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        [shared, cfg, x].iter().all(|f| {
            c.attr(1, *f)
                .is_some_and(|a| a.gen_state == GenState::Stable)
        })
    })
    .await;
    let p = c.node(1).placer.clone();
    let j = p
        .replicate(
            Selector::parse("/hub", "/hub").unwrap(),
            vec!["n2".into()],
            2,
        )
        .await
        .unwrap();
    assert!(wait_job(&c, j).await.error.is_none());
    c.converge().await;

    let req = |weight, models, store| TreeReq {
        store,
        weight,
        models,
        root: "/".into(),
        depth: 8,
        max_children: 50,
        hub: "/hub".into(),
        store_names: [
            (StoreId(1), "n1".to_string()),
            (StoreId(2), "n2".to_string()),
        ]
        .into(),
    };
    let conn = c.node(1).meta.open_reader().unwrap();
    let t = tree(&conn, &req(Weight::Logical, true, None)).unwrap();
    let repo = t.children.iter().find(|n| n.kind == "repo").expect("repo");
    assert_eq!(repo.name, "org/m");
    assert_eq!(repo.selector.as_deref(), Some("hf:org/m"));
    assert_eq!(repo.bytes, (3 << 20) + 1000);
    let names: Vec<&str> = repo.children.iter().map(|n| n.name.as_str()).collect();
    assert_eq!(names, ["model.bin", "config.json"]);
    assert_eq!(repo.children[0].hosts, ["n1", "n2"]);
    let other = t.children.iter().find(|n| n.name == "other files").unwrap();
    assert_eq!(other.bytes, 1 << 20, "only /data/x is left for other");
    assert_eq!(t.bytes, (4 << 20) + 1000);

    // Copies: the hub is on both hosts, /data/x only on n1.
    let t = tree(&conn, &req(Weight::Copies, true, None)).unwrap();
    assert_eq!(t.bytes, 2 * ((3 << 20) + 1000) + (1 << 20));
    // One host's view: n2 holds only the hub.
    let t = tree(&conn, &req(Weight::Logical, false, Some(StoreId(2)))).unwrap();
    assert_eq!(t.bytes, (3 << 20) + 1000);
    assert_eq!(t.children.len(), 1);
    assert_eq!(t.children[0].name, "hub");
}
