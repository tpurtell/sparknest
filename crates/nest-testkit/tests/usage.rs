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
