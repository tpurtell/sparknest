//! Milestone 5: placement, import, seal policies, rules.

use nest_data::vfs::oflags;
use nest_meta::SealPolicy;
use nest_place::import::{ImportOptions, SealMode};
use nest_place::{RuleSpec, Selector};
use nest_testkit::TestCluster;
use nest_types::*;
use std::path::Path;
use std::time::Duration;

async fn ready(n: u64) -> TestCluster {
    let c = TestCluster::start(n).await;
    c.eventually("serving", Duration::from_secs(8), |c| {
        (1..=n).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    // Nodes register their names in the background.
    c.eventually("names registered", Duration::from_secs(8), |c| {
        c.node(1)
            .placer
            .nodes()
            .map(|n| n.iter().all(|h| !h.name.starts_with("node")))
            .unwrap_or(false)
    })
    .await;
    c
}

/// A small Hugging Face cache: classic layout plus one shared-store blob.
fn make_hf_cache(root: &Path) {
    let repo = root.join("models--org--tiny");
    std::fs::create_dir_all(repo.join("blobs")).unwrap();
    std::fs::create_dir_all(repo.join("refs")).unwrap();
    std::fs::create_dir_all(repo.join("snapshots/aaaa")).unwrap();
    std::fs::create_dir_all(repo.join("snapshots/bbbb")).unwrap();
    std::fs::create_dir_all(root.join("blobs/0f")).unwrap();
    std::fs::write(root.join("blobs/.huggingface-shared-blobs"), b"1").unwrap();
    std::fs::write(repo.join("blobs/cfg1"), b"{\"v\":1}").unwrap();
    std::fs::write(repo.join("blobs/cfg2"), b"{\"v\":2}").unwrap();
    std::fs::write(root.join("blobs/0f/0fweights"), vec![7u8; 3 << 20]).unwrap();
    std::os::unix::fs::symlink("../../blobs/0f/0fweights", repo.join("blobs/xetetag")).unwrap();
    std::fs::write(repo.join("refs/main"), b"bbbb").unwrap();
    std::os::unix::fs::symlink("../../blobs/cfg1", repo.join("snapshots/aaaa/config.json"))
        .unwrap();
    std::os::unix::fs::symlink("../../blobs/cfg2", repo.join("snapshots/bbbb/config.json"))
        .unwrap();
    std::os::unix::fs::symlink(
        "../../blobs/xetetag",
        repo.join("snapshots/bbbb/model.safetensors"),
    )
    .unwrap();
    std::fs::write(repo.join("blobs/leftover.incomplete"), b"partial").unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn import_seal_replicate_evict_rules() {
    let c = ready(3).await;
    let n1 = c.node(1);
    // Source must share a filesystem with node 1's store.
    let src = c.state_dir(1).join("import-src");
    make_hf_cache(&src);

    // Prepare /hub for HF and import with --move.
    n1.vfs.mkdir(FileId::ROOT, b"hub", 0o755).await.unwrap();
    let hub = c.lookup(1, FileId::ROOT, "hub").unwrap();
    n1.meta
        .propose(nest_meta::Command::SetSealPolicy {
            dir: hub,
            policy: SealPolicy::RenameFromIncomplete,
            now: Timestamp::now(),
        })
        .await
        .unwrap();
    let id = n1.placer.import(ImportOptions {
        src: src.clone(),
        dst: "/hub".into(),
        r#move: true,
        seal: SealMode::Auto,
    });
    c.eventually("import done", Duration::from_secs(10), |c| {
        c.node(1).placer.job(id).is_some_and(|j| j.finished)
    })
    .await;
    let p = n1.placer.import_progress(id).unwrap();
    assert!(p.errors.is_empty(), "{:?}", p.errors);
    assert_eq!((p.files, p.symlinks, p.skipped), (5, 4, 1), "{p:?}");
    assert!(
        !src.join("models--org--tiny/blobs/cfg1").exists(),
        "moved sources are removed"
    );
    assert!(
        src.join("models--org--tiny/blobs/leftover.incomplete")
            .exists(),
        "partial downloads are left alone"
    );
    c.converge().await;

    // Blobs are sealed, refs stay writable.
    let r = n1.meta.open_reader().unwrap();
    let (w, _) = nest_place::selector::resolve_path(&r, "/hub/blobs/0f/0fweights").unwrap();
    let (refs, _) =
        nest_place::selector::resolve_path(&r, "/hub/models--org--tiny/refs/main").unwrap();
    assert!(c.attr(1, w).unwrap().sealed);
    assert!(!c.attr(1, refs).unwrap().sealed);

    // Revision selection follows snapshot symlinks into the shared store.
    let sel = Selector::parse("hf:org/tiny@main", "/hub").unwrap();
    let m = n1.placer.manifest(&sel).await.unwrap();
    let mut paths: Vec<&str> = m.entries.iter().map(|e| e.path.as_str()).collect();
    paths.sort();
    assert_eq!(
        paths,
        vec![
            "/hub/blobs/0f/0fweights",
            "/hub/models--org--tiny/blobs/cfg2",
            "/hub/models--org--tiny/refs/main"
        ]
    );
    let (_, ready1) = n1.placer.readiness(&sel, &[]).await.unwrap();
    assert!(ready1.iter().find(|h| h.host == "n1").unwrap().ready);
    assert!(!ready1.iter().find(|h| h.host == "n2").unwrap().ready);

    // Replicate to n2 and n3; the manifest is then ready everywhere.
    let job = n1
        .placer
        .replicate(sel.clone(), vec!["n2".into(), "n3".into()], 4)
        .await
        .unwrap();
    c.eventually("replicated", Duration::from_secs(20), |c| {
        c.node(1).placer.job(job).is_some_and(|j| j.finished)
    })
    .await;
    let j = n1.placer.job(job).unwrap();
    assert!(
        j.error.is_none() && j.hosts.values().all(|h| h.failed.is_empty()),
        "{j:?}"
    );
    c.converge().await;
    let (_, all) = n1.placer.readiness(&sel, &[]).await.unwrap();
    assert!(all.iter().all(|h| h.ready), "{all:?}");
    // The copy on n3 is byte-identical and served locally there.
    let key = nest_store::ObjectKey::new(w, c.attr(3, w).unwrap().generation);
    assert_eq!(
        std::fs::read(c.node(3).data.store().path(key)).unwrap(),
        vec![7u8; 3 << 20]
    );

    // Evict from n1 and n2: n2 goes, and n1 too since n3 still holds it.
    let reports = n1
        .placer
        .evict(
            &Selector::parse("/hub/blobs/0f/0fweights", "/hub").unwrap(),
            &["n1".into(), "n2".into()],
        )
        .await
        .unwrap();
    assert_eq!(
        reports.iter().map(|r| r.removed).sum::<u64>(),
        2,
        "{reports:?}"
    );
    // The last copy is protected.
    let reports = n1
        .placer
        .evict(
            &Selector::parse("/hub/blobs/0f/0fweights", "/hub").unwrap(),
            &["n3".into()],
        )
        .await
        .unwrap();
    assert_eq!(reports[0].removed, 0);
    assert_eq!(reports[0].refused.len(), 1);

    // Rules are durable intent; reconcile brings copies back.
    n1.placer
        .set_rule(
            "tiny",
            &RuleSpec {
                selector: sel.clone(),
                hosts: vec!["@all".into()],
                auto: false,
            },
        )
        .await
        .unwrap();
    let jobs = n1.placer.reconcile(Some("tiny"), 4).await.unwrap();
    c.eventually("reconciled", Duration::from_secs(20), |c| {
        jobs.iter()
            .all(|id| c.node(1).placer.job(*id).is_some_and(|j| j.finished))
    })
    .await;
    c.converge().await;
    let (_, all) = n1.placer.readiness(&sel, &[]).await.unwrap();
    assert!(all.iter().all(|h| h.ready), "{all:?}");
    assert_eq!(n1.placer.rules().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn hf_style_download_is_sealed_at_rename() {
    let c = ready(2).await;
    let v = &c.node(2).vfs;
    let hub = v.mkdir(FileId::ROOT, b"hub", 0o755).await.unwrap().id;
    c.node(2)
        .meta
        .propose(nest_meta::Command::SetSealPolicy {
            dir: hub,
            policy: SealPolicy::RenameFromIncomplete,
            now: Timestamp::now(),
        })
        .await
        .unwrap();
    let blobs = v.mkdir(hub, b"blobs", 0o755).await.unwrap().id;
    // huggingface_hub: write <etag>.incomplete, close, os.replace -> <etag>.
    let (a, fh, _) = v
        .create(
            blobs,
            b"etag.incomplete",
            0o644,
            oflags::WRONLY | oflags::EXCL,
        )
        .await
        .unwrap();
    v.write(fh, 0, vec![1u8; 1 << 20]).await.unwrap();
    v.release(fh, None).await;
    v.rename(
        blobs,
        b"etag.incomplete",
        blobs,
        b"etag",
        Default::default(),
    )
    .await
    .unwrap();
    c.eventually("sealed after finalize", Duration::from_secs(3), |c| {
        c.attr(1, a.id)
            .is_some_and(|x| x.sealed && x.gen_state == GenState::Stable)
    })
    .await;
    assert!(matches!(
        v.open(a.id, oflags::RDWR).await,
        Err(NestError::NotPermitted(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn automatic_rules_follow_new_content() {
    let c = ready(3).await;
    let v2 = &c.node(2).vfs;
    let data = v2.mkdir(FileId::ROOT, b"data", 0o755).await.unwrap().id;
    c.node(1)
        .placer
        .set_rule(
            "everything-in-data",
            &RuleSpec {
                selector: Selector::parse("/data", "/hub").unwrap(),
                hosts: vec!["@all".into()],
                auto: true,
            },
        )
        .await
        .unwrap();
    let (a, fh, _) = v2
        .create(data, b"weights.bin", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v2.write(fh, 0, vec![3u8; 2 << 20]).await.unwrap();
    v2.release(fh, None).await;
    // After the quiet period the leader applies the rule on its own.
    c.eventually("copies on every node", Duration::from_secs(20), |c| {
        let r = c.node(1).meta.open_reader().unwrap();
        nest_meta::query::replicas(&r, a.id)
            .map(|r| r.len() == 3)
            .unwrap_or(false)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn remove_and_readd_a_member() {
    use nest_place::admin::MembershipChange;
    let c = ready(3).await;
    let p = &c.node(1).placer;
    let voters = |c: &TestCluster| -> usize {
        c.node(1).placer.membership().unwrap()["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|n| n["voter"] == true)
            .count()
    };
    assert_eq!(voters(&c), 3);
    p.change_membership(MembershipChange::Remove { node: NodeId(3) })
        .await
        .unwrap();
    c.eventually("two voters", Duration::from_secs(5), |c| voters(c) == 2)
        .await;
    // The cluster keeps working without it.
    c.node(2)
        .vfs
        .mkdir(FileId::ROOT, b"after-remove", 0o755)
        .await
        .unwrap();
    let addr = c.node(3).rpc.local_addr().to_string();
    p.change_membership(MembershipChange::Add {
        node: NodeId(3),
        addr,
        voter: true,
    })
    .await
    .unwrap();
    c.eventually("three voters", Duration::from_secs(5), |c| voters(c) == 3)
        .await;
    c.converge().await;
    assert!(c.lookup(3, FileId::ROOT, "after-remove").is_some());
}

/// Migration: a second host's cache holds blobs the namespace already has.
/// Its files become that host's copies (hard links, nothing copied); a blob
/// whose size disagrees is refused; new blobs import normally; `.refs`
/// hints and the shared-store marker stay writable.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn import_on_another_host_adopts_blobs_the_namespace_has() {
    let c = ready(3).await;
    let n1 = c.node(1);
    n1.vfs.mkdir(FileId::ROOT, b"hub", 0o755).await.unwrap();
    let hub = c.lookup(1, FileId::ROOT, "hub").unwrap();
    n1.meta
        .propose(nest_meta::Command::SetSealPolicy {
            dir: hub,
            policy: SealPolicy::RenameFromIncomplete,
            now: Timestamp::now(),
        })
        .await
        .unwrap();

    let src1 = c.state_dir(1).join("cache1");
    make_hf_cache(&src1);
    std::fs::write(src1.join("blobs/0f/0fweights.refs"), b"models--org--tiny\n").unwrap();
    let p = run_import(&c, 1, src1, true).await;
    assert!(p.errors.is_empty() && p.adopted == 0, "{p:?}");

    let src2 = c.state_dir(2).join("cache2");
    make_hf_cache(&src2);
    std::fs::write(src2.join("blobs/0f/0fweights.refs"), b"models--org--tiny\n").unwrap();
    let repo2 = src2.join("models--org--tiny");
    std::fs::write(repo2.join("blobs/cfg2"), b"{\"v\"").unwrap(); // truncated
    std::fs::write(repo2.join("blobs/cfg3"), b"{\"v\":3}").unwrap(); // new
    let p = run_import(&c, 2, src2.clone(), false).await;
    assert_eq!((p.adopted, p.files), (2, 1), "{p:?}");
    assert_eq!(p.adopted_bytes, (3 << 20) + 7, "{p:?}");
    assert!(
        p.errors.len() == 1 && p.errors[0].contains("cfg2"),
        "{:?}",
        p.errors
    );
    // Linked, not copied: the source now has a second name in n2's store.
    use std::os::unix::fs::MetadataExt;
    assert_eq!(
        std::fs::metadata(src2.join("blobs/0f/0fweights"))
            .unwrap()
            .nlink(),
        2
    );
    c.converge().await;

    let r = n1.meta.open_reader().unwrap();
    let id = |p: &str| nest_place::selector::resolve_path(&r, p).unwrap().0;
    let stores = |f: FileId| {
        let mut s: Vec<u64> = nest_meta::query::replicas(&r, f)
            .unwrap()
            .iter()
            .map(|x| x.store.0)
            .collect();
        s.sort();
        s
    };
    assert_eq!(stores(id("/hub/blobs/0f/0fweights")), vec![1, 2]);
    assert_eq!(stores(id("/hub/models--org--tiny/blobs/cfg1")), vec![1, 2]);
    assert_eq!(stores(id("/hub/models--org--tiny/blobs/cfg2")), vec![1]);
    assert_eq!(stores(id("/hub/models--org--tiny/blobs/cfg3")), vec![2]);
    // n2 reads its adopted copy locally with the right bytes.
    let w = id("/hub/blobs/0f/0fweights");
    let (fh, _) = c.node(2).vfs.open(w, 0).await.unwrap();
    let data = c.node(2).vfs.read(fh, 0, 4096).await.unwrap();
    assert!(data.iter().all(|b| *b == 7));
    c.node(2).vfs.release(fh, None).await;

    let sealed = |p: &str| c.attr(1, id(p)).unwrap().sealed;
    assert!(sealed("/hub/blobs/0f/0fweights"));
    assert!(!sealed("/hub/blobs/0f/0fweights.refs"));
    assert!(!sealed("/hub/blobs/.huggingface-shared-blobs"));
}

async fn run_import(
    c: &TestCluster,
    node: u64,
    src: std::path::PathBuf,
    mv: bool,
) -> nest_place::import::ImportProgress {
    let p = &c.node(node).placer;
    let id = p.import(ImportOptions {
        src,
        dst: "/hub".into(),
        r#move: mv,
        seal: SealMode::Auto,
    });
    c.eventually("import done", Duration::from_secs(10), |c| {
        c.node(node).placer.job(id).is_some_and(|j| j.finished)
    })
    .await;
    p.import_progress(id).unwrap()
}
