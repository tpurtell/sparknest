//! The folder holding a host's copies (ADR-094): `node.data_dir`, or the
//! state dir when unset. It carries a marker naming its host, so a host
//! whose objects live apart never starts on the wrong folder (another
//! host's, or an unmounted disk's empty mountpoint, where every copy the
//! metadata lists would look lost).

use crate::config::Config;
use anyhow::{Context, bail};
use nest_data::HOST_MARKER;
use std::path::Path;

/// What the marker of host `cfg` says.
pub fn identity(cfg: &Config) -> String {
    format!("cluster={} host={}", cfg.cluster.name, cfg.node.id.0)
}

fn read(dir: &Path) -> std::io::Result<Option<String>> {
    match std::fs::read_to_string(dir.join(HOST_MARKER)) {
        Ok(s) => Ok(Some(s.trim().to_string())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// May this host open its data dir? `first_start`: it has no metadata yet.
pub fn check(cfg: &Config, first_start: bool) -> anyhow::Result<()> {
    let dir = cfg.node.data_dir();
    let want = identity(cfg);
    let found =
        read(dir).with_context(|| format!("reading {}", dir.join(HOST_MARKER).display()))?;
    match found {
        Some(m) if m == want => Ok(()),
        Some(m) => bail!(
            "{} holds another host's objects ({m}; this host is {want}): check node.data_dir",
            dir.display()
        ),
        // Objects in the state dir: the folder is this host's by being
        // there (hosts before 0.9.4 have no marker; it is written now).
        None if !cfg.node.split() => Ok(()),
        // A new host's own folder (or one brought to it, which ADR-090's
        // hold covers): its first start marks it.
        None if first_start => Ok(()),
        None => bail!(
            "node.data_dir {} has no {HOST_MARKER} marker: is its disk mounted? A host with \
             metadata starts only on the folder that holds its copies. If this is that folder \
             (moved by hand), write the marker: echo '{want}' > {}",
            dir.display(),
            dir.join(HOST_MARKER).display()
        ),
    }
}

/// Write the marker if it is missing (after [`check`] passed and the store
/// opened, so the folder exists).
pub fn mark(cfg: &Config) -> anyhow::Result<()> {
    let dir = cfg.node.data_dir();
    if read(dir)?.is_some() {
        return Ok(());
    }
    let p = dir.join(HOST_MARKER);
    let tmp = dir.join(format!("{HOST_MARKER}.tmp"));
    std::fs::write(&tmp, format!("{}\n", identity(cfg)))
        .and_then(|()| std::fs::File::open(&tmp)?.sync_all())
        .and_then(|()| std::fs::rename(&tmp, &p))
        .and_then(|()| std::fs::File::open(dir)?.sync_all())
        .with_context(|| format!("writing {}", p.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(state: &Path, data: Option<&Path>, id: u64) -> Config {
        let text = format!(
            r#"
            [node]
            id = {id}
            name = "a"
            state_dir = "{}"
            {}
            listen = "127.0.0.1:7400"
            [cluster]
            name = "c"
            secret_file = "/tmp/s"
            members = [{{ id = {id}, name = "a", addr = "127.0.0.1:7400" }}]
            "#,
            state.display(),
            data.map(|d| format!("data_dir = \"{}\"", d.display()))
                .unwrap_or_default()
        );
        let c: Config = toml::from_str(&text).unwrap();
        c.validate().unwrap();
        c
    }

    #[test]
    fn unsplit_hosts_start_as_before_and_get_marked() {
        let t = tempfile::tempdir().unwrap();
        let c = cfg(t.path(), None, 1);
        assert_eq!(c.node.data_dir(), t.path());
        assert!(!c.node.split());
        // With metadata and no marker (a host from before 0.9.4).
        check(&c, false).unwrap();
        mark(&c).unwrap();
        assert_eq!(read(t.path()).unwrap().unwrap(), "cluster=c host=1");
        check(&c, false).unwrap();
        // Another host's state dir copied here is refused.
        assert!(check(&cfg(t.path(), None, 2), false).is_err());
    }

    #[test]
    fn a_split_host_starts_only_on_its_own_folder() {
        let t = tempfile::tempdir().unwrap();
        let (state, data) = (t.path().join("state"), t.path().join("data"));
        std::fs::create_dir_all(&state).unwrap();
        let c = cfg(&state, Some(&data), 1);
        assert!(c.node.split());
        // An existing host whose folder lacks the marker: unmounted, or
        // not its folder. Refused, and nothing is created there.
        let e = check(&c, false).unwrap_err().to_string();
        assert!(e.contains("is its disk mounted"), "{e}");
        assert!(!data.exists());
        // A new host marks its folder on its first start.
        check(&c, true).unwrap();
        std::fs::create_dir_all(&data).unwrap();
        mark(&c).unwrap();
        check(&c, false).unwrap();
        // The marker stays with the objects: another host pointed here is
        // refused, first start or not.
        let other = cfg(&t.path().join("state2"), Some(&data), 2);
        assert!(check(&other, true).is_err());
        assert!(check(&other, false).is_err());
    }

    #[test]
    fn data_dir_equal_to_state_dir_is_not_a_split() {
        let t = tempfile::tempdir().unwrap();
        let c = cfg(t.path(), Some(t.path()), 1);
        assert!(!c.node.split());
        check(&c, false).unwrap();
    }

    #[test]
    fn disk_transfers_parses_and_must_be_positive() {
        let t = tempfile::tempdir().unwrap();
        let mut c = cfg(t.path(), None, 1);
        assert_eq!(c.node.disk_transfers, None);
        c.node.disk_transfers = Some(2);
        c.validate().unwrap();
        let text = toml::to_string(&c).unwrap();
        assert!(text.contains("disk_transfers = 2") && !text.contains("data_dir"));
        c.node.disk_transfers = Some(0);
        assert!(c.validate().is_err());
    }
}
