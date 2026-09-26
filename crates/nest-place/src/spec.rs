use serde::{Deserialize, Serialize};

/// What a selector names. Paths are namespace paths ("/hub/...").
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Selector {
    /// A file or a tree, following symlinks inside the namespace.
    Path { path: String },
    /// A Hugging Face repo in a hub cache directory, optionally narrowed to
    /// one revision (branch/tag name resolved through `refs/`, or a commit).
    Hf {
        hub: String,
        repo: String,
        #[serde(default)]
        revision: Option<String>,
        #[serde(default = "default_repo_type")]
        repo_type: String,
    },
}

fn default_repo_type() -> String {
    "model".into()
}

impl Selector {
    /// Parse the CLI form: a namespace path, or `hf:org/name[@revision]`
    /// (hub defaults to `/hub`; `hf-dataset:` for datasets).
    pub fn parse(s: &str, hub: &str) -> Result<Selector, String> {
        for (prefix, repo_type) in [("hf:", "model"), ("hf-dataset:", "dataset")] {
            if let Some(rest) = s.strip_prefix(prefix) {
                let (repo, revision) = match rest.split_once('@') {
                    Some((r, v)) => (r.to_string(), Some(v.to_string())),
                    None => (rest.to_string(), None),
                };
                if !repo.contains('/') {
                    return Err(format!("expected org/name in {s:?}"));
                }
                return Ok(Selector::Hf {
                    hub: hub.to_string(),
                    repo,
                    revision,
                    repo_type: repo_type.into(),
                });
            }
        }
        if !s.starts_with('/') {
            return Err(format!(
                "expected a namespace path starting with / or hf:org/name, got {s:?}"
            ));
        }
        Ok(Selector::Path {
            path: s.to_string(),
        })
    }

    pub fn describe(&self) -> String {
        match self {
            Selector::Path { path } => path.clone(),
            Selector::Hf { repo, revision, .. } => match revision {
                Some(r) => format!("hf:{repo}@{r}"),
                None => format!("hf:{repo}"),
            },
        }
    }
}

/// A durable placement rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleSpec {
    pub selector: Selector,
    /// Host names that must each hold a complete copy; "@all" means every
    /// node.
    pub hosts: Vec<String>,
    /// Re-apply automatically (debounced) after files finish being written.
    #[serde(default)]
    pub auto: bool,
}
