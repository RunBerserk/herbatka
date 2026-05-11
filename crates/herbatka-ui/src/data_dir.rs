//! Paths under the workspace repo root (same convention as broker/simulator subprocesses).

use std::path::{Path, PathBuf};

/// Repo root: two levels up from `crates/herbatka-ui`.
pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..")
}

/// Default broker layout: `data_dir` from root `herbatka.toml` is `data/logs`; topic is a subfolder.
pub fn topic_data_dir(topic: &str) -> PathBuf {
    workspace_root().join("data").join("logs").join(topic)
}

/// Delete on-disk topic segment files (and sidecars) for the given topic name.
/// Missing directory is treated as success.
pub fn remove_topic_disk_data(topic: &str) -> Result<(), String> {
    let path = topic_data_dir(topic);
    match std::fs::remove_dir_all(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("remove {}: {e}", path.display())),
    }
}
