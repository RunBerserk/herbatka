use std::path::{Path, PathBuf};

use super::{Broker, CHECKPOINT_FILE_NAME};

impl Broker {
    pub(super) fn topic_dir_path(&self, topic: &str) -> PathBuf {
        self.config.data_dir.join(topic)
    }

    pub(super) fn legacy_topic_log_path(&self, topic: &str) -> PathBuf {
        self.config.data_dir.join(format!("{topic}.log"))
    }

    pub(super) fn topic_checkpoint_path(&self, topic: &str) -> PathBuf {
        self.topic_dir_path(topic).join(CHECKPOINT_FILE_NAME)
    }
}

pub(super) fn topic_name_from_entry(path: &Path) -> Option<&str> {
    if path.is_dir() {
        path.file_name()?.to_str().filter(|name| !name.is_empty())
    } else if path.extension().and_then(|ext| ext.to_str()) == Some("log") {
        path.file_stem()?.to_str().filter(|name| !name.is_empty())
    } else {
        None
    }
}
