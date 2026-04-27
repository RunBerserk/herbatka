use std::io;
use std::path::Path;

use tracing::warn;

use super::checkpoint::{self, TopicCheckpoint};
use super::index::{self, SparseIndexEntry};
use super::Broker;

impl Broker {
    pub(super) fn load_topic_checkpoint(&self, topic: &str) -> Option<TopicCheckpoint> {
        let path = self.topic_checkpoint_path(topic);
        let raw = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return None,
            Err(e) => {
                warn!(
                    topic = %topic,
                    checkpoint = %path.display(),
                    error = %e,
                    "failed to read checkpoint, falling back to full replay"
                );
                return None;
            }
        };

        match checkpoint::parse_topic_checkpoint(&raw) {
            Ok(checkpoint) => Some(checkpoint),
            Err(e) => {
                warn!(
                    topic = %topic,
                    checkpoint = %path.display(),
                    error = %e,
                    "invalid checkpoint, falling back to full replay"
                );
                None
            }
        }
    }

    pub(super) fn load_segment_index(
        &self,
        topic: &str,
        segment_path: &Path,
    ) -> Option<Vec<SparseIndexEntry>> {
        match index::read_sparse_index(segment_path) {
            Ok(entries) => Some(entries),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => {
                warn!(
                    topic = %topic,
                    segment = %segment_path.display(),
                    error = %e,
                    "invalid segment index, falling back to full replay"
                );
                None
            }
        }
    }

    pub(super) fn persist_topic_checkpoint(&self, topic: &str) -> io::Result<()> {
        let path = self.topic_checkpoint_path(topic);
        let Some(state) = self.topics.get(topic) else {
            return Ok(());
        };
        if state.segments.is_empty() {
            match std::fs::remove_file(&path) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let checkpoint = checkpoint::checkpoint_from_snapshots(
            &state
                .segments
                .iter()
                .map(|segment| (segment.base_offset, segment.message_count, segment.size_bytes))
                .collect::<Vec<_>>(),
        );
        let encoded = checkpoint::encode_topic_checkpoint(&checkpoint);
        std::fs::write(path, encoded)
    }
}
