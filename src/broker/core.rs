//! Broker core orchestration.
//!
//! Maps topics to in-memory logs and exposes produce/fetch APIs.
//! Coordinates topic-level runtime behavior; persistence format is handled in `log::persistence`.

mod topic_paths;
mod checkpoint_io;
mod retention;

use std::collections::{BTreeSet, HashMap};
use std::fs::read_dir;
use std::io;
use std::path::PathBuf;

use super::checkpoint;
use super::checkpoint::SegmentCheckpoint;
use super::index;
use super::index::SparseIndexEntry;
use super::startup;
use crate::config::BrokerConfig;
use crate::log::message::Message;
use crate::log::store::{Log, append_to_segment_file, estimate_record_size};
use tracing::{info, warn};

const CHECKPOINT_FILE_NAME: &str = ".checkpoint";
const CHECKPOINT_FLUSH_EVERY_APPENDS: usize = 128;
const SPARSE_INDEX_STRIDE: u64 = 64;

pub struct Broker {
    topics: HashMap<String, TopicState>,
    config: BrokerConfig,
}

struct TopicState {
    log: Log,
    segments: Vec<SegmentMeta>,
    pending_checkpoint_appends: usize,
}

impl TopicState {
    fn empty() -> Self {
        Self {
            log: Log::new(),
            segments: Vec::new(),
            pending_checkpoint_appends: 0,
        }
    }
}

#[derive(Clone)]
struct SegmentMeta {
    base_offset: u64,
    message_count: u64,
    size_bytes: u64,
    path: PathBuf,
}

#[derive(Debug)]
pub enum BrokerError {
    TopicAlreadyExists,
    UnknownTopic,
    Io(io::Error),
}

impl Broker {
    // ---- Constructors ----
    pub fn new() -> Self {
        Self::with_config(BrokerConfig::default())
    }

    pub fn with_data_dir(data_dir: PathBuf) -> Self {
        let config = BrokerConfig {
            data_dir,
            ..BrokerConfig::default()
        };
        Self::with_config(config)
    }

    pub fn with_config(config: BrokerConfig) -> Self {
        Self {
            topics: HashMap::new(),
            config,
        }
    }
}

impl Broker {
    // ---- Paths and checkpoint I/O ----
}

impl Broker {
    // ---- Public broker API ----
    pub fn create_topic(&mut self, topic: String) -> Result<(), BrokerError> {
        if self.topics.contains_key(&topic) {
            return Err(BrokerError::TopicAlreadyExists);
        }

        let state = self.load_topic_state(&topic)?;
        self.topics.insert(topic, state);
        Ok(())
    }

    pub fn discover_topics_on_startup(&mut self) -> Result<(), BrokerError> {
        let entries = match read_dir(&self.config.data_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(BrokerError::Io(e)),
        };

        let mut topics = BTreeSet::new();
        for entry in entries {
            let entry = entry.map_err(BrokerError::Io)?;
            if let Some(name) = topic_paths::topic_name_from_entry(&entry.path()) {
                topics.insert(name.to_string());
            }
        }

        for topic in topics {
            if self.topics.contains_key(&topic) {
                continue;
            }
            let state = self.load_topic_state(&topic)?;
            self.topics.insert(topic, state);
        }
        Ok(())
    }

    pub fn produce(&mut self, topic: &str, message: Message) -> Result<u64, BrokerError> {
        // Durability policy is controlled by `config.fsync_policy`.
        let (offset, rolled_segment, cadence_due, sparse_index_entry) = {
            let state = self
                .topics
                .get_mut(topic)
                .ok_or(BrokerError::UnknownTopic)?;

            let next_offset = state.log.next_offset();
            let estimated = estimate_record_size(&message).map_err(BrokerError::Io)? as u64;
            let should_roll = state
                .segments
                .last()
                .map(|active| {
                    active.size_bytes > 0
                        && active.size_bytes.saturating_add(estimated) > self.config.segment_max_bytes
                })
                .unwrap_or(true);
            if should_roll {
                let path = self
                    .config
                    .data_dir
                    .join(topic)
                    .join(format!("{next_offset:020}.log"));
                state.segments.push(SegmentMeta {
                    base_offset: next_offset,
                    message_count: 0,
                    size_bytes: 0,
                    path,
                });
            }

            let active = state
                .segments
                .last_mut()
                .expect("active segment should exist");
            let message_index_in_segment = active.message_count;
            let record_position = active.size_bytes;
            let bytes_written =
                append_to_segment_file(&active.path, &message, self.config.fsync_policy)
                    .map_err(BrokerError::Io)?;
            let offset = state.log.append(message);
            active.message_count += 1;
            active.size_bytes = active.size_bytes.saturating_add(bytes_written);
            state.pending_checkpoint_appends += 1;

            let cadence_due = state.pending_checkpoint_appends >= CHECKPOINT_FLUSH_EVERY_APPENDS;
            let sparse_index_entry = if message_index_in_segment % SPARSE_INDEX_STRIDE == 0 {
                Some((
                    active.path.clone(),
                    SparseIndexEntry {
                        offset,
                        position: record_position,
                    },
                ))
            } else {
                None
            };
            (offset, should_roll, cadence_due, sparse_index_entry)
        };

        if let Some((segment_path, entry)) = sparse_index_entry
            && let Err(e) = index::append_index_entry(&segment_path, &entry)
        {
            warn!(
                topic = %topic,
                segment = %segment_path.display(),
                error = %e,
                "failed to write sparse index entry; startup will fall back safely"
            );
        }

        let retention_changed = self.enforce_retention(topic)?;
        let checkpoint_due = rolled_segment || retention_changed || cadence_due;
        if checkpoint_due {
            self.persist_topic_checkpoint(topic).map_err(BrokerError::Io)?;
            if let Some(state) = self.topics.get_mut(topic) {
                state.pending_checkpoint_appends = 0;
            }
        }
        Ok(offset)
    }

    pub fn fetch(&self, topic: &str, offset: u64) -> Result<Option<&Message>, BrokerError> {
        let state = self.topics.get(topic).ok_or(BrokerError::UnknownTopic)?;
        Ok(state.log.read(offset))
    }

    pub fn fetch_batch(
        &self,
        topic: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<&Message>, BrokerError> {
        let state = self.topics.get(topic).ok_or(BrokerError::UnknownTopic)?;
        Ok(state.log.read_range(offset, limit))
    }
}

impl Broker {
    // ---- Startup loading/discovery ----
    fn load_topic_state(&self, topic: &str) -> Result<TopicState, BrokerError> {
        let mut segments = self.discover_segments(topic).map_err(BrokerError::Io)?;
        if segments.is_empty() {
            return Ok(TopicState::empty());
        }

        let checkpoint = self.load_topic_checkpoint(topic);
        let checkpoint_by_base: HashMap<u64, &SegmentCheckpoint> = checkpoint
            .as_ref()
            .map(|cp| cp.segments.iter().map(|seg| (seg.base_offset, seg)).collect())
            .unwrap_or_default();

        let mut log = Log::with_base_offset(segments[0].base_offset);
        let segments_len = segments.len();
        let mut can_metadata_skip = true;
        let mut startup_skipped_segments = 0u64;
        let mut startup_replayed_segments = 0u64;
        let mut startup_fallback_reasons: HashMap<&'static str, u64> = HashMap::new();
        for (idx, segment) in segments.iter_mut().enumerate() {
            if log.next_offset() != segment.base_offset {
                return Err(BrokerError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "segment offsets are not contiguous",
                )));
            }
            let is_tail_segment = idx == segments_len - 1;
            let trusted_closed = if !can_metadata_skip {
                *startup_fallback_reasons
                    .entry("post_replay_skip_disabled")
                    .or_insert(0) += 1;
                false
            } else if is_tail_segment {
                *startup_fallback_reasons.entry("tail_segment").or_insert(0) += 1;
                false
            } else {
                match checkpoint_by_base.get(&segment.base_offset) {
                    None => {
                        *startup_fallback_reasons.entry("missing_checkpoint").or_insert(0) += 1;
                        false
                    }
                    Some(entry) if entry.valid_len != segment.size_bytes => {
                        *startup_fallback_reasons
                            .entry("checkpoint_valid_len_mismatch")
                            .or_insert(0) += 1;
                        false
                    }
                    Some(entry) if entry.message_count == 0 => {
                        *startup_fallback_reasons
                            .entry("checkpoint_zero_messages")
                            .or_insert(0) += 1;
                        false
                    }
                    Some(entry) => {
                        match self.load_segment_index(topic, &segment.path) {
                            None => {
                                *startup_fallback_reasons
                                    .entry("missing_or_invalid_index")
                                    .or_insert(0) += 1;
                                false
                            }
                            Some(index_entries) => {
                                if index::is_index_compatible(
                                    &index_entries,
                                    segment.base_offset,
                                    entry.message_count,
                                    entry.valid_len,
                                    SPARSE_INDEX_STRIDE,
                                ) {
                                    true
                                } else {
                                    *startup_fallback_reasons.entry("index_incompatible").or_insert(0) += 1;
                                    false
                                }
                            }
                        }
                    }
                }
            };

            if trusted_closed {
                let trusted = checkpoint_by_base
                    .get(&segment.base_offset)
                    .expect("trusted segment must have checkpoint entry");
                let replay =
                    startup::skip_trusted_closed_segment(&segment.path, &mut log, trusted.message_count)
                        .map_err(BrokerError::Io)?;
                segment.message_count = replay.message_count;
                segment.size_bytes = replay.valid_len;
                startup_skipped_segments += 1;
                continue;
            }

            let replay = startup::replay_segment_with_tail_recovery(topic, &segment.path, &mut log)
                .map_err(BrokerError::Io)?;
            segment.message_count = replay.message_count;
            segment.size_bytes = replay.valid_len;
            startup_replayed_segments += 1;
            can_metadata_skip = false;
        }

        if startup_replayed_segments > 0 {
            let mut reasons: Vec<_> = startup_fallback_reasons.into_iter().collect();
            reasons.sort_by_key(|(reason, _)| *reason);
            let fallback_reasons = reasons
                .into_iter()
                .map(|(reason, count)| format!("{reason}:{count}"))
                .collect::<Vec<_>>()
                .join(",");
            info!(
                topic = %topic,
                skipped_segments = startup_skipped_segments,
                replayed_segments = startup_replayed_segments,
                fallback_reasons = %fallback_reasons,
                "startup replay path summary"
            );
        } else {
            info!(
                topic = %topic,
                skipped_segments = startup_skipped_segments,
                replayed_segments = startup_replayed_segments,
                fallback_reasons = "",
                "startup replay path summary"
            );
        }

        let state = TopicState {
            log,
            segments,
            pending_checkpoint_appends: 0,
        };

        // Best-effort refresh so startup metadata converges even when fallback path was used.
        let checkpoint = checkpoint::checkpoint_from_snapshots(
            &state
                .segments
                .iter()
                .map(|segment| (segment.base_offset, segment.message_count, segment.size_bytes))
                .collect::<Vec<_>>(),
        );
        if let Err(e) = std::fs::write(
            self.topic_checkpoint_path(topic),
            checkpoint::encode_topic_checkpoint(&checkpoint),
        ) {
            warn!(
                topic = %topic,
                error = %e,
                "failed to persist checkpoint after startup replay"
            );
        }

        Ok(state)
    }

    fn discover_segments(&self, topic: &str) -> io::Result<Vec<SegmentMeta>> {
        let mut segments = Vec::new();
        let topic_dir = self.topic_dir_path(topic);
        match read_dir(&topic_dir) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    let path = entry.path();
                    if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
                        continue;
                    }
                    let Some(base_offset) = path
                        .file_stem()
                        .and_then(|stem| stem.to_str())
                        .and_then(|stem| stem.parse::<u64>().ok())
                    else {
                        continue;
                    };
                    let size_bytes = entry.metadata()?.len();
                    segments.push(SegmentMeta {
                        base_offset,
                        message_count: 0,
                        size_bytes,
                        path,
                    });
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        let legacy = self.legacy_topic_log_path(topic);
        if legacy.exists() && !segments.iter().any(|segment| segment.base_offset == 0) {
            let size_bytes = std::fs::metadata(&legacy)?.len();
            segments.push(SegmentMeta {
                base_offset: 0,
                message_count: 0,
                size_bytes,
                path: legacy,
            });
        }

        segments.sort_by_key(|segment| segment.base_offset);
        Ok(segments)
    }
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BrokerConfig, FsyncPolicy};
    use crate::log::message::Message;
    use std::collections::HashMap;
    use std::fs::{File, create_dir_all};
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn msg(payload: &[u8]) -> Message {
        Message {
            key: None,
            payload: payload.to_vec(),
            timestamp: SystemTime::now(),
            headers: HashMap::new(),
        }
    }

    fn isolated_broker() -> Broker {
        let dir = std::env::temp_dir().join(format!(
            "herbatka_topics_isolated_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        create_dir_all(&dir).unwrap();
        let cfg = BrokerConfig {
            data_dir: dir,
            segment_max_bytes: 80,
            max_topic_bytes: None,
            fsync_policy: FsyncPolicy::Never,
        };
        Broker::with_config(cfg)
    }

    #[test]
    fn topics_are_isolated() {
        //GIVEN
        let mut broker = isolated_broker();

        broker.create_topic("A".into()).unwrap();
        broker.create_topic("B".into()).unwrap();

        //WHEN
        broker.produce("A", msg(b"only-a")).unwrap();
        broker.produce("B", msg(b"only-b")).unwrap();

        //THEN
        let from_a = broker
            .fetch("A", 0)
            .unwrap()
            .expect("A should have offset 0");
        let from_b = broker
            .fetch("B", 0)
            .unwrap()
            .expect("B should have offset 0");

        assert_eq!(from_a.payload, b"only-a".to_vec());
        assert_eq!(from_b.payload, b"only-b".to_vec());

        // A's log should not see B's message at offset 0
        assert_ne!(from_a.payload, b"only-b".to_vec());
    }

    #[test]
    fn startup_discovery_ignores_non_log_files() {
        //GIVEN
        let mut broker = isolated_broker();
        broker.create_topic("events".into()).unwrap();
        broker.produce("events", msg(b"hello")).unwrap();
        let random_file = broker.config.data_dir.join("notes.txt");
        let mut file = File::create(random_file).unwrap();
        file.write_all(b"ignore me").unwrap();

        //WHEN
        let mut restarted = Broker::with_data_dir(broker.config.data_dir.clone());
        restarted.discover_topics_on_startup().unwrap();

        //THEN
        let recovered = restarted.fetch("events", 0).unwrap().unwrap();
        assert_eq!(recovered.payload, b"hello".to_vec());
        assert!(matches!(
            restarted.fetch("notes", 0),
            Err(BrokerError::UnknownTopic)
        ));
    }

    #[test]
    fn retention_drops_oldest_segment_first() {
        let dir = std::env::temp_dir().join(format!(
            "herbatka_retention_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        create_dir_all(&dir).unwrap();
        let cfg = BrokerConfig {
            data_dir: dir.clone(),
            segment_max_bytes: 80,
            max_topic_bytes: Some(140),
            fsync_policy: FsyncPolicy::Never,
        };
        let mut broker = Broker::with_config(cfg);
        broker.create_topic("events".into()).unwrap();
        let big = vec![b'x'; 64];
        broker.produce("events", msg(&big)).unwrap();
        broker.produce("events", msg(&big)).unwrap();
        broker.produce("events", msg(&big)).unwrap();

        assert!(broker.fetch("events", 0).unwrap().is_none());
        assert!(broker.fetch("events", 2).unwrap().is_some());
    }
}
