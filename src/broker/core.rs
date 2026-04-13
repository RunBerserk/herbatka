//! Broker core orchestration.
//!
//! Maps topics to in-memory logs and exposes produce/fetch APIs.
//! Coordinates topic-level runtime behavior; persistence format is handled in `log::persistence`.

use std::collections::{BTreeSet, HashMap};
use std::fs::{File, read_dir, remove_file};
use std::io;
use std::path::{Path, PathBuf};

use crate::config::BrokerConfig;
use crate::log::message::Message;
use crate::log::persistence::read_message;
use crate::log::store::{Log, append_to_segment_file, estimate_record_size};

pub struct Broker {
    topics: HashMap<String, TopicState>,
    config: BrokerConfig,
}

struct TopicState {
    log: Log,
    segments: Vec<SegmentMeta>,
}

impl TopicState {
    fn empty() -> Self {
        Self {
            log: Log::new(),
            segments: Vec::new(),
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

    fn topic_dir_path(&self, topic: &str) -> PathBuf {
        self.config.data_dir.join(topic)
    }

    fn legacy_topic_log_path(&self, topic: &str) -> PathBuf {
        self.config.data_dir.join(format!("{topic}.log"))
    }

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
            if let Some(name) = topic_name_from_entry(&entry.path()) {
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
        let bytes_written =
            append_to_segment_file(&active.path, &message, self.config.fsync_policy)
                .map_err(BrokerError::Io)?;
        let offset = state.log.append(message);
        active.message_count += 1;
        active.size_bytes = active.size_bytes.saturating_add(bytes_written);

        self.enforce_retention(topic)?;
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

    fn load_topic_state(&self, topic: &str) -> Result<TopicState, BrokerError> {
        let mut segments = self.discover_segments(topic).map_err(BrokerError::Io)?;
        if segments.is_empty() {
            return Ok(TopicState::empty());
        }

        let mut log = Log::with_base_offset(segments[0].base_offset);
        for segment in &mut segments {
            if log.next_offset() != segment.base_offset {
                return Err(BrokerError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "segment offsets are not contiguous",
                )));
            }
            let mut file = File::open(&segment.path).map_err(BrokerError::Io)?;
            let mut count = 0u64;
            while let Some(message) = read_message(&mut file).map_err(BrokerError::Io)? {
                log.append(message);
                count += 1;
            }
            segment.message_count = count;
        }

        Ok(TopicState { log, segments })
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

    fn enforce_retention(&mut self, topic: &str) -> Result<(), BrokerError> {
        let Some(max_topic_bytes) = self.config.max_topic_bytes else {
            return Ok(());
        };
        let Some(state) = self.topics.get_mut(topic) else {
            return Ok(());
        };

        while state.segments.len() > 1 {
            let total: u64 = state.segments.iter().map(|s| s.size_bytes).sum();
            if total <= max_topic_bytes {
                break;
            }
            let evicted = state.segments.remove(0);
            remove_file(&evicted.path).map_err(BrokerError::Io)?;
            state.log.drop_prefix(evicted.message_count as usize);
        }
        Ok(())
    }
}

fn topic_name_from_entry(path: &Path) -> Option<&str> {
    if path.is_dir() {
        path.file_name()?.to_str().filter(|name| !name.is_empty())
    } else if path.extension().and_then(|ext| ext.to_str()) == Some("log") {
        path.file_stem()?.to_str().filter(|name| !name.is_empty())
    } else {
        None
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
