//! Broker core orchestration.
//!
//! Maps topics to in-memory logs and exposes produce/fetch APIs.
//! Coordinates topic-level runtime behavior; persistence format is handled in `log::persistence`.

mod topic_paths;
mod checkpoint_io;
mod retention;
mod public_api;
mod startup_discovery;

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use crate::config::BrokerConfig;
use crate::log::message::Message;
use crate::log::store::Log;

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
        public_api::create_topic(self, topic)
    }

    pub fn discover_topics_on_startup(&mut self) -> Result<(), BrokerError> {
        public_api::discover_topics_on_startup(self)
    }

    pub fn produce(&mut self, topic: &str, message: Message) -> Result<u64, BrokerError> {
        public_api::produce(self, topic, message)
    }

    pub fn fetch(&self, topic: &str, offset: u64) -> Result<Option<&Message>, BrokerError> {
        public_api::fetch(self, topic, offset)
    }

    pub fn fetch_batch(
        &self,
        topic: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<&Message>, BrokerError> {
        public_api::fetch_batch(self, topic, offset, limit)
    }
}

impl Broker {
    // ---- Startup loading/discovery ----
    fn load_topic_state(&self, topic: &str) -> Result<TopicState, BrokerError> {
        startup_discovery::load_topic_state(self, topic)
    }

    fn discover_segments(&self, topic: &str) -> io::Result<Vec<SegmentMeta>> {
        startup_discovery::discover_segments(self, topic)
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
