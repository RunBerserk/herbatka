//! Broker core orchestration.
//!
//! Maps topics to in-memory logs and exposes produce/fetch APIs.
//! Coordinates topic-level runtime behavior; persistence format is handled in `log::persistence`.

mod checkpoint_io;
mod public_api;
mod retention;
mod segment_fetch;
mod startup_discovery;
mod topic_paths;

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

    pub fn fetch(&self, topic: &str, offset: u64) -> Result<Option<Message>, BrokerError> {
        public_api::fetch(self, topic, offset)
    }

    pub fn fetch_batch(
        &self,
        topic: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<Message>, BrokerError> {
        public_api::fetch_batch(self, topic, offset, limit)
    }
}

impl Broker {
    // ---- Startup loading/discovery ----
    fn load_topic_state(&self, topic: &str) -> Result<TopicState, BrokerError> {
        startup_discovery::discover::load_topic_state(self, topic)
    }
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}
