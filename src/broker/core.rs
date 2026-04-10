//! Broker core orchestration.
//!
//! Maps topics to in-memory logs and exposes produce/fetch APIs.
//! Coordinates topic-level runtime behavior; persistence format is handled in `log::persistence`.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use crate::log::message::Message;
use crate::log::store::{Log, append_to_topic_file};

pub struct Broker {
    // topic -> log
    topics: HashMap<String, Log>,
    /// Base directory for per-topic `*.log` files (see `topic_log_path`).
    data_dir: PathBuf,
}

#[derive(Debug)]
pub enum BrokerError {
    TopicAlreadyExists,
    UnknownTopic,
    Io(io::Error),
}

impl Broker {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
            data_dir: PathBuf::from("data/logs"),
        }
    }

    pub fn with_data_dir(data_dir: PathBuf) -> Self {
        Self {
            topics: HashMap::new(),
            data_dir,
        }
    }

    fn topic_log_path(&self, topic: &str) -> PathBuf {
        self.data_dir.join(format!("{topic}.log"))
    }

    pub fn create_topic(&mut self, topic: String) -> Result<(), BrokerError> {
        if self.topics.contains_key(&topic) {
            return Err(BrokerError::TopicAlreadyExists);
        }

        let path = self.topic_log_path(&topic);
        let log = Self::load_topic_log(&path).map_err(BrokerError::Io)?;
        self.topics.insert(topic, log);
        Ok(())
    }

    pub fn produce(&mut self, topic: &str, message: Message) -> Result<u64, BrokerError> {
        // Durability: append-only writes to the topic file; each append flushes and fsyncs it.
        let log = self
            .topics
            .get_mut(topic)
            .ok_or(BrokerError::UnknownTopic)?;
        append_to_topic_file(&self.data_dir, topic, &message).map_err(BrokerError::Io)?;
        Ok(log.append(message))
    }

    pub fn fetch(&self, topic: &str, offset: u64) -> Result<Option<&Message>, BrokerError> {
        let log = self.topics.get(topic).ok_or(BrokerError::UnknownTopic)?;
        Ok(log.read(offset))
    }

    pub fn fetch_batch(
        &self,
        topic: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<&Message>, BrokerError> {
        let log = self.topics.get(topic).ok_or(BrokerError::UnknownTopic)?;
        Ok(log.read_range(offset, limit))
    }

    fn load_topic_log(path: &Path) -> io::Result<Log> {
        match File::open(path) {
            Ok(mut f) => Log::load_from_reader(&mut f),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Log::new()),
            Err(e) => Err(e),
        }
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
    use crate::log::message::Message;
    use std::collections::HashMap;
    use std::fs::create_dir_all;
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
        Broker::with_data_dir(dir)
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
}
