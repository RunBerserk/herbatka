use std::collections::HashMap;

use crate::log::store::Log;
use crate::log::message::Message;

pub struct Broker {
    // topic -> log
    topics: HashMap<String, Log>,
}

#[derive(Debug)]
pub enum BrokerError {
    TopicAlreadyExists,
    UnknownTopic,
}

impl Broker {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    pub fn create_topic(&mut self, topic: String) -> Result<(), BrokerError> {
        if self.topics.contains_key(&topic) {
            return Err(BrokerError::TopicAlreadyExists);
        }

        self.topics.insert(topic, Log::new());
        Ok(())
    }

    pub fn produce(&mut self, topic: &str, message: Message) -> Result<u64, BrokerError> {
        let log = self
            .topics
            .get_mut(topic)
            .ok_or(BrokerError::UnknownTopic)?;
        Ok(log.append(message))
    }

    pub fn fetch(&self, topic: &str, offset: u64) -> Result<Option<&Message>, BrokerError> {
        let log = self.topics.get(topic).ok_or(BrokerError::UnknownTopic)?;
        Ok(log.read(offset))
    }
}
