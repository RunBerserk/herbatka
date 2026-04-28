use std::fs::remove_file;

use crate::broker::index;
use super::{Broker, BrokerError};

impl Broker {
    pub(super) fn enforce_retention(&mut self, topic: &str) -> Result<bool, BrokerError> {
        let Some(max_topic_bytes) = self.config.max_topic_bytes else {
            return Ok(false);
        };
        let Some(state) = self.topics.get_mut(topic) else {
            return Ok(false);
        };

        let mut changed = false;
        while state.segments.len() > 1 {
            let total: u64 = state.segments.iter().map(|s| s.size_bytes).sum();
            if total <= max_topic_bytes {
                break;
            }
            let evicted = state.segments.remove(0);
            remove_file(&evicted.path).map_err(BrokerError::Io)?;
            index::remove_sidecar_for_segment(&evicted.path).map_err(BrokerError::Io)?;
            state.log.drop_prefix(evicted.message_count as usize);
            changed = true;
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use crate::broker::core::Broker;
    use crate::config::{BrokerConfig, FsyncPolicy};
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
