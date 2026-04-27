use std::fs::remove_file;

use super::index;
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
