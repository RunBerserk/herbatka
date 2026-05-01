//! Discover topics and replay segment files during broker startup.
//!
//! Closed segments may use **trusted skip** when checkpoint lines up with sparse index stats.
//! The tail can also skip under the same gates when no prior replay has forced a full decode path
//! (**post_replay_skip_disabled**). When a trusted skip is chosen, FETCH still resolves historical offsets
//! by reading segment files plus sparse-index anchors.

use crate::broker::core::{Broker, BrokerError, TopicState};

use super::segment_scan;

pub(in crate::broker::core) fn load_topic_state(
    broker: &Broker,
    topic: &str,
) -> Result<TopicState, BrokerError> {
    super::load_topic::load_topic_state(broker, topic)
}

pub(in crate::broker::core) fn discover_topics_on_startup(
    broker: &mut Broker,
) -> Result<(), BrokerError> {
    let topics = segment_scan::enumerate_topic_directories(broker)?;

    for topic in topics {
        if broker.topics.contains_key(&topic) {
            continue;
        }
        let state = load_topic_state(broker, &topic)?;
        broker.topics.insert(topic, state);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::broker::core::BrokerError;
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

    fn isolated_broker() -> crate::broker::core::Broker {
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
        crate::broker::core::Broker::with_config(cfg)
    }

    #[test]
    fn startup_discovery_ignores_non_log_files() {
        let mut broker = isolated_broker();
        broker.create_topic("events".into()).unwrap();
        broker.produce("events", msg(b"hello")).unwrap();
        let non_log_file = broker.config.data_dir.join("notes.txt");
        let mut file = File::create(non_log_file).unwrap();
        file.write_all(b"ignore me").unwrap();

        let mut restarted =
            crate::broker::core::Broker::with_data_dir(broker.config.data_dir.clone());
        restarted.discover_topics_on_startup().unwrap();

        let recovered = restarted.fetch("events", 0).unwrap().unwrap();
        assert_eq!(recovered.payload, b"hello".to_vec());
        assert!(matches!(
            restarted.fetch("notes", 0),
            Err(BrokerError::UnknownTopic)
        ));
    }
}
