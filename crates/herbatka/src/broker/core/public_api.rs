use std::sync::{Arc, RwLock};

use crate::broker::core::{Broker, BrokerError, TopicState};
use crate::broker::index;
use crate::broker::index::SparseIndexEntry;
use crate::log::message::Message;
use crate::log::store::append_to_segment_file;
use crate::log::store::estimate_record_size;
use tracing::warn;

pub(super) fn create_topic(broker: &Broker, topic: String) -> Result<(), BrokerError> {
    let state = broker.load_topic_state(&topic)?;
    let mut map = broker.topics_write()?;
    if map.contains_key(&topic) {
        return Err(BrokerError::TopicAlreadyExists);
    }
    map.insert(topic, Arc::new(RwLock::new(state)));
    Ok(())
}

pub(super) fn discover_topics_on_startup(broker: &Broker) -> Result<(), BrokerError> {
    super::startup_discovery::discover::discover_topics_on_startup(broker)
}

pub(super) fn produce(broker: &Broker, topic: &str, message: Message) -> Result<u64, BrokerError> {
    let topic_arc = broker.get_topic_arc(topic)?;
    produce_on_topic(broker, topic, &topic_arc, message)
}

pub(super) fn produce_allow_create(
    broker: &Broker,
    topic: &str,
    message: Message,
) -> Result<u64, BrokerError> {
    let topic_arc = broker.get_or_insert_topic(topic)?;
    produce_on_topic(broker, topic, &topic_arc, message)
}

fn produce_on_topic(
    broker: &Broker,
    topic: &str,
    topic_arc: &Arc<RwLock<TopicState>>,
    message: Message,
) -> Result<u64, BrokerError> {
    let mut state = topic_arc.write().map_err(|_| BrokerError::LockPoisoned)?;

    // Durability policy is controlled by `config.fsync_policy`.
    let next_offset = state.log.next_offset();
    let estimated_bytes = estimate_record_size(&message).map_err(BrokerError::Io)? as u64;
    let rolled_segment = state
        .segments
        .last()
        .map(|active| {
            active.size_bytes > 0
                && active.size_bytes.saturating_add(estimated_bytes)
                    > broker.config.segment_max_bytes
        })
        .unwrap_or(true);
    if rolled_segment {
        let path = broker
            .config
            .data_dir
            .join(topic)
            .join(format!("{next_offset:020}.log"));
        state.segments.push(super::SegmentMeta {
            base_offset: next_offset,
            message_count: 0,
            size_bytes: 0,
            path,
        });
    }

    let active = state.segments.last().expect("active segment should exist");
    let message_index_in_segment = active.message_count;
    let record_position = active.size_bytes;
    let segment_path = active.path.clone();
    let bytes_written = append_to_segment_file(&segment_path, &message, broker.config.fsync_policy)
        .map_err(BrokerError::Io)?;
    let offset = state.log.append(message);
    let active = state
        .segments
        .last_mut()
        .expect("active segment should exist");
    active.message_count += 1;
    active.size_bytes = active.size_bytes.saturating_add(bytes_written);
    state.pending_checkpoint_appends += 1;

    let checkpoint_cadence_due =
        state.pending_checkpoint_appends >= super::CHECKPOINT_FLUSH_EVERY_APPENDS;
    let sparse_index_entry = if message_index_in_segment % super::SPARSE_INDEX_STRIDE == 0 {
        Some((
            segment_path,
            SparseIndexEntry {
                offset,
                position: record_position,
            },
        ))
    } else {
        None
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

    let retention_changed =
        super::retention::enforce_retention_on_state(&broker.config, topic, &mut state)?;
    let checkpoint_due = rolled_segment || retention_changed || checkpoint_cadence_due;
    if checkpoint_due {
        broker
            .persist_topic_checkpoint(topic, &state)
            .map_err(BrokerError::Io)?;
        state.pending_checkpoint_appends = 0;
    }
    Ok(offset)
}

/// Returns `(min_retained_offset, exclusive_end)` for reads; valid message offsets satisfy
/// `min <= offset < exclusive_end`.
pub(super) fn topic_offset_range(broker: &Broker, topic: &str) -> Result<(u64, u64), BrokerError> {
    let topic_arc = broker.get_topic_arc(topic)?;
    let state = topic_arc.read().map_err(|_| BrokerError::LockPoisoned)?;
    let min_off = super::segment_fetch::topic_min_offset(&state);
    let exclusive_end = super::segment_fetch::topic_exclusive_end(&state);
    Ok((min_off, exclusive_end))
}

pub(super) fn fetch(
    broker: &Broker,
    topic: &str,
    offset: u64,
) -> Result<Option<Message>, BrokerError> {
    let topic_arc = broker.get_topic_arc(topic)?;
    let state = topic_arc.read().map_err(|_| BrokerError::LockPoisoned)?;
    fetch_with_state(broker, topic, &state, offset)
}

fn fetch_with_state(
    broker: &Broker,
    topic: &str,
    state: &TopicState,
    offset: u64,
) -> Result<Option<Message>, BrokerError> {
    let exclusive_end = super::segment_fetch::topic_exclusive_end(state);
    let min_off = super::segment_fetch::topic_min_offset(state);

    if offset >= exclusive_end || offset < min_off {
        return Ok(None);
    }

    if let Some(m) = state.log.read(offset) {
        return Ok(Some(m.clone()));
    }

    super::segment_fetch::fetch_from_segments(broker, topic, state, offset)
}

pub(super) fn fetch_batch(
    broker: &Broker,
    topic: &str,
    offset: u64,
    limit: usize,
) -> Result<Vec<Message>, BrokerError> {
    let topic_arc = broker.get_topic_arc(topic)?;
    let state = topic_arc.read().map_err(|_| BrokerError::LockPoisoned)?;
    let exclusive_end = super::segment_fetch::topic_exclusive_end(&state);
    let min_off = super::segment_fetch::topic_min_offset(&state);

    if limit == 0 || offset >= exclusive_end || offset < min_off {
        return Ok(Vec::new());
    }

    let mut out = Vec::with_capacity(limit);
    let mut cur = offset;
    let mut collected = 0usize;
    while collected < limit && cur < exclusive_end {
        match fetch_with_state(broker, topic, &state, cur)? {
            Some(msg) => {
                out.push(msg);
                collected += 1;
                cur = cur.saturating_add(1);
            }
            None => break,
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BrokerConfig, FsyncPolicy};
    use crate::time::now_epoch_millis;
    use std::collections::HashMap;
    use std::fs::create_dir_all;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn msg(payload: &[u8]) -> Message {
        Message {
            key: None,
            payload: payload.to_vec(),
            timestamp: now_epoch_millis(),
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
            ..BrokerConfig::default()
        };
        Broker::with_config(cfg)
    }

    #[test]
    fn topics_are_isolated() {
        //GIVEN
        let broker = isolated_broker();

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
    fn concurrent_produce_on_different_topics() {
        let broker = std::sync::Arc::new(isolated_broker());
        let handles: Vec<_> = ["A", "B"]
            .into_iter()
            .map(|name| {
                let broker = std::sync::Arc::clone(&broker);
                thread::spawn(move || {
                    broker.create_topic(name.into()).unwrap();
                    for i in 0..32 {
                        let payload = format!("{name}-{i}");
                        broker.produce(name, msg(payload.as_bytes())).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            broker.fetch("A", 31).unwrap().unwrap().payload,
            b"A-31".to_vec()
        );
        assert_eq!(
            broker.fetch("B", 31).unwrap().unwrap().payload,
            b"B-31".to_vec()
        );
    }
}
