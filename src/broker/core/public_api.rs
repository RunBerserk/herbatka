use crate::broker::core::{Broker, BrokerError};
use crate::broker::index;
use crate::broker::index::SparseIndexEntry;
use crate::log::message::Message;
use crate::log::store::append_to_segment_file;
use crate::log::store::estimate_record_size;
use tracing::warn;

pub(super) fn create_topic(broker: &mut Broker, topic: String) -> Result<(), BrokerError> {
    if broker.topics.contains_key(&topic) {
        return Err(BrokerError::TopicAlreadyExists);
    }

    let state = broker.load_topic_state(&topic)?;
    broker.topics.insert(topic, state);
    Ok(())
}

pub(super) fn discover_topics_on_startup(broker: &mut Broker) -> Result<(), BrokerError> {
    super::startup_discovery::discover_topics_on_startup(broker)
}

pub(super) fn produce(
    broker: &mut Broker,
    topic: &str,
    message: Message,
) -> Result<u64, BrokerError> {
    // Durability policy is controlled by `config.fsync_policy`.
    let (offset, rolled_segment, cadence_due, sparse_index_entry) = {
        let state = broker
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
                    && active.size_bytes.saturating_add(estimated) > broker.config.segment_max_bytes
            })
            .unwrap_or(true);
        if should_roll {
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

        let active = state
            .segments
            .last_mut()
            .expect("active segment should exist");
        let message_index_in_segment = active.message_count;
        let record_position = active.size_bytes;
        let bytes_written = append_to_segment_file(&active.path, &message, broker.config.fsync_policy)
            .map_err(BrokerError::Io)?;
        let offset = state.log.append(message);
        active.message_count += 1;
        active.size_bytes = active.size_bytes.saturating_add(bytes_written);
        state.pending_checkpoint_appends += 1;

        let cadence_due =
            state.pending_checkpoint_appends >= super::CHECKPOINT_FLUSH_EVERY_APPENDS;
        let sparse_index_entry = if message_index_in_segment % super::SPARSE_INDEX_STRIDE == 0 {
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

    let retention_changed = broker.enforce_retention(topic)?;
    let checkpoint_due = rolled_segment || retention_changed || cadence_due;
    if checkpoint_due {
        broker.persist_topic_checkpoint(topic).map_err(BrokerError::Io)?;
        if let Some(state) = broker.topics.get_mut(topic) {
            state.pending_checkpoint_appends = 0;
        }
    }
    Ok(offset)
}

pub(super) fn fetch<'a>(
    broker: &'a Broker,
    topic: &str,
    offset: u64,
) -> Result<Option<&'a Message>, BrokerError> {
    let state = broker.topics.get(topic).ok_or(BrokerError::UnknownTopic)?;
    Ok(state.log.read(offset))
}

pub(super) fn fetch_batch<'a>(
    broker: &'a Broker,
    topic: &str,
    offset: u64,
    limit: usize,
) -> Result<Vec<&'a Message>, BrokerError> {
    let state = broker.topics.get(topic).ok_or(BrokerError::UnknownTopic)?;
    Ok(state.log.read_range(offset, limit))
}
