use std::collections::{BTreeSet, HashMap};
use std::fs::read_dir;
use std::io;

use tracing::{info, warn};

use crate::broker::checkpoint;
use crate::broker::checkpoint::SegmentCheckpoint;
use crate::broker::core::{Broker, BrokerError, SegmentMeta, TopicState};
use crate::broker::index;
use crate::broker::startup;
use crate::log::store::Log;

pub(super) fn discover_topics_on_startup(broker: &mut Broker) -> Result<(), BrokerError> {
    let entries = match read_dir(&broker.config.data_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(BrokerError::Io(e)),
    };

    let mut topics = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(BrokerError::Io)?;
        if let Some(name) = super::topic_paths::topic_name_from_entry(&entry.path()) {
            topics.insert(name.to_string());
        }
    }

    for topic in topics {
        if broker.topics.contains_key(&topic) {
            continue;
        }
        let state = load_topic_state(broker, &topic)?;
        broker.topics.insert(topic, state);
    }
    Ok(())
}

pub(super) fn load_topic_state(broker: &Broker, topic: &str) -> Result<TopicState, BrokerError> {
    let mut segments = broker.discover_segments(topic).map_err(BrokerError::Io)?;
    if segments.is_empty() {
        return Ok(TopicState::empty());
    }

    let checkpoint = broker.load_topic_checkpoint(topic);
    let checkpoint_by_base: HashMap<u64, &SegmentCheckpoint> = checkpoint
        .as_ref()
        .map(|cp| cp.segments.iter().map(|seg| (seg.base_offset, seg)).collect())
        .unwrap_or_default();

    let mut log = Log::with_base_offset(segments[0].base_offset);
    let segments_len = segments.len();
    let mut can_metadata_skip = true;
    let mut startup_skipped_segments = 0u64;
    let mut startup_replayed_segments = 0u64;
    let mut startup_fallback_reasons: HashMap<&'static str, u64> = HashMap::new();
    for (idx, segment) in segments.iter_mut().enumerate() {
        if log.next_offset() != segment.base_offset {
            return Err(BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment offsets are not contiguous",
            )));
        }
        let is_tail_segment = idx == segments_len - 1;
        let trusted_closed = if !can_metadata_skip {
            *startup_fallback_reasons
                .entry("post_replay_skip_disabled")
                .or_insert(0) += 1;
            false
        } else if is_tail_segment {
            *startup_fallback_reasons.entry("tail_segment").or_insert(0) += 1;
            false
        } else {
            match checkpoint_by_base.get(&segment.base_offset) {
                None => {
                    *startup_fallback_reasons.entry("missing_checkpoint").or_insert(0) += 1;
                    false
                }
                Some(entry) if entry.valid_len != segment.size_bytes => {
                    *startup_fallback_reasons
                        .entry("checkpoint_valid_len_mismatch")
                        .or_insert(0) += 1;
                    false
                }
                Some(entry) if entry.message_count == 0 => {
                    *startup_fallback_reasons
                        .entry("checkpoint_zero_messages")
                        .or_insert(0) += 1;
                    false
                }
                Some(entry) => match broker.load_segment_index(topic, &segment.path) {
                    None => {
                        *startup_fallback_reasons
                            .entry("missing_or_invalid_index")
                            .or_insert(0) += 1;
                        false
                    }
                    Some(index_entries) => {
                        if index::is_index_compatible(
                            &index_entries,
                            segment.base_offset,
                            entry.message_count,
                            entry.valid_len,
                            super::SPARSE_INDEX_STRIDE,
                        ) {
                            true
                        } else {
                            *startup_fallback_reasons
                                .entry("index_incompatible")
                                .or_insert(0) += 1;
                            false
                        }
                    }
                },
            }
        };

        if trusted_closed {
            let trusted = checkpoint_by_base
                .get(&segment.base_offset)
                .expect("trusted segment must have checkpoint entry");
            let replay =
                startup::skip_trusted_closed_segment(&segment.path, &mut log, trusted.message_count)
                    .map_err(BrokerError::Io)?;
            segment.message_count = replay.message_count;
            segment.size_bytes = replay.valid_len;
            startup_skipped_segments += 1;
            continue;
        }

        let replay = startup::replay_segment_with_tail_recovery(topic, &segment.path, &mut log)
            .map_err(BrokerError::Io)?;
        segment.message_count = replay.message_count;
        segment.size_bytes = replay.valid_len;
        startup_replayed_segments += 1;
        can_metadata_skip = false;
    }

    if startup_replayed_segments > 0 {
        let mut reasons: Vec<_> = startup_fallback_reasons.into_iter().collect();
        reasons.sort_by_key(|(reason, _)| *reason);
        let fallback_reasons = reasons
            .into_iter()
            .map(|(reason, count)| format!("{reason}:{count}"))
            .collect::<Vec<_>>()
            .join(",");
        info!(
            topic = %topic,
            skipped_segments = startup_skipped_segments,
            replayed_segments = startup_replayed_segments,
            fallback_reasons = %fallback_reasons,
            "startup replay path summary"
        );
    } else {
        info!(
            topic = %topic,
            skipped_segments = startup_skipped_segments,
            replayed_segments = startup_replayed_segments,
            fallback_reasons = "",
            "startup replay path summary"
        );
    }

    let state = TopicState {
        log,
        segments,
        pending_checkpoint_appends: 0,
    };

    // Best-effort refresh so startup metadata converges even when fallback path was used.
    let checkpoint = checkpoint::checkpoint_from_snapshots(
        &state
            .segments
            .iter()
            .map(|segment| (segment.base_offset, segment.message_count, segment.size_bytes))
            .collect::<Vec<_>>(),
    );
    if let Err(e) = std::fs::write(
        broker.topic_checkpoint_path(topic),
        checkpoint::encode_topic_checkpoint(&checkpoint),
    ) {
        warn!(
            topic = %topic,
            error = %e,
            "failed to persist checkpoint after startup replay"
        );
    }

    Ok(state)
}

pub(super) fn discover_segments(broker: &Broker, topic: &str) -> io::Result<Vec<SegmentMeta>> {
    let mut segments = Vec::new();
    let topic_dir = broker.topic_dir_path(topic);
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

    let legacy = broker.legacy_topic_log_path(topic);
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
}
