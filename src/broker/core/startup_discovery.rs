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

pub(super) const FALLBACK_REASON_POST_REPLAY_SKIP_DISABLED: &str = "post_replay_skip_disabled";
pub(super) const FALLBACK_REASON_TAIL_SEGMENT: &str = "tail_segment";
pub(super) const FALLBACK_REASON_MISSING_CHECKPOINT: &str = "missing_checkpoint";
pub(super) const FALLBACK_REASON_CHECKPOINT_VALID_LEN_MISMATCH: &str =
    "checkpoint_valid_len_mismatch";
pub(super) const FALLBACK_REASON_CHECKPOINT_ZERO_MESSAGES: &str = "checkpoint_zero_messages";
pub(super) const FALLBACK_REASON_MISSING_OR_INVALID_INDEX: &str = "missing_or_invalid_index";
pub(super) const FALLBACK_REASON_INDEX_INCOMPATIBLE: &str = "index_incompatible";

#[derive(Debug, Clone, Copy)]
struct TailSeekHint {
    start_pos: u64,
    start_offset: u64,
}

fn format_fallback_reasons(startup_fallback_reasons: HashMap<&'static str, u64>) -> String {
    let mut reasons: Vec<_> = startup_fallback_reasons.into_iter().collect();
    reasons.sort_by_key(|(reason, _)| *reason);
    reasons
        .into_iter()
        .map(|(reason, count)| format!("{reason}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn resolve_tail_seek_hint(
    segment: &SegmentMeta,
    checkpoint_entry: Option<&SegmentCheckpoint>,
    index_entries: Option<&[index::SparseIndexEntry]>,
    stride: u64,
) -> Option<TailSeekHint> {
    let checkpoint_entry = checkpoint_entry?;
    let index_entries = index_entries?;
    if checkpoint_entry.message_count == 0 {
        return None;
    }
    if checkpoint_entry.valid_len != segment.size_bytes {
        return None;
    }
    if !index::is_index_compatible(
        index_entries,
        segment.base_offset,
        checkpoint_entry.message_count,
        checkpoint_entry.valid_len,
        stride,
    ) {
        return None;
    }
    let last = index_entries.last()?;
    let expected_last_offset = segment
        .base_offset
        .saturating_add(checkpoint_entry.message_count.saturating_sub(1));
    if last.offset > expected_last_offset {
        return None;
    }
    if last.position >= checkpoint_entry.valid_len {
        return None;
    }
    Some(TailSeekHint {
        start_pos: last.position,
        start_offset: last.offset,
    })
}

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
        .map(|cp| {
            cp.segments
                .iter()
                .map(|seg| (seg.base_offset, seg))
                .collect()
        })
        .unwrap_or_default();

    let mut log = Log::with_base_offset(segments[0].base_offset);
    let segments_len = segments.len();
    let mut can_metadata_skip = true;
    let mut startup_skipped_segments = 0u64;
    let mut startup_replayed_segments = 0u64;
    let mut tail_partial_replay_used = 0u64;
    let mut tail_partial_replay_fallback = 0u64;
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
                .entry(FALLBACK_REASON_POST_REPLAY_SKIP_DISABLED)
                .or_insert(0) += 1;
            false
        } else if is_tail_segment {
            *startup_fallback_reasons
                .entry(FALLBACK_REASON_TAIL_SEGMENT)
                .or_insert(0) += 1;
            false
        } else {
            match checkpoint_by_base.get(&segment.base_offset) {
                None => {
                    *startup_fallback_reasons
                        .entry(FALLBACK_REASON_MISSING_CHECKPOINT)
                        .or_insert(0) += 1;
                    false
                }
                Some(entry) if entry.valid_len != segment.size_bytes => {
                    *startup_fallback_reasons
                        .entry(FALLBACK_REASON_CHECKPOINT_VALID_LEN_MISMATCH)
                        .or_insert(0) += 1;
                    false
                }
                Some(entry) if entry.message_count == 0 => {
                    *startup_fallback_reasons
                        .entry(FALLBACK_REASON_CHECKPOINT_ZERO_MESSAGES)
                        .or_insert(0) += 1;
                    false
                }
                Some(entry) => match broker.load_segment_index(topic, &segment.path) {
                    None => {
                        *startup_fallback_reasons
                            .entry(FALLBACK_REASON_MISSING_OR_INVALID_INDEX)
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
                                .entry(FALLBACK_REASON_INDEX_INCOMPATIBLE)
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
            let replay = startup::skip_trusted_closed_segment(
                &segment.path,
                &mut log,
                trusted.message_count,
            )
            .map_err(BrokerError::Io)?;
            segment.message_count = replay.message_count;
            segment.size_bytes = replay.valid_len;
            startup_skipped_segments += 1;
            continue;
        }

        let replay = if is_tail_segment {
            let tail_hint = resolve_tail_seek_hint(
                segment,
                checkpoint_by_base.get(&segment.base_offset).copied(),
                broker
                    .load_segment_index(topic, &segment.path)
                    .as_deref(),
                super::SPARSE_INDEX_STRIDE,
            );
            if let Some(hint) = tail_hint {
                if hint.start_offset < log.next_offset() {
                    tail_partial_replay_fallback += 1;
                    startup::replay_segment_with_tail_recovery(topic, &segment.path, &mut log)
                        .map_err(BrokerError::Io)?
                } else {
                    let skipped_prefix = hint.start_offset.saturating_sub(log.next_offset());
                    log.advance_base_offset(skipped_prefix);
                    tail_partial_replay_used += 1;
                    let replay = startup::replay_segment_with_tail_recovery_from(
                        topic,
                        &segment.path,
                        &mut log,
                        hint.start_pos,
                    )
                    .map_err(BrokerError::Io)?;
                    startup::ReplayOutcome {
                        message_count: replay.message_count.saturating_add(skipped_prefix),
                        valid_len: replay.valid_len,
                    }
                }
            } else {
                tail_partial_replay_fallback += 1;
                startup::replay_segment_with_tail_recovery(topic, &segment.path, &mut log)
                    .map_err(BrokerError::Io)?
            }
        } else {
            startup::replay_segment_with_tail_recovery(topic, &segment.path, &mut log)
                .map_err(BrokerError::Io)?
        };
        segment.message_count = replay.message_count;
        segment.size_bytes = replay.valid_len;
        startup_replayed_segments += 1;
        can_metadata_skip = false;
    }

    if startup_replayed_segments > 0 {
        let fallback_reasons = format_fallback_reasons(startup_fallback_reasons);
        info!(
            topic = %topic,
            skipped_segments = startup_skipped_segments,
            replayed_segments = startup_replayed_segments,
            tail_partial_replay_used,
            tail_partial_replay_fallback,
            fallback_reasons = %fallback_reasons,
            "startup replay path summary"
        );
    } else {
        info!(
            topic = %topic,
            skipped_segments = startup_skipped_segments,
            replayed_segments = startup_replayed_segments,
            tail_partial_replay_used,
            tail_partial_replay_fallback,
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
            .map(|segment| {
                (
                    segment.base_offset,
                    segment.message_count,
                    segment.size_bytes,
                )
            })
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
        let non_log_file = broker.config.data_dir.join("notes.txt");
        let mut file = File::create(non_log_file).unwrap();
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
    fn format_fallback_reasons_is_sorted_and_stable() {
        let mut reasons = HashMap::new();
        reasons.insert(FALLBACK_REASON_TAIL_SEGMENT, 2);
        reasons.insert(FALLBACK_REASON_MISSING_CHECKPOINT, 1);
        reasons.insert(FALLBACK_REASON_INDEX_INCOMPATIBLE, 3);

        let formatted = format_fallback_reasons(reasons);
        assert_eq!(
            formatted,
            "index_incompatible:3,missing_checkpoint:1,tail_segment:2"
        );
    }

    #[test]
    fn resolve_tail_seek_hint_requires_compatible_metadata() {
        let segment = SegmentMeta {
            base_offset: 10,
            message_count: 0,
            size_bytes: 200,
            path: std::path::PathBuf::from("dummy.log"),
        };
        let checkpoint = SegmentCheckpoint {
            base_offset: 10,
            message_count: 65,
            valid_len: 200,
        };
        let index_entries = vec![
            index::SparseIndexEntry {
                offset: 10,
                position: 0,
            },
            index::SparseIndexEntry {
                offset: 74,
                position: 150,
            },
        ];

        let hint = resolve_tail_seek_hint(
            &segment,
            Some(&checkpoint),
            Some(&index_entries),
            crate::broker::core::SPARSE_INDEX_STRIDE,
        );
        assert_eq!(hint.map(|h| h.start_offset), Some(74));
        assert_eq!(hint.map(|h| h.start_pos), Some(150));

        let mismatched_checkpoint = SegmentCheckpoint {
            valid_len: 199,
            ..checkpoint
        };
        assert!(
            resolve_tail_seek_hint(
                &segment,
                Some(&mismatched_checkpoint),
                Some(&index_entries),
                crate::broker::core::SPARSE_INDEX_STRIDE
            )
            .is_none()
        );
    }
}
