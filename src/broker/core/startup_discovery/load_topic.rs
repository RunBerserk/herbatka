use std::collections::HashMap;
use std::io;

use crate::broker::checkpoint;
use crate::broker::checkpoint::SegmentCheckpoint;
use crate::broker::core::{Broker, BrokerError, SPARSE_INDEX_STRIDE, SegmentMeta, TopicState};
use crate::broker::index;
use crate::broker::startup;
use crate::log::store::Log;
use tracing::warn;

use super::fallback::{
    FALLBACK_REASON_CHECKPOINT_VALID_LEN_MISMATCH, FALLBACK_REASON_CHECKPOINT_ZERO_MESSAGES,
    FALLBACK_REASON_INDEX_INCOMPATIBLE, FALLBACK_REASON_MISSING_CHECKPOINT,
    FALLBACK_REASON_MISSING_OR_INVALID_INDEX, FALLBACK_REASON_POST_REPLAY_SKIP_DISABLED,
    FALLBACK_REASON_TAIL_SEGMENT, FallbackCounters, log_startup_replay_summary,
};
use super::tail_seek::resolve_tail_seek_hint;

enum ClosedSkipDecision {
    SkipTrusted,
    MustReplay,
}

fn evaluate_closed_skip_eligibility(
    broker: &Broker,
    topic: &str,
    segment: &SegmentMeta,
    can_metadata_skip: bool,
    is_tail_segment: bool,
    checkpoint_by_base: &HashMap<u64, &SegmentCheckpoint>,
    counters: &mut FallbackCounters<'_>,
) -> ClosedSkipDecision {
    if is_tail_segment {
        if can_metadata_skip {
            counters.bump(FALLBACK_REASON_TAIL_SEGMENT);
        } else {
            counters.bump(FALLBACK_REASON_POST_REPLAY_SKIP_DISABLED);
        }
        return ClosedSkipDecision::MustReplay;
    }

    // SkipTrusted advances `Log::base_offset` without appending payloads; FETCH only sees replayed/
    // appended offsets. Once we take a decoding replay path, downstream closed segments must fully
    // replay so contiguous history is reflected in memory (broker_persistence regressions rely on this).
    if !can_metadata_skip {
        counters.bump(FALLBACK_REASON_POST_REPLAY_SKIP_DISABLED);
        return ClosedSkipDecision::MustReplay;
    }

    match checkpoint_by_base.get(&segment.base_offset) {
        None => {
            counters.bump(FALLBACK_REASON_MISSING_CHECKPOINT);
            ClosedSkipDecision::MustReplay
        }
        Some(entry) if entry.valid_len != segment.size_bytes => {
            counters.bump(FALLBACK_REASON_CHECKPOINT_VALID_LEN_MISMATCH);
            ClosedSkipDecision::MustReplay
        }
        Some(entry) if entry.message_count == 0 => {
            counters.bump(FALLBACK_REASON_CHECKPOINT_ZERO_MESSAGES);
            ClosedSkipDecision::MustReplay
        }
        Some(entry) => match broker.load_segment_index(topic, &segment.path) {
            None => {
                counters.bump(FALLBACK_REASON_MISSING_OR_INVALID_INDEX);
                ClosedSkipDecision::MustReplay
            }
            Some(index_entries) => {
                if index::is_index_compatible(
                    &index_entries,
                    segment.base_offset,
                    entry.message_count,
                    entry.valid_len,
                    SPARSE_INDEX_STRIDE,
                ) {
                    ClosedSkipDecision::SkipTrusted
                } else {
                    counters.bump(FALLBACK_REASON_INDEX_INCOMPATIBLE);
                    ClosedSkipDecision::MustReplay
                }
            }
        },
    }
}

fn replay_segment_with_optional_last_sparse_seek(
    broker: &Broker,
    topic: &str,
    segment: &SegmentMeta,
    log: &mut Log,
    checkpoint_by_base: &HashMap<u64, &SegmentCheckpoint>,
) -> Result<(startup::ReplayOutcome, u64, u64), BrokerError> {
    let tail_hint = resolve_tail_seek_hint(
        segment,
        checkpoint_by_base.get(&segment.base_offset).copied(),
        broker.load_segment_index(topic, &segment.path).as_deref(),
        SPARSE_INDEX_STRIDE,
    );

    Ok(match tail_hint {
        Some(hint) => {
            if hint.start_offset < log.next_offset() {
                (
                    startup::replay_segment_with_tail_recovery(topic, &segment.path, log)
                        .map_err(BrokerError::Io)?,
                    0,
                    1,
                )
            } else {
                let skipped_prefix = hint.start_offset.saturating_sub(log.next_offset());
                log.advance_base_offset(skipped_prefix);
                let replay = startup::replay_segment_with_tail_recovery_from(
                    topic,
                    &segment.path,
                    log,
                    hint.start_pos,
                )
                .map_err(BrokerError::Io)?;
                (
                    startup::ReplayOutcome {
                        message_count: replay.message_count.saturating_add(skipped_prefix),
                        valid_len: replay.valid_len,
                    },
                    1,
                    0,
                )
            }
        }
        None => (
            startup::replay_segment_with_tail_recovery(topic, &segment.path, log)
                .map_err(BrokerError::Io)?,
            0,
            1,
        ),
    })
}

fn persist_startup_checkpoint_best_effort(broker: &Broker, topic: &str, segments: &[SegmentMeta]) {
    let checkpoint = checkpoint::checkpoint_from_snapshots(
        &segments
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
}

pub(super) fn load_topic_state(broker: &Broker, topic: &str) -> Result<TopicState, BrokerError> {
    let mut segments = super::discover_segments(broker, topic).map_err(BrokerError::Io)?;
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
    // After any decoding replay (`MustReplay`), subsequent tail iterations use
    // `post_replay_skip_disabled` telemetry instead of `tail_segment`; see `evaluate_closed_skip_eligibility`.
    // Closed non-tail trusted skip relies on checkpoint + index (+ file length) only, not this flag.
    let mut can_metadata_skip = true;
    let mut startup_skipped_segments = 0u64;
    let mut startup_replayed_segments = 0u64;
    let mut tail_partial_replay_used = 0u64;
    let mut tail_partial_replay_fallback = 0u64;
    let mut closed_partial_replay_used = 0u64;
    let mut closed_partial_replay_fallback = 0u64;
    let mut startup_fallback_reasons: HashMap<&'static str, u64> = HashMap::new();

    for (idx, segment) in segments.iter_mut().enumerate() {
        if log.next_offset() != segment.base_offset {
            return Err(BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment offsets are not contiguous",
            )));
        }
        let is_tail_segment = idx == segments_len - 1;

        let mut fb = FallbackCounters::new(&mut startup_fallback_reasons);
        match evaluate_closed_skip_eligibility(
            broker,
            topic,
            segment,
            can_metadata_skip,
            is_tail_segment,
            &checkpoint_by_base,
            &mut fb,
        ) {
            ClosedSkipDecision::SkipTrusted => {
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
            ClosedSkipDecision::MustReplay => {}
        };

        let (replay, partial_used_delta, partial_fallback_delta) =
            replay_segment_with_optional_last_sparse_seek(
                broker,
                topic,
                segment,
                &mut log,
                &checkpoint_by_base,
            )?;
        if is_tail_segment {
            tail_partial_replay_used += partial_used_delta;
            tail_partial_replay_fallback += partial_fallback_delta;
        } else {
            closed_partial_replay_used += partial_used_delta;
            closed_partial_replay_fallback += partial_fallback_delta;
        }

        segment.message_count = replay.message_count;
        segment.size_bytes = replay.valid_len;
        startup_replayed_segments += 1;
        can_metadata_skip = false;
    }

    log_startup_replay_summary(
        topic,
        super::fallback::StartupReplayLogCounters {
            skipped_segments: startup_skipped_segments,
            replayed_segments: startup_replayed_segments,
            tail_partial_replay_used,
            tail_partial_replay_fallback,
            closed_partial_replay_used,
            closed_partial_replay_fallback,
        },
        startup_fallback_reasons,
    );

    let state = TopicState {
        log,
        segments,
        pending_checkpoint_appends: 0,
    };

    persist_startup_checkpoint_best_effort(broker, topic, &state.segments);

    Ok(state)
}
