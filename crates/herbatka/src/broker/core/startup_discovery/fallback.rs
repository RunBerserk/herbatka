//! Startup replay bookkeeping: canonical fallback reason strings and a structured summary log after
//! topic load (`log_startup_replay_summary`).

use std::collections::HashMap;

use tracing::info;

pub(super) const FALLBACK_REASON_POST_REPLAY_SKIP_DISABLED: &str = "post_replay_skip_disabled";
pub(super) const FALLBACK_REASON_TAIL_SEGMENT: &str = "tail_segment";
pub(super) const FALLBACK_REASON_MISSING_CHECKPOINT: &str = "missing_checkpoint";
pub(super) const FALLBACK_REASON_CHECKPOINT_VALID_LEN_MISMATCH: &str =
    "checkpoint_valid_len_mismatch";
pub(super) const FALLBACK_REASON_CHECKPOINT_ZERO_MESSAGES: &str = "checkpoint_zero_messages";
pub(super) const FALLBACK_REASON_MISSING_OR_INVALID_INDEX: &str = "missing_or_invalid_index";
pub(super) const FALLBACK_REASON_INDEX_INCOMPATIBLE: &str = "index_incompatible";

pub(super) struct FallbackCounters<'a> {
    counts: &'a mut HashMap<&'static str, u64>,
}

impl<'a> FallbackCounters<'a> {
    pub(super) fn new(counts: &'a mut HashMap<&'static str, u64>) -> Self {
        Self { counts }
    }

    pub(super) fn bump(&mut self, key: &'static str) {
        *self.counts.entry(key).or_insert(0) += 1;
    }
}

pub(super) struct StartupReplayLogCounters {
    pub(super) skipped_segments: u64,
    pub(super) replayed_segments: u64,
    pub(super) tail_partial_replay_used: u64,
    pub(super) tail_partial_replay_fallback: u64,
    pub(super) closed_partial_replay_used: u64,
    pub(super) closed_partial_replay_fallback: u64,
}

pub(super) fn format_fallback_reasons(reasons: HashMap<&'static str, u64>) -> String {
    let mut vec: Vec<_> = reasons.into_iter().collect();
    vec.sort_by_key(|(reason, _)| *reason);
    vec.into_iter()
        .map(|(reason, count)| format!("{reason}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn log_startup_replay_summary(
    topic: &str,
    counters: StartupReplayLogCounters,
    fallback_reason_map: HashMap<&'static str, u64>,
) {
    let fallback_reasons_summary = if counters.replayed_segments > 0 {
        format_fallback_reasons(fallback_reason_map)
    } else {
        String::new()
    };
    info!(
        topic = %topic,
        skipped_segments = counters.skipped_segments,
        replayed_segments = counters.replayed_segments,
        tail_partial_replay_used = counters.tail_partial_replay_used,
        tail_partial_replay_fallback = counters.tail_partial_replay_fallback,
        closed_partial_replay_used = counters.closed_partial_replay_used,
        closed_partial_replay_fallback = counters.closed_partial_replay_fallback,
        fallback_reasons = %fallback_reasons_summary,
        "startup replay path summary"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
