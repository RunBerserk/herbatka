use crate::broker::checkpoint::SegmentCheckpoint;
use crate::broker::core::SegmentMeta;
use crate::broker::index;

#[derive(Debug, Clone, Copy)]
pub(super) struct TailSeekHint {
    pub(super) start_pos: u64,
    pub(super) start_offset: u64,
}

pub(super) fn resolve_tail_seek_hint(
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn resolve_tail_seek_hint_requires_compatible_metadata() {
        let segment = SegmentMeta {
            base_offset: 10,
            message_count: 0,
            size_bytes: 200,
            path: PathBuf::from("dummy.log"),
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
