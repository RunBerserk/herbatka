//! Read persisted messages by offset when they are outside the hot in-memory [`Log`].

use std::fs::OpenOptions;
use std::io::{self, BufReader, Seek};

use crate::broker::core::{Broker, BrokerError, SegmentMeta, TopicState};
use crate::broker::index::SparseIndexEntry;
use crate::log::message::Message;
use crate::log::persistence::read_message;

pub(super) fn topic_min_offset(state: &TopicState) -> u64 {
    state
        .segments
        .first()
        .map(|s| s.base_offset)
        .unwrap_or_else(|| state.log.base_offset())
}

pub(super) fn topic_exclusive_end(state: &TopicState) -> u64 {
    if let Some(last) = state.segments.last() {
        last.base_offset.saturating_add(last.message_count)
    } else {
        state.log.next_offset()
    }
}

fn segment_covering_offset(segments: &[SegmentMeta], offset: u64) -> Option<&SegmentMeta> {
    segments.iter().find(|seg| {
        seg.message_count > 0
            && offset >= seg.base_offset
            && offset < seg.base_offset.saturating_add(seg.message_count)
    })
}

/// Byte position and logical offset of the sparse-index anchor used to begin scanning toward `target_offset`.
fn scan_start_from_index(
    entries: &[SparseIndexEntry],
    target_offset: u64,
    segment_base: u64,
    valid_len: u64,
) -> (u64, u64) {
    if entries.is_empty() {
        return (0, segment_base);
    }
    if target_offset < entries[0].offset {
        return (0, segment_base);
    }
    let idx = entries.partition_point(|e| e.offset <= target_offset);
    let i = idx.saturating_sub(1);
    let e = &entries[i];
    if e.position < valid_len {
        (e.position, e.offset)
    } else {
        (0, segment_base)
    }
}

pub(super) fn read_message_at_segment_offset_readonly(
    broker: &Broker,
    topic: &str,
    segment: &SegmentMeta,
    target_offset: u64,
) -> Result<Option<Message>, BrokerError> {
    let valid_len = segment.size_bytes;
    let index_entries = broker.load_segment_index(topic, &segment.path);
    let entries: &[SparseIndexEntry] = index_entries.as_deref().unwrap_or(&[]);

    let (start_pos, mut cur_logical) =
        scan_start_from_index(entries, target_offset, segment.base_offset, valid_len);

    let file = OpenOptions::new()
        .read(true)
        .open(&segment.path)
        .map_err(BrokerError::Io)?;
    let mut reader = BufReader::new(file);
    reader
        .seek(io::SeekFrom::Start(start_pos))
        .map_err(BrokerError::Io)?;

    while cur_logical <= target_offset {
        match read_message(&mut reader) {
            Ok(Some(msg)) => {
                if cur_logical == target_offset {
                    return Ok(Some(msg));
                }
                cur_logical = cur_logical.saturating_add(1);
            }
            Ok(None) => return Ok(None),
            Err(e) => return Err(BrokerError::Io(e)),
        }
    }
    Ok(None)
}

pub(super) fn fetch_from_segments(
    broker: &Broker,
    topic: &str,
    state: &TopicState,
    offset: u64,
) -> Result<Option<Message>, BrokerError> {
    let Some(seg) = segment_covering_offset(&state.segments, offset) else {
        return Ok(None);
    };
    read_message_at_segment_offset_readonly(broker, topic, seg, offset)
}

#[cfg(test)]
mod tests {
    use super::scan_start_from_index;
    use crate::broker::index::SparseIndexEntry;

    #[test]
    fn scan_start_picks_anchor_at_or_below_target() {
        let entries = vec![
            SparseIndexEntry {
                offset: 10,
                position: 0,
            },
            SparseIndexEntry {
                offset: 74,
                position: 400,
            },
        ];
        let (pos, logical) = scan_start_from_index(&entries, 50, 10, 10_000);
        assert_eq!((pos, logical), (0, 10));
        let (pos2, logical2) = scan_start_from_index(&entries, 74, 10, 10_000);
        assert_eq!((pos2, logical2), (400, 74));
        let (pos3, logical3) = scan_start_from_index(&entries, 200, 10, 10_000);
        assert_eq!((pos3, logical3), (400, 74));
    }

    #[test]
    fn scan_start_before_first_index_entry_scans_from_file_start() {
        let entries = vec![SparseIndexEntry {
            offset: 64,
            position: 100,
        }];
        assert_eq!(scan_start_from_index(&entries, 12, 0, 10_000), (0, 0),);
    }

    #[test]
    fn scan_start_bad_position_falls_back_to_zero() {
        let entries = vec![SparseIndexEntry {
            offset: 0,
            position: 99,
        }];
        assert_eq!(scan_start_from_index(&entries, 5, 0, 50), (0, 0),);
    }
}
