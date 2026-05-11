use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const INDEX_VERSION: &str = "v1";

#[derive(Debug, Clone)]
pub(crate) struct SparseIndexEntry {
    pub(crate) offset: u64,
    pub(crate) position: u64,
}

pub(crate) fn sidecar_path_for_segment(segment_path: &Path) -> PathBuf {
    segment_path.with_extension("idx")
}

pub(crate) fn append_index_entry(segment_path: &Path, entry: &SparseIndexEntry) -> io::Result<()> {
    let path = sidecar_path_for_segment(segment_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let needs_header = match std::fs::metadata(&path) {
        Ok(meta) => meta.len() == 0,
        Err(e) if e.kind() == io::ErrorKind::NotFound => true,
        Err(e) => return Err(e),
    };

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if needs_header {
        writeln!(file, "{INDEX_VERSION}")?;
    }
    writeln!(file, "{},{}", entry.offset, entry.position)?;
    Ok(())
}

pub(crate) fn read_sparse_index(segment_path: &Path) -> io::Result<Vec<SparseIndexEntry>> {
    let path = sidecar_path_for_segment(segment_path);
    let raw = std::fs::read_to_string(path)?;
    parse_sparse_index(&raw)
}

pub(crate) fn remove_sidecar_for_segment(segment_path: &Path) -> io::Result<()> {
    let path = sidecar_path_for_segment(segment_path);
    match std::fs::remove_file(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

pub(crate) fn is_index_compatible(
    entries: &[SparseIndexEntry],
    base_offset: u64,
    message_count: u64,
    valid_len: u64,
    stride: u64,
) -> bool {
    if message_count == 0 {
        return entries.is_empty();
    }
    if entries.is_empty() {
        return false;
    }
    if entries.first().map(|e| e.offset) != Some(base_offset) {
        return false;
    }
    if entries.iter().any(|entry| entry.position >= valid_len) {
        return false;
    }
    for pair in entries.windows(2) {
        let prev = &pair[0];
        let next = &pair[1];
        if next.offset <= prev.offset || next.position <= prev.position {
            return false;
        }
    }
    let expected_last_offset = base_offset + message_count - 1;
    let last_offset = entries.last().map(|e| e.offset).unwrap_or(base_offset);
    if last_offset > expected_last_offset {
        return false;
    }
    expected_last_offset - last_offset < stride
}

fn parse_sparse_index(raw: &str) -> io::Result<Vec<SparseIndexEntry>> {
    let mut lines = raw.lines();
    let Some(version_line) = lines.next() else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty index"));
    };
    if version_line.trim() != INDEX_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported index version",
        ));
    }

    let mut entries = Vec::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let mut parts = line.split(',');
        let offset = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing offset"))?
            .parse::<u64>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid offset"))?;
        let position = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing position"))?
            .parse::<u64>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid position"))?;
        if parts.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "too many index fields",
            ));
        }
        entries.push(SparseIndexEntry { offset, position });
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::{SparseIndexEntry, is_index_compatible};

    #[test]
    fn index_incompatible_when_first_offset_mismatches_base() {
        let entries = vec![SparseIndexEntry {
            offset: 11,
            position: 0,
        }];
        assert!(!is_index_compatible(&entries, 10, 1, 64, 64));
    }

    #[test]
    fn index_incompatible_when_position_exceeds_valid_len() {
        let entries = vec![SparseIndexEntry {
            offset: 10,
            position: 64,
        }];
        assert!(!is_index_compatible(&entries, 10, 1, 64, 64));
    }

    #[test]
    fn index_incompatible_when_offsets_or_positions_are_not_strictly_monotonic() {
        let entries_same_offset = vec![
            SparseIndexEntry {
                offset: 10,
                position: 1,
            },
            SparseIndexEntry {
                offset: 10,
                position: 2,
            },
        ];
        assert!(!is_index_compatible(&entries_same_offset, 10, 5, 64, 64));

        let entries_backwards_position = vec![
            SparseIndexEntry {
                offset: 10,
                position: 5,
            },
            SparseIndexEntry {
                offset: 11,
                position: 4,
            },
        ];
        assert!(!is_index_compatible(
            &entries_backwards_position,
            10,
            5,
            64,
            64
        ));
    }

    #[test]
    fn index_incompatible_when_last_entry_is_too_far_from_expected_last_offset() {
        let entries = vec![SparseIndexEntry {
            offset: 10,
            position: 0,
        }];
        // expected last offset = 10 + 80 - 1 = 89; gap=79 which is >= stride(64)
        assert!(!is_index_compatible(&entries, 10, 80, 64, 64));
    }
}
