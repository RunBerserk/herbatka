//! Filesystem scans for startup: topic directory names under the broker data dir and segment file
//! lists per topic (`SegmentMeta`), ordered for replay.

use std::collections::BTreeSet;
use std::fs::read_dir;
use std::io;

use crate::broker::core::{Broker, BrokerError, SegmentMeta};

pub(super) fn enumerate_topic_directories(
    broker: &Broker,
) -> Result<BTreeSet<String>, BrokerError> {
    let entries = match read_dir(&broker.config.data_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(e) => return Err(BrokerError::Io(e)),
    };

    let mut topics = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(BrokerError::Io)?;
        if let Some(name) = crate::broker::core::topic_paths::topic_name_from_entry(&entry.path()) {
            topics.insert(name.to_string());
        }
    }
    Ok(topics)
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
