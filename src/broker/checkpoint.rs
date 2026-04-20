use std::io;

const CHECKPOINT_VERSION: &str = "v1";

#[derive(Clone)]
pub(crate) struct SegmentCheckpoint {
    pub(crate) base_offset: u64,
    pub(crate) message_count: u64,
    pub(crate) valid_len: u64,
}

pub(crate) struct TopicCheckpoint {
    pub(crate) segments: Vec<SegmentCheckpoint>,
}

pub(crate) fn checkpoint_from_snapshots(snapshots: &[(u64, u64, u64)]) -> TopicCheckpoint {
    TopicCheckpoint {
        segments: snapshots
            .iter()
            .map(|(base_offset, message_count, valid_len)| SegmentCheckpoint {
                base_offset: *base_offset,
                message_count: *message_count,
                valid_len: *valid_len,
            })
            .collect(),
    }
}

pub(crate) fn encode_topic_checkpoint(checkpoint: &TopicCheckpoint) -> String {
    let mut output = String::new();
    output.push_str(CHECKPOINT_VERSION);
    output.push('\n');
    for segment in &checkpoint.segments {
        output.push_str(&format!(
            "{},{},{}\n",
            segment.base_offset, segment.message_count, segment.valid_len
        ));
    }
    output
}

pub(crate) fn parse_topic_checkpoint(raw: &str) -> io::Result<TopicCheckpoint> {
    let mut lines = raw.lines();
    let Some(version_line) = lines.next() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty checkpoint",
        ));
    };
    if version_line.trim() != CHECKPOINT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported checkpoint version",
        ));
    }

    let mut segments = Vec::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let mut parts = line.split(',');
        let base_offset = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing base_offset"))?
            .parse::<u64>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid base_offset"))?;
        let message_count = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing message_count"))?
            .parse::<u64>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid message_count"))?;
        let valid_len = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing valid_len"))?
            .parse::<u64>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid valid_len"))?;
        if parts.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "too many checkpoint fields",
            ));
        }
        segments.push(SegmentCheckpoint {
            base_offset,
            message_count,
            valid_len,
        });
    }

    Ok(TopicCheckpoint { segments })
}
