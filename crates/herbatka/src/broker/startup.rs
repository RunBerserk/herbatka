use std::fs::OpenOptions;
use std::io::{self, Seek};
use std::path::Path;

use crate::log::persistence::read_message;
use crate::log::store::Log;
use tracing::warn;

pub(crate) struct ReplayOutcome {
    pub(crate) message_count: u64,
    pub(crate) valid_len: u64,
}

pub(crate) fn skip_trusted_closed_segment(
    path: &Path,
    log: &mut Log,
    message_count: u64,
) -> io::Result<ReplayOutcome> {
    let valid_len = std::fs::metadata(path)?.len();
    log.advance_base_offset(message_count);
    Ok(ReplayOutcome {
        message_count,
        valid_len,
    })
}

pub(crate) fn replay_segment_with_tail_recovery(
    topic: &str,
    path: &Path,
    log: &mut Log,
) -> io::Result<ReplayOutcome> {
    replay_segment_with_tail_recovery_from(topic, path, log, 0)
}

pub(crate) fn replay_segment_with_tail_recovery_from(
    topic: &str,
    path: &Path,
    log: &mut Log,
    start_pos: u64,
) -> io::Result<ReplayOutcome> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    file.seek(std::io::SeekFrom::Start(start_pos))?;
    let mut count = 0u64;
    let valid_len = loop {
        let last_good_pos = file.stream_position()?;
        match read_message(&mut file) {
            Ok(Some(message)) => {
                log.append(message);
                count += 1;
            }
            Ok(None) => break file.metadata()?.len(),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                let original_len = file.metadata()?.len();
                file.set_len(last_good_pos)?;
                warn!(
                    topic = %topic,
                    segment = %path.display(),
                    from_bytes = original_len,
                    to_bytes = last_good_pos,
                    "truncated corrupted tail during startup replay"
                );
                break last_good_pos;
            }
            Err(e) => return Err(e),
        }
    };

    Ok(ReplayOutcome {
        message_count: count,
        valid_len,
    })
}
