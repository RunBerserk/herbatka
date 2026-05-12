//! Wall-clock helpers at the OS boundary.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current time as **Unix epoch milliseconds** (`u64`), for [`crate::log::message::Message::timestamp`].
///
/// Uses [`SystemTime::now`] then converts to whole milliseconds. Times before the Unix epoch
/// clamp to `0`, matching the previous persistence encode behavior (`duration_since` failure → 0 ms).
pub fn now_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
