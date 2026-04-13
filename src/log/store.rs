//! In-memory append-only log state.
//!
//! Owns entries and offset-based reads; broker uses it as runtime state.
//! Recovery can replay persisted messages into this structure.

use std::{
    fs::{OpenOptions, create_dir_all},
    io::{self, Read, Write},
    path::Path,
};

use crate::config::FsyncPolicy;
use crate::log::{
    message::{LogEntry, Message},
    persistence::{encode_message, read_message, write_message},
};

pub struct Log {
    base_offset: u64,
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        Self::with_base_offset(0)
    }

    pub fn with_base_offset(base_offset: u64) -> Self {
        Self {
            base_offset,
            entries: Vec::new(),
        }
    }

    pub fn append(&mut self, message: Message) -> u64 {
        // returns assigned offset
        let offset = self.next_offset();
        self.entries.push(LogEntry { offset, message });
        offset
    }

    pub fn drop_prefix(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        let dropped = count.min(self.entries.len());
        self.entries.drain(0..dropped);
        self.base_offset += dropped as u64;
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn read(&self, offset: u64) -> Option<&Message> {
        // None if out of range
        if offset < self.base_offset {
            return None;
        }
        let index = (offset - self.base_offset) as usize;
        self.entries.get(index).map(|entry| &entry.message)
    }

    pub fn read_range(&self, offset: u64, limit: usize) -> Vec<&Message> {
        if limit == 0 {
            return Vec::new();
        }
        if offset < self.base_offset {
            return Vec::new();
        }
        let start = (offset - self.base_offset) as usize;
        if start >= self.entries.len() {
            return Vec::new();
        }
        let end = start.saturating_add(limit).min(self.entries.len());
        self.entries[start..end]
            .iter()
            .map(|entry| &entry.message)
            .collect()
    }

    pub fn next_offset(&self) -> u64 {
        self.base_offset + self.entries.len() as u64
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn load_from_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        Self::load_from_reader_with_base(reader, 0)
    }

    pub fn load_from_reader_with_base<R: Read>(
        reader: &mut R,
        base_offset: u64,
    ) -> io::Result<Self> {
        let mut log = Self::new();
        log.base_offset = base_offset;
        while let Some(message) = read_message(reader)? {
            log.append(message);
        }
        Ok(log)
    }
}

impl Default for Log {
    fn default() -> Self {
        Self::new()
    }
}

pub fn append_to_segment_file(
    path: &Path,
    message: &Message,
    fsync_policy: FsyncPolicy,
) -> io::Result<u64> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let bytes_written = estimate_record_size(message)? as u64;
    write_message(&mut file, message)?;
    file.flush()?;
    if matches!(fsync_policy, FsyncPolicy::Always) {
        file.sync_all()?;
    }
    Ok(bytes_written)
}

pub fn estimate_record_size(message: &Message) -> io::Result<usize> {
    let encoded = encode_message(message)?;
    Ok(4 + encoded.len())
}

#[cfg(test)]
mod tests {
    use crate::log::message::Message;

    use super::*;
    use std::collections::HashMap;
    use std::time::SystemTime;

    #[test]
    fn append_returns_offset_zero_for_first_message() {
        //GIVEN
        let mut log = Log::new();

        let msg = Message {
            key: None,
            payload: b"hello".to_vec(),
            timestamp: SystemTime::now(),
            headers: HashMap::new(),
        };
        //WHEN
        let offset = log.append(msg);
        //THEN
        assert_eq!(offset, 0);
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn read_returns_correct_message_by_offset() {
        //GIVEN
        let mut log = Log::new();
        let offset = log.append(Message {
            key: None,
            payload: b"hello".to_vec(),
            timestamp: SystemTime::now(),
            headers: HashMap::new(),
        });
        //WHEN
        let msg = log.read(offset).expect("message should exist");
        //THEN
        assert_eq!(msg.payload, b"hello".to_vec());
    }

    #[test]
    fn messages_are_returned_in_order() {
        //GIVEN
        let mut log = Log::new();

        let offset1 = log.append(Message {
            key: None,
            payload: b"first".to_vec(),
            timestamp: SystemTime::now(),
            headers: HashMap::new(),
        });

        let offset2 = log.append(Message {
            key: None,
            payload: b"second".to_vec(),
            timestamp: SystemTime::now(),
            headers: HashMap::new(),
        });
        //WHEN
        let m1 = log.read(offset1).expect("first message should exist");
        let m2 = log.read(offset2).expect("second message should exist");
        //THEN
        assert_eq!(m1.payload, b"first".to_vec());
        assert_eq!(m2.payload, b"second".to_vec());
    }
}
