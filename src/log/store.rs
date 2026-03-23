use crate::log::message::{LogEntry, Message};

pub struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn append(&mut self, message: Message) -> u64 {
        // returns assigned offset
        let offset = self.entries.len() as u64;
        self.entries.push(LogEntry { offset, message });
        offset
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn read(&self, offset: u64) -> Option<&Message> {
        // None if out of range
        self.entries
            .get(offset as usize)
            .map(|entry| &entry.message)
    }

    pub fn read_range(&self, offset: u64, limit: usize) -> Vec<&Message> {
        if limit == 0 {
            return Vec::new();
        }
        let start = offset as usize;
        if start >= self.entries.len() {
            return Vec::new();
        }
        let end = start.saturating_add(limit).min(self.entries.len());
        self.entries[start..end]
            .iter()
            .map(|entry| &entry.message)
            .collect()
    }
}

impl Default for Log {
    fn default() -> Self {
        Self::new()
    }
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
