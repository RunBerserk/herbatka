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

    pub fn read(&self, offset: u64) -> Option<&Message> {
        // None if out of range
        self.entries
            .get(offset as usize)
            .map(|entry| &entry.message)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
