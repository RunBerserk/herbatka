use std::{collections::HashMap, time::SystemTime};

pub struct Message {
    /// Optional key for partitioning / ordering (e.g. car_id)
    pub key: Option<Vec<u8>>,
    /// Raw payload (e.g. Protobuf, JSON)
    pub payload: Vec<u8>,
    pub timestamp: SystemTime,
    pub headers: HashMap<String, Vec<u8>>, //eg. 0 heartbeat,1 controll...
}

pub struct LogEntry {
    /// Monotonically increasing offset
    pub offset: u64,

    /// The actual message
    pub message: Message,
}
