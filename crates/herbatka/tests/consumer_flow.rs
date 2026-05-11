//! Integration tests for consumer-style broker reads.
//!
//! Verifies ordered batch fetching and offset progression using the broker API.

use herbatka::broker::core::Broker;
use herbatka::log::message::Message;
use std::collections::HashMap;
use std::fs::create_dir_all;
use std::time::{SystemTime, UNIX_EPOCH};

fn message(payload: &[u8]) -> Message {
    Message {
        key: None,
        payload: payload.to_vec(),
        timestamp: SystemTime::now(),
        headers: HashMap::new(),
    }
}

#[test]
fn consumer_drains_topic_in_order() {
    //GIVEN — isolated data_dir so a leftover `data/logs/events.log` cannot affect this test.
    let dir = std::env::temp_dir().join(format!(
        "herbatka_consumer_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    create_dir_all(&dir).unwrap();
    let mut broker = Broker::with_data_dir(dir);
    broker.create_topic("events".into()).unwrap();

    broker.produce("events", message(b"m1")).unwrap();
    broker.produce("events", message(b"m2")).unwrap();
    broker.produce("events", message(b"m3")).unwrap();

    //WHEN
    let mut next_offset = 0u64;
    let mut seen = Vec::new();

    loop {
        let batch = broker
            .fetch_batch("events", next_offset, 2) // small batches on purpose
            .unwrap();

        if batch.is_empty() {
            break;
        }

        for msg in &batch {
            seen.push(msg.payload.clone());
        }

        next_offset += batch.len() as u64;
    }
    //THEN
    assert_eq!(seen, vec![b"m1".to_vec(), b"m2".to_vec(), b"m3".to_vec()]);
    assert_eq!(next_offset, 3); // high-water: next read would be at 3 (empty)
}
