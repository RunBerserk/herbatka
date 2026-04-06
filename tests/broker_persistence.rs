//! Integration test: produce persists to disk and a new broker reloads the topic.
//!
//! After restart, only the fresh broker calls `create_topic` once (empty `topics`), matching
//! real process restart — a second `create_topic` on the same in-memory broker would return
//! `TopicAlreadyExists`.

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
fn produce_survives_broker_restart() {
    let dir = std::env::temp_dir().join(format!(
        "herbatka_persist_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    create_dir_all(&dir).unwrap();

    let mut broker = Broker::with_data_dir(dir.clone());
    broker.create_topic("t".into()).unwrap();

    broker.produce("t", message(b"first")).unwrap();
    broker.produce("t", message(b"second")).unwrap();

    let mut restarted = Broker::with_data_dir(dir);
    restarted.create_topic("t".into()).unwrap();

    assert_eq!(
        restarted.fetch("t", 0).unwrap().unwrap().payload,
        b"first".to_vec()
    );
    assert_eq!(
        restarted.fetch("t", 1).unwrap().unwrap().payload,
        b"second".to_vec()
    );

    let batch = restarted.fetch_batch("t", 0, 10).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].payload, b"first".to_vec());
    assert_eq!(batch[1].payload, b"second".to_vec());
}
