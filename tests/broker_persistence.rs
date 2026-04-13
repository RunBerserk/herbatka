//! Integration test: produce persists to disk and a new broker reloads the topic.
//!
//! Startup discovery should load persisted topic logs without manual `create_topic` calls.

use herbatka::broker::core::{Broker, BrokerError};
use herbatka::config::{BrokerConfig, FsyncPolicy};
use herbatka::log::message::Message;
use std::collections::HashMap;
use std::fs::{File, create_dir_all, read_dir};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn message(payload: &[u8]) -> Message {
    Message {
        key: None,
        payload: payload.to_vec(),
        timestamp: SystemTime::now(),
        headers: HashMap::new(),
    }
}

fn temp_data_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "{prefix}_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn produce_survives_broker_restart() {
    let dir = temp_data_dir("herbatka_persist");

    let mut broker = Broker::with_data_dir(dir.clone());
    broker.create_topic("t".into()).unwrap();

    broker.produce("t", message(b"first")).unwrap();
    broker.produce("t", message(b"second")).unwrap();

    let mut restarted = Broker::with_data_dir(dir);
    restarted.discover_topics_on_startup().unwrap();

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

#[test]
fn startup_discovery_loads_multiple_topics() {
    let dir = temp_data_dir("herbatka_persist_multi");
    let mut broker = Broker::with_data_dir(dir.clone());
    broker.create_topic("a".into()).unwrap();
    broker.create_topic("b".into()).unwrap();
    broker.produce("a", message(b"one")).unwrap();
    broker.produce("b", message(b"two")).unwrap();

    let mut restarted = Broker::with_data_dir(dir);
    restarted.discover_topics_on_startup().unwrap();

    assert_eq!(restarted.fetch("a", 0).unwrap().unwrap().payload, b"one");
    assert_eq!(restarted.fetch("b", 0).unwrap().unwrap().payload, b"two");
}

#[test]
fn startup_discovery_ignores_non_log_files() {
    let dir = temp_data_dir("herbatka_persist_ignore");
    let mut broker = Broker::with_data_dir(dir.clone());
    broker.create_topic("events".into()).unwrap();
    broker.produce("events", message(b"hello")).unwrap();

    let mut random_file = File::create(dir.join("notes.txt")).unwrap();
    random_file.write_all(b"not a topic log").unwrap();

    let mut restarted = Broker::with_data_dir(dir);
    restarted.discover_topics_on_startup().unwrap();

    assert_eq!(
        restarted.fetch("events", 0).unwrap().unwrap().payload,
        b"hello"
    );
    assert!(matches!(
        restarted.fetch("notes", 0),
        Err(BrokerError::UnknownTopic)
    ));
}

#[test]
fn startup_discovery_is_noop_for_missing_data_dir() {
    let dir = std::env::temp_dir().join(format!(
        "herbatka_missing_dir_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut broker = Broker::with_data_dir(dir);
    broker.discover_topics_on_startup().unwrap();
}

#[test]
fn restart_replays_multiple_segments_in_order() {
    let dir = temp_data_dir("herbatka_segments_restart");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 80,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();
    let big = vec![b'z'; 64];
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();

    let mut restarted = Broker::with_config(cfg);
    restarted.discover_topics_on_startup().unwrap();
    assert!(restarted.fetch("events", 0).unwrap().is_some());
    assert!(restarted.fetch("events", 1).unwrap().is_some());
    assert!(restarted.fetch("events", 2).unwrap().is_some());
}

#[test]
fn segment_rollover_creates_multiple_files() {
    let dir = temp_data_dir("herbatka_segments_files");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 80,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg);
    broker.create_topic("events".into()).unwrap();
    let big = vec![b'a'; 64];
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();

    let topic_dir = dir.join("events");
    let files = read_dir(topic_dir).unwrap().count();
    assert!(files >= 2);
}

#[test]
fn retention_evicts_old_offsets_when_max_topic_bytes_is_set() {
    let dir = temp_data_dir("herbatka_retention_offsets");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 80,
        max_topic_bytes: Some(140),
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg);
    broker.create_topic("events".into()).unwrap();
    let big = vec![b'b'; 64];
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();

    assert!(broker.fetch("events", 0).unwrap().is_none());
    assert!(broker.fetch("events", 2).unwrap().is_some());
}
