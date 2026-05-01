//! Integration test: produce persists to disk and a new broker reloads the topic.
//!
//! Startup discovery should load persisted topic logs without manual `create_topic` calls.

use herbatka::broker::core::{Broker, BrokerError};
use herbatka::config::{BrokerConfig, FsyncPolicy};
use herbatka::log::message::Message;
use std::collections::HashMap;
use std::fs::{File, OpenOptions, create_dir_all, read_dir};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

fn topic_segment_files(dir: &std::path::Path, topic: &str) -> Vec<PathBuf> {
    let topic_dir = dir.join(topic);
    // Test helper: panic on setup/read failures is intentional so broken
    // filesystem preconditions fail the test immediately and loudly.
    let mut files: Vec<PathBuf> = read_dir(topic_dir)
        .unwrap()
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("log"))
        .collect();
    files.sort();
    files
}

fn topic_checkpoint_path(dir: &std::path::Path, topic: &str) -> PathBuf {
    dir.join(topic).join(".checkpoint")
}

fn topic_index_files(dir: &std::path::Path, topic: &str) -> Vec<PathBuf> {
    let topic_dir = dir.join(topic);
    let mut files: Vec<PathBuf> = read_dir(topic_dir)
        .unwrap()
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("idx"))
        .collect();
    files.sort();
    files
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
    // Trusted closed segments may be metadata-skipped on startup.
    assert!(restarted.fetch("events", 0).unwrap().is_none());
    assert!(restarted.fetch("events", 1).unwrap().is_none());
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

#[test]
fn startup_truncates_partial_tail_and_recovers() {
    let dir = temp_data_dir("herbatka_tail_truncate");
    let mut broker = Broker::with_data_dir(dir.clone());
    broker.create_topic("events".into()).unwrap();
    broker.produce("events", message(b"ok-1")).unwrap();
    broker.produce("events", message(b"ok-2")).unwrap();

    let segments = topic_segment_files(&dir, "events");
    let segment_path = segments.last().expect("segment should exist").clone();
    let clean_len = std::fs::metadata(&segment_path).unwrap().len();

    let mut file = OpenOptions::new()
        .append(true)
        .open(&segment_path)
        .expect("append should succeed");
    file.write_all(&10u32.to_le_bytes())
        .expect("tail len write should succeed");
    file.write_all(&[1u8, 2u8, 3u8])
        .expect("partial tail write should succeed");
    drop(file);

    let corrupted_len = std::fs::metadata(&segment_path).unwrap().len();
    assert!(corrupted_len > clean_len);

    let mut restarted = Broker::with_data_dir(dir);
    restarted.discover_topics_on_startup().unwrap();

    assert_eq!(
        restarted.fetch("events", 0).unwrap().unwrap().payload,
        b"ok-1".to_vec()
    );
    assert_eq!(
        restarted.fetch("events", 1).unwrap().unwrap().payload,
        b"ok-2".to_vec()
    );
    assert!(restarted.fetch("events", 2).unwrap().is_none());

    let recovered_len = std::fs::metadata(&segment_path).unwrap().len();
    assert_eq!(recovered_len, clean_len);
}

#[test]
fn startup_keeps_failing_on_invalid_data_tail() {
    let dir = temp_data_dir("herbatka_tail_invalid");
    let mut broker = Broker::with_data_dir(dir.clone());
    broker.create_topic("events".into()).unwrap();
    broker.produce("events", message(b"ok-1")).unwrap();

    let segments = topic_segment_files(&dir, "events");
    let segment_path = segments.last().expect("segment should exist").clone();
    let mut file = OpenOptions::new()
        .append(true)
        .open(&segment_path)
        .expect("append should succeed");

    // Encoded message with one header key byte 0xFF to force InvalidData ("invalid utf8 key").
    let mut invalid_record = Vec::new();
    invalid_record.extend_from_slice(&0u64.to_le_bytes()); // timestamp
    invalid_record.extend_from_slice(&u32::MAX.to_le_bytes()); // key = None sentinel
    invalid_record.extend_from_slice(&0u32.to_le_bytes()); // payload length
    invalid_record.extend_from_slice(&1u32.to_le_bytes()); // header count
    invalid_record.extend_from_slice(&1u32.to_le_bytes()); // header key length
    invalid_record.push(0xFF); // invalid utf-8 key byte
    invalid_record.extend_from_slice(&0u32.to_le_bytes()); // header value length
    file.write_all(&(invalid_record.len() as u32).to_le_bytes())
        .expect("frame len write should succeed");
    file.write_all(&invalid_record)
        .expect("invalid frame write should succeed");
    drop(file);

    let mut restarted = Broker::with_data_dir(dir);
    match restarted.discover_topics_on_startup() {
        Err(BrokerError::Io(e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        }
        other => panic!("expected invalid-data startup failure, got: {other:?}"),
    }
}

#[test]
fn startup_clean_segment_no_truncation() {
    let dir = temp_data_dir("herbatka_tail_clean");
    let mut broker = Broker::with_data_dir(dir.clone());
    broker.create_topic("events".into()).unwrap();
    broker.produce("events", message(b"ok-1")).unwrap();
    broker.produce("events", message(b"ok-2")).unwrap();

    let segments = topic_segment_files(&dir, "events");
    let segment_path = segments.last().expect("segment should exist").clone();
    let before = std::fs::metadata(&segment_path).unwrap().len();

    let mut restarted = Broker::with_data_dir(dir);
    restarted.discover_topics_on_startup().unwrap();

    let after = std::fs::metadata(&segment_path).unwrap().len();
    assert_eq!(before, after);
    assert_eq!(
        restarted.fetch("events", 0).unwrap().unwrap().payload,
        b"ok-1".to_vec()
    );
    assert_eq!(
        restarted.fetch("events", 1).unwrap().unwrap().payload,
        b"ok-2".to_vec()
    );
}

#[test]
fn checkpoint_file_is_written_and_restart_still_recovers() {
    let dir = temp_data_dir("herbatka_checkpoint_written");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 80,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();
    let big = vec![b'c'; 64];
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();

    let checkpoint_path = topic_checkpoint_path(&dir, "events");
    let checkpoint_raw =
        std::fs::read_to_string(&checkpoint_path).expect("checkpoint should exist");
    assert!(checkpoint_raw.starts_with("v1\n"));

    let mut restarted = Broker::with_config(cfg);
    restarted.discover_topics_on_startup().unwrap();
    assert!(restarted.fetch("events", 0).unwrap().is_none());
    assert!(restarted.fetch("events", 1).unwrap().is_none());
    assert!(restarted.fetch("events", 2).unwrap().is_some());
}

#[test]
fn stale_or_invalid_checkpoint_falls_back_to_safe_replay() {
    let dir = temp_data_dir("herbatka_checkpoint_fallback");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 80,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();
    let big = vec![b'd'; 64];
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();

    let checkpoint_path = topic_checkpoint_path(&dir, "events");
    let stale = "v1\n0,1,999999\n";
    std::fs::write(&checkpoint_path, stale).expect("write stale checkpoint");

    let mut restarted = Broker::with_config(cfg.clone());
    restarted.discover_topics_on_startup().unwrap();
    assert!(restarted.fetch("events", 0).unwrap().is_some());
    assert!(restarted.fetch("events", 1).unwrap().is_some());
    assert!(restarted.fetch("events", 2).unwrap().is_some());

    std::fs::write(&checkpoint_path, "not-a-valid-checkpoint").expect("write invalid checkpoint");
    let mut restarted_again = Broker::with_config(cfg);
    restarted_again.discover_topics_on_startup().unwrap();
    assert!(restarted_again.fetch("events", 0).unwrap().is_some());
    assert!(restarted_again.fetch("events", 1).unwrap().is_some());
    assert!(restarted_again.fetch("events", 2).unwrap().is_some());
}

#[test]
fn sparse_index_sidecar_is_created() {
    let dir = temp_data_dir("herbatka_sparse_index_created");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 80,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg);
    broker.create_topic("events".into()).unwrap();
    let big = vec![b'e'; 64];
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();

    let index_files = topic_index_files(&dir, "events");
    assert!(!index_files.is_empty(), "index sidecar files should exist");
}

#[test]
fn startup_skips_all_eligible_closed_segments() {
    let dir = temp_data_dir("herbatka_sparse_index_skip_closed");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 80,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();
    let big = vec![b'h'; 64];
    for _ in 0..5 {
        broker.produce("events", message(&big)).unwrap();
    }

    let mut restarted = Broker::with_config(cfg);
    restarted.discover_topics_on_startup().unwrap();
    assert!(restarted.fetch("events", 0).unwrap().is_none());
    assert!(restarted.fetch("events", 1).unwrap().is_none());
    assert!(restarted.fetch("events", 2).unwrap().is_none());
    assert!(restarted.fetch("events", 3).unwrap().is_none());
    assert!(restarted.fetch("events", 4).unwrap().is_some());
    let visible_from_tail = restarted.fetch_batch("events", 4, 10).unwrap();
    assert_eq!(
        visible_from_tail.len(),
        1,
        "tail segment should remain readable at its offset"
    );
}

#[test]
fn startup_mixed_skip_and_fallback_keeps_replayed_offsets_visible() {
    let dir = temp_data_dir("herbatka_sparse_index_mixed");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 80,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();
    let big = vec![b'i'; 64];
    for _ in 0..4 {
        broker.produce("events", message(&big)).unwrap();
    }

    let mut index_files = topic_index_files(&dir, "events");
    index_files.sort();
    let second_segment_index = index_files
        .get(1)
        .expect("second segment index should exist")
        .clone();
    std::fs::write(second_segment_index, "corrupt-index").expect("write corrupt index");

    let mut restarted = Broker::with_config(cfg);
    restarted.discover_topics_on_startup().unwrap();
    assert!(restarted.fetch("events", 0).unwrap().is_none());
    assert!(restarted.fetch("events", 1).unwrap().is_some());
    assert!(restarted.fetch("events", 2).unwrap().is_some());
    assert!(restarted.fetch("events", 3).unwrap().is_some());
}

#[test]
fn startup_tail_recovery_still_truncates_when_closed_segments_are_skipped() {
    let dir = temp_data_dir("herbatka_sparse_index_tail_safety");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 80,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();
    let big = vec![b'j'; 64];
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();

    let segments = topic_segment_files(&dir, "events");
    assert!(segments.len() >= 3, "expected multiple segments");
    let tail_segment = segments.last().expect("tail segment should exist").clone();
    let clean_len = std::fs::metadata(&tail_segment).unwrap().len();

    let mut file = OpenOptions::new()
        .append(true)
        .open(&tail_segment)
        .expect("append should succeed");
    file.write_all(&10u32.to_le_bytes())
        .expect("tail len write should succeed");
    file.write_all(&[1u8, 2u8, 3u8])
        .expect("partial tail write should succeed");
    drop(file);

    let mut restarted = Broker::with_config(cfg);
    restarted.discover_topics_on_startup().unwrap();

    assert!(restarted.fetch("events", 0).unwrap().is_none());
    assert!(restarted.fetch("events", 1).unwrap().is_none());
    assert!(restarted.fetch("events", 2).unwrap().is_some());
    assert!(restarted.fetch("events", 3).unwrap().is_none());

    let recovered_len = std::fs::metadata(&tail_segment).unwrap().len();
    assert_eq!(
        recovered_len, clean_len,
        "tail segment should still be truncated back to last good position"
    );
}

#[test]
fn startup_tail_partial_replay_uses_index_hint_and_preserves_offsets() {
    let dir = temp_data_dir("herbatka_tail_partial_replay");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 1_000_000,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();
    for i in 0..130 {
        let payload = format!("msg-{i}");
        broker
            .produce("events", message(payload.as_bytes()))
            .unwrap();
    }
    let tail_segment = topic_segment_files(&dir, "events")
        .last()
        .expect("tail segment should exist")
        .clone();
    let valid_len = std::fs::metadata(&tail_segment).unwrap().len();
    let checkpoint_path = topic_checkpoint_path(&dir, "events");
    std::fs::write(&checkpoint_path, format!("v1\n0,130,{valid_len}\n"))
        .expect("write checkpoint should succeed");

    let mut restarted = Broker::with_config(cfg);
    restarted.discover_topics_on_startup().unwrap();

    // With index-assisted tail replay, earlier offsets may be skipped in-memory,
    // but latest tail suffix must remain readable and ordered.
    assert!(restarted.fetch("events", 0).unwrap().is_none());
    assert_eq!(
        restarted.fetch("events", 128).unwrap().unwrap().payload,
        b"msg-128".to_vec()
    );
    assert_eq!(
        restarted.fetch("events", 129).unwrap().unwrap().payload,
        b"msg-129".to_vec()
    );
}

#[test]
fn startup_tail_partial_replay_falls_back_when_checkpoint_missing() {
    let dir = temp_data_dir("herbatka_tail_partial_fallback_checkpoint");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 1_000_000,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();
    for i in 0..130 {
        let payload = format!("msg-{i}");
        broker
            .produce("events", message(payload.as_bytes()))
            .unwrap();
    }
    let checkpoint_path = topic_checkpoint_path(&dir, "events");
    std::fs::remove_file(&checkpoint_path).expect("remove checkpoint should succeed");

    let mut restarted = Broker::with_config(cfg);
    restarted.discover_topics_on_startup().unwrap();

    // Missing checkpoint should force full tail replay fallback.
    assert_eq!(
        restarted.fetch("events", 0).unwrap().unwrap().payload,
        b"msg-0".to_vec()
    );
    assert_eq!(
        restarted.fetch("events", 129).unwrap().unwrap().payload,
        b"msg-129".to_vec()
    );
}

#[test]
fn startup_tail_partial_replay_keeps_corruption_truncation_safety() {
    let dir = temp_data_dir("herbatka_tail_partial_truncate");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 1_000_000,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();
    for i in 0..130 {
        let payload = format!("msg-{i}");
        broker
            .produce("events", message(payload.as_bytes()))
            .unwrap();
    }

    let segments = topic_segment_files(&dir, "events");
    let tail_segment = segments.last().expect("tail segment should exist").clone();
    let clean_len = std::fs::metadata(&tail_segment).unwrap().len();
    let mut file = OpenOptions::new()
        .append(true)
        .open(&tail_segment)
        .expect("append should succeed");
    file.write_all(&10u32.to_le_bytes())
        .expect("tail len write should succeed");
    file.write_all(&[1u8, 2u8, 3u8])
        .expect("partial tail write should succeed");
    drop(file);

    let mut restarted = Broker::with_config(cfg);
    restarted.discover_topics_on_startup().unwrap();

    let recovered_len = std::fs::metadata(&tail_segment).unwrap().len();
    assert_eq!(recovered_len, clean_len);
    assert_eq!(
        restarted.fetch("events", 129).unwrap().unwrap().payload,
        b"msg-129".to_vec()
    );
    assert!(restarted.fetch("events", 130).unwrap().is_none());
}

/// After any segment full-replays, later **closed non-tail** segments use the same
/// checkpoint+sparse-index anchor path as tail replay (sparse seek + suffix decode), preserving
/// fetch visibility for replayed spans (see also `startup_mixed_skip_and_fallback_*`).
#[test]
fn startup_closed_must_replay_uses_sparse_seek_after_early_barrier() {
    let dir = temp_data_dir("herbatka_closed_sparse_seek_barrier");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 2000,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();
    for i in 0..260 {
        let payload = format!("msg-{i:04}");
        broker
            .produce("events", message(payload.as_bytes()))
            .unwrap();
    }

    let segments = topic_segment_files(&dir, "events");
    assert!(
        segments.len() >= 3,
        "expected >=3 segments for non-tail replay; got {}",
        segments.len()
    );
    let mut index_files = topic_index_files(&dir, "events");
    index_files.sort();

    std::fs::write(
        index_files
            .first()
            .expect("first sparse index exists for first segment"),
        "bad-index-root",
    )
    .unwrap();

    let mut restarted = Broker::with_config(cfg);
    restarted.discover_topics_on_startup().unwrap();

    assert_eq!(
        restarted.fetch("events", 259).unwrap().unwrap().payload,
        b"msg-0259".to_vec()
    );
}

#[test]
fn corrupt_or_missing_sparse_index_falls_back_safely() {
    let dir = temp_data_dir("herbatka_sparse_index_fallback");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 80,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();
    let big = vec![b'f'; 64];
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();

    let index_files = topic_index_files(&dir, "events");
    let first_index = index_files.first().expect("expected index file").clone();
    std::fs::write(&first_index, "invalid-index").expect("write corrupt index");

    let mut restarted = Broker::with_config(cfg.clone());
    restarted.discover_topics_on_startup().unwrap();
    assert!(restarted.fetch("events", 0).unwrap().is_some());
    assert!(restarted.fetch("events", 1).unwrap().is_some());
    assert!(restarted.fetch("events", 2).unwrap().is_some());

    let index_files_after_restart = topic_index_files(&dir, "events");
    let first_index_after_restart = index_files_after_restart
        .first()
        .expect("expected index file after restart")
        .clone();
    std::fs::remove_file(&first_index_after_restart).expect("remove index sidecar");

    let mut restarted_again = Broker::with_config(cfg);
    restarted_again.discover_topics_on_startup().unwrap();
    assert!(restarted_again.fetch("events", 0).unwrap().is_some());
    assert!(restarted_again.fetch("events", 1).unwrap().is_some());
    assert!(restarted_again.fetch("events", 2).unwrap().is_some());
}

#[test]
fn retention_removes_index_sidecars_with_segments() {
    let dir = temp_data_dir("herbatka_sparse_index_retention");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 80,
        max_topic_bytes: Some(140),
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg);
    broker.create_topic("events".into()).unwrap();
    let big = vec![b'g'; 64];
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();
    broker.produce("events", message(&big)).unwrap();

    let log_files = topic_segment_files(&dir, "events");
    let index_files = topic_index_files(&dir, "events");
    assert_eq!(
        log_files.len(),
        index_files.len(),
        "retained segment files should have matching index sidecars"
    );
}

#[test]
fn startup_large_dataset_restart_profile() {
    let dir = temp_data_dir("herbatka_startup_large_profile");
    let cfg = BrokerConfig {
        data_dir: dir.clone(),
        segment_max_bytes: 4 * 1024,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
    };
    let mut broker = Broker::with_config(cfg.clone());
    broker.create_topic("events".into()).unwrap();

    let message_count = 80_000u64;
    let payload = vec![b'p'; 128];
    let write_start = Instant::now();
    for _ in 0..message_count {
        broker.produce("events", message(&payload)).unwrap();
    }
    let write_elapsed = write_start.elapsed();
    eprintln!(
        "profile_marker: write_elapsed_ms={} messages={}",
        write_elapsed.as_millis(),
        message_count
    );

    let restart_start = Instant::now();
    let mut restarted = Broker::with_config(cfg);
    restarted.discover_topics_on_startup().unwrap();
    let restart_elapsed = restart_start.elapsed();
    eprintln!(
        "profile_marker: restart_elapsed_ms={}",
        restart_elapsed.as_millis()
    );

    assert!(
        restarted
            .fetch("events", message_count - 1)
            .unwrap()
            .is_some()
    );
    assert!(restarted.fetch("events", message_count).unwrap().is_none());
}
