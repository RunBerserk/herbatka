//! Framed TCP produce/fetch across broker restart on the same data directory.
//!
//! Deep startup recovery (tail truncate, checkpoint/index fallback) lives in
//! `broker_persistence.rs`; this file closes the external-client gap only.

use herbatka::broker::core::Broker;
use herbatka::log::message::Message;
use herbatka::tcp::command::Response;
use herbatka::tcp::frame::{
    HANDSHAKE_CLIENT_V1, decode_response_frame, encode_fetch, encode_produce, read_frame,
};
use herbatka::tcp::server::{SharedBroker, handle_client};
use herbatka::time::now_epoch_millis;
use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

fn tcp_test_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "herbatka_recovery_{}_{}_{}",
        label,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos()
    ));
    create_dir_all(&dir).expect("create data dir");
    dir
}

fn spawn_test_server(broker: SharedBroker) -> (thread::JoinHandle<()>, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind should succeed");
    let addr = listener.local_addr().expect("local addr should exist");
    let broker_for_thread = Arc::clone(&broker);
    let server_thread = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept should succeed");
        handle_client(stream, &broker_for_thread).expect("client handling should succeed");
    });
    (server_thread, addr)
}

fn perform_wire_handshake(client: &mut TcpStream, reader: &mut BufReader<TcpStream>) {
    client
        .write_all(HANDSHAKE_CLIENT_V1)
        .expect("handshake write");
    client.flush().expect("flush handshake");
    let mut ack = String::new();
    reader.read_line(&mut ack).expect("ack read");
    assert_eq!(
        ack.trim_end_matches(['\r', '\n']),
        "HERBATKA OK/1",
        "unexpected ack: {ack:?}"
    );
}

fn framed_produce_all(addr: SocketAddr, topic: &str, payloads: &[&[u8]]) {
    let mut client = TcpStream::connect(addr).expect("connect should succeed");
    let mut reader = BufReader::new(client.try_clone().expect("clone should succeed"));
    perform_wire_handshake(&mut client, &mut reader);

    for (i, body) in payloads.iter().enumerate() {
        let frame = encode_produce(topic, body).expect("encode produce");
        client.write_all(&frame).expect("produce write");
        client.flush().expect("produce flush");
        let resp = read_frame(&mut reader).expect("produce response");
        assert_eq!(
            decode_response_frame(&resp).expect("decode produce response"),
            Response::OkOffset(i as u64),
            "produce offset mismatch at index {i}"
        );
    }
}

fn framed_fetch_all(addr: SocketAddr, topic: &str, expected_payloads: &[&[u8]]) {
    let mut client = TcpStream::connect(addr).expect("connect should succeed");
    let mut reader = BufReader::new(client.try_clone().expect("clone should succeed"));
    perform_wire_handshake(&mut client, &mut reader);

    for (i, expected) in expected_payloads.iter().enumerate() {
        let frame = encode_fetch(topic, i as u64).expect("encode fetch");
        client.write_all(&frame).expect("fetch write");
        client.flush().expect("fetch flush");
        let resp = read_frame(&mut reader).expect("fetch response");
        match decode_response_frame(&resp).expect("decode fetch response") {
            Response::Message { offset, payload } => {
                assert_eq!(offset, i as u64);
                assert_eq!(payload.as_slice(), *expected);
            }
            other => panic!("expected Message at offset {i}: {other:?}"),
        }
    }

    let past_tail = encode_fetch(topic, expected_payloads.len() as u64).expect("encode fetch tail");
    client.write_all(&past_tail).expect("fetch tail write");
    client.flush().expect("fetch tail flush");
    let resp = read_frame(&mut reader).expect("fetch none response");
    assert_eq!(
        decode_response_frame(&resp).expect("decode fetch none"),
        Response::None
    );
}

fn restart_broker(dir: PathBuf) -> SharedBroker {
    let mut broker = Broker::with_data_dir(dir);
    broker
        .discover_topics_on_startup()
        .expect("startup discovery should succeed");
    Arc::new(RwLock::new(broker))
}

fn message(payload: &[u8]) -> Message {
    Message {
        key: None,
        payload: payload.to_vec(),
        timestamp: now_epoch_millis(),
        headers: HashMap::new(),
    }
}

fn topic_segment_files(dir: &Path, topic: &str) -> Vec<PathBuf> {
    let topic_dir = dir.join(topic);
    let mut files: Vec<PathBuf> = read_dir(topic_dir)
        .expect("read topic dir")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("log"))
        .collect();
    files.sort();
    files
}

#[test]
fn tcp_framed_produce_survives_broker_restart() {
    let dir = tcp_test_dir("restart_produce");
    let payloads: &[&[u8]] = &[b"alpha", b"beta"];
    let topic = "recovery.t";

    let broker_a = Arc::new(RwLock::new(Broker::with_data_dir(dir.clone())));
    let (server_a, addr_a) = spawn_test_server(Arc::clone(&broker_a));
    framed_produce_all(addr_a, topic, payloads);
    drop(broker_a);
    server_a.join().expect("server A should join");

    let broker_b = restart_broker(dir);
    let (server_b, addr_b) = spawn_test_server(Arc::clone(&broker_b));
    framed_fetch_all(addr_b, topic, payloads);
    drop(broker_b);
    server_b.join().expect("server B should join");
}

#[test]
fn tcp_framed_fetch_after_tail_truncation_on_restart() {
    let dir = tcp_test_dir("restart_tail");
    let topic = "recovery.events";

    let mut broker = Broker::with_data_dir(dir.clone());
    broker.create_topic(topic.into()).expect("create topic");
    broker
        .produce(topic, message(b"ok-1"))
        .expect("produce ok-1");
    broker
        .produce(topic, message(b"ok-2"))
        .expect("produce ok-2");

    let segments = topic_segment_files(&dir, topic);
    let segment_path = segments.last().expect("segment should exist").clone();
    let clean_len = std::fs::metadata(&segment_path)
        .expect("segment metadata")
        .len();

    let mut file = OpenOptions::new()
        .append(true)
        .open(&segment_path)
        .expect("append should succeed");
    file.write_all(&10u32.to_le_bytes())
        .expect("tail len write");
    file.write_all(&[1u8, 2u8, 3u8])
        .expect("partial tail write");
    drop(file);

    assert!(
        std::fs::metadata(&segment_path).expect("metadata").len() > clean_len,
        "tail should be corrupted"
    );

    let shared = restart_broker(dir.clone());
    let (server, addr) = spawn_test_server(Arc::clone(&shared));
    framed_fetch_all(addr, topic, &[b"ok-1", b"ok-2"]);
    drop(shared);
    server.join().expect("server should join");

    assert_eq!(
        std::fs::metadata(&segment_path).expect("metadata").len(),
        clean_len,
        "segment should be truncated to last valid record"
    );
}
