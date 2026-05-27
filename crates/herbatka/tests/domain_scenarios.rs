//! Domain hardening: framed TCP produce/fetch with non-fleet JSON payloads.
//!
//! Car fleet is covered elsewhere (`fleet_protobuf_roundtrip`, simulator/UI).

use herbatka::broker::core::Broker;
use herbatka::tcp::command::Response;
use herbatka::tcp::frame::{
    HANDSHAKE_CLIENT_V1, decode_response_frame, encode_fetch, encode_produce, read_frame,
};
use herbatka::tcp::server::{SharedBroker, handle_client};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

fn tcp_test_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "herbatka_domain_{}_{}_{}",
        label,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos()
    ))
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

fn framed_produce_fetch_roundtrip(addr: SocketAddr, topic: &str, payloads: &[&[u8]]) {
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

    for (i, expected) in payloads.iter().enumerate() {
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

    let past_tail = encode_fetch(topic, payloads.len() as u64).expect("encode fetch tail");
    client.write_all(&past_tail).expect("fetch tail write");
    client.flush().expect("fetch tail flush");
    let resp = read_frame(&mut reader).expect("fetch none response");
    assert_eq!(
        decode_response_frame(&resp).expect("decode fetch none"),
        Response::None
    );
}

fn run_domain_roundtrip(label: &str, topic: &str, payloads: Vec<Vec<u8>>) {
    let dir = tcp_test_dir(label);
    let broker = Arc::new(Broker::with_data_dir(dir));
    let (server_thread, addr) = spawn_test_server(Arc::clone(&broker));

    let payload_refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
    framed_produce_fetch_roundtrip(addr, topic, &payload_refs);

    drop(broker);
    server_thread.join().expect("server thread should join");
}

#[test]
fn tcp_framed_stock_quotes_roundtrip() {
    let payloads = vec![
        br#"{"symbol":"AAPL","price":182.5,"ts_ms":1700000000000}"#.to_vec(),
        br#"{"symbol":"MSFT","price":420.0,"ts_ms":1700000000100}"#.to_vec(),
    ];
    run_domain_roundtrip("stock", "demo.market.quotes", payloads);
}

#[test]
fn tcp_framed_logistics_shipments_roundtrip() {
    let payloads = vec![
        br#"{"shipment_id":"ship-1","status":"in_transit","hub_id":"HAM-01","ts_ms":1700000000000}"#
            .to_vec(),
        br#"{"shipment_id":"ship-2","status":"delivered","hub_id":"BER-02","ts_ms":1700000000100}"#
            .to_vec(),
    ];
    run_domain_roundtrip("logistics", "demo.logistics.shipments", payloads);
}
