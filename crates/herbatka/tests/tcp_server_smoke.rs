use herbatka::broker::core::Broker;
use herbatka::tcp::command::Response;
use herbatka::tcp::frame::{
    HANDSHAKE_CLIENT_V1, HEADER_LEN, MAX_FRAME_PAYLOAD, OP_PRODUCE, WIRE_VERSION_V1,
    decode_response_frame, encode_fetch, encode_produce, encode_topic_bounds, read_frame,
};
use herbatka::tcp::server::{SharedBroker, handle_client, serve};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn tcp_test_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "herbatka_tcp_{}_{}_{}",
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

/// Like [`spawn_test_server`], but exposes `handle_client`'s `io::Result` (e.g. oversize first line).
fn spawn_test_server_returns_client_result(
    broker: SharedBroker,
) -> (thread::JoinHandle<std::io::Result<()>>, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind should succeed");
    let addr = listener.local_addr().expect("local addr should exist");
    let broker_for_thread = Arc::clone(&broker);
    let server_thread = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept should succeed");
        handle_client(stream, &broker_for_thread)
    });
    (server_thread, addr)
}

fn write_expect_line(
    client: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    write: &[u8],
    expect: &str,
) {
    client.write_all(write).expect("write should succeed");
    client.flush().expect("flush should succeed");
    let mut line = String::new();
    reader.read_line(&mut line).expect("read should succeed");
    assert_eq!(line, expect);
}

/// Parses `MSG <offset> <payload>` (payload may contain spaces).
fn msg_payload(trimmed: &str) -> String {
    let rest = trimmed.strip_prefix("MSG ").expect("expected MSG line");
    let mut parts = rest.splitn(2, ' ');
    let _offset = parts.next().expect("missing offset");
    parts.next().unwrap_or("").to_string()
}

#[test]
fn tcp_produce_and_fetch_smoke() {
    //GIVEN
    let dir = tcp_test_dir("smoke");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let (server_thread, addr) = spawn_test_server(Arc::clone(&broker));

    let mut client = TcpStream::connect(addr).expect("connect should succeed");
    let mut reader = BufReader::new(client.try_clone().expect("clone should succeed"));

    //WHEN / THEN
    write_expect_line(
        &mut client,
        &mut reader,
        b"PRODUCE t hello-world\n",
        "OK 0\n",
    );
    write_expect_line(
        &mut client,
        &mut reader,
        b"FETCH t 0\n",
        "MSG 0 hello-world\n",
    );
    write_expect_line(&mut client, &mut reader, b"FETCH t 99\n", "NONE\n");

    drop(reader);
    drop(client);
    server_thread.join().expect("server thread should join");
}

#[test]
fn tcp_legacy_parse_error_returns_err_line() {
    let dir = tcp_test_dir("legacy_parse_err");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let (server_thread, addr) = spawn_test_server(Arc::clone(&broker));

    let mut client = TcpStream::connect(addr).expect("connect");
    let mut reader = BufReader::new(client.try_clone().expect("clone"));

    write_expect_line(&mut client, &mut reader, b"PING\n", "ERR unknown command\n");

    drop(reader);
    drop(client);
    server_thread.join().expect("join");
}

#[test]
fn tcp_legacy_fetch_unknown_topic_returns_err() {
    let dir = tcp_test_dir("legacy_unknown_topic");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let (server_thread, addr) = spawn_test_server(Arc::clone(&broker));

    let mut client = TcpStream::connect(addr).expect("connect");
    let mut reader = BufReader::new(client.try_clone().expect("clone"));

    write_expect_line(
        &mut client,
        &mut reader,
        b"FETCH not_a_topic_yet 0\n",
        "ERR unknown topic\n",
    );

    drop(reader);
    drop(client);
    server_thread.join().expect("join");
}

#[test]
fn tcp_legacy_first_line_over_max_rejected() {
    let dir = tcp_test_dir("oversize_line");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let (server_thread, addr) = spawn_test_server_returns_client_result(Arc::clone(&broker));

    let mut client = TcpStream::connect(addr).expect("connect");
    let payload = vec![b'x'; 64 * 1024];
    client.write_all(&payload).expect("write");
    client.flush().expect("flush");
    drop(client);

    let server_res = server_thread.join().expect("join");
    assert!(
        server_res.is_err(),
        "expected oversize first line to fail server handler: {server_res:?}"
    );
}

#[test]
fn tcp_produce_multi_then_fetch_drain_in_order() {
    //GIVEN
    let dir = tcp_test_dir("multi_drain");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let (server_thread, addr) = spawn_test_server(Arc::clone(&broker));

    let mut client = TcpStream::connect(addr).expect("connect should succeed");
    let mut reader = BufReader::new(client.try_clone().expect("clone should succeed"));

    //WHEN
    write_expect_line(&mut client, &mut reader, b"PRODUCE t first\n", "OK 0\n");
    write_expect_line(&mut client, &mut reader, b"PRODUCE t second\n", "OK 1\n");

    let mut payloads = Vec::new();
    let mut offset = 0u64;
    loop {
        let req = format!("FETCH t {offset}\n");
        client
            .write_all(req.as_bytes())
            .expect("write should succeed");
        client.flush().expect("flush should succeed");
        let mut line = String::new();
        reader.read_line(&mut line).expect("read should succeed");
        assert!(!line.is_empty(), "unexpected EOF");
        let trimmed = line.trim_end();
        if trimmed == "NONE" {
            break;
        }
        assert!(
            trimmed.starts_with("MSG "),
            "unexpected response: {trimmed}"
        );
        payloads.push(msg_payload(trimmed));
        offset += 1;
    }

    //THEN
    assert_eq!(payloads, vec!["first".to_string(), "second".to_string()]);

    drop(reader);
    drop(client);
    server_thread.join().expect("server thread should join");
}

#[test]
fn tcp_framed_handshake_produce_fetch_roundtrip() {
    let dir = tcp_test_dir("framed");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let (server_thread, addr) = spawn_test_server(Arc::clone(&broker));

    let mut client = TcpStream::connect(addr).expect("connect");
    let mut reader = BufReader::new(client.try_clone().expect("clone"));

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

    let p_frame = encode_produce("ft", b"\0blob").unwrap();
    client.write_all(&p_frame).expect("produce");
    client.flush().expect("flush produce");
    let resp = read_frame(&mut reader).expect("produce response");
    assert_eq!(decode_response_frame(&resp).unwrap(), Response::OkOffset(0));

    let f_frame = encode_fetch("ft", 0).unwrap();
    client.write_all(&f_frame).expect("fetch");
    client.flush().expect("flush fetch");
    let resp2 = read_frame(&mut reader).expect("fetch response");
    match decode_response_frame(&resp2).unwrap() {
        Response::Message { offset, payload } => {
            assert_eq!(offset, 0);
            assert_eq!(payload, b"\0blob".to_vec());
        }
        other => panic!("expected Message framed response: {other:?}"),
    }

    let f_tail = encode_fetch("ft", 99).unwrap();
    client.write_all(&f_tail).expect("fetch tail");
    client.flush().expect("flush fetch tail");
    let resp3 = read_frame(&mut reader).expect("fetch none");
    assert_eq!(decode_response_frame(&resp3).unwrap(), Response::None);

    drop(reader);
    drop(client);
    server_thread.join().expect("join");
}

#[test]
fn tcp_framed_handshake_cr_lf_accepted() {
    let dir = tcp_test_dir("framed_crlf");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let (server_thread, addr) = spawn_test_server(Arc::clone(&broker));

    let mut client = TcpStream::connect(addr).expect("connect");
    let mut reader = BufReader::new(client.try_clone().expect("clone"));

    client
        .write_all(b"HERBATKA WIRE/1\r\n")
        .expect("handshake write CRLF");
    client.flush().expect("flush handshake");
    let mut ack = String::new();
    reader.read_line(&mut ack).expect("ack read");
    assert_eq!(
        ack.trim_end_matches(['\r', '\n']),
        "HERBATKA OK/1",
        "unexpected ack: {ack:?}"
    );

    let p_frame = encode_produce("cr", b"y").unwrap();
    client.write_all(&p_frame).expect("produce");
    client.flush().expect("flush produce");
    let resp = read_frame(&mut reader).expect("produce response");
    assert_eq!(decode_response_frame(&resp).unwrap(), Response::OkOffset(0));

    drop(reader);
    drop(client);
    server_thread.join().expect("join");
}

#[test]
fn tcp_framed_unknown_op_then_recover() {
    let dir = tcp_test_dir("framed_bad_op");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let (server_thread, addr) = spawn_test_server(Arc::clone(&broker));

    let mut client = TcpStream::connect(addr).expect("connect");
    let mut reader = BufReader::new(client.try_clone().expect("clone"));

    client
        .write_all(HANDSHAKE_CLIENT_V1)
        .expect("handshake write");
    client.flush().expect("flush handshake");
    let mut ack = String::new();
    reader.read_line(&mut ack).expect("ack read");
    assert_eq!(ack.trim_end_matches(['\r', '\n']), "HERBATKA OK/1");

    let mut bad = vec![WIRE_VERSION_V1, 99u8, 0, 0];
    bad.extend_from_slice(&0u32.to_le_bytes());
    client.write_all(&bad).expect("unknown op frame");
    client.flush().expect("flush");
    let err_frame = read_frame(&mut reader).expect("error response");
    match decode_response_frame(&err_frame).expect("decode err") {
        Response::Error(reason) => assert!(reason.contains("unknown op"), "{reason}"),
        other => panic!("expected Error framed response: {other:?}"),
    }

    let p_frame = encode_produce("recv", b"after").unwrap();
    client.write_all(&p_frame).expect("produce after error");
    client.flush().expect("flush produce");
    let ok_frame = read_frame(&mut reader).expect("ok response");
    assert_eq!(
        decode_response_frame(&ok_frame).unwrap(),
        Response::OkOffset(0)
    );

    drop(reader);
    drop(client);
    server_thread.join().expect("join");
}

#[test]
fn tcp_framed_oversized_declared_payload_returns_error_frame() {
    let dir = tcp_test_dir("framed_oversize_hdr");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let (server_thread, addr) = spawn_test_server(Arc::clone(&broker));

    let mut client = TcpStream::connect(addr).expect("connect");
    let mut reader = BufReader::new(client.try_clone().expect("clone"));

    client
        .write_all(HANDSHAKE_CLIENT_V1)
        .expect("handshake write");
    client.flush().expect("flush handshake");
    let mut ack = String::new();
    reader.read_line(&mut ack).expect("ack read");
    assert_eq!(ack.trim_end_matches(['\r', '\n']), "HERBATKA OK/1");

    let mut hdr = [0u8; HEADER_LEN];
    hdr[0] = WIRE_VERSION_V1;
    hdr[1] = OP_PRODUCE;
    hdr[4..8].copy_from_slice(&(MAX_FRAME_PAYLOAD + 1).to_le_bytes());
    client.write_all(&hdr).expect("bad header");
    client.flush().expect("flush header");
    let resp = read_frame(&mut reader).expect("error frame");
    match decode_response_frame(&resp).expect("decode") {
        Response::Error(reason) => assert!(
            reason.contains("payload too large"),
            "unexpected reason: {reason}"
        ),
        other => panic!("expected Error: {other:?}"),
    }

    drop(reader);
    drop(client);
    server_thread.join().expect("join");
}

#[test]
fn tcp_framed_topic_bounds_roundtrip() {
    let dir = tcp_test_dir("topic_bounds");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let (server_thread, addr) = spawn_test_server(Arc::clone(&broker));

    let mut client = TcpStream::connect(addr).expect("connect");
    let mut reader = BufReader::new(client.try_clone().expect("clone"));

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

    let p_frame = encode_produce("bx", b"x").unwrap();
    client.write_all(&p_frame).expect("produce");
    client.flush().expect("flush produce");
    let r1 = read_frame(&mut reader).expect("produce response");
    assert_eq!(decode_response_frame(&r1).unwrap(), Response::OkOffset(0));

    let b_frame = encode_topic_bounds("bx").unwrap();
    client.write_all(&b_frame).expect("bounds");
    client.flush().expect("flush bounds");
    let r2 = read_frame(&mut reader).expect("bounds response");
    assert_eq!(
        decode_response_frame(&r2).unwrap(),
        Response::TopicBounds {
            min_offset: 0,
            exclusive_end: 1,
        }
    );

    drop(reader);
    drop(client);
    server_thread.join().expect("join");
}

#[test]
fn tcp_framed_two_clients_concurrent_produce() {
    let dir = tcp_test_dir("concurrent_two");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let broker_srv = Arc::clone(&broker);
    let _server_thread = thread::spawn(move || {
        let _ = serve(listener, broker_srv);
    });

    let barrier = Arc::new(Barrier::new(2));
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut client_handles = vec![];
    for id in 0..2 {
        let barrier = Arc::clone(&barrier);
        client_handles.push(thread::spawn(move || {
            let topic = format!("cc-topic-{id}");
            let mut stream = TcpStream::connect(addr).expect("connect");
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            stream
                .write_all(HANDSHAKE_CLIENT_V1)
                .expect("handshake write");
            stream.flush().expect("flush handshake");
            let mut ack = String::new();
            reader.read_line(&mut ack).expect("ack read");
            assert_eq!(
                ack.trim_end_matches(['\r', '\n']),
                "HERBATKA OK/1",
                "unexpected ack: {ack:?}"
            );
            barrier.wait();
            let body = format!("payload-{id}");
            let p_frame = encode_produce(&topic, body.as_bytes()).expect("encode produce");
            stream.write_all(&p_frame).expect("produce");
            stream.flush().expect("flush produce");
            let r = read_frame(&mut reader).expect("produce response");
            assert_eq!(
                decode_response_frame(&r).expect("decode"),
                Response::OkOffset(0)
            );
            assert!(
                Instant::now() < deadline,
                "concurrent produce exceeded deadline"
            );
        }));
    }
    for h in client_handles {
        h.join().expect("client thread");
    }
}

#[test]
fn tcp_framed_concurrent_fetch_same_topic() {
    let dir = tcp_test_dir("concurrent_fetch");
    let inner = Broker::with_data_dir(dir);
    let topic = "cf-topic";
    inner.create_topic(topic.to_string()).expect("create topic");
    let msg = herbatka::log::message::Message {
        key: None,
        payload: b"seed-payload".to_vec(),
        timestamp: 1,
        headers: std::collections::HashMap::new(),
    };
    inner.produce(topic, msg).expect("seed produce");
    let broker: SharedBroker = Arc::new(inner);

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let broker_srv = Arc::clone(&broker);
    let _server_thread = thread::spawn(move || {
        let _ = serve(listener, broker_srv);
    });

    const READERS: usize = 8;
    let barrier = Arc::new(Barrier::new(READERS + 1));
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut handles = vec![];
    for _ in 0..READERS {
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).expect("connect");
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            stream
                .write_all(HANDSHAKE_CLIENT_V1)
                .expect("handshake write");
            stream.flush().expect("flush handshake");
            let mut ack = String::new();
            reader.read_line(&mut ack).expect("ack read");
            assert_eq!(
                ack.trim_end_matches(['\r', '\n']),
                "HERBATKA OK/1",
                "unexpected ack: {ack:?}"
            );
            barrier.wait();
            for _ in 0..32 {
                let f = encode_fetch(topic, 0).expect("encode fetch");
                stream.write_all(&f).expect("write fetch");
                stream.flush().expect("flush fetch");
                let resp = read_frame(&mut reader).expect("read response");
                match decode_response_frame(&resp).expect("decode") {
                    Response::Message { offset, payload } => {
                        assert_eq!(offset, 0);
                        assert_eq!(payload.as_slice(), b"seed-payload");
                    }
                    other => panic!("expected Message got {other:?}"),
                }
            }
            assert!(
                Instant::now() < deadline,
                "concurrent fetch exceeded deadline"
            );
        }));
    }
    barrier.wait();
    for h in handles {
        h.join().expect("reader thread join");
    }
}

#[test]
fn tcp_framed_concurrent_produce_different_topics() {
    const CLIENTS: usize = 8;
    let dir = tcp_test_dir("concurrent_produce_topics");
    let broker = Arc::new(Broker::with_data_dir(dir));
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let broker_srv = Arc::clone(&broker);
    let _server_thread = thread::spawn(move || {
        let _ = serve(listener, broker_srv);
    });

    let barrier = Arc::new(Barrier::new(CLIENTS + 1));
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut handles = vec![];
    for id in 0..CLIENTS {
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let topic = format!("cc-topic-{id}");
            let mut stream = TcpStream::connect(addr).expect("connect");
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            stream
                .write_all(HANDSHAKE_CLIENT_V1)
                .expect("handshake write");
            stream.flush().expect("flush handshake");
            let mut ack = String::new();
            reader.read_line(&mut ack).expect("ack read");
            assert_eq!(
                ack.trim_end_matches(['\r', '\n']),
                "HERBATKA OK/1",
                "unexpected ack: {ack:?}"
            );
            barrier.wait();
            let body = format!("payload-{id}");
            let p_frame = encode_produce(&topic, body.as_bytes()).expect("encode produce");
            stream.write_all(&p_frame).expect("produce");
            stream.flush().expect("flush produce");
            let r = read_frame(&mut reader).expect("produce response");
            assert_eq!(
                decode_response_frame(&r).expect("decode"),
                Response::OkOffset(0)
            );
            assert!(
                Instant::now() < deadline,
                "concurrent produce exceeded deadline"
            );
        }));
    }
    barrier.wait();
    for h in handles {
        h.join().expect("client thread join");
    }
}
