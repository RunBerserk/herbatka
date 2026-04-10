use herbatka::broker::core::Broker;
use herbatka::tcp::server::handle_client;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

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

fn spawn_test_server(broker: Arc<Mutex<Broker>>) -> (thread::JoinHandle<()>, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind should succeed");
    let addr = listener.local_addr().expect("local addr should exist");
    let broker_for_thread = Arc::clone(&broker);
    let server_thread = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept should succeed");
        handle_client(stream, &broker_for_thread).expect("client handling should succeed");
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
    let broker = Arc::new(Mutex::new(Broker::with_data_dir(dir)));
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
fn tcp_produce_multi_then_fetch_drain_in_order() {
    //GIVEN
    let dir = tcp_test_dir("multi_drain");
    let broker = Arc::new(Mutex::new(Broker::with_data_dir(dir)));
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
