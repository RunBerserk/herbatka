use herbatka::broker::core::Broker;
use herbatka::tcp::server::handle_client;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn tcp_produce_and_fetch_smoke() {
    //GIVEN
    let dir = std::env::temp_dir().join(format!(
        "herbatka_tcp_smoke_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos()
    ));

    let broker = Arc::new(Mutex::new(Broker::with_data_dir(dir)));
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind should succeed");
    let addr = listener.local_addr().expect("local addr should exist");

    let broker_for_thread = Arc::clone(&broker);
    let server_thread = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept should succeed");
        handle_client(stream, &broker_for_thread).expect("client handling should succeed");
    });

    let mut client = TcpStream::connect(addr).expect("connect should succeed");
    let mut reader = BufReader::new(client.try_clone().expect("clone should succeed"));

    //WHEN
    client
        .write_all(b"PRODUCE t hello-world\n")
        .expect("write should succeed");
    //THEN
    let mut line = String::new();
    reader.read_line(&mut line).expect("read should succeed");
    assert_eq!(line, "OK 0\n");

    //WHEN
    client
        .write_all(b"FETCH t 0\n")
        .expect("write should succeed");
    //THEN
    line.clear();
    reader.read_line(&mut line).expect("read should succeed");
    assert_eq!(line, "MSG 0 hello-world\n");

    //WHEN
    client
        .write_all(b"FETCH t 99\n")
        .expect("write should succeed");
    //THEN
    line.clear();
    reader.read_line(&mut line).expect("read should succeed");
    assert_eq!(line, "NONE\n");

    drop(reader);
    drop(client);
    server_thread.join().expect("server thread should join");
}
