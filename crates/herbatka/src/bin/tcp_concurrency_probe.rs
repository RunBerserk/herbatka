//! Drive multiple framed TCP clients against a running broker to capture baseline
//! concurrency behavior (serial `accept` + `handle_client` in `tcp/server.rs`).
//!
//! Usage: `tcp_concurrency_probe --addr HOST:PORT [options]`
//! See `USAGE` for flags.

use std::io::Write;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use herbatka::observability;
use herbatka::tcp::command::Response;
use herbatka::tcp::frame::{
    decode_response_frame, encode_fetch, encode_produce, perform_client_handshake, read_frame,
};
use tracing::error;

const USAGE: &str = "\
usage: tcp_concurrency_probe --addr HOST:PORT [options]

Options:
  --clients N           number of worker clients (default: 8)
  --duration-secs N     framed workload duration per client after handshake (default: 60)
  --no-watchdog         do not run the 9th short-lived watchdog client
  --watchdog-topic S    topic for watchdog produce/fetch (default: v1cc-watchdog)

Notes:
  With the current broker, TCP accepts are served one client at a time until disconnect.
  Parallel `TcpStream::connect` calls may complete in the kernel backlog, but the server
  only reads the framed handshake after `accept` runs; workers therefore serialize.
";

struct Args {
    addr: String,
    clients: usize,
    duration: Duration,
    watchdog: bool,
    watchdog_topic: String,
}

fn main() {
    observability::init();
    if let Err(e) = run() {
        error!("{e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = parse_args()?;
    if args.clients < 1 {
        return Err("--clients must be >= 1".into());
    }

    let first_handshake_done = Arc::new(AtomicBool::new(false));
    let (watchdog_tx, watchdog_rx) = mpsc::channel::<()>();
    let notify_tx: Option<mpsc::Sender<()>> = if args.watchdog {
        Some(watchdog_tx.clone())
    } else {
        None
    };

    let wall = Instant::now();
    let results: Arc<Mutex<Vec<WorkerMetrics>>> = Arc::new(Mutex::new(Vec::new()));

    let watchdog_handle = if args.watchdog {
        let addr = args.addr.clone();
        let topic = args.watchdog_topic.clone();
        Some(thread::spawn(move || {
            let _ = watchdog_rx.recv();
            let t0 = Instant::now();
            match watchdog_roundtrip(&addr, &topic) {
                Ok(()) => {
                    let ms = t0.elapsed().as_secs_f64() * 1000.0;
                    format!("probe_watchdog_ok elapsed_ms={ms:.1}")
                }
                Err(e) => {
                    let ms = t0.elapsed().as_secs_f64() * 1000.0;
                    format!("probe_watchdog_err elapsed_ms={ms:.1} error={e}")
                }
            }
        }))
    } else {
        None
    };

    let addr = args.addr.clone();
    let duration = args.duration;
    let clients = args.clients;
    let first_flag = Arc::clone(&first_handshake_done);

    let mut handles = Vec::new();
    for id in 0..clients {
        let addr = addr.clone();
        let results = Arc::clone(&results);
        let first_flag = Arc::clone(&first_flag);
        let notify_tx = notify_tx.clone();
        handles.push(thread::spawn(move || {
            let m = worker(id, &addr, duration, &first_flag, notify_tx.as_ref());
            results.lock().expect("metrics lock").push(m);
        }));
    }

    for h in handles {
        h.join().map_err(|_| "worker thread panicked")?;
    }

    let watchdog_line = if let Some(h) = watchdog_handle {
        Some(h.join().map_err(|_| "watchdog thread panicked")?)
    } else {
        None
    };

    let mut rows = Arc::try_unwrap(results)
        .map_err(|_| "results still referenced")?
        .into_inner()
        .map_err(|_| "metrics mutex poisoned")?;
    rows.sort_by_key(|r| r.client_id);
    for r in &rows {
        println!(
            "probe_worker client_id={} connect_ms={:.1} handshake_ms={:.1} workload_s={:.1} total_worker_s={:.1}",
            r.client_id, r.connect_ms, r.handshake_ms, r.workload_s, r.total_worker_s
        );
    }

    if let Some(line) = watchdog_line {
        println!("{line}");
    }

    println!(
        "probe_summary clients={} duration_per_client_s={:.1} total_wall_s={:.3}",
        clients,
        duration.as_secs_f64(),
        wall.elapsed().as_secs_f64()
    );
    println!("probe_note handshake_ms_includes_server_queue_when_tcp_connected_before_accept");

    Ok(())
}

#[derive(Debug)]
struct WorkerMetrics {
    client_id: usize,
    connect_ms: f64,
    handshake_ms: f64,
    workload_s: f64,
    total_worker_s: f64,
}

fn worker(
    client_id: usize,
    addr: &str,
    duration: Duration,
    first_handshake_done: &AtomicBool,
    notify_tx: Option<&mpsc::Sender<()>>,
) -> WorkerMetrics {
    let t_worker = Instant::now();
    let t0 = Instant::now();
    let mut stream = TcpStream::connect(addr).expect("connect");
    let connect_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t1 = Instant::now();
    perform_client_handshake(&mut stream).expect("handshake");
    let handshake_ms = t1.elapsed().as_secs_f64() * 1000.0;

    if let Some(tx) = notify_tx
        && !first_handshake_done.swap(true, Ordering::SeqCst)
    {
        let _ = tx.send(());
    }

    let t_work = Instant::now();
    run_workload(&mut stream, client_id, duration);
    let workload_s = t_work.elapsed().as_secs_f64();

    drop(stream);

    WorkerMetrics {
        client_id,
        connect_ms,
        handshake_ms,
        workload_s,
        total_worker_s: t_worker.elapsed().as_secs_f64(),
    }
}

fn run_workload(stream: &mut TcpStream, client_id: usize, duration: Duration) {
    let topic = format!("v1cc-{client_id}");
    let payload: Vec<u8> = b"probe-payload-512b-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        .iter()
        .cycle()
        .take(512)
        .copied()
        .collect();
    let deadline = Instant::now() + duration;
    let mut next_offset: u64 = 0;

    while Instant::now() < deadline {
        // ~66% produce: two produces then one fetch per "cycle"
        for _ in 0..2 {
            produce_one(stream, &topic, &payload, &mut next_offset);
            thread::sleep(Duration::from_millis(330));
        }
        fetch_one(stream, &topic, next_offset.saturating_sub(1));
        thread::sleep(Duration::from_millis(330));
    }
}

fn produce_one(stream: &mut TcpStream, topic: &str, body: &[u8], next_offset: &mut u64) {
    let frame = encode_produce(topic, body).expect("encode produce");
    stream.write_all(&frame).expect("write produce");
    stream.flush().expect("flush produce");
    let buf = read_frame(stream).expect("read produce response");
    match decode_response_frame(&buf).expect("decode produce response") {
        Response::OkOffset(off) => *next_offset = off.saturating_add(1),
        Response::Error(e) => panic!("produce error: {e}"),
        other => panic!("unexpected produce response: {other:?}"),
    }
}

fn fetch_one(stream: &mut TcpStream, topic: &str, offset: u64) {
    let frame = encode_fetch(topic, offset).expect("encode fetch");
    stream.write_all(&frame).expect("write fetch");
    stream.flush().expect("flush fetch");
    let buf = read_frame(stream).expect("read fetch response");
    match decode_response_frame(&buf).expect("decode fetch response") {
        Response::Message { .. } | Response::None => {}
        Response::Error(e) => panic!("fetch error: {e}"),
        other => panic!("unexpected fetch response: {other:?}"),
    }
}

fn watchdog_roundtrip(addr: &str, topic: &str) -> Result<(), String> {
    let mut stream = TcpStream::connect(addr).map_err(|e| format!("watchdog connect: {e}"))?;
    perform_client_handshake(&mut stream).map_err(|e| format!("watchdog handshake: {e}"))?;

    let body = b"watchdog-once";
    let frame = encode_produce(topic, body).map_err(|e| e.to_string())?;
    stream
        .write_all(&frame)
        .map_err(|e| format!("watchdog produce write: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("watchdog produce flush: {e}"))?;
    let buf = read_frame(&mut stream).map_err(|e| format!("watchdog produce read: {e}"))?;
    let off =
        match decode_response_frame(&buf).map_err(|e| format!("watchdog produce decode: {e}"))? {
            Response::OkOffset(o) => o,
            Response::Error(e) => return Err(format!("watchdog produce err: {e}")),
            other => return Err(format!("watchdog produce unexpected: {other:?}")),
        };

    let frame = encode_fetch(topic, off).map_err(|e| e.to_string())?;
    stream
        .write_all(&frame)
        .map_err(|e| format!("watchdog fetch write: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("watchdog fetch flush: {e}"))?;
    let buf2 = read_frame(&mut stream).map_err(|e| format!("watchdog fetch read: {e}"))?;
    match decode_response_frame(&buf2).map_err(|e| format!("watchdog fetch decode: {e}"))? {
        Response::Message { offset, .. } if offset == off => Ok(()),
        Response::Error(e) => Err(format!("watchdog fetch err: {e}")),
        other => Err(format!("watchdog fetch unexpected: {other:?}")),
    }
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let mut addr: Option<String> = None;
    let mut clients = 8usize;
    let mut duration_secs = 60u64;
    let mut watchdog = true;
    let mut watchdog_topic = "v1cc-watchdog".to_string();

    while let Some(a) = args.next() {
        match a.as_str() {
            "--help" | "-h" => return Err(USAGE.into()),
            "--addr" => {
                addr = Some(
                    args.next()
                        .ok_or_else(|| "--addr needs a value".to_string())?,
                );
            }
            "--clients" => {
                let v = args
                    .next()
                    .ok_or_else(|| "--clients needs a value".to_string())?;
                clients = v.parse().map_err(|_| format!("invalid --clients {v}"))?;
            }
            "--duration-secs" => {
                let v = args
                    .next()
                    .ok_or_else(|| "--duration-secs needs a value".to_string())?;
                duration_secs = v
                    .parse()
                    .map_err(|_| format!("invalid --duration-secs {v}"))?;
            }
            "--no-watchdog" => watchdog = false,
            "--watchdog-topic" => {
                watchdog_topic = args
                    .next()
                    .ok_or_else(|| "--watchdog-topic needs a value".to_string())?;
            }
            other => return Err(format!("unknown argument: {other}\n{USAGE}")),
        }
    }

    let addr = addr.ok_or_else(|| format!("missing --addr\n{USAGE}"))?;
    if duration_secs == 0 {
        return Err("--duration-secs must be > 0".into());
    }

    Ok(Args {
        addr,
        clients,
        duration: Duration::from_secs(duration_secs),
        watchdog,
        watchdog_topic,
    })
}
