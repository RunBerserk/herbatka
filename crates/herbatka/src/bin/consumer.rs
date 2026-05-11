//! Minimal TCP client: drain-reads a topic with framed `FETCH` after wire handshake.
//! Usage: `consumer <addr> <topic> <start_offset>` (see `USAGE`).

use std::env;
use std::io::{self, Write};
use std::net::TcpStream;

use herbatka::observability;
use herbatka::tcp::command::Response;
use herbatka::tcp::frame::{
    decode_response_frame, encode_fetch, perform_client_handshake, read_frame,
};
use tracing::error;

const USAGE: &str = "usage: consumer <addr> <topic> <start_offset>";

fn main() {
    observability::init();
    if let Err(e) = run() {
        error!("{e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let (addr, topic, start_offset) = parse_args()?;
    validate_topic(&topic)?;
    drain_fetch_loop(&addr, &topic, start_offset)
}

fn parse_args() -> Result<(String, String, u64), String> {
    let mut args = env::args().skip(1);

    let addr = args.next().ok_or_else(usage_error)?;
    let topic = args.next().ok_or_else(usage_error)?;
    let offset_str = args.next().ok_or_else(usage_error)?;

    if args.next().is_some() {
        return Err(usage_error());
    }

    let start_offset = offset_str
        .parse::<u64>()
        .map_err(|_| format!("invalid start_offset: {offset_str}"))?;

    Ok((addr, topic, start_offset))
}

fn usage_error() -> String {
    USAGE.to_string()
}

fn validate_topic(topic: &str) -> Result<(), String> {
    if topic.trim().is_empty() {
        return Err("topic must not be empty".into());
    }
    Ok(())
}

fn drain_fetch_loop(addr: &str, topic: &str, mut offset: u64) -> Result<(), String> {
    let mut stream =
        TcpStream::connect(addr).map_err(|e| format!("connect failed to {addr}: {e}"))?;
    perform_client_handshake(&mut stream).map_err(|e| format!("wire handshake failed: {e}"))?;

    loop {
        let outcome = send_fetch_and_handle(&mut stream, topic, offset)?;
        match outcome {
            FetchOutcome::Done => break,
            FetchOutcome::Message {
                payload,
                next_offset,
            } => {
                println!("{}", String::from_utf8_lossy(&payload));
                io::stdout()
                    .flush()
                    .map_err(|e| format!("stdout flush failed: {e}"))?;
                offset = next_offset;
            }
        }
    }

    Ok(())
}

enum FetchOutcome {
    Done,
    Message { payload: Vec<u8>, next_offset: u64 },
}

fn send_fetch_and_handle(
    stream: &mut TcpStream,
    topic: &str,
    offset: u64,
) -> Result<FetchOutcome, String> {
    let frame = encode_fetch(topic, offset).map_err(|e| e.to_string())?;
    stream
        .write_all(&frame)
        .map_err(|e| format!("write failed: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("socket flush failed: {e}"))?;

    let buf = read_frame(stream).map_err(|e| format!("read failed: {e}"))?;
    let response = decode_response_frame(&buf).map_err(|e| format!("invalid response: {e}"))?;

    match response {
        Response::None => Ok(FetchOutcome::Done),
        Response::Message {
            offset: msg_offset,
            payload,
        } => Ok(FetchOutcome::Message {
            payload,
            next_offset: msg_offset.saturating_add(1),
        }),
        Response::Error(reason) => Err(reason),
        Response::OkOffset(_) => Err(format!(
            "unexpected OkOffset response for FETCH at offset {offset}"
        )),
    }
}
