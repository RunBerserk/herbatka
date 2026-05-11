//! Minimal TCP client: sends one framed `PRODUCE` to the broker after wire handshake.
//! Usage: `producer <addr> <topic> <payload>` (see `USAGE`).

use std::env;
use std::io::Write;
use std::net::TcpStream;

use herbatka::observability;
use herbatka::tcp::command::Response;
use herbatka::tcp::frame::{
    decode_response_frame, encode_produce, perform_client_handshake, read_frame,
};
use tracing::error;

const USAGE: &str = "usage: producer <addr> <topic> <payload>";

fn main() {
    observability::init();
    if let Err(e) = run() {
        error!("{e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let (addr, topic, payload) = parse_args()?;
    validate_topic_and_payload(&topic, &payload)?;
    send_produce_and_print_response(&addr, &topic, &payload)
}

fn parse_args() -> Result<(String, String, String), String> {
    let mut args = env::args().skip(1);

    let addr = args.next().ok_or_else(usage_error)?;
    let topic = args.next().ok_or_else(usage_error)?;
    let payload = args.next().ok_or_else(usage_error)?;

    if args.next().is_some() {
        return Err(usage_error());
    }

    Ok((addr, topic, payload))
}

fn usage_error() -> String {
    USAGE.to_string()
}

fn validate_topic_and_payload(topic: &str, payload: &str) -> Result<(), String> {
    if topic.trim().is_empty() {
        return Err("topic must not be empty".into());
    }
    if payload.is_empty() {
        return Err("payload must not be empty".into());
    }
    Ok(())
}

fn send_produce_and_print_response(addr: &str, topic: &str, payload: &str) -> Result<(), String> {
    let mut stream =
        TcpStream::connect(addr).map_err(|e| format!("connect failed to {addr}: {e}"))?;
    perform_client_handshake(&mut stream).map_err(|e| format!("wire handshake failed: {e}"))?;

    let frame = encode_produce(topic, payload.as_bytes()).map_err(|e| e.to_string())?;
    stream
        .write_all(&frame)
        .map_err(|e| format!("write failed: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("socket flush failed: {e}"))?;

    let buf = read_frame(&mut stream).map_err(|e| format!("read failed: {e}"))?;
    let response = decode_response_frame(&buf).map_err(|e| format!("invalid response: {e}"))?;

    match response {
        Response::OkOffset(off) => println!("OK {off}"),
        Response::Error(reason) => {
            println!("ERR {reason}");
        }
        _ => {
            return Err("unexpected response to PRODUCE".into());
        }
    }

    Ok(())
}
