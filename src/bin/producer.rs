//! Minimal TCP client: sends one `PRODUCE` line to the broker and prints the response.
//! Usage: `producer <addr> <topic> <payload>` (see `USAGE`).

use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;

const USAGE: &str = "usage: producer <addr> <topic> <payload>";

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
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

    let request = format!("PRODUCE {topic} {payload}\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write failed: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("socket flush failed: {e}"))?;

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .map_err(|e| format!("read failed: {e}"))?;

    if response.is_empty() {
        return Err("server closed connection without response".into());
    }

    print!("{response}");
    io::stdout()
        .flush()
        .map_err(|e| format!("stdout flush failed: {e}"))?;

    Ok(())
}
