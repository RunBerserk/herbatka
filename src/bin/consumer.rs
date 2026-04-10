//! Minimal TCP client: drain-reads a topic by sending repeated `FETCH` lines until `NONE`.
//! Usage: `consumer <addr> <topic> <start_offset>` (see `USAGE`).

use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;

use herbatka::observability;
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

    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|e| format!("clone failed: {e}"))?,
    );

    loop {
        let line = read_fetch_response_line(&mut stream, &mut reader, topic, offset)?;
        let trimmed = line.trim_end();

        match handle_fetch_line(trimmed)? {
            FetchLineOutcome::Done => break,
            FetchLineOutcome::Message {
                payload,
                next_offset,
            } => {
                println!("{payload}");
                io::stdout()
                    .flush()
                    .map_err(|e| format!("stdout flush failed: {e}"))?;
                offset = next_offset;
            }
        }
    }

    Ok(())
}

fn read_fetch_response_line(
    stream: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    topic: &str,
    offset: u64,
) -> Result<String, String> {
    let request = format!("FETCH {topic} {offset}\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write failed: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("socket flush failed: {e}"))?;

    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|e| format!("read failed: {e}"))?;

    if line.is_empty() {
        return Err("server closed connection without response".into());
    }

    Ok(line)
}

enum FetchLineOutcome {
    /// End of drain (`NONE`).
    Done,
    /// One message; next fetch uses `next_offset`.
    Message { payload: String, next_offset: u64 },
}

fn handle_fetch_line(trimmed: &str) -> Result<FetchLineOutcome, String> {
    if trimmed == "NONE" {
        return Ok(FetchLineOutcome::Done);
    }

    if let Some(rest) = trimmed.strip_prefix("ERR ") {
        return Err(rest.to_string());
    }

    if trimmed.starts_with("OK ") {
        return Err(format!("unexpected response for FETCH: {trimmed}"));
    }

    if let Some((msg_offset, payload)) = parse_msg_line(trimmed) {
        return Ok(FetchLineOutcome::Message {
            payload,
            next_offset: msg_offset.saturating_add(1),
        });
    }

    Err(format!("unexpected response line: {trimmed}"))
}

/// Parses `MSG <offset> <payload>` (payload may contain spaces; only line ending is trimmed).
fn parse_msg_line(line: &str) -> Option<(u64, String)> {
    let rest = line.strip_prefix("MSG ")?;
    let mut parts = rest.splitn(2, ' ');
    let offset_str = parts.next()?;
    let payload = parts.next().unwrap_or("");
    let offset = offset_str.parse().ok()?;
    Some((offset, payload.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{FetchLineOutcome, handle_fetch_line, parse_msg_line};

    #[test]
    fn parse_msg_with_spaces_in_payload() {
        let (o, p) = parse_msg_line("MSG 3 speed is 120").expect("parse");
        assert_eq!(o, 3);
        assert_eq!(p, "speed is 120");
    }

    #[test]
    fn handle_fetch_line_none() {
        assert!(matches!(
            handle_fetch_line("NONE").unwrap(),
            FetchLineOutcome::Done
        ));
    }

    #[test]
    fn handle_fetch_line_msg() {
        match handle_fetch_line("MSG 1 hello world").unwrap() {
            FetchLineOutcome::Message {
                payload,
                next_offset,
            } => {
                assert_eq!(payload, "hello world");
                assert_eq!(next_offset, 2);
            }
            FetchLineOutcome::Done => panic!("expected Message"),
        }
    }

    #[test]
    fn handle_fetch_line_err() {
        assert!(handle_fetch_line("ERR bad").is_err());
    }
}
