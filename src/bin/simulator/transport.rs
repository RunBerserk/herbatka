use std::io::BufReader;
use std::net::TcpStream;
use std::thread::sleep;
use std::time::Duration;

use super::Summary;

const CONNECT_RETRY_MAX_ATTEMPTS: u32 = 3;

fn open_connection_once(addr: &str) -> Result<(TcpStream, BufReader<TcpStream>), String> {
    let stream = TcpStream::connect(addr).map_err(|e| format!("connect failed to {addr}: {e}"))?;
    let reader_stream = stream
        .try_clone()
        .map_err(|e| format!("clone failed: {e}"))?;
    let reader = BufReader::new(reader_stream);
    Ok((stream, reader))
}

pub(super) fn connect_with_retry(
    addr: &str,
    summary: &mut Summary,
) -> Result<(TcpStream, BufReader<TcpStream>), String> {
    for attempt in 1..=CONNECT_RETRY_MAX_ATTEMPTS {
        match open_connection_once(addr) {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                summary.connect_errors += 1;
                if attempt == CONNECT_RETRY_MAX_ATTEMPTS {
                    return Err(e);
                }
                sleep(retry_backoff(attempt));
            }
        }
    }
    Err("unreachable retry state".to_string())
}

pub(super) fn retry_backoff(attempt: u32) -> Duration {
    // 1->200ms, 2->400ms, ...
    Duration::from_millis(200u64.saturating_mul(attempt as u64))
}

pub(super) fn build_produce_line(topic: &str, payload: &str) -> Result<String, String> {
    if topic.trim().is_empty() {
        return Err("topic must not be empty".to_string());
    }
    if payload.is_empty() {
        return Err("payload must not be empty".to_string());
    }
    if payload.contains('\n') || payload.contains('\r') {
        return Err("payload must not contain newlines".to_string());
    }
    Ok(format!("PRODUCE {topic} {payload}\n"))
}
