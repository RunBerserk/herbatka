use std::net::TcpStream;
use std::thread::sleep;
use std::time::Duration;

use herbatka::tcp::frame::perform_client_handshake;

use super::Summary;

const CONNECT_RETRY_MAX_ATTEMPTS: u32 = 3;

fn open_connection_once(addr: &str) -> Result<TcpStream, String> {
    let mut stream =
        TcpStream::connect(addr).map_err(|e| format!("connect failed to {addr}: {e}"))?;
    perform_client_handshake(&mut stream).map_err(|e| format!("wire handshake failed: {e}"))?;
    Ok(stream)
}

pub(super) fn connect_with_retry(addr: &str, summary: &mut Summary) -> Result<TcpStream, String> {
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

pub(super) fn build_produce_frame(topic: &str, payload: &str) -> Result<Vec<u8>, String> {
    build_produce_frame_bytes(topic, payload.as_bytes())
}

pub(super) fn build_produce_frame_bytes(topic: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    if topic.trim().is_empty() {
        return Err("topic must not be empty".to_string());
    }
    if body.is_empty() {
        return Err("payload must not be empty".to_string());
    }
    herbatka::tcp::frame::encode_produce(topic, body).map_err(|e| e.to_string())
}
