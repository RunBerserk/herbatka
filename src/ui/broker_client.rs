use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

#[derive(Debug)]
pub enum BrokerResponse {
    Message { offset: u64, payload: String },
    None,
}

pub struct BrokerClient {
    addr: String,
    topic: String,
    stream: Option<TcpStream>,
}

impl BrokerClient {
    pub fn new(addr: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            topic: topic.into(),
            stream: None,
        }
    }

    pub fn poll_from_offset(
        &mut self,
        start_offset: u64,
        max_messages: usize,
    ) -> Result<Vec<(u64, String)>, String> {
        let mut offset = start_offset;
        let mut collected = Vec::new();

        for _ in 0..max_messages {
            match self.fetch_once(offset)? {
                BrokerResponse::None => break,
                BrokerResponse::Message {
                    offset: msg_offset,
                    payload,
                } => {
                    collected.push((msg_offset, payload));
                    offset = msg_offset.saturating_add(1);
                }
            }
        }

        Ok(collected)
    }

    fn fetch_once(&mut self, offset: u64) -> Result<BrokerResponse, String> {
        let topic = self.topic.clone();
        let stream = self.ensure_connected()?;
        let request = format!("FETCH {topic} {offset}\n");
        if let Err(e) = stream.write_all(request.as_bytes()) {
            let message = self.reset_with_error(format!("fetch write failed: {e}"));
            return Err(message);
        }
        if let Err(e) = stream.flush() {
            let message = self.reset_with_error(format!("fetch flush failed: {e}"));
            return Err(message);
        }

        let reader_stream = match stream.try_clone() {
            Ok(reader_stream) => reader_stream,
            Err(e) => {
                let message = self.reset_with_error(format!("reader clone failed: {e}"));
                return Err(message);
            }
        };
        let mut reader = BufReader::new(reader_stream);
        let mut line = String::new();
        if let Err(e) = reader.read_line(&mut line) {
            let message = self.reset_with_error(format!("fetch read failed: {e}"));
            return Err(message);
        }
        if line.is_empty() {
            return Err(self.reset_with_error("broker closed connection".to_string()));
        }

        parse_response_line(line.trim_end())
    }

    fn ensure_connected(&mut self) -> Result<&mut TcpStream, String> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr)
                .map_err(|e| format!("connect {} failed: {e}", self.addr))?;
            stream
                .set_read_timeout(Some(Duration::from_millis(400)))
                .map_err(|e| format!("set read timeout failed: {e}"))?;
            stream
                .set_write_timeout(Some(Duration::from_millis(400)))
                .map_err(|e| format!("set write timeout failed: {e}"))?;
            self.stream = Some(stream);
        }
        self.stream
            .as_mut()
            .ok_or_else(|| "connection not available".to_string())
    }

    fn reset_with_error(&mut self, message: String) -> String {
        self.stream = None;
        message
    }
}

fn parse_response_line(line: &str) -> Result<BrokerResponse, String> {
    if line == "NONE" {
        return Ok(BrokerResponse::None);
    }
    if let Some(rest) = line.strip_prefix("MSG ") {
        let mut parts = rest.splitn(2, ' ');
        let offset = parts
            .next()
            .ok_or_else(|| "missing message offset".to_string())?
            .parse::<u64>()
            .map_err(|_| "invalid message offset".to_string())?;
        let payload = parts
            .next()
            .ok_or_else(|| "missing message payload".to_string())?;
        return Ok(BrokerResponse::Message {
            offset,
            payload: payload.to_string(),
        });
    }
    if let Some(reason) = line.strip_prefix("ERR ") {
        return Err(format!("broker error: {reason}"));
    }
    Err(format!("unexpected broker response: {line}"))
}
