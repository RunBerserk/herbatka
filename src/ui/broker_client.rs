use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::tcp::command::Response;
use crate::tcp::frame::{
    decode_response_frame, encode_fetch, perform_client_handshake, read_frame,
};

#[derive(Debug)]
pub enum BrokerResponse {
    Message { offset: u64, payload: String },
    None,
}

pub struct BrokerClient {
    addr: String,
    topic: String,
    connection: Option<BrokerConnection>,
}

struct BrokerConnection {
    writer: TcpStream,
    reader: TcpStream,
}

const CONNECT_TIMEOUT: Duration = Duration::from_millis(35);
const IO_TIMEOUT: Duration = Duration::from_millis(400);

fn classify_connect_error(addr: &str, error: &std::io::Error) -> String {
    match error.kind() {
        std::io::ErrorKind::ConnectionRefused => {
            format!("broker unavailable at {addr} (connection refused)")
        }
        std::io::ErrorKind::TimedOut => format!("broker unavailable at {addr} (connect timeout)"),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::AddrNotAvailable => {
            format!("broker address {addr} is invalid or unavailable")
        }
        _ => format!("connect {addr} failed: {error}"),
    }
}

fn classify_io_error(phase: &str, error: &std::io::Error) -> String {
    match error.kind() {
        std::io::ErrorKind::TimedOut => format!("broker {phase} timeout"),
        std::io::ErrorKind::ConnectionReset
        | std::io::ErrorKind::ConnectionAborted
        | std::io::ErrorKind::BrokenPipe
        | std::io::ErrorKind::UnexpectedEof => format!("broker connection lost during {phase}"),
        _ => format!("broker {phase} failed: {error}"),
    }
}

impl BrokerClient {
    pub fn new(addr: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            topic: topic.into(),
            connection: None,
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
        let result = {
            let connection = self.ensure_connected()?;
            fetch_once_on_connection(connection, &topic, offset)
        };
        result.map_err(|message| self.reset_with_error(message))
    }

    fn ensure_connected(&mut self) -> Result<&mut BrokerConnection, String> {
        if self.connection.is_none() {
            let target = self
                .addr
                .to_socket_addrs()
                .map_err(|e| format!("resolve {} failed: {e}", self.addr))?
                .next()
                .ok_or_else(|| format!("resolve {} returned no addresses", self.addr))?;
            let mut stream = TcpStream::connect_timeout(&target, CONNECT_TIMEOUT)
                .map_err(|e| classify_connect_error(&self.addr, &e))?;
            stream
                .set_read_timeout(Some(IO_TIMEOUT))
                .map_err(|e| format!("set read timeout failed: {e}"))?;
            stream
                .set_write_timeout(Some(IO_TIMEOUT))
                .map_err(|e| format!("set write timeout failed: {e}"))?;
            perform_client_handshake(&mut stream)
                .map_err(|e| format!("wire handshake failed: {e}"))?;
            let reader = stream
                .try_clone()
                .map_err(|e| format!("reader clone failed: {e}"))?;
            self.connection = Some(BrokerConnection {
                writer: stream,
                reader,
            });
        }
        self.connection
            .as_mut()
            .ok_or_else(|| "connection not available".to_string())
    }

    fn reset_with_error(&mut self, message: String) -> String {
        self.connection = None;
        message
    }
}

fn fetch_once_on_connection(
    connection: &mut BrokerConnection,
    topic: &str,
    offset: u64,
) -> Result<BrokerResponse, String> {
    let frame = encode_fetch(topic, offset).map_err(|e| e.to_string())?;
    connection
        .writer
        .write_all(&frame)
        .map_err(|e| classify_io_error("write", &e))?;
    connection
        .writer
        .flush()
        .map_err(|e| classify_io_error("flush", &e))?;

    let buf = read_frame(&mut connection.reader).map_err(|e| e.to_string())?;

    parse_framed_response(&buf)
}

fn parse_framed_response(buf: &[u8]) -> Result<BrokerResponse, String> {
    let response = decode_response_frame(buf).map_err(|e| format!("broker wire error: {e}"))?;
    match response {
        Response::None => Ok(BrokerResponse::None),
        Response::Message { offset, payload } => Ok(BrokerResponse::Message {
            offset,
            payload: String::from_utf8_lossy(&payload).into_owned(),
        }),
        Response::Error(reason) => Err(format!("broker error: {reason}")),
        Response::OkOffset(_) => Err("unexpected OkOffset response to FETCH".to_string()),
    }
}
