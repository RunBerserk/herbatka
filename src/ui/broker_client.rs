use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

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
    reader: BufReader<TcpStream>,
}

const CONNECT_TIMEOUT: Duration = Duration::from_millis(35);
const IO_TIMEOUT: Duration = Duration::from_millis(400);

fn classify_connect_error(addr: &str, error: &io::Error) -> String {
    match error.kind() {
        io::ErrorKind::ConnectionRefused => {
            format!("broker unavailable at {addr} (connection refused)")
        }
        io::ErrorKind::TimedOut => format!("broker unavailable at {addr} (connect timeout)"),
        io::ErrorKind::NotFound | io::ErrorKind::AddrNotAvailable => {
            format!("broker address {addr} is invalid or unavailable")
        }
        _ => format!("connect {addr} failed: {error}"),
    }
}

fn classify_io_error(phase: &str, error: &io::Error) -> String {
    match error.kind() {
        io::ErrorKind::TimedOut => format!("broker {phase} timeout"),
        io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::UnexpectedEof => format!("broker connection lost during {phase}"),
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
        let request = format!("FETCH {} {offset}\n", self.topic);
        let result = {
            let connection = self.ensure_connected()?;
            fetch_once_on_connection(connection, &request)
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
            let stream = TcpStream::connect_timeout(&target, CONNECT_TIMEOUT)
                .map_err(|e| classify_connect_error(&self.addr, &e))?;
            stream
                .set_read_timeout(Some(IO_TIMEOUT))
                .map_err(|e| format!("set read timeout failed: {e}"))?;
            stream
                .set_write_timeout(Some(IO_TIMEOUT))
                .map_err(|e| format!("set write timeout failed: {e}"))?;
            let reader_stream = stream
                .try_clone()
                .map_err(|e| format!("reader clone failed: {e}"))?;
            let reader = BufReader::new(reader_stream);
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
    request: &str,
) -> Result<BrokerResponse, String> {
    connection
        .writer
        .write_all(request.as_bytes())
        .map_err(|e| classify_io_error("write", &e))?;
    connection
        .writer
        .flush()
        .map_err(|e| classify_io_error("flush", &e))?;

    let mut line = String::new();
    connection
        .reader
        .read_line(&mut line)
        .map_err(|e| classify_io_error("read", &e))?;
    if line.is_empty() {
        return Err("broker closed connection".to_string());
    }

    parse_response_line(line.trim_end())
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
