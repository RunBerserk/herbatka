//! TCP server runtime for Herbatka.
//! Accepts client connections, parses legacy line protocol or framed wire v1 after handshake,
//! and dispatches broker operations.

use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::broker::core::{Broker, BrokerError};
use crate::log::message::Message;
use crate::tcp::frame::{
    HANDSHAKE_SERVER_ACK_V1, WireError, decode_client_frame, encode_response, read_first_line,
    read_frame, write_frame,
};
use crate::tcp::protocol::{Request, Response, format_response, parse_request};
use tracing::{debug, error, info, warn};

pub fn run(addr: &str, broker: Arc<Mutex<Broker>>) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    info!(%addr, "herbatka tcp listening");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(e) = handle_client(stream, &broker) {
                    error!("client handling error: {e}");
                }
            }
            Err(e) => warn!("accept error: {e}"),
        }
    }
    Ok(())
}

pub fn handle_client(mut stream: TcpStream, broker: &Arc<Mutex<Broker>>) -> io::Result<()> {
    debug!(peer = ?stream.peer_addr(), "connected");

    let first_raw = match read_first_line(&mut stream) {
        Ok(b) => b,
        Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
    };

    if is_handshake_v1(&first_raw) {
        stream.write_all(HANDSHAKE_SERVER_ACK_V1)?;
        stream.flush()?;
        return run_framed_connection(stream, broker);
    }

    let mut reader = BufReader::new(stream);

    let first_line = std::string::String::from_utf8_lossy(&first_raw);

    let response = match parse_request(trim_line(first_line.trim_end_matches('\n'))) {
        Ok(request) => dispatch_request(request, broker),
        Err(msg) => Response::Error(msg.to_string()),
    };

    write_legacy_response(reader.get_mut(), &response)?;

    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }

        let response = match parse_request(trim_line(line.trim_end())) {
            Ok(request) => dispatch_request(request, broker),
            Err(msg) => Response::Error(msg.to_string()),
        };

        write_legacy_response(reader.get_mut(), &response)?;
    }

    Ok(())
}

fn trim_line(s: &str) -> &str {
    s.trim_end_matches(['\n', '\r'])
}

fn write_legacy_response(stream: &mut TcpStream, response: &Response) -> io::Result<()> {
    let out = format_response(response);
    stream.write_all(out.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn is_handshake_v1(raw: &[u8]) -> bool {
    let mut s = raw.strip_suffix(b"\n").unwrap_or(raw);
    s = s.strip_suffix(b"\r").unwrap_or(s);
    s == b"HERBATKA WIRE/1"
}

fn run_framed_connection(mut stream: TcpStream, broker: &Arc<Mutex<Broker>>) -> io::Result<()> {
    loop {
        let frame = match read_frame(&mut stream) {
            Ok(f) => f,
            Err(e) => match e {
                WireError::Io(_) | WireError::Truncated => return Ok(()),
                other => {
                    if let Ok(enc) = encode_response(&Response::Error(other.to_string())) {
                        let _ = write_frame(&mut stream, &enc);
                    }
                    return Ok(());
                }
            },
        };

        let req = match decode_client_frame(&frame) {
            Ok(r) => r,
            Err(e) => {
                let enc = encode_response(&Response::Error(e.to_string())).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("wire encode: {e}"))
                })?;
                write_frame(&mut stream, &enc)?;
                continue;
            }
        };

        let resp = dispatch_request(req, broker);
        let enc = encode_response(&resp).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("wire encode response: {e}"),
            )
        })?;
        write_frame(&mut stream, &enc)?;
    }
}

fn dispatch_request(request: Request, broker: &Arc<Mutex<Broker>>) -> Response {
    match request {
        Request::Produce { topic, payload } => handle_produce(&topic, &payload, broker),
        Request::Fetch { topic, offset } => handle_fetch(&topic, offset, broker),
    }
}

fn handle_produce(topic: &str, payload: &[u8], broker: &Arc<Mutex<Broker>>) -> Response {
    let mut broker_guard = match broker.lock() {
        Ok(guard) => guard,
        Err(_) => return Response::Error("internal lock error".to_string()),
    };

    match broker_guard.produce(topic, build_message(payload)) {
        Ok(offset) => Response::OkOffset(offset),
        Err(BrokerError::UnknownTopic) => {
            if let Err(e) = broker_guard.create_topic(topic.to_string()) {
                return map_broker_error(e);
            }
            match broker_guard.produce(topic, build_message(payload)) {
                Ok(offset) => Response::OkOffset(offset),
                Err(e) => map_broker_error(e),
            }
        }
        Err(e) => map_broker_error(e),
    }
}

fn handle_fetch(topic: &str, offset: u64, broker: &Arc<Mutex<Broker>>) -> Response {
    let broker_guard = match broker.lock() {
        Ok(guard) => guard,
        Err(_) => return Response::Error("internal lock error".to_string()),
    };

    match broker_guard.fetch(topic, offset) {
        Ok(Some(message)) => Response::Message {
            offset,
            payload: message.payload.clone(),
        },
        Ok(None) => Response::None,
        Err(e) => map_broker_error(e),
    }
}

fn map_broker_error(error: BrokerError) -> Response {
    match error {
        BrokerError::TopicAlreadyExists => Response::Error("topic already exists".to_string()),
        BrokerError::UnknownTopic => Response::Error("unknown topic".to_string()),
        BrokerError::Io(e) => Response::Error(format!("io error: {e}")),
    }
}

fn build_message(payload: &[u8]) -> Message {
    Message {
        key: None,
        payload: payload.to_vec(),
        timestamp: SystemTime::now(),
        headers: std::collections::HashMap::new(),
    }
}
