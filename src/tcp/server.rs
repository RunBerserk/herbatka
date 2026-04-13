//! TCP server runtime for Herbatka.
//! Accepts client connections, parses protocol lines, and dispatches broker operations.
//! Maps broker/protocol outcomes to wire responses with connection-level logging.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::broker::core::{Broker, BrokerError};
use crate::log::message::Message;
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
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }

        let response = match parse_request(&line) {
            Ok(request) => dispatch_request(request, broker),
            Err(msg) => Response::Error(msg.to_string()),
        };

        let out = format_response(&response);
        stream.write_all(out.as_bytes())?;
        stream.flush()?;
    }

    Ok(())
}

fn dispatch_request(request: Request, broker: &Arc<Mutex<Broker>>) -> Response {
    match request {
        Request::Produce { topic, payload } => handle_produce(&topic, &payload, broker),
        Request::Fetch { topic, offset } => handle_fetch(&topic, offset, broker),
    }
}

fn handle_produce(topic: &str, payload: &str, broker: &Arc<Mutex<Broker>>) -> Response {
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
            payload: String::from_utf8_lossy(&message.payload).to_string(),
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

fn build_message(payload: &str) -> Message {
    Message {
        key: None,
        payload: payload.as_bytes().to_vec(),
        timestamp: SystemTime::now(),
        headers: HashMap::new(),
    }
}
