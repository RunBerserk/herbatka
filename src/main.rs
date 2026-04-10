use std::io;
use std::sync::{Arc, Mutex};

use herbatka::broker::core::Broker;
use herbatka::tcp::server;

fn main() {
    let addr = "127.0.0.1:7000";
    let broker = Arc::new(Mutex::new(Broker::new()));
    if let Err(e) = server::run(addr, broker) {
        if e.kind() == io::ErrorKind::AddrInUse {
            eprintln!(
                "server error: {e}\n\
                 hint: port 7000 is already in use (another herbatka or app). \
                 Stop that process or pick a different port."
            );
        } else {
            eprintln!("server error: {e}");
        }
    }
}
