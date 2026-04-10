use std::sync::{Arc, Mutex};

use herbatka::broker::core::Broker;
use herbatka::tcp::server;

fn main() {
    let addr = "127.0.0.1:7000";
    let broker = Arc::new(Mutex::new(Broker::new()));
    println!("herbatka tcp listening on {addr}");
    if let Err(e) = server::run(addr, broker) {
        eprintln!("server error: {e}");
    }
}
