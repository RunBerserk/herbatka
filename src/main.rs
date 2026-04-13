use std::io;
use std::sync::{Arc, Mutex};

use herbatka::broker::core::Broker;
use herbatka::observability;
use herbatka::tcp::server;
use tracing::error;

fn main() {
    observability::init();
    let addr = "127.0.0.1:7000";
    let broker = Arc::new(Mutex::new(Broker::new()));
    {
        let mut broker_guard = match broker.lock() {
            Ok(guard) => guard,
            Err(_) => {
                error!("broker startup discovery failed: internal lock error");
                return;
            }
        };
        if let Err(e) = broker_guard.discover_topics_on_startup() {
            error!("broker startup discovery failed: {:?}", e);
            return;
        }
    }
    if let Err(e) = server::run(addr, broker) {
        if e.kind() == io::ErrorKind::AddrInUse {
            error!(
                "server error: {e}\n\
                 hint: port 7000 is already in use (another herbatka or app). \
                 Stop that process or pick a different port."
            );
        } else {
            error!("server error: {e}");
        }
    }
}
