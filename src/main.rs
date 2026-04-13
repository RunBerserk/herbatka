use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use herbatka::broker::core::Broker;
use herbatka::config::load_broker_config;
use herbatka::observability;
use herbatka::tcp::server;
use tracing::error;

fn main() {
    observability::init();
    let addr = "127.0.0.1:7000";
    let config_path =
        std::env::var("HERBATKA_CONFIG").unwrap_or_else(|_| "herbatka.toml".to_string());
    let broker_config = match load_broker_config(Path::new(&config_path)) {
        Ok(config) => config,
        Err(e) => {
            error!("failed to load broker config from {}: {e}", config_path);
            return;
        }
    };
    let broker = Arc::new(Mutex::new(Broker::with_config(broker_config)));
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
