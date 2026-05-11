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
    let config_path =
        std::env::var("HERBATKA_CONFIG").unwrap_or_else(|_| "herbatka.toml".to_string());
    let broker_config = match load_broker_config(Path::new(&config_path)) {
        Ok(config) => config,
        Err(e) => {
            error!("failed to load broker config from {}: {e}", config_path);
            return;
        }
    };
    let bind_addr = broker_config.listen_addr.clone();
    let mut broker = Broker::with_config(broker_config);
    if let Err(e) = broker.discover_topics_on_startup() {
        error!("broker startup discovery failed: {:?}", e);
        return;
    }
    let broker = Arc::new(Mutex::new(broker));
    if let Err(e) = server::run(bind_addr.as_str(), broker) {
        if e.kind() == io::ErrorKind::AddrInUse {
            error!(
                "server error: {e}\n\
                 hint: listen address {} is already in use (another herbatka or app). \
                 Stop that process or change `listen_addr` in herbatka.toml.",
                bind_addr
            );
        } else {
            error!("server error: {e}");
        }
    }
}
