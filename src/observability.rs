//! Tracing subscriber setup for binaries (`RUST_LOG`, default `info` for `herbatka`).

use tracing_subscriber::EnvFilter;

/// Installs a fmt subscriber with env-based filtering. Safe to call more than once; ignores duplicate init.
pub fn init() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("herbatka=info"));

    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
