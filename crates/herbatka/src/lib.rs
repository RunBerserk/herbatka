//! Herbatka — mini Kafka–style log and broker.
//!
//! Library crate: used by the broker binary and by integration tests.

pub mod broker;
pub mod config;
pub mod log;
pub mod time;

pub use herbatka_wire::generated_schemas;
pub use herbatka_wire::observability;

pub mod tcp;
