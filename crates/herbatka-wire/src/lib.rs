//! Herbatka wire protocol: framed v1, text commands, and optional fleet protobuf bindings.

pub mod generated_schemas;
pub mod observability;
pub mod tcp {
    pub mod command;
    pub mod frame;
    pub mod protocol;
}
