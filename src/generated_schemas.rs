//! Protobuf definitions for fleet-style logical channels.
//!
//! Source of truth: [`proto/herbatka_fleet.proto`](../../proto/herbatka_fleet.proto). This module is
//! **generated at build time** — change the `.proto` and rebuild instead of editing the included file.

#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_errors_doc)]

include!(concat!(env!("OUT_DIR"), "/herbatka.fleet.rs"));
