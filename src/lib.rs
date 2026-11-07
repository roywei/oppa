pub mod broker;
pub mod channel;
pub mod channels;
pub mod config;
pub mod connectors;
pub mod forget;
pub mod harness;
pub mod heartbeat;
pub mod knowledge;
pub mod launch;
pub mod mcp;
pub mod memory_sync;
pub mod people;
pub mod process;
pub mod prompt;
pub mod runtime;
pub mod schedule;
pub mod service;
pub mod store;
pub mod tasks;
pub mod vault;
pub mod worker;

/// The largest note, in bytes, the broker lets a model read or commit in one
/// call. `oppa memory import` skips larger files so every note it brings in
/// stays readable.
pub const MAX_NOTE: usize = 256 * 1024;
