//! Spawn `herbatka` as a child and pump stdout/stderr into a [`LogLine`](super::process_log::LogLine) channel.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::Sender;
use std::thread;

use super::child_output::pump_read;
use super::process_log::{LogLine, LogSource, LogStream};

/// Runs `cargo run -q --bin herbatka` with working directory = crate / repo root.  
/// Stdout and stderr are read on background threads; on `Stop` the caller should
/// `kill` the `Child` so the reader threads see EOF and exit.
pub fn spawn_broker(log_tx: &Sender<LogLine>) -> Result<Child, String> {
    let workdir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut child = Command::new("cargo")
        .args(["run", "-q", "--bin", "herbatka"])
        .current_dir(workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to start broker: {e}"))?;

    let out = child
        .stdout
        .take()
        .ok_or_else(|| "broker: no stdout handle".to_string())?;
    let err = child
        .stderr
        .take()
        .ok_or_else(|| "broker: no stderr handle".to_string())?;

    let tx1 = log_tx.clone();
    let tx2 = log_tx.clone();
    thread::spawn(move || pump_read(LogSource::Broker, LogStream::Stdout, out, tx1));
    thread::spawn(move || pump_read(LogSource::Broker, LogStream::Stderr, err, tx2));

    Ok(child)
}
