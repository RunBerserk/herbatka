//! Spawn the `simulator` binary and pump stdout/stderr into [`LogLine`](super::process_log::LogLine).
//!
//! CLI knobs beyond addr/topic are fixed for the shell MVP; a settings panel can pass overrides later.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::Sender;
use std::thread;

use super::child_output::pump_read;
use super::process_log::{LogLine, LogSource, LogStream};

const SIM_DEFAULT_VEHICLES: u64 = 5;
const SIM_DEFAULT_RATE: u64 = 10;
/// ~24h: simulator requires `--duration-secs`; user stops early via UI **Stop** (`kill`).
const SIM_DEFAULT_DURATION_SECS: u64 = 24 * 60 * 60;

/// Runs `cargo run -q --bin simulator -- ...` with `current_dir` = repo root.  
/// On **Stop**, the caller should `kill` the `Child` so reader threads see EOF and exit.
pub fn spawn_simulator(
    log_tx: &Sender<LogLine>,
    addr: &str,
    topic: &str,
) -> Result<Child, String> {
    let workdir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut child = Command::new("cargo")
        .arg("run")
        .arg("-q")
        .arg("--bin")
        .arg("simulator")
        .arg("--")
        .arg("--addr")
        .arg(addr)
        .arg("--topic")
        .arg(topic)
        .arg("--vehicles")
        .arg(SIM_DEFAULT_VEHICLES.to_string())
        .arg("--rate")
        .arg(SIM_DEFAULT_RATE.to_string())
        .arg("--duration-secs")
        .arg(SIM_DEFAULT_DURATION_SECS.to_string())
        .arg("--scenario")
        .arg("steady")
        .arg("--load-profile")
        .arg("constant")
        .current_dir(workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to start simulator: {e}"))?;

    let out = child
        .stdout
        .take()
        .ok_or_else(|| "simulator: no stdout handle".to_string())?;
    let err = child
        .stderr
        .take()
        .ok_or_else(|| "simulator: no stderr handle".to_string())?;

    let tx1 = log_tx.clone();
    let tx2 = log_tx.clone();
    thread::spawn(move || pump_read(LogSource::Simulator, LogStream::Stdout, out, tx1));
    thread::spawn(move || pump_read(LogSource::Simulator, LogStream::Stderr, err, tx2));

    Ok(child)
}
