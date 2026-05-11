//! Spawn the `simulator` binary and pump stdout/stderr into [`LogLine`](super::process_log::LogLine).
//!
//! CLI knobs beyond addr/topic are fixed for the shell MVP; a settings panel can pass overrides later.

use std::process::{Child, Command, Stdio};
use std::sync::mpsc::Sender;
use std::thread;

use super::child_output::pump_read;
use super::data_dir;
use super::process_log::{LogLine, LogSource, LogStream};

const SIM_DEFAULT_VEHICLES: u64 = 5;
const SIM_DEFAULT_RATE: u64 = 10;
/// ~24h: simulator requires `--duration-secs`; user stops early via UI **Stop** (`kill`).
const SIM_DEFAULT_DURATION_SECS: u64 = 24 * 60 * 60;

/// Fixed preset for the "Quick demo" UI button (short, bursty load).
const QUICK_DEMO_DURATION_SECS: u64 = 5;
const QUICK_DEMO_SEED: u64 = 42;

fn base_simulator_command(addr: &str, topic: &str) -> Command {
    let mut command = Command::new("cargo");
    command.args([
        "run",
        "-q",
        "-p",
        "herbatka-simulator",
        "--bin",
        "simulator",
        "--",
    ]);
    command.args(["--addr", addr, "--topic", topic]);
    command
        .arg("--vehicles")
        .arg(SIM_DEFAULT_VEHICLES.to_string());
    command.arg("--rate").arg(SIM_DEFAULT_RATE.to_string());
    command
}

fn spawn_with_log_pumps(
    mut command: Command,
    log_tx: &Sender<LogLine>,
    spawn_error_message: &'static str,
) -> Result<Child, String> {
    let mut child = command
        .current_dir(data_dir::workspace_root())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("{spawn_error_message}: {e}"))?;

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

/// Runs `cargo run -q -p herbatka-simulator --bin simulator -- ...` with `current_dir` = workspace repo root.
/// On **Stop**, the caller should `kill` the `Child` so reader threads see EOF and exit.
pub fn spawn_simulator(
    log_tx: &Sender<LogLine>,
    addr: &str,
    topic: &str,
    seed: Option<u64>,
) -> Result<Child, String> {
    let mut command = base_simulator_command(addr, topic);
    command
        .arg("--duration-secs")
        .arg(SIM_DEFAULT_DURATION_SECS.to_string())
        .arg("--scenario")
        .arg("steady")
        .arg("--load-profile")
        .arg("constant");
    if let Some(seed) = seed {
        command.arg("--seed").arg(seed.to_string());
    }
    spawn_with_log_pumps(command, log_tx, "failed to start simulator")
}

/// Short demo run: burst + ramp + fixed seed (matches common local smoke commands).
pub fn spawn_quick_demo_simulator(
    log_tx: &Sender<LogLine>,
    addr: &str,
    topic: &str,
) -> Result<Child, String> {
    let mut command = base_simulator_command(addr, topic);
    command
        .arg("--duration-secs")
        .arg(QUICK_DEMO_DURATION_SECS.to_string())
        .arg("--scenario")
        .arg("burst")
        .arg("--load-profile")
        .arg("ramp")
        .arg("--seed")
        .arg(QUICK_DEMO_SEED.to_string());
    spawn_with_log_pumps(command, log_tx, "failed to start simulator (quick demo)")
}
