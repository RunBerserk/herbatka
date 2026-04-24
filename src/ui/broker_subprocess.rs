//! Spawn `herbatka` as a child and pump stdout/stderr into a [`LogLine`](super::process_log::LogLine) channel.

use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::Sender;
use std::thread;

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

fn pump_read(
    source: LogSource,
    stream: LogStream,
    r: impl Read + Send + 'static,
    tx: Sender<LogLine>,
) {
    let mut reader = BufReader::new(r);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                if tx
                    .send(LogLine {
                        source,
                        stream,
                        text: line.clone(),
                    })
                    .is_err()
                {
                    break;
                }
            }
            Err(e) => {
                let _ = tx.send(LogLine {
                    source,
                    stream,
                    text: format!("<read error: {e}>\n"),
                });
                break;
            }
        }
    }
}
