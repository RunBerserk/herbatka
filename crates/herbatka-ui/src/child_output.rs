//! Read lines from a child `Read` and forward them as [`LogLine`]s (shared by broker/simulator spawners).

use std::io::{BufRead, BufReader, Read};
use std::sync::mpsc::Sender;

use super::process_log::{LogLine, LogSource, LogStream};

pub fn pump_read(
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
