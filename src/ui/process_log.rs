//! Bounded in-memory log for process stdout/stderr, fed from a channel.

use std::collections::VecDeque;
use std::sync::mpsc;

/// Where a log line came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSource {
    Broker,
    Simulator,
}

/// Which stream the line was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// One process output line (may include a trailing newline from `read_line`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    pub source: LogSource,
    pub stream: LogStream,
    pub text: String,
}

impl LogLine {
    /// Single string for the bottom panel: `[broker][out] ...` (trim end for display if needed).
    pub fn display_line(&self) -> String {
        let s = match self.source {
            LogSource::Broker => "broker",
            LogSource::Simulator => "sim",
        };
        let t = match self.stream {
            LogStream::Stdout => "out",
            LogStream::Stderr => "err",
        };
        format!("[{s}][{t}] {}", self.text.trim_end())
    }
}

const DEFAULT_MAX_LINES: usize = 2_000;

/// FIFO ring: oldest lines are dropped when over capacity.
pub struct LogRing {
    lines: VecDeque<String>,
    max_lines: usize,
}

impl LogRing {
    pub fn new(max_lines: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            max_lines: max_lines.max(1),
        }
    }

    pub fn with_default_cap() -> Self {
        Self::new(DEFAULT_MAX_LINES)
    }

    pub fn push_line(&mut self, line: &LogLine) {
        self.push_str(&line.display_line());
    }

    pub fn push_str(&mut self, s: &str) {
        self.lines.push_back(s.to_string());
        while self.lines.len() > self.max_lines {
            self.lines.pop_front();
        }
    }

    pub fn clear(&mut self) {
        self.lines.clear();
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// Full text for a monospace `ScrollArea` (bounded by `max_lines` already).
    pub fn as_single_text(&self) -> String {
        self.lines
            .iter()
            .enumerate()
            .fold(String::new(), |acc, (i, l)| {
                if i == 0 {
                    l.clone()
                } else {
                    format!("{acc}\n{l}")
                }
            })
    }
}

/// How many `try_recv` calls per frame to avoid stalling the UI.
pub const DRAIN_PER_FRAME_CAP: usize = 200;

/// Drain up to `DRAIN_PER_FRAME_CAP` (or a custom `cap`) into `ring`.  
/// Returns `true` if at least one line was received.
pub fn drain_into_ring(
    rx: &mpsc::Receiver<LogLine>,
    ring: &mut LogRing,
    cap: usize,
) -> bool {
    let mut any = false;
    for _ in 0..cap {
        match rx.try_recv() {
            Ok(line) => {
                ring.push_line(&line);
                any = true;
            }
            Err(mpsc::TryRecvError::Empty) => break,
            Err(mpsc::TryRecvError::Disconnected) => break,
        }
    }
    any
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(text: &str) -> LogLine {
        LogLine {
            source: LogSource::Broker,
            stream: LogStream::Stdout,
            text: text.to_string(),
        }
    }

    #[test]
    fn ring_drops_oldest_when_over_max() {
        let mut r = LogRing::new(2);
        r.push_str("a");
        r.push_str("b");
        r.push_str("c");
        assert_eq!(r.len(), 2);
        let t = r.as_single_text();
        assert!(!t.contains("a"));
        assert!(t.contains("b") && t.contains("c"));
    }

    #[test]
    fn drain_batches() {
        let (tx, rx) = mpsc::channel();
        for i in 0..10 {
            let _ = tx.send(line(&i.to_string()));
        }
        drop(tx);
        let mut r = LogRing::new(1000);
        assert!(drain_into_ring(&rx, &mut r, 5));
        assert_eq!(r.len(), 5);
        assert!(drain_into_ring(&rx, &mut r, 5));
        assert_eq!(r.len(), 10);
        assert!(!drain_into_ring(&rx, &mut r, 5));
    }
}
