//! Fleet-style simulator: sends JSON `PRODUCE` events to the broker.
//! Usage:
//!   simulator --addr <host:port> --topic <name> --vehicles <n> --rate <events_per_sec> --duration-secs <n>

use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::thread::sleep;
use std::time::{Duration, Instant};

use herbatka::observability;
use tracing::error;

const USAGE: &str = "usage: simulator --addr <host:port> --topic <name> --vehicles <n> --rate <events_per_sec> --duration-secs <n>";

#[derive(Debug, Clone, PartialEq, Eq)]
struct SimulatorArgs {
    addr: String,
    topic: String,
    vehicles: u64,
    rate: u64,
    duration_secs: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Summary {
    produced_ok: u64,
    produced_err: u64,
}

fn main() {
    observability::init();
    if let Err(e) = run() {
        error!("{e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = parse_args()?;
    let summary = run_simulation(&args)?;
    println!(
        "simulation done: ok={}, err={}, total={}",
        summary.produced_ok,
        summary.produced_err,
        summary.produced_ok + summary.produced_err
    );
    Ok(())
}

fn parse_args() -> Result<SimulatorArgs, String> {
    let args: Vec<String> = env::args().skip(1).collect();
    parse_args_from(&args)
}

fn parse_args_from(args: &[String]) -> Result<SimulatorArgs, String> {
    if args.len() != 10 {
        return Err(USAGE.to_string());
    }

    let mut addr: Option<String> = None;
    let mut topic: Option<String> = None;
    let mut vehicles: Option<u64> = None;
    let mut rate: Option<u64> = None;
    let mut duration_secs: Option<u64> = None;

    let mut i = 0usize;
    while i < args.len() {
        let flag = &args[i];
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--addr" => addr = Some(value.clone()),
            "--topic" => topic = Some(value.clone()),
            "--vehicles" => vehicles = Some(parse_positive_u64("--vehicles", value)?),
            "--rate" => rate = Some(parse_positive_u64("--rate", value)?),
            "--duration-secs" => duration_secs = Some(parse_positive_u64("--duration-secs", value)?),
            _ => return Err(format!("unknown flag: {flag}\n{USAGE}")),
        }
        i += 2;
    }

    let parsed = SimulatorArgs {
        addr: addr.ok_or_else(|| format!("missing --addr\n{USAGE}"))?,
        topic: topic.ok_or_else(|| format!("missing --topic\n{USAGE}"))?,
        vehicles: vehicles.ok_or_else(|| format!("missing --vehicles\n{USAGE}"))?,
        rate: rate.ok_or_else(|| format!("missing --rate\n{USAGE}"))?,
        duration_secs: duration_secs.ok_or_else(|| format!("missing --duration-secs\n{USAGE}"))?,
    };

    if parsed.topic.trim().is_empty() {
        return Err("topic must not be empty".to_string());
    }

    Ok(parsed)
}

fn parse_positive_u64(name: &str, raw: &str) -> Result<u64, String> {
    let parsed = raw
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be > 0"));
    }
    Ok(parsed)
}

fn run_simulation(args: &SimulatorArgs) -> Result<Summary, String> {
    let total_events = args.rate.saturating_mul(args.duration_secs);
    let interval = Duration::from_secs_f64(1.0f64 / args.rate as f64);
    let start = Instant::now();

    let mut stream =
        TcpStream::connect(&args.addr).map_err(|e| format!("connect failed to {}: {e}", args.addr))?;
    let reader_stream = stream
        .try_clone()
        .map_err(|e| format!("clone failed: {e}"))?;
    let mut reader = BufReader::new(reader_stream);

    let mut summary = Summary::default();
    for seq in 0..total_events {
        let target = start + interval.mul_f64(seq as f64);
        let now = Instant::now();
        if now < target {
            sleep(target - now);
        }

        let vehicle_id = seq % args.vehicles;
        let payload = build_event_payload(seq, vehicle_id);
        let request = build_produce_line(&args.topic, &payload)?;
        stream
            .write_all(request.as_bytes())
            .map_err(|e| format!("write failed at seq {seq}: {e}"))?;
        stream
            .flush()
            .map_err(|e| format!("flush failed at seq {seq}: {e}"))?;

        let mut response = String::new();
        reader
            .read_line(&mut response)
            .map_err(|e| format!("read failed at seq {seq}: {e}"))?;
        if response.is_empty() {
            return Err(format!("server closed connection at seq {seq}"));
        }

        if response.starts_with("OK ") {
            summary.produced_ok += 1;
        } else {
            summary.produced_err += 1;
        }
    }

    Ok(summary)
}

fn build_event_payload(seq: u64, vehicle_id: u64) -> String {
    // Deterministic, human-readable event shape for repeatable MVP runs.
    // Fixed timestamp anchor plus 100 ms/event to simulate a stable event clock.
    let ts_ms = 1_700_000_000_000u64.saturating_add(seq.saturating_mul(100));
    // Keep speeds in a simple moving-vehicle range: 20..109 km/h.
    let speed = 20u64 + (seq % 90);
    // Base coordinates around central Poland; per-vehicle / per-event drift is synthetic.
    let lat = 52.0f64 + (vehicle_id as f64 * 0.0001f64);
    let lon = 21.0f64 + (seq as f64 * 0.00001f64);
    format!(
        "{{\"vehicle_id\":\"veh-{vehicle_id}\",\"ts_ms\":{ts_ms},\"speed\":{speed},\"lat\":{lat:.6},\"lon\":{lon:.6}}}"
    )
}

fn build_produce_line(topic: &str, payload: &str) -> Result<String, String> {
    if topic.trim().is_empty() {
        return Err("topic must not be empty".to_string());
    }
    if payload.is_empty() {
        return Err("payload must not be empty".to_string());
    }
    if payload.contains('\n') || payload.contains('\r') {
        return Err("payload must not contain newlines".to_string());
    }
    Ok(format!("PRODUCE {topic} {payload}\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_from_accepts_valid_flags() {
        let args = vec![
            "--addr".to_string(),
            "127.0.0.1:7000".to_string(),
            "--topic".to_string(),
            "events".to_string(),
            "--vehicles".to_string(),
            "3".to_string(),
            "--rate".to_string(),
            "10".to_string(),
            "--duration-secs".to_string(),
            "5".to_string(),
        ];

        let parsed = parse_args_from(&args).expect("parse should succeed");
        assert_eq!(
            parsed,
            SimulatorArgs {
                addr: "127.0.0.1:7000".to_string(),
                topic: "events".to_string(),
                vehicles: 3,
                rate: 10,
                duration_secs: 5,
            }
        );
    }

    #[test]
    fn parse_args_from_rejects_non_positive_values() {
        let args = vec![
            "--addr".to_string(),
            "127.0.0.1:7000".to_string(),
            "--topic".to_string(),
            "events".to_string(),
            "--vehicles".to_string(),
            "0".to_string(),
            "--rate".to_string(),
            "10".to_string(),
            "--duration-secs".to_string(),
            "5".to_string(),
        ];
        assert!(parse_args_from(&args).is_err());
    }

    #[test]
    fn event_payload_contains_expected_json_fields() {
        let payload = build_event_payload(7, 2);
        assert!(payload.contains("\"vehicle_id\":\"veh-2\""));
        assert!(payload.contains("\"ts_ms\":"));
        assert!(payload.contains("\"speed\":"));
        assert!(payload.contains("\"lat\":"));
        assert!(payload.contains("\"lon\":"));
        assert!(!payload.contains('\n'));
    }

    #[test]
    fn produce_line_rejects_newline_payload() {
        let line = build_produce_line("events", "bad\npayload");
        assert!(line.is_err());
    }

    #[test]
    fn produce_line_is_single_line_wire_message() {
        let payload = build_event_payload(1, 1);
        let line = build_produce_line("events", &payload).expect("line should build");
        assert!(line.starts_with("PRODUCE events "));
        assert!(line.ends_with('\n'));
        assert_eq!(line.matches('\n').count(), 1);
    }
}
