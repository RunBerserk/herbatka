//! Fleet-style simulator: sends JSON `PRODUCE` events to the broker.
//! Usage:
//!   simulator --addr <host:port> --topic <name> --vehicles <n> --rate <events_per_sec> --duration-secs <n> [--scenario <steady|burst|idle|reconnect>] [--seed <u64>] [--quiet]

use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::thread::sleep;
use std::time::{Duration, Instant};

use herbatka::observability;
use tracing::error;

const USAGE: &str = "usage: simulator --addr <host:port> --topic <name> --vehicles <n> --rate <events_per_sec> --duration-secs <n> [--scenario <steady|burst|idle|reconnect>] [--seed <u64>] [--quiet]";
const DEFAULT_SEED: u64 = 0xC0FFEE1234;
const CONNECT_RETRY_MAX_ATTEMPTS: u32 = 3;
const PROGRESS_EVERY_SECS: u64 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SimulatorArgs {
    addr: String,
    topic: String,
    vehicles: u64,
    rate: u64,
    duration_secs: u64,
    scenario: ScenarioKind,
    seed: Option<u64>,
    quiet: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Summary {
    produced_ok: u64,
    produced_err: u64,
    connect_errors: u64,
    write_errors: u64,
    read_errors: u64,
    non_ok_responses: u64,
    reconnect_attempts: u64,
    reconnect_successes: u64,
    skipped_events: u64,
}

impl Summary {
    fn total(&self) -> u64 {
        self.produced_ok + self.produced_err
    }

    fn to_report(self) -> String {
        format!(
            "simulation done: ok={}, err={}, total={} | connect_err={}, write_err={}, read_err={}, non_ok={}, reconnect_attempts={}, reconnect_ok={}, skipped={}",
            self.produced_ok,
            self.produced_err,
            self.total(),
            self.connect_errors,
            self.write_errors,
            self.read_errors,
            self.non_ok_responses,
            self.reconnect_attempts,
            self.reconnect_successes,
            self.skipped_events
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScenarioKind {
    Steady,
    Burst,
    Idle,
    Reconnect,
}

impl ScenarioKind {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "steady" => Some(Self::Steady),
            "burst" => Some(Self::Burst),
            "idle" => Some(Self::Idle),
            "reconnect" => Some(Self::Reconnect),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Steady => "steady",
            Self::Burst => "burst",
            Self::Idle => "idle",
            Self::Reconnect => "reconnect",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cadence {
    Base,
    Fast,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScenarioDecision {
    send: bool,
    reconnect: bool,
    cadence: Cadence,
}

#[derive(Debug, Clone, Copy)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        // Small deterministic LCG for reproducible simulator variation.
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        self.state
    }

    fn next_i32_inclusive(&mut self, min: i32, max: i32) -> i32 {
        let span = (max - min + 1) as u64;
        min + (self.next_u64() % span) as i32
    }
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
    let summary = run_simulation(&args).map_err(|e| format!("simulation failed: {e}"))?;
    if summary.produced_ok == 0 {
        return Err("simulation completed but no successful sends".to_string());
    }
    println!("{}", summary.to_report());
    Ok(())
}

fn parse_args() -> Result<SimulatorArgs, String> {
    let args: Vec<String> = env::args().skip(1).collect();
    parse_args_from(&args)
}

fn parse_args_from(args: &[String]) -> Result<SimulatorArgs, String> {
    if args.len() < 10 {
        return Err(USAGE.to_string());
    }

    let mut addr: Option<String> = None;
    let mut topic: Option<String> = None;
    let mut vehicles: Option<u64> = None;
    let mut rate: Option<u64> = None;
    let mut duration_secs: Option<u64> = None;
    let mut scenario = ScenarioKind::Steady;
    let mut seed: Option<u64> = None;
    let mut quiet = false;

    let mut i = 0usize;
    while i < args.len() {
        let flag = &args[i];
        if flag == "--quiet" {
            quiet = true;
            i += 1;
            continue;
        }
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--addr" => addr = Some(value.clone()),
            "--topic" => topic = Some(value.clone()),
            "--vehicles" => vehicles = Some(parse_positive_u64("--vehicles", value)?),
            "--rate" => rate = Some(parse_positive_u64("--rate", value)?),
            "--duration-secs" => duration_secs = Some(parse_positive_u64("--duration-secs", value)?),
            "--scenario" => {
                scenario = ScenarioKind::parse(value)
                    .ok_or_else(|| "scenario must be one of: steady, burst, idle, reconnect".to_string())?;
            }
            "--seed" => {
                seed = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| "--seed must be a non-negative integer".to_string())?,
                );
            }
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
        scenario,
        seed,
        quiet,
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
    let interval_base = Duration::from_secs_f64(1.0f64 / args.rate as f64);
    let mut next_tick = Instant::now();
    let mut next_progress = Instant::now() + Duration::from_secs(PROGRESS_EVERY_SECS);
    let seed = args.seed.unwrap_or(DEFAULT_SEED);
    let mut rng = DeterministicRng::new(seed);
    let run_start = Instant::now();

    let mut summary = Summary::default();
    let (mut stream, mut reader) = connect_with_retry(&args.addr, &mut summary)?;
    for seq in 0..total_events {
        let decision = scenario_decision(args.scenario, seq);
        let interval = match decision.cadence {
            Cadence::Base => interval_base,
            Cadence::Fast => interval_base.div_f64(2.0),
        };
        next_tick += interval;
        let now = Instant::now();
        if now < next_tick {
            sleep(next_tick - now);
        }

        if decision.reconnect {
            summary.reconnect_attempts += 1;
            let (new_stream, new_reader) = connect_with_retry(&args.addr, &mut summary)?;
            summary.reconnect_successes += 1;
            stream = new_stream;
            reader = new_reader;
        }

        if !decision.send {
            summary.skipped_events += 1;
            continue;
        }

        let vehicle_id = seq % args.vehicles;
        let payload = build_event_payload(seq, vehicle_id, &mut rng);
        let request = build_produce_line(&args.topic, &payload)?;
        stream
            .write_all(request.as_bytes())
            .map_err(|e| {
                summary.produced_err += 1;
                summary.write_errors += 1;
                format!("write failed at seq {seq}: {e}")
            })?;
        stream
            .flush()
            .map_err(|e| {
                summary.produced_err += 1;
                summary.write_errors += 1;
                format!("flush failed at seq {seq}: {e}")
            })?;

        let mut response = String::new();
        reader
            .read_line(&mut response)
            .map_err(|e| {
                summary.produced_err += 1;
                summary.read_errors += 1;
                format!("read failed at seq {seq}: {e}")
            })?;
        if response.is_empty() {
            summary.produced_err += 1;
            summary.read_errors += 1;
            return Err(format!("server closed connection at seq {seq}"));
        }

        if response.starts_with("OK ") {
            summary.produced_ok += 1;
        } else {
            summary.produced_err += 1;
            summary.non_ok_responses += 1;
        }

        if !args.quiet && Instant::now() >= next_progress {
            let elapsed = run_start.elapsed().as_secs();
            println!(
                "progress: scenario={} elapsed={}s ok={} err={} total={} reconnect_ok={}",
                args.scenario.as_str(),
                elapsed,
                summary.produced_ok,
                summary.produced_err,
                summary.total(),
                summary.reconnect_successes
            );
            next_progress += Duration::from_secs(PROGRESS_EVERY_SECS);
        }
    }

    Ok(summary)
}

fn open_connection_once(addr: &str) -> Result<(TcpStream, BufReader<TcpStream>), String> {
    let stream = TcpStream::connect(addr).map_err(|e| format!("connect failed to {addr}: {e}"))?;
    let reader_stream = stream
        .try_clone()
        .map_err(|e| format!("clone failed: {e}"))?;
    let reader = BufReader::new(reader_stream);
    Ok((stream, reader))
}

fn connect_with_retry(addr: &str, summary: &mut Summary) -> Result<(TcpStream, BufReader<TcpStream>), String> {
    for attempt in 1..=CONNECT_RETRY_MAX_ATTEMPTS {
        match open_connection_once(addr) {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                summary.connect_errors += 1;
                if attempt == CONNECT_RETRY_MAX_ATTEMPTS {
                    return Err(e);
                }
                sleep(retry_backoff(attempt));
            }
        }
    }
    Err("unreachable retry state".to_string())
}

fn retry_backoff(attempt: u32) -> Duration {
    // 1->200ms, 2->400ms, ...
    Duration::from_millis(200u64.saturating_mul(attempt as u64))
}

fn scenario_decision(kind: ScenarioKind, seq: u64) -> ScenarioDecision {
    match kind {
        ScenarioKind::Steady => ScenarioDecision {
            send: true,
            reconnect: false,
            cadence: Cadence::Base,
        },
        ScenarioKind::Burst => {
            let in_burst = (seq / 20) % 2 == 1;
            ScenarioDecision {
                send: true,
                reconnect: false,
                cadence: if in_burst { Cadence::Fast } else { Cadence::Base },
            }
        }
        ScenarioKind::Idle => {
            let in_idle = (seq / 30) % 2 == 1;
            ScenarioDecision {
                send: !in_idle,
                reconnect: false,
                cadence: Cadence::Base,
            }
        }
        ScenarioKind::Reconnect => ScenarioDecision {
            send: true,
            reconnect: seq > 0 && seq.is_multiple_of(25),
            cadence: Cadence::Base,
        },
    }
}

fn build_event_payload(seq: u64, vehicle_id: u64, rng: &mut DeterministicRng) -> String {
    // Deterministic, human-readable event shape for repeatable MVP runs.
    // Fixed timestamp anchor plus 100 ms/event to simulate a stable event clock.
    let ts_ms = 1_700_000_000_000u64.saturating_add(seq.saturating_mul(100));
    // Keep speeds in a simple moving-vehicle range: 20..109 km/h.
    let speed_base = 20u64 + (seq % 90);
    let speed_jitter = rng.next_i32_inclusive(-2, 2);
    let speed = (speed_base as i32 + speed_jitter).max(0) as u64;
    // Base coordinates around central Poland; per-vehicle / per-event drift is synthetic.
    let lat = 52.0f64 + (vehicle_id as f64 * 0.0001f64);
    let lon_jitter = rng.next_i32_inclusive(-1, 1) as f64 * 0.000001f64;
    let lon = 21.0f64 + (seq as f64 * 0.00001f64) + lon_jitter;
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
            "--scenario".to_string(),
            "burst".to_string(),
            "--seed".to_string(),
            "7".to_string(),
            "--quiet".to_string(),
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
                scenario: ScenarioKind::Burst,
                seed: Some(7),
                quiet: true,
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
    fn parse_args_from_defaults_to_steady_when_omitted() {
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
        assert_eq!(parsed.scenario, ScenarioKind::Steady);
        assert_eq!(parsed.seed, None);
        assert!(!parsed.quiet);
    }

    #[test]
    fn parse_args_from_rejects_invalid_scenario() {
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
            "--scenario".to_string(),
            "weird".to_string(),
        ];
        assert!(parse_args_from(&args).is_err());
    }

    #[test]
    fn event_payload_contains_expected_json_fields() {
        let mut rng = DeterministicRng::new(123);
        let payload = build_event_payload(7, 2, &mut rng);
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
        let mut rng = DeterministicRng::new(42);
        let payload = build_event_payload(1, 1, &mut rng);
        let line = build_produce_line("events", &payload).expect("line should build");
        assert!(line.starts_with("PRODUCE events "));
        assert!(line.ends_with('\n'));
        assert_eq!(line.matches('\n').count(), 1);
    }

    #[test]
    fn same_seed_produces_same_payload_sequence() {
        let mut rng_a = DeterministicRng::new(77);
        let mut rng_b = DeterministicRng::new(77);
        let a1 = build_event_payload(1, 1, &mut rng_a);
        let a2 = build_event_payload(2, 1, &mut rng_a);
        let b1 = build_event_payload(1, 1, &mut rng_b);
        let b2 = build_event_payload(2, 1, &mut rng_b);
        assert_eq!(a1, b1);
        assert_eq!(a2, b2);
    }

    #[test]
    fn scenario_decision_changes_behavior() {
        let steady = scenario_decision(ScenarioKind::Steady, 10);
        assert!(steady.send);
        assert!(!steady.reconnect);
        assert_eq!(steady.cadence, Cadence::Base);

        let burst_fast = scenario_decision(ScenarioKind::Burst, 25);
        assert_eq!(burst_fast.cadence, Cadence::Fast);

        let idle_pause = scenario_decision(ScenarioKind::Idle, 35);
        assert!(!idle_pause.send);

        let reconnect_mark = scenario_decision(ScenarioKind::Reconnect, 25);
        assert!(reconnect_mark.reconnect);
    }

    #[test]
    fn retry_backoff_increases_with_attempt() {
        assert_eq!(retry_backoff(1), Duration::from_millis(200));
        assert_eq!(retry_backoff(2), Duration::from_millis(400));
    }

    #[test]
    fn summary_report_contains_all_main_counters() {
        let summary = Summary {
            produced_ok: 3,
            produced_err: 1,
            connect_errors: 2,
            write_errors: 1,
            read_errors: 0,
            non_ok_responses: 0,
            reconnect_attempts: 1,
            reconnect_successes: 1,
            skipped_events: 4,
        };
        let report = summary.to_report();
        assert!(report.contains("ok=3"));
        assert!(report.contains("err=1"));
        assert!(report.contains("connect_err=2"));
        assert!(report.contains("skipped=4"));
    }
}
