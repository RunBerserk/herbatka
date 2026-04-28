//! Fleet-style simulator: sends JSON `PRODUCE` events to the broker.
//! Usage:
//!   simulator --addr <host:port> --topic <name> --vehicles <n> --rate <events_per_sec> --duration-secs <n> [--scenario <steady|burst|idle|reconnect>] [--load-profile <constant|ramp|spike>] [--seed <u64>] [--quiet]

use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::thread::sleep;
use std::time::{Duration, Instant};

use herbatka::observability;
use tracing::error;

const USAGE: &str = "usage: simulator --addr <host:port> --topic <name> --vehicles <n> --rate <events_per_sec> --duration-secs <n> [--scenario <steady|burst|idle|reconnect>] [--load-profile <constant|ramp|spike>] [--seed <u64>] [--quiet]";
const DEFAULT_SEED: u64 = 0xC0FFEE1234;
const CONNECT_RETRY_MAX_ATTEMPTS: u32 = 3;
const PROGRESS_EVERY_SECS: u64 = 5;
const EARTH_METERS_PER_DEGREE: f64 = 111_320.0;
const WALL_MIN_LAT: f64 = 51.985;
const WALL_MAX_LAT: f64 = 52.015;
const WALL_MIN_LON: f64 = 20.975;
const WALL_MAX_LON: f64 = 21.025;
const BASE_CENTER_LAT: f64 = 52.0;
const BASE_CENTER_LON: f64 = 21.0;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SimulatorArgs {
    addr: String,
    topic: String,
    vehicles: u64,
    rate: u64,
    duration_secs: u64,
    scenario: ScenarioKind,
    load_profile: LoadProfileKind,
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
enum LoadProfileKind {
    Constant,
    Ramp,
    Spike,
}

impl LoadProfileKind {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "constant" => Some(Self::Constant),
            "ramp" => Some(Self::Ramp),
            "spike" => Some(Self::Spike),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Constant => "constant",
            Self::Ramp => "ramp",
            Self::Spike => "spike",
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

#[derive(Debug, Clone)]
struct VehicleMotionState {
    lat: f64,
    lon: f64,
    heading_deg: f64,
    last_update_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct MovementSnapshot {
    lat: f64,
    lon: f64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        // Small deterministic LCG for reproducible simulator variation.
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
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
    let mut load_profile = LoadProfileKind::Constant;
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
            "--duration-secs" => {
                duration_secs = Some(parse_positive_u64("--duration-secs", value)?)
            }
            "--scenario" => {
                scenario = ScenarioKind::parse(value).ok_or_else(|| {
                    "scenario must be one of: steady, burst, idle, reconnect".to_string()
                })?;
            }
            "--load-profile" => {
                load_profile = LoadProfileKind::parse(value).ok_or_else(|| {
                    "load profile must be one of: constant, ramp, spike".to_string()
                })?;
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
        load_profile,
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
    let mut next_tick = Instant::now();
    let mut next_progress = Instant::now() + Duration::from_secs(PROGRESS_EVERY_SECS);
    let seed = args.seed.unwrap_or(DEFAULT_SEED);
    let mut rng = DeterministicRng::new(seed);
    let run_start = Instant::now();
    let mut motion = init_vehicle_motion(args.vehicles, &mut rng, run_start);

    let mut summary = Summary::default();
    let (mut stream, mut reader) = connect_with_retry(&args.addr, &mut summary)?;
    for seq in 0..total_events {
        let decision = scenario_decision(args.scenario, seq);
        let profile_multiplier = load_profile_multiplier(args.load_profile, seq, total_events);
        let interval = effective_interval(args.rate, decision.cadence, profile_multiplier);
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
        let speed = speed_for_event(seq, &mut rng);
        let tick_now = Instant::now();
        let vehicle = &mut motion[vehicle_id as usize];
        let dt = tick_now
            .checked_duration_since(vehicle.last_update_at)
            .unwrap_or(interval);
        vehicle.last_update_at = tick_now;
        let snapshot = advance_vehicle_with_walls(vehicle, speed, dt, vehicle_id, &mut rng);
        let payload = build_event_payload(seq, vehicle_id, speed, snapshot);
        let request = build_produce_line(&args.topic, &payload)?;
        stream.write_all(request.as_bytes()).map_err(|e| {
            summary.produced_err += 1;
            summary.write_errors += 1;
            format!("write failed at seq {seq}: {e}")
        })?;
        stream.flush().map_err(|e| {
            summary.produced_err += 1;
            summary.write_errors += 1;
            format!("flush failed at seq {seq}: {e}")
        })?;

        let mut response = String::new();
        reader.read_line(&mut response).map_err(|e| {
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
                "progress: scenario={} load_profile={} elapsed={}s ok={} err={} total={} reconnect_ok={}",
                args.scenario.as_str(),
                args.load_profile.as_str(),
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

fn speed_for_event(seq: u64, rng: &mut DeterministicRng) -> u64 {
    let speed_base = 20u64 + (seq % 90);
    let speed_jitter = rng.next_i32_inclusive(-2, 2);
    (speed_base as i32 + speed_jitter).max(0) as u64
}

fn init_vehicle_motion(
    vehicles: u64,
    rng: &mut DeterministicRng,
    start_at: Instant,
) -> Vec<VehicleMotionState> {
    (0..vehicles)
        .map(|idx| {
            let spread = (idx as f64 % 10.0) * 0.0009;
            let lat_sign = if idx % 2 == 0 { 1.0 } else { -1.0 };
            let lon_sign = if (idx / 2) % 2 == 0 { 1.0 } else { -1.0 };
            let lat = BASE_CENTER_LAT + lat_sign * spread;
            let lon = BASE_CENTER_LON + lon_sign * spread;
            let heading_deg = (rng.next_u64() % 360) as f64;
            VehicleMotionState {
                lat,
                lon,
                heading_deg,
                last_update_at: start_at,
            }
        })
        .collect()
}

fn advance_vehicle_with_walls(
    state: &mut VehicleMotionState,
    speed_kmh: u64,
    dt: Duration,
    vehicle_id: u64,
    rng: &mut DeterministicRng,
) -> MovementSnapshot {
    let dt_secs = dt.as_secs_f64().max(0.001);
    let meters = (speed_kmh as f64 * 1000.0 / 3600.0) * dt_secs;
    let heading_rad = state.heading_deg.to_radians();
    let north_m = meters * heading_rad.cos();
    let east_m = meters * heading_rad.sin();

    let lat_delta = north_m / EARTH_METERS_PER_DEGREE;
    let lon_scale = (state.lat.to_radians().cos()).abs().max(0.1);
    let lon_delta = east_m / (EARTH_METERS_PER_DEGREE * lon_scale);

    state.lat += lat_delta;
    state.lon += lon_delta;

    let mut hit_wall = false;
    if state.lat < WALL_MIN_LAT {
        state.lat = WALL_MIN_LAT;
        hit_wall = true;
    } else if state.lat > WALL_MAX_LAT {
        state.lat = WALL_MAX_LAT;
        hit_wall = true;
    }
    if state.lon < WALL_MIN_LON {
        state.lon = WALL_MIN_LON;
        hit_wall = true;
    } else if state.lon > WALL_MAX_LON {
        state.lon = WALL_MAX_LON;
        hit_wall = true;
    }

    if hit_wall {
        let turn_clockwise = (vehicle_id + rng.next_u64()).is_multiple_of(2);
        state.heading_deg = if turn_clockwise {
            state.heading_deg + 90.0
        } else {
            state.heading_deg - 90.0
        };
        state.heading_deg = state.heading_deg.rem_euclid(360.0);
        let jitter = rng.next_i32_inclusive(-8, 8) as f64;
        state.heading_deg = (state.heading_deg + jitter).rem_euclid(360.0);
    }

    MovementSnapshot {
        lat: state.lat,
        lon: state.lon,
    }
}

fn open_connection_once(addr: &str) -> Result<(TcpStream, BufReader<TcpStream>), String> {
    let stream = TcpStream::connect(addr).map_err(|e| format!("connect failed to {addr}: {e}"))?;
    let reader_stream = stream
        .try_clone()
        .map_err(|e| format!("clone failed: {e}"))?;
    let reader = BufReader::new(reader_stream);
    Ok((stream, reader))
}

fn connect_with_retry(
    addr: &str,
    summary: &mut Summary,
) -> Result<(TcpStream, BufReader<TcpStream>), String> {
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
                cadence: if in_burst {
                    Cadence::Fast
                } else {
                    Cadence::Base
                },
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

fn load_profile_multiplier(kind: LoadProfileKind, seq: u64, total_events: u64) -> f64 {
    match kind {
        LoadProfileKind::Constant => 1.0,
        LoadProfileKind::Ramp => {
            if total_events <= 1 {
                return 1.0;
            }
            let progress = seq as f64 / (total_events - 1) as f64;
            0.5 + progress
        }
        LoadProfileKind::Spike => {
            let in_spike_window = seq % 40 >= 32;
            if in_spike_window { 2.0 } else { 1.0 }
        }
    }
}

fn effective_interval(base_rate: u64, cadence: Cadence, profile_multiplier: f64) -> Duration {
    let cadence_multiplier = match cadence {
        Cadence::Base => 1.0,
        Cadence::Fast => 2.0,
    };
    let effective_rate = ((base_rate as f64) * cadence_multiplier * profile_multiplier).max(0.001);
    Duration::from_secs_f64(1.0 / effective_rate)
}

fn build_event_payload(
    seq: u64,
    vehicle_id: u64,
    speed: u64,
    snapshot: MovementSnapshot,
) -> String {
    // Deterministic, human-readable event shape for repeatable MVP runs.
    // Fixed timestamp anchor plus 100 ms/event to simulate a stable event clock.
    let ts_ms = 1_700_000_000_000u64.saturating_add(seq.saturating_mul(100));
    format!(
        "{{\"vehicle_id\":\"veh-{vehicle_id}\",\"ts_ms\":{ts_ms},\"speed\":{speed},\"lat\":{:.6},\"lon\":{:.6}}}",
        snapshot.lat, snapshot.lon
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
            "--load-profile".to_string(),
            "ramp".to_string(),
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
                load_profile: LoadProfileKind::Ramp,
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
        assert_eq!(parsed.load_profile, LoadProfileKind::Constant);
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
    fn parse_args_from_rejects_invalid_load_profile() {
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
            "--load-profile".to_string(),
            "wild".to_string(),
        ];
        assert!(parse_args_from(&args).is_err());
    }

    #[test]
    fn event_payload_contains_expected_json_fields() {
        let payload = build_event_payload(
            7,
            2,
            64,
            MovementSnapshot {
                lat: 52.000123,
                lon: 21.000456,
            },
        );
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
        let payload = build_event_payload(
            1,
            1,
            40,
            MovementSnapshot {
                lat: 52.0,
                lon: 21.0,
            },
        );
        let line = build_produce_line("events", &payload).expect("line should build");
        assert!(line.starts_with("PRODUCE events "));
        assert!(line.ends_with('\n'));
        assert_eq!(line.matches('\n').count(), 1);
    }

    #[test]
    fn same_seed_produces_same_payload_sequence() {
        let mut rng_a = DeterministicRng::new(77);
        let mut rng_b = DeterministicRng::new(77);
        let started = Instant::now();
        let mut a = init_vehicle_motion(1, &mut rng_a, started);
        let mut b = init_vehicle_motion(1, &mut rng_b, started);
        let dt = Duration::from_millis(100);
        let mut seq_a = Vec::new();
        let mut seq_b = Vec::new();
        for seq in 0..8 {
            let speed_a = speed_for_event(seq, &mut rng_a);
            let speed_b = speed_for_event(seq, &mut rng_b);
            let snap_a = advance_vehicle_with_walls(&mut a[0], speed_a, dt, 0, &mut rng_a);
            let snap_b = advance_vehicle_with_walls(&mut b[0], speed_b, dt, 0, &mut rng_b);
            seq_a.push((snap_a.lat, snap_a.lon));
            seq_b.push((snap_b.lat, snap_b.lon));
        }
        assert_eq!(seq_a, seq_b);
    }

    #[test]
    fn wall_collision_clamps_and_turns_vehicle() {
        let mut rng = DeterministicRng::new(5);
        let mut state = VehicleMotionState {
            lat: WALL_MAX_LAT - 0.00001,
            lon: BASE_CENTER_LON,
            heading_deg: 0.0,
            last_update_at: Instant::now(),
        };
        let snap = advance_vehicle_with_walls(&mut state, 110, Duration::from_secs(2), 1, &mut rng);
        assert!(snap.lat <= WALL_MAX_LAT);
        assert!(snap.lat >= WALL_MIN_LAT);
        assert!(snap.lon <= WALL_MAX_LON);
        assert!(snap.lon >= WALL_MIN_LON);
        assert!(state.heading_deg != 0.0);
    }

    #[test]
    fn movement_stays_within_walls_over_many_steps() {
        let mut rng = DeterministicRng::new(11);
        let mut states = init_vehicle_motion(3, &mut rng, Instant::now());
        for seq in 0..400 {
            for (id, state) in states.iter_mut().enumerate() {
                let speed = speed_for_event(seq, &mut rng);
                let snap = advance_vehicle_with_walls(
                    state,
                    speed,
                    Duration::from_millis(120),
                    id as u64,
                    &mut rng,
                );
                assert!((WALL_MIN_LAT..=WALL_MAX_LAT).contains(&snap.lat));
                assert!((WALL_MIN_LON..=WALL_MAX_LON).contains(&snap.lon));
            }
        }
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
    fn load_profile_multiplier_changes_over_time() {
        let constant = load_profile_multiplier(LoadProfileKind::Constant, 10, 100);
        assert!((constant - 1.0).abs() < f64::EPSILON);

        let ramp_start = load_profile_multiplier(LoadProfileKind::Ramp, 0, 100);
        let ramp_end = load_profile_multiplier(LoadProfileKind::Ramp, 99, 100);
        assert!((ramp_start - 0.5).abs() < 1e-9);
        assert!((ramp_end - 1.5).abs() < 1e-9);

        let spike_normal = load_profile_multiplier(LoadProfileKind::Spike, 15, 100);
        let spike_peak = load_profile_multiplier(LoadProfileKind::Spike, 35, 100);
        assert!((spike_normal - 1.0).abs() < f64::EPSILON);
        assert!((spike_peak - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn effective_interval_is_positive_and_fast_cadence_is_faster() {
        let base = effective_interval(10, Cadence::Base, 1.0);
        let fast = effective_interval(10, Cadence::Fast, 1.0);
        assert!(base > Duration::ZERO);
        assert!(fast > Duration::ZERO);
        assert!(fast < base);
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
