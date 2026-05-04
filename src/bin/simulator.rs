//! Fleet-style simulator: sends telemetry-shaped events on framed wire v1 (handshake then `PRODUCE` frames).
//! Default payload is JSON; **`--payload-format protobuf`** emits `FleetTelemetryEvent` protobuf
//! (`herbatka::generated_schemas`) — align the **`--topic`** with consumers (for example `*.telemetry`).
//! Usage:
//!   simulator ... [--payload-format json|protobuf] [--scenario ...] [--load-profile ...] [--seed <u64>] [--quiet]

use std::thread::sleep;
use std::time::{Duration, Instant};

#[path = "simulator/cli.rs"]
mod cli;
#[path = "simulator/event_cycle.rs"]
mod event_cycle;
#[path = "simulator/movement.rs"]
mod movement;
#[path = "simulator/transport.rs"]
mod transport;

use event_cycle::execute_event_cycle;
use herbatka::observability;
use movement::{
    MovementSnapshot, VehicleMotionState, advance_vehicle_with_walls, init_vehicle_motion,
    speed_for_event,
};
use tracing::error;
use transport::connect_with_retry;

const USAGE: &str = "usage: simulator --addr <host:port> --topic <name> --vehicles <n> --rate <events_per_sec> --duration-secs <n> [--payload-format json|protobuf] [--scenario <steady|burst|idle|reconnect>] [--load-profile <constant|ramp|spike>] [--seed <u64>] [--quiet]";
const DEFAULT_SEED: u64 = 0xC0FFEE1234;
const PROGRESS_EVERY_SECS: u64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum PayloadFormat {
    #[default]
    Json,
    Protobuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SimulatorArgs {
    addr: String,
    topic: String,
    vehicles: u64,
    rate: u64,
    duration_secs: u64,
    payload_format: PayloadFormat,
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
    cli::parse_args()
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
    let mut stream = connect_with_retry(&args.addr, &mut summary)?;
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
            stream = connect_with_retry(&args.addr, &mut summary)?;
            summary.reconnect_successes += 1;
        }

        if !decision.send {
            summary.skipped_events += 1;
            continue;
        }

        let mut io = event_cycle::EventIo {
            stream: &mut stream,
        };
        execute_event_cycle(
            args,
            seq,
            interval,
            &mut motion,
            &mut rng,
            &mut io,
            &mut summary,
        )?;

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

fn build_event_telemetry_proto(
    seq: u64,
    vehicle_id: u64,
    speed: u64,
    snapshot: MovementSnapshot,
) -> Result<Vec<u8>, String> {
    use herbatka::generated_schemas::FleetTelemetryEvent;
    use prost::Message as ProstEncode;
    let ts_ms = 1_700_000_000_000u64.saturating_add(seq.saturating_mul(100));
    let ev = FleetTelemetryEvent {
        vehicle_id: format!("veh-{vehicle_id}"),
        ts_ms,
        speed,
        lat: snapshot.lat,
        lon: snapshot.lon,
    };
    let mut buf = Vec::new();
    ev.encode(&mut buf)
        .map_err(|e| format!("telemetry protobuf encode failed: {e}"))?;
    Ok(buf)
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

#[cfg(test)]
mod tests {
    use super::*;
    use movement::{BASE_CENTER_LON, WALL_MAX_LAT, WALL_MAX_LON, WALL_MIN_LAT, WALL_MIN_LON};

    fn build_produce_frame(topic: &str, payload: &str) -> Result<Vec<u8>, String> {
        transport::build_produce_frame(topic, payload)
    }

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

        let parsed = cli::parse_args_from(&args).expect("parse should succeed");
        assert_eq!(
            parsed,
            SimulatorArgs {
                addr: "127.0.0.1:7000".to_string(),
                topic: "events".to_string(),
                vehicles: 3,
                rate: 10,
                duration_secs: 5,
                payload_format: PayloadFormat::Json,
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
        assert!(cli::parse_args_from(&args).is_err());
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
        let parsed = cli::parse_args_from(&args).expect("parse should succeed");
        assert_eq!(parsed.scenario, ScenarioKind::Steady);
        assert_eq!(parsed.load_profile, LoadProfileKind::Constant);
        assert_eq!(parsed.seed, None);
        assert_eq!(parsed.payload_format, PayloadFormat::Json);
        assert!(!parsed.quiet);
    }

    #[test]
    fn parse_args_from_accepts_payload_format_protobuf() {
        let args = vec![
            "--addr".to_string(),
            "127.0.0.1:7000".to_string(),
            "--topic".to_string(),
            "fleet1.telemetry".to_string(),
            "--vehicles".to_string(),
            "2".to_string(),
            "--rate".to_string(),
            "10".to_string(),
            "--duration-secs".to_string(),
            "5".to_string(),
            "--payload-format".to_string(),
            "protobuf".to_string(),
        ];
        let parsed = cli::parse_args_from(&args).expect("parse should succeed");
        assert_eq!(parsed.payload_format, PayloadFormat::Protobuf);
    }

    #[test]
    fn parse_args_from_rejects_invalid_payload_format() {
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
            "--payload-format".to_string(),
            "xml".to_string(),
        ];
        assert!(cli::parse_args_from(&args).is_err());
    }

    #[test]
    fn telemetry_proto_matches_json_field_semantics_roundtrip_decode() {
        use herbatka::generated_schemas::FleetTelemetryEvent;
        use prost::Message as ProstMsg;
        let snap = MovementSnapshot {
            lat: 52.000123,
            lon: 21.000456,
        };
        let json_line = build_event_payload(9, 2, 88, snap);
        let bin = build_event_telemetry_proto(9, 2, 88, snap).expect("proto encode");
        let ev = FleetTelemetryEvent::decode(&*bin).expect("proto decode");
        assert!(json_line.contains("\"vehicle_id\":\"veh-2\""));
        assert!(json_line.contains("\"speed\":88"));
        assert_eq!(ev.vehicle_id, "veh-2");
        assert_eq!(ev.speed, 88);
        assert_eq!(ev.lat, snap.lat);
        assert_eq!(ev.lon, snap.lon);
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
        assert!(cli::parse_args_from(&args).is_err());
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
        assert!(cli::parse_args_from(&args).is_err());
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
    fn produce_frame_allows_newlines_in_json() {
        let f = build_produce_frame("events", "a\nb").expect("frame");
        assert_eq!(
            f.first().copied(),
            Some(herbatka::tcp::frame::WIRE_VERSION_V1)
        );
        assert_eq!(f.get(1).copied(), Some(herbatka::tcp::frame::OP_PRODUCE));
    }

    #[test]
    fn produce_frame_contains_topic_utf8_after_header() {
        let payload = build_event_payload(
            1,
            1,
            40,
            MovementSnapshot {
                lat: 52.0,
                lon: 21.0,
            },
        );
        let buf = build_produce_frame("events", &payload).expect("frame");
        let req = herbatka::tcp::frame::decode_client_frame(&buf).expect("decode");
        assert_eq!(
            req,
            herbatka::tcp::command::Request::Produce {
                topic: "events".into(),
                payload: payload.as_bytes().to_vec(),
            }
        );
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
        assert_eq!(transport::retry_backoff(1), Duration::from_millis(200));
        assert_eq!(transport::retry_backoff(2), Duration::from_millis(400));
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
