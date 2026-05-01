use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use super::{
    DeterministicRng, SimulatorArgs, Summary, VehicleMotionState, advance_vehicle_with_walls,
    build_event_payload, build_produce_line, speed_for_event,
};

pub(super) fn execute_event_cycle(
    args: &SimulatorArgs,
    seq: u64,
    interval: Duration,
    motion: &mut [VehicleMotionState],
    rng: &mut DeterministicRng,
    stream: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    summary: &mut Summary,
) -> Result<(), String> {
    let vehicle_id = seq % args.vehicles;
    let speed = speed_for_event(seq, rng);
    let tick_now = Instant::now();
    let vehicle = &mut motion[vehicle_id as usize];
    let dt = tick_now
        .checked_duration_since(vehicle.last_update_at)
        .unwrap_or(interval);
    vehicle.last_update_at = tick_now;
    let snapshot = advance_vehicle_with_walls(vehicle, speed, dt, vehicle_id, rng);
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

    Ok(())
}
