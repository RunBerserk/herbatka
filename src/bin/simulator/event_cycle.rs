use std::io::Write;
use std::net::TcpStream;
use std::time::{Duration, Instant};

use herbatka::tcp::command::Response;
use herbatka::tcp::frame::{decode_response_frame, read_frame};

use super::{
    DeterministicRng, PayloadFormat, SimulatorArgs, Summary, VehicleMotionState,
    advance_vehicle_with_walls, build_event_payload, build_event_telemetry_proto, speed_for_event,
};

use super::transport;

pub(super) struct EventIo<'a> {
    pub stream: &'a mut TcpStream,
}

pub(super) fn execute_event_cycle(
    args: &SimulatorArgs,
    seq: u64,
    interval: Duration,
    motion: &mut [VehicleMotionState],
    rng: &mut DeterministicRng,
    io: &mut EventIo<'_>,
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
    let frame = match args.payload_format {
        PayloadFormat::Json => {
            let payload = build_event_payload(seq, vehicle_id, speed, snapshot);
            transport::build_produce_frame(&args.topic, &payload)?
        }
        PayloadFormat::Protobuf => {
            let bytes = build_event_telemetry_proto(seq, vehicle_id, speed, snapshot)?;
            transport::build_produce_frame_bytes(&args.topic, &bytes)?
        }
    };
    io.stream.write_all(&frame).map_err(|e| {
        summary.produced_err += 1;
        summary.write_errors += 1;
        format!("write failed at seq {seq}: {e}")
    })?;
    io.stream.flush().map_err(|e| {
        summary.produced_err += 1;
        summary.write_errors += 1;
        format!("flush failed at seq {seq}: {e}")
    })?;

    let resp_buf = read_frame(io.stream).map_err(|e| {
        summary.produced_err += 1;
        summary.read_errors += 1;
        format!("read failed at seq {seq}: {e}")
    })?;
    let response = decode_response_frame(&resp_buf).map_err(|e| {
        summary.produced_err += 1;
        summary.non_ok_responses += 1;
        format!("invalid response frame at seq {seq}: {e}")
    })?;

    match response {
        Response::OkOffset(_) => {
            summary.produced_ok += 1;
            Ok(())
        }
        Response::Error(_) => {
            summary.produced_err += 1;
            summary.non_ok_responses += 1;
            Ok(())
        }
        Response::None | Response::Message { .. } => {
            summary.produced_err += 1;
            summary.non_ok_responses += 1;
            Err(format!(
                "unexpected Produce response at seq {seq} (expected OkOffset)"
            ))
        }
    }
}
