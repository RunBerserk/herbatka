use std::time::{Duration, Instant};

use super::DeterministicRng;

pub(super) const EARTH_METERS_PER_DEGREE: f64 = 111_320.0;
pub(super) const WALL_MIN_LAT: f64 = 51.985;
pub(super) const WALL_MAX_LAT: f64 = 52.015;
pub(super) const WALL_MIN_LON: f64 = 20.975;
pub(super) const WALL_MAX_LON: f64 = 21.025;
pub(super) const BASE_CENTER_LAT: f64 = 52.0;
pub(super) const BASE_CENTER_LON: f64 = 21.0;

#[derive(Debug, Clone)]
pub(super) struct VehicleMotionState {
    pub(super) lat: f64,
    pub(super) lon: f64,
    pub(super) heading_deg: f64,
    pub(super) last_update_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct MovementSnapshot {
    pub(super) lat: f64,
    pub(super) lon: f64,
}

pub(super) fn speed_for_event(seq: u64, rng: &mut DeterministicRng) -> u64 {
    let speed_base = 20u64 + (seq % 90);
    let speed_jitter = rng.next_i32_inclusive(-2, 2);
    (speed_base as i32 + speed_jitter).max(0) as u64
}

pub(super) fn init_vehicle_motion(
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

pub(super) fn advance_vehicle_with_walls(
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
