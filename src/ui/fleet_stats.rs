//! Read-only fleet aggregates for the UI stats panel. Uses **simulation time** (`ts_ms`) for
//! staleness: a vehicle is stale if it is more than [`STALE_THRESHOLD_MS`] behind the newest
//! `ts_ms` in the current snapshot set (works with the simulator’s synthetic monotonic clock).

use std::collections::BTreeMap;

use super::model::VehicleSnapshot;

/// How far behind the fleet’s newest `ts_ms` a vehicle may be before it counts as stale.
pub const STALE_THRESHOLD_MS: u64 = 10_000;

/// Aggregated numbers derived only from the in-memory fleet map and the UI read cursor
/// (`next_offset` = next `FETCH` offset; not the broker’s global high-water mark).
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct FleetStats {
    pub online: usize,
    pub stale: usize,
    /// Arithmetic mean of `speed` (km/h); `None` if the fleet is empty.
    pub avg_speed_kmh: Option<f64>,
    pub read_next_offset: u64,
    /// Highest `last_offset` among buffered snapshots, if any.
    pub newest_buffered_offset: Option<u64>,
    /// `read_next - 1 - newest_buffered` when both sides are known; 0 = UI read head matches buffer.
    pub lag_events: Option<u64>,
}

pub fn compute_fleet_stats(
    fleet: &BTreeMap<String, VehicleSnapshot>,
    read_next_offset: u64,
) -> FleetStats {
    let n = fleet.len();
    if n == 0 {
        return FleetStats {
            online: 0,
            stale: 0,
            avg_speed_kmh: None,
            read_next_offset,
            newest_buffered_offset: None,
            lag_events: None,
        };
    }

    let max_ts = fleet
        .values()
        .map(|s| s.ts_ms)
        .max()
        .expect("non-empty map");

    let mut stale = 0usize;
    for s in fleet.values() {
        if max_ts.saturating_sub(s.ts_ms) > STALE_THRESHOLD_MS {
            stale += 1;
        }
    }
    let online = n - stale;

    let sum_speed: u64 = fleet.values().map(|s| s.speed).sum();
    let avg_speed_kmh = Some(sum_speed as f64 / n as f64);

    let newest_buffered_offset = fleet.values().map(|s| s.last_offset).max();

    let lag_events = match newest_buffered_offset {
        None => None,
        Some(_max_off) if read_next_offset == 0 => None,
        Some(max_off) => {
            let read_at = read_next_offset.saturating_sub(1);
            Some(read_at.saturating_sub(max_off))
        }
    };

    FleetStats {
        online,
        stale,
        avg_speed_kmh,
        read_next_offset,
        newest_buffered_offset,
        lag_events,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(ts_ms: u64, speed: u64, last_offset: u64) -> VehicleSnapshot {
        VehicleSnapshot {
            ts_ms,
            speed,
            lat: 0.0,
            lon: 0.0,
            last_offset,
        }
    }

    #[test]
    fn empty_fleet() {
        let m = BTreeMap::new();
        let s = compute_fleet_stats(&m, 0);
        assert_eq!(s, FleetStats {
            online: 0,
            stale: 0,
            avg_speed_kmh: None,
            read_next_offset: 0,
            newest_buffered_offset: None,
            lag_events: None,
        });
    }

    #[test]
    fn single_vehicle_avg_and_no_stale() {
        let mut m = BTreeMap::new();
        m.insert("veh-0".to_string(), snap(1000, 42, 5));
        let s = compute_fleet_stats(&m, 6);
        assert_eq!(s.avg_speed_kmh, Some(42.0));
        assert_eq!(s.online, 1);
        assert_eq!(s.stale, 0);
        assert_eq!(s.newest_buffered_offset, Some(5));
        assert_eq!(s.lag_events, Some(0));
    }

    #[test]
    fn two_vehicles_one_stale_by_relative_ts() {
        let mut m = BTreeMap::new();
        m.insert("a".to_string(), snap(0, 20, 1));
        m.insert("b".to_string(), snap(20_000, 30, 2));
        let s = compute_fleet_stats(&m, 3);
        assert_eq!(s.stale, 1);
        assert_eq!(s.online, 1);
        assert!((s.avg_speed_kmh.unwrap() - 25.0).abs() < 1e-9);
    }
}
