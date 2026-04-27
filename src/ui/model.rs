use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct FleetEvent {
    pub vehicle_id: String,
    pub ts_ms: u64,
    pub speed: u64,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone)]
pub struct VehicleSnapshot {
    pub ts_ms: u64,
    pub speed: u64,
    pub lat: f64,
    pub lon: f64,
    pub last_offset: u64,
}

pub fn apply_payload(
    fleet: &mut BTreeMap<String, VehicleSnapshot>,
    offset: u64,
    payload: &str,
) -> Result<(), String> {
    let event: FleetEvent = serde_json::from_str(payload)
        .map_err(|e| format!("invalid fleet payload at offset {offset}: {e}"))?;
    let FleetEvent {
        vehicle_id,
        ts_ms,
        speed,
        lat,
        lon,
    } = event;

    let snapshot = VehicleSnapshot {
        ts_ms,
        speed,
        lat,
        lon,
        last_offset: offset,
    };
    fleet.insert(vehicle_id, snapshot);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload_from_produce_line(line: &str) -> Option<&str> {
        let (_, payload) = line.split_once("events ")?;
        Some(payload)
    }

    #[test]
    fn veh0_lat_lon_changes_across_log_lines() {
        let log_lines = [
            "PRODUCE events {\"vehicle_id\":\"veh-0\",\"ts_ms\":1700000000000,\"speed\":40,\"lat\":52.000120,\"lon\":20.999680}",
            "PRODUCE events {\"vehicle_id\":\"veh-1\",\"ts_ms\":1700000000100,\"speed\":42,\"lat\":52.001000,\"lon\":21.001000}",
            "PRODUCE events {\"vehicle_id\":\"veh-0\",\"ts_ms\":1700000000200,\"speed\":41,\"lat\":52.000125,\"lon\":20.999684}",
            "PRODUCE events {\"vehicle_id\":\"veh-0\",\"ts_ms\":1700000000300,\"speed\":39,\"lat\":52.000131,\"lon\":20.999689}",
        ];

        let mut fleet = BTreeMap::new();
        let mut veh0_coords = Vec::new();

        for (offset, line) in log_lines.iter().enumerate() {
            let payload = payload_from_produce_line(line).expect("line must contain payload");
            apply_payload(&mut fleet, offset as u64, payload).expect("payload should parse");
            if let Some(snapshot) = fleet.get("veh-0") {
                veh0_coords.push((snapshot.lat, snapshot.lon));
            }
        }

        assert!(veh0_coords.len() >= 2, "need at least two veh-0 samples");
        let first = veh0_coords[0];
        let moved = veh0_coords.iter().any(|coord| *coord != first);
        assert!(moved, "veh-0 coordinates should change across log lines");
    }
}
