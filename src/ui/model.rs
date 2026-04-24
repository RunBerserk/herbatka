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
    pub vehicle_id: String,
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

    let snapshot = VehicleSnapshot {
        vehicle_id: event.vehicle_id.clone(),
        ts_ms: event.ts_ms,
        speed: event.speed,
        lat: event.lat,
        lon: event.lon,
        last_offset: offset,
    };
    fleet.insert(event.vehicle_id, snapshot);
    Ok(())
}
