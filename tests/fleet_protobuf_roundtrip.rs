//! Framed-style opaque body: protobuf encode → broker → fetch → decode.

use herbatka::broker::core::Broker;
use herbatka::config::{BrokerConfig, FsyncPolicy};
use herbatka::generated_schemas::FleetTelemetryEvent;
use herbatka::log::message::Message;
use prost::Message as ProstMessageTrait;
use std::collections::HashMap;
use std::fs::create_dir_all;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(prefix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "{prefix}_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn fleet_telemetry_protobuf_roundtrips_via_broker() {
    let dir = temp_dir("herbatka_fleet_pb");
    let cfg = BrokerConfig {
        data_dir: dir,
        segment_max_bytes: 65536,
        max_topic_bytes: None,
        fsync_policy: FsyncPolicy::Never,
        ..BrokerConfig::default()
    };
    let mut broker = Broker::with_config(cfg);
    broker.create_topic("demo.scope.telemetry".into()).unwrap();

    let original = FleetTelemetryEvent {
        vehicle_id: "veh-7".into(),
        ts_ms: 1_700_000_000_100,
        speed: 42,
        lat: 52.123456,
        lon: 21.654321,
    };

    let mut wire = Vec::new();
    original.encode(&mut wire).unwrap();

    let msg = Message {
        key: None,
        payload: wire,
        timestamp: SystemTime::now(),
        headers: HashMap::new(),
    };
    broker.produce("demo.scope.telemetry", msg).unwrap();

    let entry = broker
        .fetch("demo.scope.telemetry", 0)
        .unwrap()
        .expect("offset 0 exists");
    let decoded = FleetTelemetryEvent::decode(&*entry.payload).expect("valid protobuf");

    assert_eq!(decoded.vehicle_id, original.vehicle_id);
    assert_eq!(decoded.ts_ms, original.ts_ms);
    assert_eq!(decoded.speed, original.speed);
    assert_eq!(decoded.lat, original.lat);
    assert_eq!(decoded.lon, original.lon);
}
