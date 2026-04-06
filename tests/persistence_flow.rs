//! Integration tests for message persistence flow.
//!
//! Verifies framed write/read roundtrips in memory and on real files under `data/logs`.

use herbatka::log::message::Message;
use herbatka::log::persistence::{read_message, write_message};
use std::collections::HashMap;
use std::fs::{File, OpenOptions, create_dir_all, remove_file};
use std::io;
use std::io::Cursor;
use std::time::{Duration, UNIX_EPOCH};

#[test]
fn persistence_flow_write_then_read_one_message() {
    //GIVEN
    let mut headers = HashMap::new();
    headers.insert("content-type".to_string(), b"application/json".to_vec());

    let original = Message {
        key: Some(b"car-42".to_vec()),
        payload: br#"{"speed":123}"#.to_vec(),
        timestamp: UNIX_EPOCH + Duration::from_millis(1_700_000_000_123),
        headers,
    };

    let mut bytes = Vec::new();

    //WHEN
    write_message(&mut bytes, &original).expect("write should succeed");
    let mut reader = Cursor::new(bytes);

    let decoded = read_message(&mut reader)
        .expect("read should succeed")
        .expect("message should exist");

    //THEN
    assert_eq!(decoded.key, original.key);
    assert_eq!(decoded.payload, original.payload);
    assert_eq!(decoded.timestamp, original.timestamp);
    assert_eq!(decoded.headers, original.headers);

    let eof = read_message(&mut reader).expect("second read should succeed");
    assert!(eof.is_none(), "expected EOF after single message");
}

#[test]
fn persistence_flow_real_file_in_data_logs() -> io::Result<()> {
    //GIVEN
    create_dir_all("data/logs")?;
    let path = "data/logs/integration_persistence_flow.log";

    match remove_file(path) {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }

    let mut headers = HashMap::new();
    headers.insert("content-type".to_string(), b"application/json".to_vec());

    let original = Message {
        key: Some(b"car-42-json".to_vec()),
        payload: br#"{"speed":123}"#.to_vec(),
        timestamp: UNIX_EPOCH + Duration::from_millis(1_700_000_000_123),
        headers,
    };

    //WHEN
    {
        let mut writer = OpenOptions::new().create(true).append(true).open(path)?;
        write_message(&mut writer, &original)?;
    }

    let mut reader = File::open(path)?;
    let decoded = read_message(&mut reader)?.expect("expected one message");
    let eof = read_message(&mut reader)?;

    //THEN
    assert_eq!(decoded.key, original.key);
    assert_eq!(decoded.payload, original.payload);
    assert_eq!(decoded.timestamp, original.timestamp);
    assert_eq!(decoded.headers, original.headers);
    assert!(eof.is_none(), "expected EOF after reading one message");

    remove_file(path)?;
    Ok(())
}

#[test]
fn persistence_flow_real_file_in_data_logs_protobuf() -> io::Result<()> {
    //GIVEN
    create_dir_all("data/logs")?;
    let path = "data/logs/integration_persistence_flow_protobuf.log";

    match remove_file(path) {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }

    let mut headers = HashMap::new();
    headers.insert("content-type".to_string(), b"application/protobuf".to_vec());

    let original = Message {
        key: Some(b"car-42-proto".to_vec()),
        payload: vec![0x08, 0x96, 0x01, 0x12, 0x05, b'h', b'e', b'l', b'l', b'o'],
        timestamp: UNIX_EPOCH + Duration::from_millis(1_700_000_000_123),
        headers,
    };

    //WHEN
    {
        let mut writer = OpenOptions::new().create(true).append(true).open(path)?;
        write_message(&mut writer, &original)?;
    }

    let mut reader = File::open(path)?;
    let decoded = read_message(&mut reader)?.expect("expected one message");
    let eof = read_message(&mut reader)?;

    //THEN
    assert_eq!(decoded.key, original.key);
    assert_eq!(decoded.payload, original.payload);
    assert_eq!(decoded.timestamp, original.timestamp);
    assert_eq!(decoded.headers, original.headers);
    assert!(eof.is_none(), "expected EOF after reading one message");

    remove_file(path)?;
    Ok(())
}
