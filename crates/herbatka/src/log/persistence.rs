//! Binary persistence format for `Message`.
//!
//! Encodes/decodes messages and reads/writes length-framed records to `Read`/`Write`.
//! This module handles bytes and stream I/O; broker decides topic paths and file lifecycle.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::time::{Duration, UNIX_EPOCH};

use crate::log::message::Message;

const NONE_KEY_SENTINEL: u32 = u32::MAX;

// ---------- Public API ----------

pub fn encode_message(message: &Message) -> io::Result<Vec<u8>> {
    let timestamp = encode_timestamp(message);
    let key = encode_key(message);
    let payload = encode_payload(message);
    let headers = encode_headers(message);

    let mut buf = Vec::new();
    buf.extend_from_slice(&timestamp);
    buf.extend_from_slice(&key);
    buf.extend_from_slice(&payload);
    buf.extend_from_slice(&headers);

    Ok(buf)
}

fn encode_headers(message: &Message) -> Vec<u8> {
    let mut buf = Vec::new();
    push_u32(&mut buf, message.headers.len() as u32);

    for (key, value) in &message.headers {
        push_len_prefixed_bytes(&mut buf, key.as_bytes());
        push_len_prefixed_bytes(&mut buf, value);
    }

    buf
}

fn encode_payload(message: &Message) -> Vec<u8> {
    let mut buf = Vec::new();
    push_len_prefixed_bytes(&mut buf, &message.payload);
    buf
}

fn encode_key(message: &Message) -> Vec<u8> {
    let mut buf = Vec::new();
    match &message.key {
        None => {
            push_u32(&mut buf, NONE_KEY_SENTINEL);
        }
        Some(key) => {
            push_len_prefixed_bytes(&mut buf, key);
        }
    }
    buf
}

fn encode_timestamp(message: &Message) -> Vec<u8> {
    let ts = message
        .timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_millis(0))
        .as_millis() as u64;

    let mut buf = Vec::new();
    buf.extend_from_slice(&ts.to_le_bytes());
    buf
}

fn push_len_prefixed_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(buf, bytes.len() as u32);
    buf.extend_from_slice(bytes);
}

fn push_u32(buf: &mut Vec<u8>, n: u32) {
    buf.extend_from_slice(&n.to_le_bytes());
}

pub fn decode_message(mut data: &[u8]) -> io::Result<Message> {
    let timestamp = decode_timestamp(&mut data)?;
    let key = decode_key(&mut data)?;
    let payload = decode_payload(&mut data)?;
    let headers = decode_headers(&mut data)?;

    Ok(Message {
        key,
        payload,
        timestamp,
        headers,
    })
}

fn decode_timestamp(data: &mut &[u8]) -> io::Result<std::time::SystemTime> {
    let ts = read_u64(data)?;
    Ok(UNIX_EPOCH + Duration::from_millis(ts))
}

fn decode_key(data: &mut &[u8]) -> io::Result<Option<Vec<u8>>> {
    let key_len = read_u32(data)?;
    if key_len == NONE_KEY_SENTINEL {
        return Ok(None);
    }

    Ok(Some(read_bytes(data, key_len as usize)?))
}

fn decode_payload(data: &mut &[u8]) -> io::Result<Vec<u8>> {
    read_len_prefixed_bytes(data)
}

fn decode_headers(data: &mut &[u8]) -> io::Result<HashMap<String, Vec<u8>>> {
    let header_count = read_u32(data)? as usize;
    let mut headers = HashMap::with_capacity(header_count);

    for _ in 0..header_count {
        let key = read_len_prefixed_utf8(data)?;
        let value = read_len_prefixed_bytes(data)?;
        headers.insert(key, value);
    }

    Ok(headers)
}

// ---------- File helpers ----------

pub fn write_message<W: Write>(writer: &mut W, message: &Message) -> io::Result<()> {
    let encoded = encode_message(message)?;
    let len = encoded.len() as u32;

    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&encoded)?;
    Ok(())
}

pub fn read_message<R: Read>(reader: &mut R) -> io::Result<Option<Message>> {
    let mut len_buf = [0u8; 4];

    // try read length
    match reader.read_exact(&mut len_buf) {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok(None); // end of file
        }
        Err(e) => return Err(e),
    }

    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;

    let msg = decode_message(&buf)?;
    Ok(Some(msg))
}

// ---------- Helpers ----------

fn read_u32(data: &mut &[u8]) -> io::Result<u32> {
    if data.len() < 4 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "u32"));
    }
    let (int_bytes, rest) = data.split_at(4);
    *data = rest;
    Ok(u32::from_le_bytes(int_bytes.try_into().unwrap()))
}

fn read_u64(data: &mut &[u8]) -> io::Result<u64> {
    if data.len() < 8 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "u64"));
    }
    let (int_bytes, rest) = data.split_at(8);
    *data = rest;
    Ok(u64::from_le_bytes(int_bytes.try_into().unwrap()))
}

fn read_bytes(data: &mut &[u8], len: usize) -> io::Result<Vec<u8>> {
    if data.len() < len {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "bytes"));
    }
    let (bytes, rest) = data.split_at(len);
    *data = rest;
    Ok(bytes.to_vec())
}

fn read_len_prefixed_bytes(data: &mut &[u8]) -> io::Result<Vec<u8>> {
    let len = read_u32(data)? as usize;
    read_bytes(data, len)
}

fn read_len_prefixed_utf8(data: &mut &[u8]) -> io::Result<String> {
    String::from_utf8(read_len_prefixed_bytes(data)?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 key"))
}

#[cfg(test)]
mod tests {
    use super::{decode_message, encode_message};
    use crate::log::message::Message;
    use std::collections::HashMap;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn encode_decode_roundtrip_json_payload() {
        //GIVEN
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), b"application/json".to_vec());

        let original = Message {
            key: Some(b"car-42-json".to_vec()),
            payload: br#"{"speed":123}"#.to_vec(),
            // use exact millisecond value to match encoder precision
            timestamp: UNIX_EPOCH + Duration::from_millis(1_700_000_000_123),
            headers,
        };

        //WHEN
        let encoded = encode_message(&original).expect("encode should succeed");

        let decoded = decode_message(&encoded).expect("decode should succeed");

        //THEN
        assert_eq!(decoded.key, original.key);
        assert_eq!(decoded.payload, original.payload);
        assert_eq!(decoded.timestamp, original.timestamp);
        assert_eq!(decoded.headers, original.headers);
    }

    #[test]
    fn encode_decode_roundtrip_protobuf_payload() {
        //GIVEN
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), b"application/protobuf".to_vec());
        // Example protobuf wire bytes (just raw bytes for test purposes)
        let proto_payload = vec![0x08, 0x96, 0x01, 0x12, 0x05, b'h', b'e', b'l', b'l', b'o'];
        let original = Message {
            key: Some(b"car-23-proto".to_vec()),
            payload: proto_payload,
            // encoder stores millis, so use exact millis here
            timestamp: UNIX_EPOCH + Duration::from_millis(1_700_000_000_123),
            headers,
        };

        //WHEN
        let encoded = encode_message(&original).expect("encode should succeed");
        let decoded = decode_message(&encoded).expect("decode should succeed");

        //THEN
        assert_eq!(decoded.key, original.key);
        assert_eq!(decoded.payload, original.payload);
        assert_eq!(decoded.timestamp, original.timestamp);
        assert_eq!(decoded.headers, original.headers);
    }

    #[test]
    fn encode_decode_roundtrip_empty_edge_values() {
        //GIVEN
        let original = Message {
            key: None,           // no key
            payload: Vec::new(), // empty payload
            timestamp: UNIX_EPOCH + Duration::from_millis(0),
            headers: HashMap::new(), // empty headers
        };

        //WHEN
        let encoded = encode_message(&original).expect("encode should succeed");
        let decoded = decode_message(&encoded).expect("decode should succeed");

        //THEN
        assert_eq!(decoded.key, original.key);
        assert_eq!(decoded.payload, original.payload);
        assert_eq!(decoded.timestamp, original.timestamp);
        assert_eq!(decoded.headers, original.headers);
    }

    #[test]
    fn encode_decode_roundtrip_headers_multiple_and_varied_sizes() {
        //GIVEN
        let mut headers = HashMap::new();
        headers.insert("a".to_string(), b"x".to_vec()); // tiny
        headers.insert("content-type".to_string(), b"application/protobuf".to_vec()); // medium
        headers.insert(
            "x-very-long-header-name-for-size-test".to_string(),
            vec![0u8; 256], // larger binary value
        );
        let original = Message {
            key: Some(b"car-headers-test".to_vec()),
            payload: b"payload".to_vec(),
            timestamp: UNIX_EPOCH + Duration::from_millis(1_700_000_000_555),
            headers,
        };

        //WHEN
        let encoded = encode_message(&original).expect("encode should succeed");
        let decoded = decode_message(&encoded).expect("decode should succeed");

        //THEN
        assert_eq!(decoded.headers, original.headers);
        assert_eq!(decoded.headers.len(), 3);
        assert_eq!(decoded.headers.get("a"), Some(&b"x".to_vec()));
        assert_eq!(
            decoded.headers.get("content-type"),
            Some(&b"application/protobuf".to_vec())
        );
        assert_eq!(
            decoded
                .headers
                .get("x-very-long-header-name-for-size-test")
                .map(Vec::len),
            Some(256)
        );
    }
}
