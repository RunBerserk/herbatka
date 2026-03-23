use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::time::{Duration, UNIX_EPOCH};

use crate::log::message::Message;

const NONE_KEY_SENTINEL: u32 = u32::MAX;

// ---------- Public API ----------

pub fn encode_message(message: &Message) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();

    // timestamp (u64 millis since epoch)
    let ts = message
        .timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_millis(0))
        .as_millis() as u64;
    buf.extend_from_slice(&ts.to_le_bytes());

    // key
    match &message.key {
        None => {
            buf.extend_from_slice(&NONE_KEY_SENTINEL.to_le_bytes());
        }
        Some(key) => {
            let len = key.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(key);
        }
    }

    // payload
    let payload_len = message.payload.len() as u32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.extend_from_slice(&message.payload);

    // headers
    let header_count = message.headers.len() as u32;
    buf.extend_from_slice(&header_count.to_le_bytes());

    for (k, v) in &message.headers {
        let k_bytes = k.as_bytes();
        let k_len = k_bytes.len() as u32;
        buf.extend_from_slice(&k_len.to_le_bytes());
        buf.extend_from_slice(k_bytes);

        let v_len = v.len() as u32;
        buf.extend_from_slice(&v_len.to_le_bytes());
        buf.extend_from_slice(v);
    }

    Ok(buf)
}

pub fn decode_message(mut data: &[u8]) -> io::Result<Message> {
    // timestamp
    let ts = read_u64(&mut data)?;
    let timestamp = UNIX_EPOCH + Duration::from_millis(ts);

    // key
    let key_len = read_u32(&mut data)?;
    let key = if key_len == NONE_KEY_SENTINEL {
        None
    } else {
        Some(read_bytes(&mut data, key_len as usize)?)
    };

    // payload
    let payload_len = read_u32(&mut data)? as usize;
    let payload = read_bytes(&mut data, payload_len)?;

    // headers
    let header_count = read_u32(&mut data)?;
    let mut headers = HashMap::new();

    for _ in 0..header_count {
        let k_len = read_u32(&mut data)? as usize;
        let key_str = String::from_utf8(read_bytes(&mut data, k_len)?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 key"))?;

        let v_len = read_u32(&mut data)? as usize;
        let value = read_bytes(&mut data, v_len)?;

        headers.insert(key_str, value);
    }

    Ok(Message {
        key,
        payload,
        timestamp,
        headers,
    })
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
