//! Length-prefixed binary wire protocol v1 (after `HERBATKA WIRE/1` handshake).
//! See `docs/tcp-wire-protocol.md`.

use std::io::{self, Read, Write};
use std::net::TcpStream;

use crate::tcp::command::{Request, Response};

/// Client handshake line including LF.
pub const HANDSHAKE_CLIENT_V1: &[u8] = b"HERBATKA WIRE/1\n";
/// Server acknowledgement including LF.
pub const HANDSHAKE_SERVER_ACK_V1: &[u8] = b"HERBATKA OK/1\n";

pub const WIRE_VERSION_V1: u8 = 1;
pub const HEADER_LEN: usize = 8;

pub const OP_PRODUCE: u8 = 1;
pub const OP_FETCH: u8 = 2;

pub const OP_OK_OFFSET: u8 = 16;
pub const OP_MSG: u8 = 17;
pub const OP_NONE: u8 = 18;
pub const OP_ERR: u8 = 19;

pub const MAX_FRAME_PAYLOAD: u32 = 16 * 1024 * 1024;
pub const MAX_TOPIC_BYTES: usize = 4096;
pub const MAX_FIRST_LINE_BYTES: usize = 64 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum WireError {
    Io(String),
    UnsupportedVersion(u8),
    UnsupportedFlags(u16),
    PayloadTooLarge(u32),
    TopicTooLarge(usize),
    TopicEmpty,
    Truncated,
    Utf8Topic,
    UnknownOp(u8),
    Decode(&'static str),
}

impl From<io::Error> for WireError {
    fn from(e: io::Error) -> Self {
        WireError::Io(e.to_string())
    }
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::Io(s) => write!(f, "io: {s}"),
            WireError::UnsupportedVersion(v) => write!(f, "unsupported wire version {v}"),
            WireError::UnsupportedFlags(fl) => write!(f, "unsupported flags {fl}"),
            WireError::PayloadTooLarge(n) => write!(f, "payload too large ({n} bytes)"),
            WireError::TopicTooLarge(n) => write!(f, "topic too large ({n} bytes)"),
            WireError::TopicEmpty => write!(f, "topic empty"),
            WireError::Truncated => write!(f, "truncated frame"),
            WireError::Utf8Topic => write!(f, "topic is not valid UTF-8"),
            WireError::UnknownOp(op) => write!(f, "unknown op {op}"),
            WireError::Decode(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for WireError {}

fn put_u16(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

fn put_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

fn read_u16(data: &[u8], cursor: &mut usize) -> Result<u16, WireError> {
    if *cursor + 2 > data.len() {
        return Err(WireError::Truncated);
    }
    let v = u16::from_le_bytes(data[*cursor..*cursor + 2].try_into().unwrap());
    *cursor += 2;
    Ok(v)
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Result<u32, WireError> {
    if *cursor + 4 > data.len() {
        return Err(WireError::Truncated);
    }
    let v = u32::from_le_bytes(data[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(v)
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Result<u64, WireError> {
    if *cursor + 8 > data.len() {
        return Err(WireError::Truncated);
    }
    let v = u64::from_le_bytes(data[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(v)
}

fn read_exact_slice<'a>(
    data: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> Result<&'a [u8], WireError> {
    if *cursor + len > data.len() {
        return Err(WireError::Truncated);
    }
    let s = &data[*cursor..*cursor + len];
    *cursor += len;
    Ok(s)
}

fn validate_inner_payload_budget(len: u32) -> Result<(), WireError> {
    if len > MAX_FRAME_PAYLOAD {
        return Err(WireError::PayloadTooLarge(len));
    }
    Ok(())
}

fn write_header(out: &mut [u8], op: u8, payload_len: u32) -> Result<(), WireError> {
    validate_inner_payload_budget(payload_len)?;
    out[0] = WIRE_VERSION_V1;
    out[1] = op;
    put_u16(out, 2, 0);
    put_u32(out, 4, payload_len);
    Ok(())
}

/// Produce request frame: topic UTF-8, arbitrary body bytes.
pub fn encode_produce(topic: &str, body: &[u8]) -> Result<Vec<u8>, WireError> {
    encode_produce_checked(topic.as_bytes(), body)
}

pub fn encode_produce_checked(topic_utf8: &[u8], body: &[u8]) -> Result<Vec<u8>, WireError> {
    if topic_utf8.is_empty() {
        return Err(WireError::TopicEmpty);
    }
    if topic_utf8.len() > MAX_TOPIC_BYTES || topic_utf8.len() > u16::MAX as usize {
        return Err(WireError::TopicTooLarge(topic_utf8.len()));
    }

    let mut payload = Vec::new();
    payload.extend_from_slice(&(topic_utf8.len() as u16).to_le_bytes());
    payload.extend_from_slice(topic_utf8);
    let body_len: u32 = body
        .len()
        .try_into()
        .map_err(|_| WireError::PayloadTooLarge(u32::MAX))?;
    payload.extend_from_slice(&body_len.to_le_bytes());
    payload.extend_from_slice(body);

    let inner_len_u32: u32 = payload
        .len()
        .try_into()
        .map_err(|_| WireError::PayloadTooLarge(u32::MAX))?;
    validate_inner_payload_budget(inner_len_u32)?;

    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.resize(HEADER_LEN, 0);
    write_header(&mut out, OP_PRODUCE, inner_len_u32)?;
    out.extend_from_slice(&payload);
    Ok(out)
}

pub fn encode_fetch(topic: &str, offset: u64) -> Result<Vec<u8>, WireError> {
    let topic_utf8 = topic.as_bytes();
    if topic_utf8.is_empty() {
        return Err(WireError::TopicEmpty);
    }
    if topic_utf8.len() > MAX_TOPIC_BYTES || topic_utf8.len() > u16::MAX as usize {
        return Err(WireError::TopicTooLarge(topic_utf8.len()));
    }

    let mut payload = Vec::new();
    payload.extend_from_slice(&(topic_utf8.len() as u16).to_le_bytes());
    payload.extend_from_slice(topic_utf8);
    payload.extend_from_slice(&offset.to_le_bytes());

    let inner_len_u32: u32 = payload
        .len()
        .try_into()
        .map_err(|_| WireError::PayloadTooLarge(u32::MAX))?;
    validate_inner_payload_budget(inner_len_u32)?;

    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.resize(HEADER_LEN, 0);
    write_header(&mut out, OP_FETCH, inner_len_u32)?;
    out.extend_from_slice(&payload);
    Ok(out)
}

pub fn encode_response(resp: &Response) -> Result<Vec<u8>, WireError> {
    match resp {
        Response::OkOffset(off) => {
            let payload: [u8; 8] = off.to_le_bytes();
            let mut out = vec![0u8; HEADER_LEN + 8];
            write_header(&mut out, OP_OK_OFFSET, 8)?;
            out[HEADER_LEN..].copy_from_slice(&payload);
            Ok(out)
        }
        Response::None => {
            let mut out = vec![0u8; HEADER_LEN];
            write_header(&mut out, OP_NONE, 0)?;
            Ok(out)
        }
        Response::Message { offset, payload } => {
            let body_len: u32 = payload
                .len()
                .try_into()
                .map_err(|_| WireError::PayloadTooLarge(u32::MAX))?;
            let inner = 8usize
                .checked_add(4)
                .and_then(|n| n.checked_add(payload.len()))
                .ok_or(WireError::PayloadTooLarge(u32::MAX))?;
            let inner_u32: u32 = inner
                .try_into()
                .map_err(|_| WireError::PayloadTooLarge(u32::MAX))?;
            validate_inner_payload_budget(inner_u32)?;

            let mut out = Vec::with_capacity(HEADER_LEN + inner);
            out.resize(HEADER_LEN, 0);
            write_header(&mut out, OP_MSG, inner_u32)?;
            out.extend_from_slice(&offset.to_le_bytes());
            out.extend_from_slice(&body_len.to_le_bytes());
            out.extend_from_slice(payload);
            Ok(out)
        }
        Response::Error(reason) => {
            let r = reason.as_bytes();
            let rlen: u32 = r
                .len()
                .try_into()
                .map_err(|_| WireError::PayloadTooLarge(u32::MAX))?;
            let inner = 4usize
                .checked_add(r.len())
                .ok_or(WireError::PayloadTooLarge(u32::MAX))?;
            let inner_u32: u32 = inner
                .try_into()
                .map_err(|_| WireError::PayloadTooLarge(u32::MAX))?;
            validate_inner_payload_budget(inner_u32)?;

            let mut out = Vec::with_capacity(HEADER_LEN + inner);
            out.resize(HEADER_LEN, 0);
            write_header(&mut out, OP_ERR, inner_u32)?;
            out.extend_from_slice(&rlen.to_le_bytes());
            out.extend_from_slice(r);
            Ok(out)
        }
    }
}

fn decode_produce_body(payload: &[u8]) -> Result<Request, WireError> {
    let mut c = 0usize;
    let topic_len = read_u16(payload, &mut c)? as usize;
    if topic_len == 0 {
        return Err(WireError::TopicEmpty);
    }
    if topic_len > MAX_TOPIC_BYTES {
        return Err(WireError::TopicTooLarge(topic_len));
    }
    let topic_bytes = read_exact_slice(payload, &mut c, topic_len)?;
    let topic = std::str::from_utf8(topic_bytes)
        .map_err(|_| WireError::Utf8Topic)?
        .to_string();
    let body_len = read_u32(payload, &mut c)? as usize;
    let body = read_exact_slice(payload, &mut c, body_len)?;
    if c != payload.len() {
        return Err(WireError::Decode("trailing bytes in produce body"));
    }
    Ok(Request::Produce {
        topic,
        payload: body.to_vec(),
    })
}

fn decode_fetch_body(payload: &[u8]) -> Result<Request, WireError> {
    let mut c = 0usize;
    let topic_len = read_u16(payload, &mut c)? as usize;
    if topic_len == 0 {
        return Err(WireError::TopicEmpty);
    }
    if topic_len > MAX_TOPIC_BYTES {
        return Err(WireError::TopicTooLarge(topic_len));
    }
    let topic_bytes = read_exact_slice(payload, &mut c, topic_len)?;
    let topic = std::str::from_utf8(topic_bytes)
        .map_err(|_| WireError::Utf8Topic)?
        .to_string();
    let offset = read_u64(payload, &mut c)?;
    if c != payload.len() {
        return Err(WireError::Decode("trailing bytes in fetch body"));
    }
    Ok(Request::Fetch { topic, offset })
}

/// Parse client request from one full frame (header + payload already concatenated).
pub fn decode_client_frame(buf: &[u8]) -> Result<Request, WireError> {
    if buf.len() < HEADER_LEN {
        return Err(WireError::Truncated);
    }
    let version = buf[0];
    if version != WIRE_VERSION_V1 {
        return Err(WireError::UnsupportedVersion(version));
    }
    let op = buf[1];
    let flags = u16::from_le_bytes(buf[2..4].try_into().unwrap());
    if flags != 0 {
        return Err(WireError::UnsupportedFlags(flags));
    }
    let payload_len = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    validate_inner_payload_budget(payload_len)?;
    let need = HEADER_LEN + payload_len as usize;
    if buf.len() != need {
        return Err(WireError::Decode("frame length mismatch"));
    }
    let payload = &buf[HEADER_LEN..];
    match op {
        OP_PRODUCE => decode_produce_body(payload),
        OP_FETCH => decode_fetch_body(payload),
        _ => Err(WireError::UnknownOp(op)),
    }
}

pub fn decode_response_frame(buf: &[u8]) -> Result<Response, WireError> {
    if buf.len() < HEADER_LEN {
        return Err(WireError::Truncated);
    }
    let version = buf[0];
    if version != WIRE_VERSION_V1 {
        return Err(WireError::UnsupportedVersion(version));
    }
    let op = buf[1];
    let flags = u16::from_le_bytes(buf[2..4].try_into().unwrap());
    if flags != 0 {
        return Err(WireError::UnsupportedFlags(flags));
    }
    let payload_len = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    validate_inner_payload_budget(payload_len)?;
    let need = HEADER_LEN + payload_len as usize;
    if buf.len() != need {
        return Err(WireError::Decode("frame length mismatch"));
    }
    let p = &buf[HEADER_LEN..];
    let mut c = 0usize;
    match op {
        OP_OK_OFFSET => {
            if p.len() != 8 {
                return Err(WireError::Decode("ok offset body size"));
            }
            let off = read_u64(p, &mut c)?;
            Ok(Response::OkOffset(off))
        }
        OP_NONE if p.is_empty() => Ok(Response::None),
        OP_MSG => {
            let offset = read_u64(p, &mut c)?;
            let body_len = read_u32(p, &mut c)? as usize;
            let body = read_exact_slice(p, &mut c, body_len)?;
            if c != p.len() {
                return Err(WireError::Decode("trailing bytes in msg body"));
            }
            Ok(Response::Message {
                offset,
                payload: body.to_vec(),
            })
        }
        OP_ERR => {
            let rlen = read_u32(p, &mut c)? as usize;
            let reason_bytes = read_exact_slice(p, &mut c, rlen)?;
            if c != p.len() {
                return Err(WireError::Decode("trailing bytes in err body"));
            }
            let reason = std::str::from_utf8(reason_bytes)
                .map_err(|_| WireError::Utf8Topic)?
                .to_string();
            Ok(Response::Error(reason))
        }
        _ => Err(WireError::UnknownOp(op)),
    }
}

/// Read exactly one frame from `r`; returns contiguous buffer (header + payload).
pub fn read_frame<R: Read>(r: &mut R) -> Result<Vec<u8>, WireError> {
    let mut hdr = [0u8; HEADER_LEN];
    r.read_exact(&mut hdr)?;
    let version = hdr[0];
    if version != WIRE_VERSION_V1 {
        return Err(WireError::UnsupportedVersion(version));
    }
    let flags = u16::from_le_bytes(hdr[2..4].try_into().unwrap());
    if flags != 0 {
        return Err(WireError::UnsupportedFlags(flags));
    }
    let payload_len = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
    validate_inner_payload_budget(payload_len)?;
    let mut buf = vec![0u8; HEADER_LEN + payload_len as usize];
    buf[..HEADER_LEN].copy_from_slice(&hdr);
    r.read_exact(&mut buf[HEADER_LEN..])?;
    Ok(buf)
}

pub fn write_frame<W: Write>(w: &mut W, frame: &[u8]) -> io::Result<()> {
    w.write_all(frame)?;
    w.flush()?;
    Ok(())
}

/// Read first line bytes including `\n`, bounded by `MAX_FIRST_LINE_BYTES`.
/// Server handshake acknowledgement line equals [`HANDSHAKE_SERVER_ACK_V1`] (allow optional `\r` before `\n`).
pub fn is_server_ack_v1(raw: &[u8]) -> bool {
    let s = raw.strip_suffix(b"\n").unwrap_or(raw);
    let s = s.strip_suffix(b"\r").unwrap_or(s);
    s == b"HERBATKA OK/1"
}

/// Client-side: send [`HANDSHAKE_CLIENT_V1`] and read one line; OK when [`is_server_ack_v1`] matches.
pub fn perform_client_handshake(stream: &mut TcpStream) -> Result<(), WireError> {
    stream.write_all(HANDSHAKE_CLIENT_V1)?;
    stream.flush()?;
    let ack = read_first_line(stream)?;
    if !is_server_ack_v1(&ack) {
        return Err(WireError::Decode(
            "server did not accept HERBATKA WIRE/1 (expected HERBATKA OK/1)",
        ));
    }
    Ok(())
}

pub fn read_first_line<R: Read>(r: &mut R) -> Result<Vec<u8>, WireError> {
    let mut buf = Vec::new();
    let mut one = [0u8; 1];
    loop {
        if buf.len() >= MAX_FIRST_LINE_BYTES {
            return Err(WireError::PayloadTooLarge(buf.len() as u32));
        }
        r.read_exact(&mut one)?;
        buf.push(one[0]);
        if one[0] == b'\n' {
            break;
        }
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_produce_binary_body() {
        let topic = "events";
        let body = vec![0, 255, 10, 13, 32];
        let f = encode_produce(topic, &body).unwrap();
        let req = decode_client_frame(&f).unwrap();
        match req {
            Request::Produce { topic: t, payload } => {
                assert_eq!(t, "events");
                assert_eq!(payload, body);
            }
            _ => panic!("expected Produce"),
        }
        let mut resp = Response::OkOffset(0);
        let out = encode_response(&resp).unwrap();
        let back = decode_response_frame(&out).unwrap();
        assert_eq!(back, resp);

        resp = Response::Message {
            offset: 1,
            payload: body.clone(),
        };
        let out = encode_response(&resp).unwrap();
        let back = decode_response_frame(&out).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn roundtrip_fetch() {
        let f = encode_fetch("t", 42).unwrap();
        let req = decode_client_frame(&f).unwrap();
        assert_eq!(
            req,
            Request::Fetch {
                topic: "t".into(),
                offset: 42
            }
        );
    }

    #[test]
    fn decode_truncated_buffer() {
        assert!(matches!(
            decode_client_frame(&[1, 1, 0, 0]),
            Err(WireError::Truncated) | Err(WireError::Decode(_))
        ));
    }

    #[test]
    fn decode_oversize_declared_payload() {
        let mut hdr = vec![1u8, 1, 0, 0, 0x00, 0x00, 0x00, 0x01];
        hdr.extend(std::iter::repeat_n(0u8, 100));
        assert!(matches!(
            decode_client_frame(&hdr),
            Err(WireError::Decode(_)) | Err(WireError::Truncated)
        ));
    }

    #[test]
    fn reject_flags_nonzero() {
        let mut bad = encode_produce("a", b"x").unwrap();
        bad[2] = 1;
        assert!(matches!(
            decode_client_frame(&bad),
            Err(WireError::UnsupportedFlags(_))
        ));
    }

    #[test]
    fn reject_version() {
        let mut f = encode_fetch("x", 0).unwrap();
        f[0] = 2;
        assert!(matches!(
            decode_client_frame(&f),
            Err(WireError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn max_frame_payload_rejected_in_header() {
        let mut hdr = [0u8; HEADER_LEN];
        hdr[0] = WIRE_VERSION_V1;
        hdr[1] = OP_NONE;
        hdr[4..8].copy_from_slice(&(MAX_FRAME_PAYLOAD + 1).to_le_bytes());
        assert!(read_frame(&mut std::io::Cursor::new(hdr)).is_err());
    }

    #[test]
    fn err_response_roundtrip() {
        let r = Response::Error("bad topic".into());
        let b = encode_response(&r).unwrap();
        assert_eq!(decode_response_frame(&b).unwrap(), r);
    }

    #[test]
    fn none_response_roundtrip() {
        let r = Response::None;
        let b = encode_response(&r).unwrap();
        assert_eq!(decode_response_frame(&b).unwrap(), r);
    }
}
