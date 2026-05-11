//! Decode message bodies for human-oriented output (legacy `MSG` lines, CLI, JSON-oriented UI).
//!
//! Spec: [docs/reference/tcp-wire-protocol.md](../../../../docs/reference/tcp-wire-protocol.md) — *Message body encoding (authoritative vs display)*.

use std::borrow::Cow;

/// Lossy UTF-8 decode for message **body** bytes when emitting or consuming text-oriented views.
///
/// Preserves valid UTF-8; invalid sequences become U+FFFD per `from_utf8_lossy`. Does **not**
/// alter the authoritative byte representation — use raw `Vec<u8>` / framed payloads for codecs.
#[inline]
pub fn lossy_utf8_message_body_for_display(bytes: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}
