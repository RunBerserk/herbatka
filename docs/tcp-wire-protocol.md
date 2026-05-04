# Herbatka TCP wire protocol

This document specifies the **legacy line protocol** and **framed wire v1** used by the `herbatka` TCP server. It is the contract for client implementations and future evolution (e.g. Protobuf inside frame bodies).

## Connections

- TCP, configured by `listen_addr` in `herbatka.toml` (default `127.0.0.1:7000`).
- One broker request per client message in both modes; server sends exactly one response frame or one response line per request (sequential, no pipelining assumption required for v1).

## Legacy text mode (backward compatible)

Used when the **first line** from the client is **not** the framed-mode handshake (see below).

- Messages are **newline-delimited** (`\n` LF). Optional `\r` before `\n` should be handled by trimming; clients should send `\n`.
- **PRODUCE:** `PRODUCE <topic><space><payload...>\n` — only the first space separates topic from payload; the remainder of the line is the payload (may contain spaces).
- **FETCH:** `FETCH <topic> <offset>\n` — topic must not contain whitespace; offset is decimal `u64`.
- **Responses:** single line each, newline-terminated:
  - `OK <u64 offset>\n`
  - `MSG <u64 offset> <payload>\n` — payload is the rest of the line after the second space; UTF-8 oriented in practice (broker may have lossy behavior for non‑UTF‑8).
  - `NONE\n`
  - `ERR <reason>\n` — human-readable reason.

**Limits:** The server reads the first line with a bounded buffer (64 KiB including newline). Oversize lines are rejected.

**Interoperability caveat:** Binary payloads are **not** reliably representable in legacy `MSG`/`PRODUCE` lines; use framed v1 for opaque bytes.

---

## Framed wire v1

### Handshake (client first, then server)

After TCP connect, the client sends **exactly** one line (UTF-8):

```text
HERBATKA WIRE/1\n
```

The server responds with **exactly**:

```text
HERBATKA OK/1\n
```

After this, **all further data** on the connection uses **binary frames** described below. No interleaved line protocol.

If the first line is not `HERBATKA WIRE/1\n`, the server treats it as the first **legacy** command and never enters framed mode on that connection.

### Frame envelope (both directions)

Multi-byte integers are **little-endian** unless noted.

| Offset | Size | Field |
|--------|------|--------|
| 0 | 1 | `version` — must be `1` for wire v1 |
| 1 | 1 | `op` — opcode (see below) |
| 2 | 2 | `flags` — reserved, must be `0` in v1 |
| 4 | 4 | `payload_len` — byte length of payload following the header |

**Header size:** 8 bytes.

**Limits:** `payload_len` must not exceed **16 MiB** (`16_777_216`). The server rejects larger values without allocating the payload. Topics in request bodies are additionally capped to **4096 UTF-8 bytes** (enforced when decoding produce/fetch bodies).

Invalid `version` or non-zero `flags` (v1) result in the server closing the connection or returning an error frame, depending on recoverability; implementations should treat framing errors as fatal for the connection.

### Client → server ops and bodies

| `op` | Name | `payload` layout |
|------|------|------------------|
| `1` | Produce | `topic_len: u16 LE`, `topic: UTF-8` (`topic_len` bytes), `body_len: u32 LE`, `body: [u8]` (`body_len` bytes). `body` may be empty only if `body_len == 0`; `topic` must not be empty. |
| `2` | Fetch | `topic_len: u16 LE`, `topic: UTF-8`, `offset: u64 LE` |

### Server → client ops and bodies

| `op` | Name | Meaning | `payload` layout |
|------|------|---------|------------------|
| `16` | OkOffset | Produce accepted | `offset: u64 LE` |
| `17` | Message | Fetch returned a record | `offset: u64 LE`, `body_len: u32 LE`, `body` (`body_len` bytes) — raw message bytes |
| `18` | None | No message at offset | empty |
| `19` | Error | Request failed | `reason_len: u32 LE`, `reason: UTF-8` (`reason_len` bytes) |

Responses use the same 8-byte envelope as requests.

### Error handling

- Legacy: `ERR …` line with free-form text.
- Framed: **Error** opcode with UTF-8 reason. Clients should not assume nested framing after an error unless a future version specifies it.

---

## Compatibility and future work

- **Protobuf** (or similar) may be embedded in Produce `body` / Message `body` without changing this envelope once clients agree on serialization.
- **Legacy mode** remains available for debugging and trivial clients (`telnet`/netcat style).
- Negotiation rule: handshake line is deterministic; strings `PRODUCE` and `FETCH` do **not** collide with `HERBATKA WIRE/1`.
