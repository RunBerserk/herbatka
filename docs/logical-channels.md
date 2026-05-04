# Logical channels (topic naming)

Applications can treat Herbatka as **three logical lanes**—**heartbeat**, **control**, and **telemetry**—without any broker API for “channel.” The broker only knows **topic names** and **opaque payloads**; semantics are a **convention** between producers and consumers (fleet, IoT, or any other domain).

## Lanes (intent)

| Lane | Role |
|------|------|
| **Heartbeat** | Periodic liveness / cadence (“still here”), small volume. |
| **Control** | Commands, acks, errors, warnings, critical signals; prefer **small** payloads and tight operational lifecycle (enforced by **policy and retention** once per-topic limits exist). |
| **Telemetry** | Event-driven data, **full** payloads (sensors, telemetry JSON, etc.). |

## Topic naming (canonical)

Use **three separate topics** per scope, with a fixed suffix:

`<scope>.heartbeat`  
`<scope>.control`  
`<scope>.telemetry`

- **`<scope>`** is any stable string for your deployment: tenant, environment, product, or fleet slice (e.g. `acme.prod`, `fleet1`, `region-eu`).
- The broker does **not** parse `scope` or suffix; it routes purely by the full topic string.

### Alternative (product prefix first)

If you prefer a shared product prefix: `myapp.<scope>.heartbeat`, or flat names like `events-heartbeat` / `events-control` / `events-telemetry`, that is fine—as long as producers and consumers agree. The important part is **three distinct topics**, not the exact spelling.

## Relation to other docs

- Transport: [TCP wire protocol](tcp-wire-protocol.md) (unchanged; channel is not on the wire—only in topic choice and payload).
- Default simulator/UI topic `events` in examples is **telemetry-style** naming (single topic); splitting into three topics is an application choice documented here, not required for MVP demos.

## Protobuf payloads (optional)

Protobuf (or other encodings) are **opaque bytes** inside the framed Produce/Fetch **`body`**; the envelope is unchanged—see [TCP wire protocol — compatibility](tcp-wire-protocol.md#compatibility-and-future-work).

**Convention:** the **topic string** selects the decoder—not the broker. Per deployment, agree which message type corresponds to each topic, for example:

| Topic pattern (example) | Suggested protobuf message (`herbatka.fleet`) |
|-------------------------|---------------------------------------------|
| `<scope>.heartbeat` | [`FleetHeartbeat`](../proto/herbatka_fleet.proto) |
| `<scope>.control` | [`FleetControlEnvelope`](../proto/herbatka_fleet.proto) |
| `<scope>.telemetry` | [`FleetTelemetryEvent`](../proto/herbatka_fleet.proto) |

Rust types are codegen’d into the crate (see [`src/generated_schemas.rs`](../src/generated_schemas.rs)). **FleetTelemetryEvent** mirrors the MVP JSON **`FleetEvent`** shape (`vehicle_id`, `ts_ms`, `speed`, `lat`, `lon`) used by simulator/UI demos so consumers can migrate topic-by-topic.

**Headers:** persisted [`Message`](../src/log/message.rs) records may carry **`headers`** (e.g. `content-type`). The TCP server path today attaches **empty** headers; metadata is purely by convention unless the wire gains an optional metadata field later.

Default demo topic **`events`** remains **JSON** in examples unless you explicitly opt into Protobuf producers (simulator **`--payload-format protobuf`**).

## Caveats (today)

- **Retention caps**: global **`max_topic_bytes`** in [`herbatka.toml`](../herbatka.toml) applies to every topic unless you set a **`[per_topic_max_bytes]`** entry for the **exact** topic string (e.g. `"<scope>.control"` vs `"<scope>.telemetry"`). There is no prefix or wildcard matching yet.
