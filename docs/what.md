# WHAT

A minimal, low-latency event streaming broker in Rust inspired by Apache Kafka.

## Features

- Fleet event simulator (replaced later by real data)
- Ingestion layer
- Event bus with separated channels
  - heartbeat
  - control (ack, error)
  - payload (e.g., telemetry)
- Stream processor
- Gateway
- UI
- Storage

