# HOW

## Approach
How the project solves the problem.

High-level strategy.

## Architecture
Main components and their responsibilities.

- Car events simulation (later also 1x real IoT device, with real problems)
- Ingestion layer
- Event bus with separated channels (QUIC + Protobuf)
  - heartbeat: low-priority stream
  - control (ack, error): reliable, ordered stream
  - payload (e.g., telemetry): high-throughput stream, can tolerate some loss under pressure
- Stream processor
- Gateway
- UI
- Storage




## Design Principles
Rules for implementation.

Examples:


## Decisions
Important decisions and their reasoning.

Example:
I am using LibA instead of LibB, because...
Advantages:
    better for usecaseA
Downside:
    lack of featureC

## Workflow
Typical usage flow.

Example:



## Constraints
Technical constraints.

Examples:


###
template generated with nooj