# HOW

## Approach
How the project solves the problem.

High-level strategy.

## Architecture
Main components and their responsibilities.

-Car events simulation(later also 1x real IOT device, with real problems)
-Ingestion Layer 
-event bus with seperated channels, quic+protobuf
  -heartbeat , low-priority stream
  -controll (ack,error), reliable, ordered stream
  -payload(eg. telemetry), high-throughput stream, can tolerate some loss under pressure
-stream processor
-gateway
-ui
-storage




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