# WHAT

A minimal, low-latency event streaming broker in Rust inspired by Apache Kafka.

## Features

-fleet event simulator(replaced later by real data)
-ingestion layer
-event bus with seperated channels
  -heartbeat
  -controll (ack,error)
  -payload(eg. telemetry)
-stream processor
-gateway
-ui
-storage

