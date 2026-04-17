# HOW

## Approach
How the project solves the problem.

High-level strategy.

## Architecture
Main components and their responsibilities.

### Request flow

`PRODUCE` and `FETCH` processing from client through TCP server/protocol to broker.

![Request flow](../assets/diagrams/svg/request-flow.svg)

### Persistence and recovery

Startup topic discovery/replay and produce path persistence/retention lifecycle.

![Persistence and recovery flow](../assets/diagrams/svg/persistence-recovery.svg)

### Replay segment recovery (corrupted-tail handling)

Startup replay behavior for clean EOF, truncated tail (`UnexpectedEof`), and hard-fail corruption paths.

![Replay segment recovery flow](../assets/diagrams/svg/replay-segment-recovery.svg)

### Simulator load behavior

How `--scenario` and `--load-profile` combine into effective event cadence.

![Simulator load flow](../assets/diagrams/svg/simulator-load-flow.svg)




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