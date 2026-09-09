# Distributed Key-Value Store with Anti-Entropy Push-Pull

A peer-to-peer distributed key-value store built with **Python**, **ZeroMQ**, and the **Anti-Entropy Push-Pull** epidemic protocol.

The project demonstrates how a group of independent nodes can maintain a shared state and achieve eventual consistency without a central coordinator, while handling partial network failures, packet loss, crash faults (fail-stop), and concurrent updates via Last-Write-Wins (LWW) conflict resolution.

---

## Table of Contents

- [Overview](#overview)
- [Goals](#goals)
- [Architecture](#architecture)
- [Anti-Entropy Pattern](#anti-entropy-pattern)
- [Conflict Resolution (LWW)](#conflict-resolution-lww)
- [Fault Tolerance and Chaos Engineering](#fault-tolerance-and-chaos-engineering)
- [Project Structure](#project-structure)
- [Technologies](#technologies)
- [Requirements](#requirements)
- [Configuration](#configuration)
- [Running the Project](#running-the-project)
- [CLI Endpoints](#cli-endpoints)
- [Usage Examples](#usage-examples)
- [State Inspection](#state-inspection)
- [Failure Scenarios](#failure-scenarios)
- [Limitations](#limitations)
- [Possible Improvements](#possible-improvements)

---

## Overview

This project simulates a distributed database involving independent peer-to-peer nodes organized in a predefined topology. 

Instead of relying on a centralized coordinator, the system uses an epidemic gossip protocol. A client can inject a new Key-Value pair into any arbitrary node in the network. Through periodic communication rounds, nodes exchange information and converge to a globally consistent state.

The system is intentionally designed as a didactic project. Its primary objective is to demonstrate the semantics and failure handling of a distributed P2P architecture rather than provide a production-ready database.

---

## Goals

The project demonstrates the following distributed systems concepts:

- independent peer-to-peer nodes;
- asynchronous communication using ZeroMQ (REQ/REP);
- eventual consistency via gossip protocols;
- partial failure management (omission faults);
- crash fault tolerance (fail-stop scenarios);
- thread-safe state management;
- Last-Write-Wins (LWW) conflict resolution via logical timestamps;
- separation between background daemon threads and execution loops.

---

## Architecture

```text
    (0,0) ── (0,1) ── (0,2)
      │        │        │
    (1,0) ── (1,1) ── (1,2)
      │        │        │
    (2,0) ── (2,1) ── (2,2)

```

The network is organized as a 3x3 Grid overlay network. Each node:

* Operates as an independent process.


* Only knows its direct cardinal neighbors (North, South, East, West).
* Owns its own local state (a dictionary) and communicates through TCP sockets.


* There is no shared in-memory state between services.



---

## Anti-Entropy Pattern

The system divides a synchronization round into three operations:

```text
Push
Pull
Merge

```

### Push

Every `ROUND_INTERVAL` seconds, a node selects a random neighbor (based on the `FANOUT` parameter) and sends its current state.

### Pull

The receiving neighbor replies by sending its own current state back to the initiator.

### Merge

Both nodes perform a local merge, updating their internal dictionaries. If a missing key is found, or if a key has a newer timestamp, the local store is updated.

---

## Conflict Resolution (LWW)

In a distributed environment, network delays can cause out-of-order messages. The protocol resolves conflicts using a **Last-Write-Wins (LWW)** strategy.

Every time a new data point is injected via the API, a Unix timestamp is attached:

```python
"key": {"value": "some_data", "timestamp": 1787496207.89}

```

During the `Merge` phase, the participant applies the update only if:

1. The key does not exist locally.
2. The incoming timestamp is strictly greater than the local timestamp.

---

## Fault Tolerance and Chaos Engineering

The architecture is built to withstand non-Byzantine failures.

### Packet Loss (Omission Faults)

The network layer explicitly simulates unreliability. A configurable `PACKET_LOSS_RATE` (e.g., 20%) causes nodes to randomly drop outgoing packets.
The system does not crash; it logs a `[PACKET LOSS]` warning, closes the socket, and simply retries in the next round.

### Fail-Stop (Crash Faults)

A node can experience a hard crash (`os._exit(1)`), simulating a hardware failure or power loss without sending TCP `FIN` packets.
If a healthy node attempts to push/pull from a dead node, the socket relies on `REQUEST_TIMEOUT_MS`. The exception `zmq.Again` is caught, a timeout warning is logged, and the network routes around the dead node (topological convergence).

---

## Project Structure

```text
Distributed-AntiEntropy-Protocol/
│
├── antientropy.py
└── README.md

```

---

## Technologies

* Python 3.10+


* ZeroMQ (`pyzmq`)
* `threading`
* `argparse`

---

## Requirements

To run the project, install the ZeroMQ bindings for Python:

```bash
pip install pyzmq

```

No external databases or orchestrators are required.

---

## Configuration

The main environment constants at the top of the script are:

| Variable | Default | Purpose |
| --- | --- | --- |
| `GRID_SIZE` | `3` | Defines the N×N grid topology (default: 9 nodes) |
| `BASE_PORT` | `5550` | Base TCP port for P2P communication |
| `ROUND_INTERVAL` | `2.0` | Seconds between gossip rounds |
| `FANOUT` | `1` | Number of neighbors contacted per round |
| `REQUEST_TIMEOUT_MS` | `800` | Network timeout for unreachable nodes |
| `PACKET_LOSS_RATE` | `0.2` | Probability (20%) of dropping a packet |

---

## Running the Project

### Demo Mode (Recommended)

To run the complete 3x3 grid automatically in a single terminal:

```bash
python antientropy.py --demo

```

This spawns 9 subprocesses. Node 4 (the center node) is initialized with a seed value to demonstrate the initial outward propagation.

### Manual Node Startup

To start nodes individually (e.g., in `tmux` or multiple terminal windows):

```bash
python antientropy.py --id 0
python antientropy.py --id 1
python antientropy.py --id 4 --seed-value "initial_state"

```

---

## CLI Endpoints

The script acts as a client API to interact with the running network.

### Inject Data (PUT)

To insert a new key-value pair into a specific node (e.g., Node 0):

```bash
python antientropy.py --put 0 new_key new_value

```

### Chaos Engineering (KILL)

To force a hard crash on a specific node (e.g., Node 4) bypassing exception handling:

```bash
python antientropy.py --kill 4

```

---

## Usage Examples

### 1. Observe the Initial Propagation

Start the network:

```bash
python antientropy.py --demo

```

*Expected behavior: The seed value in Node 4 spreads to its neighbors, eventually reaching the corners (Nodes 0, 2, 6, 8).*

### 2. Inject New Data

In a second terminal, execute:

```bash
python antientropy.py --put 0 order_id 9942

```

*Expected behavior: Node 0 prints `[PUT ESTERNA]`. Within a few seconds, all other nodes print `AGGIORNAMENTO STORE` as they learn about the new key.*

---

## Failure Scenarios

| Scenario | System Reaction | Final State |
| --- | --- | --- |
| Packet is dropped (`PACKET_LOSS_RATE`) | Sender aborts round; logs `[PACKET LOSS]` | Recovers automatically in the next round

 |
| Neighbor node crashes (Fail-Stop) | Initiator hits `REQUEST_TIMEOUT_MS`; logs timeout | Survived nodes route around the partition; eventual consistency is maintained

 |

---

## Limitations

This is a didactic implementation. The following limitations are intentional:

* Participant state is kept in memory and is lost when the process restarts.


* Hardcoded grid topology logic.
* LWW timestamps rely on local system clocks (no logical Vector Clocks).
* Security and service-to-service authentication are not implemented.



---

## Possible Improvements

Future developments could focus on the following areas:

* **Vector Clocks:** Replacing system timestamps with Vector Clocks to handle concurrent updates more accurately.
* **Dynamic Topology:** Allowing nodes to join and leave gracefully (discovery service).
* **Persistence:** Appending the state to an SQLite or JSON file to recover from crashes.



```

```
