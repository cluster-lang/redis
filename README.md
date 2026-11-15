# cluster-redis ⚡

> **Pure Cluster Language In-Memory Key-Value Store & RESP Engine**  
> *Zero foreign dependencies, zero C++ headers, 100% pure `.cl` code.*

`cluster-redis` is a blazingly fast, native in-memory database built entirely in the Cluster Language (`.cl`). It implements the Redis Serialization Protocol (RESP), TTL-based expiration, atomic arithmetic counters, FIFO/LIFO list queues, and native `.clb` binary snapshot persistence.

---

## ✨ Features

- **RESP Protocol Support**: Simple Strings (`+`), Errors (`-`), Integers (`:`), Bulk Strings (`$`), and Arrays (`*`).
- **Core Commands**:
  - Strings & Keys: `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`, `KEYS`, `DBSIZE`, `FLUSHALL`
  - Atomic Counters: `INCR`, `DECR`, `INCRBY`
  - Queues & Lists: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`
  - Snapshot Persistence: `SAVE <path>`, `LOAD <path>` via `.clb` (Cluster Binary Bundle format)
  - TTL Expiration: `SET key val EX <seconds>`
- **Ultra-Low Latency**: Compiles straight to machine code via Cluster's native LLVM backend (`cl run` / `cl build -O3`).
- **Zero Foreign Libraries**: 100% written in pure Cluster language and standard modules (`std/clb.cl`).

---

## 🚀 Getting Started

### Prerequisites

Ensure the Cluster Language toolchain (`cl`) is installed and available in your `PATH`.

```bash
cl version
```

### Running the Demo

```bash
cd cluster-redis
cl run src/main.cl
```

### Running the Test Suite

```bash
cl run tests/test_redis.cl
```

Output:
```text
--- Testing Cluster-Redis Engine ---
-> String commands (PING, ECHO, SET, GET, EXISTS) passed!
-> Atomic counter operations (INCR, DECR, INCRBY) passed!
-> List and queue operations (LPUSH, RPUSH, LPOP, RPOP, LRANGE) passed!
-> TTL key expiration engine passed!
-> RESP wire protocol parser passed!
-> .clb Snapshot persistence & recovery passed!
-> Running in-memory throughput benchmark (5,000 ops)...
-> 5,000 operations completed with 100% data integrity!
=================================================================
           ALL CLUSTER-REDIS TESTS PASSED (100% OK)!             
=================================================================
```

---

## 📂 Project Architecture

```
cluster-redis/
├── cl.toml               # Package configuration
├── README.md             # Documentation & Benchmarks
├── src/
│   ├── main.cl           # Entrypoint demonstration
│   ├── resp.cl           # RESP protocol serializer & parser
│   ├── store.cl          # In-memory storage engine, TTL & .clb persistence
│   └── server.cl         # Command dispatcher
└── tests/
    └── test_redis.cl     # Full automated test suite & benchmark
```

---

## 📜 License

Licensed under MIT OR Apache-2.0. Built by Alamgir ZK.
