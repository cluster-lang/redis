# cluster-redis ⚡

> **Pure Cluster Language In-Memory Key-Value Server Daemon & RESP Engine**  
> *Zero foreign dependencies, zero C++ headers, 100% pure `.cl` code.*

`cluster-redis` is a blazingly fast, standalone in-memory database daemon built entirely in the Cluster Language (`.cl`). It implements the Redis Serialization Protocol (RESP), TTL-based expiration, atomic arithmetic counters, FIFO/LIFO list queues, Hash tables, Sets, multi-key operations, TCP socket server networking (`std/net.cl`), and native `.clb` binary snapshot persistence.

It connects directly with the official `redis-cli` and handles over **200,000 requests/sec** under official `redis-benchmark`.

---

## ✨ Features

- **RESP Protocol Engine**: Simple Strings (`+`), Errors (`-`), Integers (`:`), Bulk Strings (`$`), and Arrays (`*`).
- **Comprehensive Command Suite**:
  - **Connection & Server**: `PING`, `ECHO`, `QUIT`, `SELECT`, `CLIENT`, `COMMAND`, `CONFIG`, `INFO`, `DBSIZE`, `FLUSHALL`, `FLUSHDB`
  - **Strings & Arithmetic**: `SET`, `GET`, `MSET`, `MGET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `STRLEN`, `APPEND`
  - **Keys & Expiration**: `DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `TTL`, `PERSIST`, `KEYS`
  - **Lists & Queues**: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`
  - **Hashes**: `HSET`, `HMSET`, `HGET`, `HMGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`, `HKEYS`, `HVALS`
  - **Sets**: `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`
  - **Snapshot Persistence**: `SAVE <path>`, `LOAD <path>` via high-density `.clb` (Cluster Binary Bundle format)
- **High-Performance TCP Server**: Native non-blocking TCP socket engine powered by `std/net.cl` with concurrent connection handling.
- **Official Tooling Compatible**: 100% drop-in compatible with official `redis-cli`, Python `redis-py`, Go `go-redis`, Node.js `ioredis`, and `redis-benchmark`.

---

## 🚀 Getting Started

### 1. Build Native Standalone Binary

```bash
cd cluster-redis
cl build src/main.cl -o bin/cluster-redis
```

### 2. Start the Server

```bash
# Start on default port 6379
./bin/cluster-redis

# Or specify a custom port (e.g. 6380)
./bin/cluster-redis 6380
```

### 3. Connect with Official `redis-cli`

```bash
# Ping the server
redis-cli -p 6379 PING
# Output: PONG

# Set and Get keys
redis-cli -p 6379 SET my_key "Hello Pure Cluster!"
redis-cli -p 6379 GET my_key
# Output: "Hello Pure Cluster!"

# Hash tables
redis-cli -p 6379 HSET user:1 name "Alice" role "Admin"
redis-cli -p 6379 HGETALL user:1

# Sets
redis-cli -p 6379 SADD tags "Cluster" "LLVM" "Native"
redis-cli -p 6379 SMEMBERS tags

# Persistence
redis-cli -p 6379 SAVE dump.clb
```

### 4. Running the Test Suite

```bash
cl build tests/test_redis.cl -o test_runner && ./test_runner
```

Output:
```text
--- Testing Cluster-Redis Comprehensive Engine ---
-> 1. String & Connection commands passed!
-> 2. Atomic counter operations passed!
-> 3. List and queue operations passed!
-> 4. Hash table operations passed!
-> 5. Set operations passed!
-> 6. TTL key expiration passed!
-> 7. Multi-key operations passed!
-> 8. .clb Snapshot persistence & recovery passed!
-> 9. Pipelining wire parser passed!
=================================================================
         ALL CLUSTER-REDIS TESTS PASSED WITH 100% SUCCESS!       
=================================================================
```

---

## ⚡ Benchmarks

Benchmarked against official `redis-benchmark` over local TCP loopback:

```bash
redis-benchmark -p 6380 -t ping,set,get -c 50 -n 10000 -q
```

Results:
```text
PING_INLINE: 133,333.33 requests per second, p50=0.183 msec
PING_MBULK:  204,081.64 requests per second, p50=0.135 msec
SET:         204,081.64 requests per second, p50=0.127 msec
GET:         204,081.64 requests per second, p50=0.127 msec
```

---

## 📂 Project Architecture

```
cluster-redis/
├── cl.toml               # Package configuration
├── README.md             # Documentation & Benchmarks
├── bin/
│   └── cluster-redis     # Pure native ELF binary (161KB)
├── src/
│   ├── main.cl           # Standalone TCP server daemon & client loop
│   ├── resp.cl           # RESP protocol parser & pipeline decoder
│   ├── store.cl          # In-memory storage engine, Hashes, Sets, Lists, TTL & .clb
│   └── server.cl         # Command dispatcher & Redis protocol handler
└── tests/
    └── test_redis.cl     # Full automated test suite & verification
```

---

## 📜 License

Licensed under MIT OR Apache-2.0. Built by Alamgir ZK.
