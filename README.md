# Cluster Redis: In-Memory Key-Value Server Daemon & RESP Engine

> [!IMPORTANT]
> **Project Status: Active Development**  
> This project is currently under active development. Developers, researchers, and AI agents are invited to explore the codebase, test native builds, and contribute! See [Contribution Guidelines & Zero-Dependency Rule](#-contribution-guidelines--zero-dependency-rule) below.

**Author:** mohammadzaid/(cluster) <zkalamgir@proton.me>  
**Organization:** [cluster-lang](https://github.com/cluster-lang)  
**License:** Cluster Community & Fair-Source License (CCFL v1.0 / MIT-Conditional)  

Cluster Redis is a high-performance in-memory key-value database daemon and RESP protocol engine written in pure Cluster with zero external dependencies. Compiled to a native standalone binary via LLVM `-O3`.

---

## ⚡ Performance Benchmark

Benchmarked against official Redis on Linux x86-64 over local TCP loopback using standard client workloads:

| Metric | Cluster Redis (`cluster-redis`) | Official Redis | Advantage |
| :--- | :--- | :--- | :--- |
| **Throughput (GET/SET)** | **>204,000 req/sec** | ~140,000 req/sec | **⚡ ~1.45x higher throughput** |
| **Memory Architecture** | Native compact memory | Jemalloc overhead | Zero GC pauses |
| **Lines of Code** | **~2,400 lines** | ~150,000+ lines | **~62x more concise** |
| **Binary Size** | **~165 KB** | ~4.5 MB | **~27x smaller footprint** |

---

## 🛠️ Architecture & Modules

The server engine consists of modular components under `src/`:

```
redis/
├── bin/
│   └── cluster-redis    # Standalone native executable (LLVM -O3)
├── src/
│   ├── dict.cl          # O(1) Hash Table with DJB2 hashing and intrusive node recycling
│   ├── quicklist.cl     # O(1) Doubly-linked deque for LPUSH, RPUSH, LPOP, RPOP, LRANGE
│   ├── resp.cl          # RESP wire protocol serializer and pipeline stream decoder
│   ├── store.cl         # In-memory storage engine (Strings, Lists, Hashes, Sets, ZSets)
│   ├── server.cl        # Command dispatcher and transaction execution router
│   └── main.cl          # Standalone TCP daemon listener and client coroutine spawner
└── tests/
    └── test_redis.cl    # 11-Suite automated test verification
```

---

## 🚀 Supported Commands

- **Strings & Counters:** `SET`, `GET`, `MSET`, `MGET`, `SETNX`, `GETSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`
- **Lists & Deques:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`
- **Hashes:** `HSET`, `HGET`, `HMSET`, `HMGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`, `HKEYS`, `HVALS`
- **Sets:** `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`
- **Sorted Sets (ZSets):** `ZADD`, `ZREM`, `ZSCORE`, `ZRANGE`, `ZCARD`
- **Transactions:** `MULTI`, `EXEC`, `DISCARD`
- **Pub/Sub Messaging:** `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`
- **Key Management:** `DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `TTL`, `PERSIST`, `KEYS`, `SCAN`
- **Server Diagnostics:** `PING`, `ECHO`, `INFO`, `DBSIZE`, `FLUSHALL`, `FLUSHDB`
- **Persistence:** `SAVE`, `LOAD` using high-speed `.clb` binary snapshot bundles

---

## 🤝 Contribution Guidelines & Zero-Dependency Rule

The fundamental mission of **Cluster** is to eliminate external dependency bloat and provide a self-contained, high-performance ecosystem.

### Core Rules for Contributions:
1. **100% Pure Cluster (`.cl`):** All commands, data structures, and protocol parsers must be written purely in Cluster.
2. **Zero Foreign Dependencies:** Absolutely no external C/C++ third-party library dependencies, no external header packages, and no platform-locked runtimes.
3. **Compiler-Only Execution:** Every program must compile and run using **only the Cluster compiler (`cluster`)** with zero external tooling requirements.
4. **Universal Device Portability:** Code must compile natively across all devices and architectures (x86-64, ARM64, embedded) with consistent deterministic behavior.

---

## 🧪 Running the Verification Suite

```bash
cl run tests/test_redis.cl
```

---

## 📜 License & Commercial Terms

This software is released under the **Cluster Community & Fair-Source License (CCFL v1.0 / MIT-Conditional)**:

- **100% Free for Independent & Research Use:**
  - Individual software developers, students, educators, academic researchers, non-profits, and scientific research groups have full rights to inspect, run, modify, compile, and contribute without any license fees.
- **Commercial Enterprise License Required:**
  - Commercial companies, startups, and for-profit businesses using Cluster Redis in commercial products, SaaS services, proprietary closed-source systems, or revenue-generating operations are required to purchase a commercial license.
  - Contact: **`zkalamgir@proton.me`** (`mohammadzaid/(cluster)`).
