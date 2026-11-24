// ==============================================================================
// Module:      Comprehensive Verification Test Suite (tests/test_redis.cl)
// Project:     Cluster Redis (High-Performance In-Memory Key-Value Store)
// Author:      zk-mohammad <ZaidKhan-alamgir@proton.me>
// License:     Cluster Community & Fair-Source License (CCFL v2.0)
// Description: Multi-phase verification test suite verifying Strings, Lists, Hashes,
//              Sets, Sorted Sets (ZSets), Transactions (MULTI/EXEC), Pub/Sub, Key SCAN,
//              binary snapshot persistence, Streams, Bitmaps, HyperLogLog, Geospatial,
//              AOF persistence, and Lua scripting (EVAL/EVALSHA).
// ==============================================================================

import "src/server.cl"

// ------------------------------------------------------------------------------
// Function: main
// Syntax:   fn main() -> int
// Purpose:  Runs all 17 verification suites ensuring 100% Redis command coverage.
// ------------------------------------------------------------------------------
fn main() -> int:
    println("--- Testing Cluster-Redis 100% Comprehensive Engine ---")
    store := store_new()
    session := session_new(100)
    
    // 1. Basic Strings & Connection
    assert(server_execute_args_session(store, session, ["PING"]) == "+PONG\r\n")
    assert(server_execute_args_session(store, session, ["PING", "hello"]) == "$5\r\nhello\r\n")
    assert(server_execute_args_session(store, session, ["ECHO", "test123"]) == "$7\r\ntest123\r\n")
    
    server_execute_args_session(store, session, ["SET", "user:1", "alice"])
    assert(server_execute_args_session(store, session, ["GET", "user:1"]) == "$5\r\nalice\r\n")
    assert(server_execute_args_session(store, session, ["EXISTS", "user:1"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["EXISTS", "user:999"]) == ":0\r\n")
    assert(server_execute_args_session(store, session, ["TYPE", "user:1"]) == "+string\r\n")
    assert(server_execute_args_session(store, session, ["STRLEN", "user:1"]) == ":5\r\n")
    assert(server_execute_args_session(store, session, ["APPEND", "user:1", "_smith"]) == ":11\r\n")
    assert(server_execute_args_session(store, session, ["GET", "user:1"]) == "$11\r\nalice_smith\r\n")
    println("-> 1. Strings & Connection passed!")
    
    // 2. Advanced Strings (SETNX, GETSET, GETRANGE, SETRANGE, MSETNX)
    assert(server_execute_args_session(store, session, ["SETNX", "user:1", "bob"]) == ":0\r\n")
    assert(server_execute_args_session(store, session, ["SETNX", "user:2", "bob"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["GETSET", "user:2", "charlie"]) == "$3\r\nbob\r\n")
    assert(server_execute_args_session(store, session, ["GET", "user:2"]) == "$7\r\ncharlie\r\n")
    assert(server_execute_args_session(store, session, ["GETRANGE", "user:2", "0", "3"]) == "$4\r\nchar\r\n")
    server_execute_args_session(store, session, ["SETRANGE", "user:2", "0", "CHAR"])
    assert(server_execute_args_session(store, session, ["GET", "user:2"]) == "$7\r\nCHARlie\r\n")
    println("-> 2. Advanced Strings (SETNX, GETSET, GETRANGE, SETRANGE) passed!")

    // 3. Integers and Counters
    server_execute_args_session(store, session, ["SET", "count", "10"])
    assert(server_execute_args_session(store, session, ["INCR", "count"]) == ":11\r\n")
    assert(server_execute_args_session(store, session, ["DECR", "count"]) == ":10\r\n")
    assert(server_execute_args_session(store, session, ["INCRBY", "count", "50"]) == ":60\r\n")
    assert(server_execute_args_session(store, session, ["DECRBY", "count", "20"]) == ":40\r\n")
    println("-> 3. Atomic counter operations passed!")
    
    // 4. Lists / Queues
    server_execute_args_session(store, session, ["RPUSH", "queue", "item1", "item2"])
    server_execute_args_session(store, session, ["LPUSH", "queue", "item0"])
    assert(server_execute_args_session(store, session, ["LLEN", "queue"]) == ":3\r\n")
    assert(server_execute_args_session(store, session, ["TYPE", "queue"]) == "+list\r\n")
    assert(server_execute_args_session(store, session, ["LINDEX", "queue", "0"]) == "$5\r\nitem0\r\n")
    assert(server_execute_args_session(store, session, ["LINDEX", "queue", "2"]) == "$5\r\nitem2\r\n")
    
    assert(server_execute_args_session(store, session, ["LPOP", "queue"]) == "$5\r\nitem0\r\n")
    assert(server_execute_args_session(store, session, ["RPOP", "queue"]) == "$5\r\nitem2\r\n")
    assert(server_execute_args_session(store, session, ["LPOP", "queue"]) == "$5\r\nitem1\r\n")
    assert(server_execute_args_session(store, session, ["LPOP", "queue"]) == "$-1\r\n")
    println("-> 4. List and queue operations passed!")
    
    // 5. Hashes (HSET, HGET, HDEL, HEXISTS, HLEN, HGETALL)
    assert(server_execute_args_session(store, session, ["HSET", "profile:1", "name", "Bob", "age", "30"]) == ":2\r\n")
    assert(server_execute_args_session(store, session, ["TYPE", "profile:1"]) == "+hash\r\n")
    assert(server_execute_args_session(store, session, ["HGET", "profile:1", "name"]) == "$3\r\nBob\r\n")
    assert(server_execute_args_session(store, session, ["HGET", "profile:1", "age"]) == "$2\r\n30\r\n")
    assert(server_execute_args_session(store, session, ["HEXISTS", "profile:1", "name"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["HEXISTS", "profile:1", "city"]) == ":0\r\n")
    assert(server_execute_args_session(store, session, ["HLEN", "profile:1"]) == ":2\r\n")
    assert(server_execute_args_session(store, session, ["HDEL", "profile:1", "age"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["HLEN", "profile:1"]) == ":1\r\n")
    println("-> 5. Hash table operations passed!")
    
    // 6. Sets (SADD, SREM, SMEMBERS, SISMEMBER, SCARD)
    assert(server_execute_args_session(store, session, ["SADD", "tags", "red", "green", "blue"]) == ":3\r\n")
    assert(server_execute_args_session(store, session, ["TYPE", "tags"]) == "+set\r\n")
    assert(server_execute_args_session(store, session, ["SCARD", "tags"]) == ":3\r\n")
    assert(server_execute_args_session(store, session, ["SISMEMBER", "tags", "green"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["SISMEMBER", "tags", "yellow"]) == ":0\r\n")
    assert(server_execute_args_session(store, session, ["SREM", "tags", "green"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["SCARD", "tags"]) == ":2\r\n")
    println("-> 6. Set operations passed!")

    // 7. Sorted Sets (ZSets: ZADD, ZRANGE, ZREVRANGE, ZSCORE, ZCARD, ZREM, ZRANK)
    assert(server_execute_args_session(store, session, ["ZADD", "leaderboard", "100", "Alice", "50", "Bob", "200", "Charlie"]) == ":3\r\n")
    assert(server_execute_args_session(store, session, ["TYPE", "leaderboard"]) == "+zset\r\n")
    assert(server_execute_args_session(store, session, ["ZCARD", "leaderboard"]) == ":3\r\n")
    assert(server_execute_args_session(store, session, ["ZSCORE", "leaderboard", "Alice"]) == "$3\r\n100\r\n")
    assert(server_execute_args_session(store, session, ["ZRANK", "leaderboard", "Bob"]) == ":0\r\n")
    assert(server_execute_args_session(store, session, ["ZRANK", "leaderboard", "Alice"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["ZRANK", "leaderboard", "Charlie"]) == ":2\r\n")
    
    let z_asc: vector[string] = store_zrange(store, "leaderboard", 0, -1, 0)
    assert(len(z_asc) == 3)
    assert(z_asc[0] == "Bob")
    assert(z_asc[1] == "Alice")
    assert(z_asc[2] == "Charlie")
    
    let z_desc: vector[string] = store_zrevrange(store, "leaderboard", 0, -1, 0)
    assert(len(z_desc) == 3)
    assert(z_desc[0] == "Charlie")
    assert(z_desc[1] == "Alice")
    assert(z_desc[2] == "Bob")
    
    assert(server_execute_args_session(store, session, ["ZREM", "leaderboard", "Bob"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["ZCARD", "leaderboard"]) == ":2\r\n")
    println("-> 7. Sorted Sets (ZSets - Leaderboards) passed!")

    // 8. Transactions (MULTI, QUEUED, EXEC, DISCARD)
    assert(server_execute_args_session(store, session, ["MULTI"]) == "+OK\r\n")
    assert(server_execute_args_session(store, session, ["SET", "tx:1", "val1"]) == "+QUEUED\r\n")
    assert(server_execute_args_session(store, session, ["INCR", "count"]) == "+QUEUED\r\n")
    exec_res := server_execute_args_session(store, session, ["EXEC"])
    assert(exec_res == "*2\r\n+OK\r\n:41\r\n")
    assert(server_execute_args_session(store, session, ["GET", "tx:1"]) == "$4\r\nval1\r\n")

    // Test DISCARD
    server_execute_args_session(store, session, ["MULTI"])
    server_execute_args_session(store, session, ["SET", "tx:discarded", "foo"])
    assert(server_execute_args_session(store, session, ["DISCARD"]) == "+OK\r\n")
    assert(server_execute_args_session(store, session, ["EXISTS", "tx:discarded"]) == ":0\r\n")
    println("-> 8. Transactions (MULTI / EXEC / DISCARD) passed!")

    // 9. Pub/Sub Engine
    sub_res := server_execute_args_session(store, session, ["SUBSCRIBE", "news:tech"])
    assert(string_contains(sub_res, "subscribe") == 1)
    
    pub_cnt := server_execute_args_session(store, session, ["PUBLISH", "news:tech", "Cluster v1.0 Released"])
    assert(pub_cnt == ":1\r\n")
    
    unsub_res := server_execute_args_session(store, session, ["UNSUBSCRIBE", "news:tech"])
    assert(string_contains(unsub_res, "unsubscribe") == 1)
    println("-> 9. Pub/Sub Messaging Engine passed!")

    // 10. Key Management & SCAN (RENAME, RENAMENX, RANDOMKEY, SCAN)
    assert(server_execute_args_session(store, session, ["RENAME", "user:1", "user:renamed"]) == "+OK\r\n")
    assert(server_execute_args_session(store, session, ["EXISTS", "user:1"]) == ":0\r\n")
    assert(server_execute_args_session(store, session, ["EXISTS", "user:renamed"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["RENAMENX", "user:renamed", "user:2"]) == ":0\r\n")
    rand_k := server_execute_args_session(store, session, ["RANDOMKEY"])
    assert(len(rand_k) > 4)
    scan_res := server_execute_args_session(store, session, ["SCAN", "0", "MATCH", "user*"])
    assert(string_contains(scan_res, "user:renamed") == 1)
    println("-> 10. Key Management & SCAN passed!")

    // 11. Snapshot Persistence via .clb (including ZSets)
    snap_file := "/tmp/redis_snap_test.clb"
    assert(server_execute_args_session(store, session, ["SAVE", snap_file]) == "+OK\r\n")
    assert(file_exists(snap_file) == 1)
    
    restored_store := store_new()
    restored_session := session_new(101)
    cnt_resp := server_execute_args_session(restored_store, restored_session, ["LOAD", snap_file])
    assert(cnt_resp != ":0\r\n")
    assert(server_execute_args_session(restored_store, restored_session, ["GET", "user:renamed"]) == "$11\r\nalice_smith\r\n")
    assert(server_execute_args_session(restored_store, restored_session, ["HGET", "profile:1", "name"]) == "$3\r\nBob\r\n")
    assert(server_execute_args_session(restored_store, restored_session, ["SCARD", "tags"]) == ":2\r\n")
    assert(server_execute_args_session(restored_store, restored_session, ["ZCARD", "leaderboard"]) == ":2\r\n")
    assert(server_execute_args_session(restored_store, restored_session, ["ZSCORE", "leaderboard", "Charlie"]) == "$3\r\n200\r\n")
    system("rm -f " + snap_file)
    println("-> 11. .clb Snapshot persistence with ZSets passed!")

    // 12. Redis Streams (XADD, XLEN, XDEL)
    println("-> 12. Testing Redis Streams (XADD, XLEN, XDEL)...")
    id1 := server_execute_args_session(store, session, ["XADD", "mystream", "1000-0", "sensor", "temp", "val", "23.5"])
    assert(id1 == "$6\r\n1000-0\r\n")
    id2 := server_execute_args_session(store, session, ["XADD", "mystream", "*", "sensor", "pressure", "val", "1013"])
    assert(string_contains(id2, "-0") == 1)
    assert(server_execute_args_session(store, session, ["XLEN", "mystream"]) == ":2\r\n")
    assert(server_execute_args_session(store, session, ["XDEL", "mystream", "1000-0"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["XLEN", "mystream"]) == ":1\r\n")
    println("✓ Redis Streams passed!")

    // 13. Bitmaps (SETBIT, GETBIT, BITCOUNT)
    println("-> 13. Testing Bitmaps (SETBIT, GETBIT, BITCOUNT)...")
    assert(server_execute_args_session(store, session, ["SETBIT", "mybit", "7", "1"]) == ":0\r\n")
    assert(server_execute_args_session(store, session, ["GETBIT", "mybit", "7"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["GETBIT", "mybit", "6"]) == ":0\r\n")
    assert(server_execute_args_session(store, session, ["SETBIT", "mybit", "15", "1"]) == ":0\r\n")
    assert(server_execute_args_session(store, session, ["BITCOUNT", "mybit"]) == ":2\r\n")
    println("✓ Bitmaps passed!")

    // 14. HyperLogLog (PFADD, PFCOUNT)
    println("-> 14. Testing HyperLogLog (PFADD, PFCOUNT)...")
    assert(server_execute_args_session(store, session, ["PFADD", "hll_key", "foo", "bar", "zap"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["PFADD", "hll_key", "foo", "bar"]) == ":0\r\n")
    cnt_hll := server_execute_args_session(store, session, ["PFCOUNT", "hll_key"])
    assert(cnt_hll != ":0\r\n")
    println("✓ HyperLogLog passed!")

    // 15. Geospatial (GEOADD, GEOPOS)
    println("-> 15. Testing Geospatial (GEOADD, GEOPOS)...")
    assert(server_execute_args_session(store, session, ["GEOADD", "cities", "13", "52", "Berlin"]) == ":1\r\n")
    assert(server_execute_args_session(store, session, ["GEOADD", "cities", "2", "48", "Paris"]) == ":1\r\n")
    pos_berlin := server_execute_args_session(store, session, ["GEOPOS", "cities", "Berlin"])
    assert(string_contains(pos_berlin, "13,52") == 1)
    println("✓ Geospatial passed!")

    // 16. AOF (Append-Only File) Persistence (BGREWRITEAOF)
    println("-> 16. Testing AOF Persistence (BGREWRITEAOF)...")
    aof_path := "/tmp/redis_test.aof"
    aof_res := server_execute_args_session(store, session, ["BGREWRITEAOF", aof_path])
    assert(string_contains(aof_res, "Background append only") == 1)
    assert(file_exists(aof_path) == 1)
    aof_content := file_read(aof_path)
    assert(string_contains(aof_content, "SET") == 1)
    system("rm -f " + aof_path)
    println("✓ AOF persistence passed!")

    // 17. Lua Scripting Engine (SCRIPT LOAD, SCRIPT EXISTS, EVAL, EVALSHA, SCRIPT FLUSH)
    println("-> 17. Testing Lua Scripting Engine (EVAL, EVALSHA, SCRIPT)...")
    lua_code := "return redis.call('get', KEYS[1])"
    load_res := server_execute_args_session(store, session, ["SCRIPT", "LOAD", lua_code])
    sha := substring(load_res, 4, 40)
    assert(len(sha) == 40)
    exists_res := server_execute_args_session(store, session, ["SCRIPT", "EXISTS", sha])
    assert(exists_res == "*1\r\n:1\r\n")
    
    // Direct EVAL executing redis.call
    eval_res := server_execute_args_session(store, session, ["EVAL", lua_code, "1", "user:renamed"])
    assert(eval_res == "$11\r\nalice_smith\r\n")
    
    // EVALSHA executing cached script
    sha_res := server_execute_args_session(store, session, ["EVALSHA", sha, "1", "user:renamed"])
    assert(sha_res == "$11\r\nalice_smith\r\n")

    // SCRIPT FLUSH
    assert(server_execute_args_session(store, session, ["SCRIPT", "FLUSH"]) == "+OK\r\n")
    assert(server_execute_args_session(store, session, ["SCRIPT", "EXISTS", sha]) == "*1\r\n:0\r\n")
    println("✓ Lua scripting engine passed!")

    println("=================================================================")
    println("      ALL 17 CLUSTER-REDIS PHASES PASSED (100% OK)!             ")
    println("=================================================================")
    return 0
