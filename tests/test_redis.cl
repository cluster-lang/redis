// ==============================================================================
// Unit Test Suite: Cluster-Redis In-Memory Engine & RESP Protocol
// ==============================================================================

import "src/server.cl"

fn main() -> int:
    println("--- Testing Cluster-Redis Comprehensive Engine ---")
    store := store_new()
    
    // 1. Basic Strings & Connection
    assert(server_execute(store, "PING") == "+PONG\r\n")
    assert(server_execute(store, "PING hello") == "$5\r\nhello\r\n")
    assert(server_execute(store, "ECHO test123") == "$7\r\ntest123\r\n")
    
    server_execute(store, "SET user:1 alice")
    assert(server_execute(store, "GET user:1") == "$5\r\nalice\r\n")
    assert(server_execute(store, "EXISTS user:1") == ":1\r\n")
    assert(server_execute(store, "EXISTS user:999") == ":0\r\n")
    assert(server_execute(store, "TYPE user:1") == "+string\r\n")
    assert(server_execute(store, "STRLEN user:1") == ":5\r\n")
    assert(server_execute(store, "APPEND user:1 _smith") == ":11\r\n")
    assert(server_execute(store, "GET user:1") == "$11\r\nalice_smith\r\n")
    println("-> 1. String & Connection commands passed!")
    
    // 2. Integers and Counters
    server_execute(store, "SET count 10")
    assert(server_execute(store, "INCR count") == ":11\r\n")
    assert(server_execute(store, "DECR count") == ":10\r\n")
    assert(server_execute(store, "INCRBY count 50") == ":60\r\n")
    assert(server_execute(store, "DECRBY count 20") == ":40\r\n")
    println("-> 2. Atomic counter operations passed!")
    
    // 3. Lists / Queues
    server_execute(store, "RPUSH queue item1")
    server_execute(store, "RPUSH queue item2")
    server_execute(store, "LPUSH queue item0")
    assert(server_execute(store, "LLEN queue") == ":3\r\n")
    assert(server_execute(store, "TYPE queue") == "+list\r\n")
    
    let q_res: vector[string] = store_lrange(store, "queue", 0, -1)
    assert(len(q_res) == 3)
    assert(q_res[0] == "item0")
    assert(q_res[1] == "item1")
    assert(q_res[2] == "item2")
    
    assert(server_execute(store, "LPOP queue") == "$5\r\nitem0\r\n")
    assert(server_execute(store, "RPOP queue") == "$5\r\nitem2\r\n")
    assert(server_execute(store, "LPOP queue") == "$5\r\nitem1\r\n")
    assert(server_execute(store, "LPOP queue") == "$-1\r\n")
    println("-> 3. List and queue operations passed!")
    
    // 4. Hashes (HSET, HGET, HDEL, HEXISTS, HLEN, HGETALL)
    assert(server_execute(store, "HSET profile:1 name Bob age 30") == ":2\r\n")
    assert(server_execute(store, "TYPE profile:1") == "+hash\r\n")
    assert(server_execute(store, "HGET profile:1 name") == "$3\r\nBob\r\n")
    assert(server_execute(store, "HGET profile:1 age") == "$2\r\n30\r\n")
    assert(server_execute(store, "HEXISTS profile:1 name") == ":1\r\n")
    assert(server_execute(store, "HEXISTS profile:1 city") == ":0\r\n")
    assert(server_execute(store, "HLEN profile:1") == ":2\r\n")
    assert(server_execute(store, "HDEL profile:1 age") == ":1\r\n")
    assert(server_execute(store, "HLEN profile:1") == ":1\r\n")
    println("-> 4. Hash table operations passed!")
    
    // 5. Sets (SADD, SREM, SMEMBERS, SISMEMBER, SCARD)
    assert(server_execute(store, "SADD tags red green blue") == ":3\r\n")
    assert(server_execute(store, "TYPE tags") == "+set\r\n")
    assert(server_execute(store, "SCARD tags") == ":3\r\n")
    assert(server_execute(store, "SISMEMBER tags green") == ":1\r\n")
    assert(server_execute(store, "SISMEMBER tags yellow") == ":0\r\n")
    assert(server_execute(store, "SREM tags green") == ":1\r\n")
    assert(server_execute(store, "SCARD tags") == ":2\r\n")
    println("-> 5. Set operations passed!")
    
    // 6. TTL, Expiration & Persistence
    server_execute(store, "SET session:abc 999 EX 10")
    assert(store_exists(store, "session:abc") == 1)
    assert(server_execute(store, "TTL session:abc") == ":10\r\n")
    
    store_advance_time(store, 5)
    assert(store_exists(store, "session:abc") == 1)
    
    store_advance_time(store, 6)
    assert(store_exists(store, "session:abc") == 0)
    assert(server_execute(store, "GET session:abc") == "$-1\r\n")
    println("-> 6. TTL key expiration passed!")
    
    // 7. Multi-key operations (MSET, MGET, DEL)
    assert(server_execute(store, "MSET k1 v1 k2 v2 k3 v3") == "+OK\r\n")
    assert(server_execute(store, "GET k1") == "$2\r\nv1\r\n")
    assert(server_execute(store, "GET k2") == "$2\r\nv2\r\n")
    assert(server_execute(store, "DEL k1 k2 k3") == ":3\r\n")
    println("-> 7. Multi-key operations passed!")
    
    // 8. Snapshot Persistence via .clb
    snap_file := "/tmp/redis_full_snap.clb"
    assert(server_execute(store, "SAVE " + snap_file) == "+OK\r\n")
    assert(file_exists(snap_file) == 1)
    
    restored_store := store_new()
    cnt_resp := server_execute(restored_store, "LOAD " + snap_file)
    assert(cnt_resp != ":0\r\n")
    assert(store_get(restored_store, "user:1") == "alice_smith")
    assert(server_execute(restored_store, "HGET profile:1 name") == "$3\r\nBob\r\n")
    assert(server_execute(restored_store, "SCARD tags") == ":2\r\n")
    println("-> 8. .clb Snapshot persistence & recovery passed!")
    
    // 9. Pipeline parsing test
    pipeline_raw := "*3\r\n$3\r\nSET\r\n$4\r\npip1\r\n$4\r\nval1\r\n*2\r\n$3\r\nGET\r\n$4\r\npip1\r\n"
    pipe_out := server_process_buffer(store, pipeline_raw)
    assert(pipe_out == "+OK\r\n$4\r\nval1\r\n")
    println("-> 9. Pipelining wire parser passed!")
    
    println("=================================================================")
    println("         ALL CLUSTER-REDIS TESTS PASSED WITH 100% SUCCESS!       ")
    println("=================================================================")
    return 0
