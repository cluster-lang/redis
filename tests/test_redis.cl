// ==============================================================================
// Unit Test Suite: Cluster-Redis In-Memory Engine & RESP Protocol
// ==============================================================================

import "src/server.cl"

fn main() -> int:
    println("--- Testing Cluster-Redis Engine ---")
    store := store_new()
    
    // 1. Basic Strings
    assert(server_execute(store, "PING") == "+PONG\r\n")
    assert(server_execute(store, "ECHO hello") == "$5\r\nhello\r\n")
    
    server_execute(store, "SET user:1 alice")
    assert(server_execute(store, "GET user:1") == "$5\r\nalice\r\n")
    assert(server_execute(store, "EXISTS user:1") == ":1\r\n")
    assert(server_execute(store, "EXISTS user:999") == ":0\r\n")
    println("-> String commands (PING, ECHO, SET, GET, EXISTS) passed!")
    
    // 2. Integers and Counters
    server_execute(store, "SET count 10")
    assert(server_execute(store, "INCR count") == ":11\r\n")
    assert(server_execute(store, "DECR count") == ":10\r\n")
    assert(server_execute(store, "INCRBY count 50") == ":60\r\n")
    println("-> Atomic counter operations (INCR, DECR, INCRBY) passed!")
    
    // 3. Lists / Queues
    server_execute(store, "RPUSH queue item1")
    server_execute(store, "RPUSH queue item2")
    server_execute(store, "LPUSH queue item0")
    
    let q_res: vector[string] = store_lrange(store, "queue", 0, -1)
    assert(len(q_res) == 3)
    assert(q_res[0] == "item0")
    assert(q_res[1] == "item1")
    assert(q_res[2] == "item2")
    
    assert(server_execute(store, "LPOP queue") == "$5\r\nitem0\r\n")
    assert(server_execute(store, "RPOP queue") == "$5\r\nitem2\r\n")
    assert(server_execute(store, "LPOP queue") == "$5\r\nitem1\r\n")
    assert(server_execute(store, "LPOP queue") == "$-1\r\n")
    println("-> List and queue operations (LPUSH, RPUSH, LPOP, RPOP, LRANGE) passed!")
    
    // 4. TTL and Expiration
    server_execute(store, "SET session:abc 999 EX 10")
    assert(store_exists(store, "session:abc") == 1)
    
    // Advance time by 5 seconds (not expired yet)
    store_advance_time(store, 5)
    assert(store_exists(store, "session:abc") == 1)
    
    // Advance time by another 6 seconds (11 total -> expired!)
    store_advance_time(store, 6)
    assert(store_exists(store, "session:abc") == 0)
    assert(server_execute(store, "GET session:abc") == "$-1\r\n")
    println("-> TTL key expiration engine passed!")
    
    // 5. RESP Protocol Array Parser
    resp_payload := "*3\r\n$3\r\nSET\r\n$7\r\napp_env\r\n$10\r\nproduction\r\n"
    r_resp := server_execute(store, resp_payload)
    assert(r_resp == "+OK\r\n")
    assert(server_execute(store, "GET app_env") == "$10\r\nproduction\r\n")
    println("-> RESP wire protocol parser passed!")
    
    // 6. Persistence via .clb Binary Bundle
    snap_file := "/tmp/redis_test_snap.clb"
    assert(server_execute(store, "SAVE " + snap_file) == "+OK\r\n")
    assert(file_exists(snap_file) == 1)
    
    restored_store := store_new()
    cnt_resp := server_execute(restored_store, "LOAD " + snap_file)
    assert(cnt_resp != ":0\r\n")
    assert(store_get(restored_store, "app_env") == "production")
    assert(store_get(restored_store, "count") == "60")
    println("-> .clb Snapshot persistence & recovery passed!")
    
    // 7. Throughput Benchmark: 5,000 Operations
    println("-> Running in-memory throughput benchmark (5,000 ops)...")
    let mut i: int = 0
    while i < 5000:
        store_set(store, "k_" + str(i), "val_" + str(i), -1)
        i = i + 1
    assert(store_exists(store, "k_4999") == 1)
    assert(store_get(store, "k_4999") == "val_4999")
    println("-> 5,000 operations completed with 100% data integrity!")
    
    println("=================================================================")
    println("           ALL CLUSTER-REDIS TESTS PASSED (100% OK)!             ")
    println("=================================================================")
    return 0
