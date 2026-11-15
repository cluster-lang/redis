// ==============================================================================
// Cluster-Redis: Standalone Fast In-Memory Key-Value Server
// ==============================================================================

import "src/server.cl"

fn main() -> int:
    println("=================================================================")
    println("          Cluster-Redis v1.0.0 (Pure Cluster Language)           ")
    println("   Zero C-ABI Dependencies | RESP Protocol | .clb Persistence    ")
    println("=================================================================")
    
    store := store_new()
    
    // Demonstrate basic commands
    println("1. Testing Basic String Operations:")
    r1 := server_execute(store, "SET server:name cluster-redis-node-1")
    println("   SET server:name -> " + r1)
    
    r2 := server_execute(store, "GET server:name")
    println("   GET server:name -> " + r2)
    
    // Demonstrate Atomic Counter
    println("2. Testing Atomic Counters:")
    server_execute(store, "SET page:views 100")
    r3 := server_execute(store, "INCR page:views")
    println("   INCR page:views -> " + r3)
    r4 := server_execute(store, "INCRBY page:views 25")
    println("   INCRBY page:views 25 -> " + r4)
    
    // Demonstrate Lists (Queue)
    println("3. Testing List / Queue Operations:")
    server_execute(store, "RPUSH task_queue job_alpha")
    server_execute(store, "RPUSH task_queue job_beta")
    server_execute(store, "RPUSH task_queue job_gamma")
    r_list := server_execute(store, "LRANGE task_queue 0 -1")
    println("   LRANGE task_queue 0 -1 -> \n" + r_list)
    
    r_pop := server_execute(store, "LPOP task_queue")
    println("   LPOP task_queue -> " + r_pop)
    
    // Demonstrate Snapshot Persistence via .clb
    snap_path := "/tmp/cluster_redis_dump.clb"
    println("4. Saving snapshot to .clb binary bundle: " + snap_path)
    server_execute(store, "SAVE " + snap_path)
    
    // Create new store and restore
    new_store := store_new()
    println("5. Starting new instance and restoring from snapshot...")
    server_execute(new_store, "LOAD " + snap_path)
    
    r_restored := server_execute(new_store, "GET server:name")
    println("   GET server:name on new instance -> " + r_restored)
    assert(store_get(new_store, "server:name") == "cluster-redis-node-1")
    assert(store_get(new_store, "page:views") == "126")
    
    println("=================================================================")
    println("         CLUSTER-REDIS SERVER EXECUTED WITH 100% SUCCESS!        ")
    println("=================================================================")
    return 0
