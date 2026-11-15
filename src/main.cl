// ==============================================================================
// Cluster-Redis: Standalone Fast In-Memory Key-Value Server Daemon
// 100% Pure Cluster Language - Zero Foreign Dependencies
// ==============================================================================

import "std/net.cl"
import "src/server.cl"

fn handle_client(store: RedisStore, client_fd: int) -> int:
    session := session_new(client_fd)
    while 1:
        req := tcp_read(client_fd, 65536)
        if len(req) == 0:
            break
        resp := server_process_buffer_session(store, session, req)
        if len(resp) > 0:
            tcp_write(client_fd, resp)
    store_unsubscribe_all(store, client_fd)
    tcp_close(client_fd)
    return 0

fn run_self_test(store: RedisStore) -> int:
    println("Running in-process engine verification...")
    r1 := server_execute(store, "SET test:key cluster-redis-ok")
    assert(r1 == "+OK\r\n")
    r2 := server_execute(store, "GET test:key")
    assert(r2 == "$16\r\ncluster-redis-ok\r\n")
    println("✓ In-memory engine and protocol verified!")
    return 0

fn main() -> int:
    let mut port: int = 6379
    let mut mode_test: int = 0

    if len(sys_args) > 0:
        arg0 := sys_args[0]
        if arg0 == "--test" or arg0 == "test":
            mode_test = 1
        elif len(arg0) > 0 and char_at(arg0, 0) != "-":
            p := to_int(arg0)
            if p > 0:
                port = p

    store := store_new()

    if mode_test == 1:
        return run_self_test(store)

    println("                _._                                                  ")
    println("           _.-``__ ''-._                                             ")
    println("      _.-``    `.  `_.  ''-._           Cluster-Redis 1.0.0 (64 bit) ")
    println("  .-`` .-```.```\\/    _.,_ ''-._        Running in standalone mode   ")
    println(" (    '      ,       .-`  | `,    )     Port: " + str(port) + "      ")
    println(" |`-._`-...-` __...-.``-._|'` _.-'|     Zero C-ABI Dependencies      ")
    println(" |    `-._   `._    /     _.-'    |     100% Pure Cluster Language   ")
    println("  `-._    `-._  `-./  _.-'    _.-'                                   ")
    println(" |`-._`-._    `-.__.-'    _.-'_.-'|     Ready to accept connections  ")
    println(" |    `-._`-._        _.-'_.-'    |                                  ")
    println("  `-._    `-._`-.__.-'_.-'    _.-'                                   ")
    println("      `-._    `-.__.-'    _.-'                                       ")
    println("          `-._        _.-'                                           ")
    println("              `-.__.-'                                               ")
    println("")

    server_fd := tcp_listen(port)
    if server_fd < 0:
        println("[!] Fatal: Failed to bind to TCP port " + str(port))
        return 1

    println("[*] Cluster-Redis server listening on 0.0.0.0:" + str(port))
    println("[*] Ready to accept Redis clients, redis-cli, and redis-benchmark.")

    while 1:
        client_fd := tcp_accept(server_fd)
        if client_fd >= 0:
            spawn handle_client(store, client_fd)

    tcp_close(server_fd)
    return 0
