// ==============================================================================
// Cluster-Redis: Core Command Dispatcher & Server Protocol Processor
// 100% Pure Cluster Language - Zero Dependencies
// ==============================================================================

import "src/resp.cl"
import "src/store.cl"

fn str_to_upper(s: string) -> string:
    let mut out: string = ""
    let mut i: int = 0
    while i < len(s):
        c := char_at(s, i)
        if c == "a": out = out + "A"
        elif c == "b": out = out + "B"
        elif c == "c": out = out + "C"
        elif c == "d": out = out + "D"
        elif c == "e": out = out + "E"
        elif c == "f": out = out + "F"
        elif c == "g": out = out + "G"
        elif c == "h": out = out + "H"
        elif c == "i": out = out + "I"
        elif c == "j": out = out + "J"
        elif c == "k": out = out + "K"
        elif c == "l": out = out + "L"
        elif c == "m": out = out + "M"
        elif c == "n": out = out + "N"
        elif c == "o": out = out + "O"
        elif c == "p": out = out + "P"
        elif c == "q": out = out + "Q"
        elif c == "r": out = out + "R"
        elif c == "s": out = out + "S"
        elif c == "t": out = out + "T"
        elif c == "u": out = out + "U"
        elif c == "v": out = out + "V"
        elif c == "w": out = out + "W"
        elif c == "x": out = out + "X"
        elif c == "y": out = out + "Y"
        elif c == "z": out = out + "Z"
        else: out = out + c
        i = i + 1
    return out

fn server_info(s: RedisStore) -> string:
    let mut info: string = "# Server\r\n"
    info = info + "redis_version:1.0.0-cluster\r\n"
    info = info + "redis_mode:standalone\r\n"
    info = info + "os:Linux (pure-cluster-native)\r\n"
    info = info + "arch_bits:64\r\n"
    info = info + "tcp_port:6379\r\n"
    info = info + "\r\n# Clients\r\n"
    info = info + "connected_clients:1\r\n"
    info = info + "\r\n# Stats\r\n"
    info = info + "total_commands_processed:" + str(s.total_commands) + "\r\n"
    info = info + "\r\n# Keyspace\r\n"
    info = info + "db0:keys=" + str(len(s.entries)) + ",expires=0,avg_ttl=0\r\n"
    return resp_encode_bulk(info)

fn server_execute_args(s: RedisStore, args: vector[string]) -> string:
    if len(args) == 0:
        return resp_encode_error("empty command")

    s.total_commands = s.total_commands + 1
    cmd := str_to_upper(args[0])

    // --- CONNECTION & DIAGNOSTICS ---
    if cmd == "PING":
        if len(args) > 1:
            return resp_encode_bulk(args[1])
        return resp_encode_simple("PONG")

    if cmd == "ECHO":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'echo' command")
        return resp_encode_bulk(args[1])

    if cmd == "QUIT":
        return resp_encode_simple("OK")

    if cmd == "SELECT":
        return resp_encode_simple("OK")

    if cmd == "CLIENT":
        return resp_encode_simple("OK")

    if cmd == "COMMAND":
        return resp_encode_empty_array()

    if cmd == "CONFIG":
        if len(args) >= 3:
            sub := str_to_upper(args[1])
            if sub == "GET":
                let resp: vector[string] = [args[2], ""]
                return resp_encode_array(resp)
        return resp_encode_simple("OK")

    if cmd == "INFO":
        return server_info(s)

    if cmd == "DBSIZE":
        return resp_encode_int(len(s.entries))

    if cmd == "FLUSHALL" or cmd == "FLUSHDB":
        store_flushall(s)
        return resp_encode_simple("OK")

    // --- STRINGS ---
    if cmd == "SET":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'set' command")
        key := args[1]
        val := args[2]
        let mut ttl: int = -1
        if len(args) >= 5:
            opt := str_to_upper(args[3])
            if opt == "EX":
                ttl = to_int(args[4])
        store_set(s, key, val, ttl)
        return resp_encode_simple("OK")

    if cmd == "GET":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'get' command")
        key := args[1]
        if store_exists(s, key) == 0:
            return resp_encode_null()
        return resp_encode_bulk(store_get(s, key))

    if cmd == "MSET":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'mset' command")
        let mut i: int = 1
        while i + 1 < len(args):
            store_set(s, args[i], args[i + 1], -1)
            i = i + 2
        return resp_encode_simple("OK")

    if cmd == "MGET":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'mget' command")
        let vals: vector[string] = []
        let mut i: int = 1
        while i < len(args):
            k := args[i]
            if store_exists(s, k) == 1:
                vals.push(store_get(s, k))
            else:
                vals.push("")
            i = i + 1
        return resp_encode_array(vals)

    if cmd == "INCR":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'incr' command")
        return resp_encode_int(store_incr(s, args[1], 1))

    if cmd == "DECR":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'decr' command")
        return resp_encode_int(store_incr(s, args[1], -1))

    if cmd == "INCRBY":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'incrby' command")
        return resp_encode_int(store_incr(s, args[1], to_int(args[2])))

    if cmd == "DECRBY":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'decrby' command")
        return resp_encode_int(store_incr(s, args[1], 0 - to_int(args[2])))

    if cmd == "STRLEN":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'strlen' command")
        return resp_encode_int(store_strlen(s, args[1]))

    if cmd == "APPEND":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'append' command")
        return resp_encode_int(store_append(s, args[1], args[2]))

    // --- KEYS ---
    if cmd == "DEL":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'del' command")
        let mut count: int = 0
        let mut i: int = 1
        while i < len(args):
            count = count + store_del(s, args[i])
            i = i + 1
        return resp_encode_int(count)

    if cmd == "EXISTS":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'exists' command")
        let mut count: int = 0
        let mut i: int = 1
        while i < len(args):
            count = count + store_exists(s, args[i])
            i = i + 1
        return resp_encode_int(count)

    if cmd == "TYPE":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'type' command")
        return resp_encode_simple(store_type(s, args[1]))

    if cmd == "KEYS":
        pattern := "*"
        if len(args) > 1:
            pattern = args[1]
        return resp_encode_array(store_keys(s, pattern))

    if cmd == "EXPIRE":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'expire' command")
        return resp_encode_int(store_expire(s, args[1], to_int(args[2])))

    if cmd == "TTL":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'ttl' command")
        return resp_encode_int(store_ttl(s, args[1]))

    if cmd == "PERSIST":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'persist' command")
        return resp_encode_int(store_persist(s, args[1]))

    // --- LISTS ---
    if cmd == "LPUSH":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'lpush' command")
        let mut last_len: int = 0
        let mut i: int = 2
        while i < len(args):
            last_len = store_lpush(s, args[1], args[i])
            i = i + 1
        return resp_encode_int(last_len)

    if cmd == "RPUSH":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'rpush' command")
        let mut last_len: int = 0
        let mut i: int = 2
        while i < len(args):
            last_len = store_rpush(s, args[1], args[i])
            i = i + 1
        return resp_encode_int(last_len)

    if cmd == "LPOP":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'lpop' command")
        v := store_lpop(s, args[1])
        if len(v) == 0:
            return resp_encode_null()
        return resp_encode_bulk(v)

    if cmd == "RPOP":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'rpop' command")
        v := store_rpop(s, args[1])
        if len(v) == 0:
            return resp_encode_null()
        return resp_encode_bulk(v)

    if cmd == "LLEN":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'llen' command")
        return resp_encode_int(store_llen(s, args[1]))

    if cmd == "LRANGE":
        if len(args) < 4:
            return resp_encode_error("wrong number of arguments for 'lrange' command")
        st := to_int(args[2])
        sp := to_int(args[3])
        return resp_encode_array(store_lrange(s, args[1], st, sp))

    if cmd == "LINDEX":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'lindex' command")
        val := store_lindex(s, args[1], to_int(args[2]))
        if len(val) == 0:
            return resp_encode_null()
        return resp_encode_bulk(val)

    // --- HASHES ---
    if cmd == "HSET":
        if len(args) < 4:
            return resp_encode_error("wrong number of arguments for 'hset' command")
        let mut added: int = 0
        let mut i: int = 2
        while i + 1 < len(args):
            added = added + store_hset(s, args[1], args[i], args[i + 1])
            i = i + 2
        return resp_encode_int(added)

    if cmd == "HMSET":
        if len(args) < 4:
            return resp_encode_error("wrong number of arguments for 'hmset' command")
        let mut i: int = 2
        while i + 1 < len(args):
            store_hset(s, args[1], args[i], args[i + 1])
            i = i + 2
        return resp_encode_simple("OK")

    if cmd == "HGET":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'hget' command")
        if store_hexists(s, args[1], args[2]) == 0:
            return resp_encode_null()
        return resp_encode_bulk(store_hget(s, args[1], args[2]))

    if cmd == "HMGET":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'hmget' command")
        let vals: vector[string] = []
        let mut i: int = 2
        while i < len(args):
            fld := args[i]
            if store_hexists(s, args[1], fld) == 1:
                vals.push(store_hget(s, args[1], fld))
            else:
                vals.push("")
            i = i + 1
        return resp_encode_array(vals)

    if cmd == "HDEL":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'hdel' command")
        let mut count: int = 0
        let mut i: int = 2
        while i < len(args):
            count = count + store_hdel(s, args[1], args[i])
            i = i + 1
        return resp_encode_int(count)

    if cmd == "HEXISTS":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'hexists' command")
        return resp_encode_int(store_hexists(s, args[1], args[2]))

    if cmd == "HLEN":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'hlen' command")
        return resp_encode_int(store_hlen(s, args[1]))

    if cmd == "HGETALL":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'hgetall' command")
        return resp_encode_array(store_hgetall(s, args[1]))

    if cmd == "HKEYS":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'hkeys' command")
        return resp_encode_array(store_hkeys(s, args[1]))

    if cmd == "HVALS":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'hvals' command")
        return resp_encode_array(store_hvals(s, args[1]))

    // --- SETS ---
    if cmd == "SADD":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'sadd' command")
        let mut added: int = 0
        let mut i: int = 2
        while i < len(args):
            added = added + store_sadd(s, args[1], args[i])
            i = i + 1
        return resp_encode_int(added)

    if cmd == "SREM":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'srem' command")
        let mut count: int = 0
        let mut i: int = 2
        while i < len(args):
            count = count + store_srem(s, args[1], args[i])
            i = i + 1
        return resp_encode_int(count)

    if cmd == "SMEMBERS":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'smembers' command")
        return resp_encode_array(store_smembers(s, args[1]))

    if cmd == "SISMEMBER":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'sismember' command")
        return resp_encode_int(store_sismember(s, args[1], args[2]))

    if cmd == "SCARD":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'scard' command")
        return resp_encode_int(store_scard(s, args[1]))

    // --- PERSISTENCE (.clb) ---
    if cmd == "SAVE":
        path := "dump.clb"
        if len(args) > 1:
            path = args[1]
        ok := store_save_rdb(s, path)
        if ok == 1:
            return resp_encode_simple("OK")
        return resp_encode_error("failed to save .clb snapshot")

    if cmd == "LOAD":
        path := "dump.clb"
        if len(args) > 1:
            path = args[1]
        cnt := store_load_rdb(s, path)
        return resp_encode_int(cnt)

    return resp_encode_error("unknown command '" + args[0] + "'")

// Processes all (potentially pipelined) commands in raw request buffer
fn server_process_buffer(s: RedisStore, raw: string) -> string:
    cursor := cursor_new(0)
    let mut out: string = ""
    while cursor.pos < len(raw):
        old_pos := cursor.pos
        args := resp_parse_command_at(raw, cursor)
        if len(args) == 0:
            break
        res := server_execute_args(s, args)
        out = out + res
        if cursor.pos <= old_pos:
            cursor.pos = cursor.pos + 1
    return out

// Single request entry point
fn server_execute(s: RedisStore, raw_req: string) -> string:
    return server_process_buffer(s, raw_req)
