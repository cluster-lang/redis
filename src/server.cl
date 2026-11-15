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
        if c == "a":
            out = out + "A"
        elif c == "b":
            out = out + "B"
        elif c == "c":
            out = out + "C"
        elif c == "d":
            out = out + "D"
        elif c == "e":
            out = out + "E"
        elif c == "f":
            out = out + "F"
        elif c == "g":
            out = out + "G"
        elif c == "h":
            out = out + "H"
        elif c == "i":
            out = out + "I"
        elif c == "j":
            out = out + "J"
        elif c == "k":
            out = out + "K"
        elif c == "l":
            out = out + "L"
        elif c == "m":
            out = out + "M"
        elif c == "n":
            out = out + "N"
        elif c == "o":
            out = out + "O"
        elif c == "p":
            out = out + "P"
        elif c == "q":
            out = out + "Q"
        elif c == "r":
            out = out + "R"
        elif c == "s":
            out = out + "S"
        elif c == "t":
            out = out + "T"
        elif c == "u":
            out = out + "U"
        elif c == "v":
            out = out + "V"
        elif c == "w":
            out = out + "W"
        elif c == "x":
            out = out + "X"
        elif c == "y":
            out = out + "Y"
        elif c == "z":
            out = out + "Z"
        else:
            out = out + c
        i = i + 1
    return out

fn server_execute(s: RedisStore, raw_req: string) -> string:
    args := resp_parse_command(raw_req)
    if len(args) == 0:
        return resp_encode_error("empty command")

    cmd := str_to_upper(args[0])

    // PING [message]
    if cmd == "PING":
        if len(args) > 1:
            return resp_encode_bulk(args[1])
        return resp_encode_simple("PONG")

    // ECHO <message>
    if cmd == "ECHO":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'echo' command")
        return resp_encode_bulk(args[1])

    // SET <key> <val> [EX seconds]
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

    // GET <key>
    if cmd == "GET":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'get' command")
        key := args[1]
        if store_exists(s, key) == 0:
            return resp_encode_null()
        val := store_get(s, key)
        return resp_encode_bulk(val)

    // DEL <key> [key ...]
    if cmd == "DEL":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'del' command")
        let mut count: int = 0
        let mut i: int = 1
        while i < len(args):
            count = count + store_del(s, args[i])
            i = i + 1
        return resp_encode_int(count)

    // EXISTS <key>
    if cmd == "EXISTS":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'exists' command")
        let mut count: int = 0
        let mut i: int = 1
        while i < len(args):
            count = count + store_exists(s, args[i])
            i = i + 1
        return resp_encode_int(count)

    // INCR <key>
    if cmd == "INCR":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'incr' command")
        res := store_incr(s, args[1], 1)
        return resp_encode_int(res)

    // DECR <key>
    if cmd == "DECR":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'decr' command")
        res := store_incr(s, args[1], -1)
        return resp_encode_int(res)

    // INCRBY <key> <val>
    if cmd == "INCRBY":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'incrby' command")
        by_v := to_int(args[2])
        res := store_incr(s, args[1], by_v)
        return resp_encode_int(res)

    // LPUSH <key> <val>
    if cmd == "LPUSH":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'lpush' command")
        res := store_lpush(s, args[1], args[2])
        return resp_encode_int(res)

    // RPUSH <key> <val>
    if cmd == "RPUSH":
        if len(args) < 3:
            return resp_encode_error("wrong number of arguments for 'rpush' command")
        res := store_rpush(s, args[1], args[2])
        return resp_encode_int(res)

    // LPOP <key>
    if cmd == "LPOP":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'lpop' command")
        v := store_lpop(s, args[1])
        if len(v) == 0:
            return resp_encode_null()
        return resp_encode_bulk(v)

    // RPOP <key>
    if cmd == "RPOP":
        if len(args) < 2:
            return resp_encode_error("wrong number of arguments for 'rpop' command")
        v := store_rpop(s, args[1])
        if len(v) == 0:
            return resp_encode_null()
        return resp_encode_bulk(v)

    // LRANGE <key> <start> <stop>
    if cmd == "LRANGE":
        if len(args) < 4:
            return resp_encode_error("wrong number of arguments for 'lrange' command")
        st := to_int(args[2])
        sp := to_int(args[3])
        list_res := store_lrange(s, args[1], st, sp)
        return resp_encode_array(list_res)

    // KEYS <pattern>
    if cmd == "KEYS":
        pattern := "*"
        if len(args) > 1:
            pattern = args[1]
        k_list := store_keys(s, pattern)
        return resp_encode_array(k_list)

    // DBSIZE
    if cmd == "DBSIZE":
        return resp_encode_int(len(s.entries))

    // FLUSHALL
    if cmd == "FLUSHALL":
        store_flushall(s)
        return resp_encode_simple("OK")

    // SAVE <filepath>
    if cmd == "SAVE":
        path := "dump.clb"
        if len(args) > 1:
            path = args[1]
        ok := store_save_rdb(s, path)
        if ok == 1:
            return resp_encode_simple("OK")
        return resp_encode_error("failed to save RDB snapshot")

    // LOAD <filepath>
    if cmd == "LOAD":
        path := "dump.clb"
        if len(args) > 1:
            path = args[1]
        cnt := store_load_rdb(s, path)
        return resp_encode_int(cnt)

    return resp_encode_error("unknown command '" + args[0] + "'")
