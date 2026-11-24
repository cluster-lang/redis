// ==============================================================================
// Module:      In-Memory Storage Engine (store.cl)
// Project:     Cluster Redis (High-Performance In-Memory Key-Value Store)
// Author:      zk-mohammad <ZaidKhan-alamgir@proton.me>
// License:     Cluster Community & Fair-Source License (CCFL v2.0)
// Description: Multi-model in-memory database store supporting Strings, Lists, Hashes,
//              Sets, Sorted Sets (ZSets), Streams (XADD/XLEN/XRANGE/XDEL), Bitmaps,
//              HyperLogLog (PFADD/PFCOUNT), Geospatial, TTL expiration, Pub/Sub channels,
//              AOF logging, and .clb binary snapshot persistence.
// ==============================================================================

import "src/dict.cl"
import "src/quicklist.cl"
import "std/clb.cl"
import "std/net.cl"
import "std/sync.cl"

model RedisEntry:
    key: string
    val: string
    val_type: string
    qlist: QuickList
    list_vals: vector[string]
    hash_keys: vector[string]
    hash_vals: vector[string]
    set_members: vector[string]
    zset_members: vector[string]
    zset_scores: vector[string]
    stream_ids: vector[string]
    stream_fields: vector[string]
    hll_registers: vector[int]
    expire_at: int

model PubSubSubscription:
    channel: string
    client_fd: int

model RedisStore:
    entries: vector[RedisEntry]
    key_dict: Dict
    lock: int
    subs: vector[PubSubSubscription]
    now_tick: int
    total_commands: int
    aof_enabled: int
    aof_filepath: string
    script_hashes: vector[string]
    script_sources: vector[string]

fn entry_new_string(key: string, val: string, exp: int) -> RedisEntry:
    ql := quicklist_new()
    let el: vector[string] = []
    let hk: vector[string] = []
    let hv: vector[string] = []
    let sm: vector[string] = []
    let zm: vector[string] = []
    let zs: vector[string] = []
    let st_id: vector[string] = []
    let st_f: vector[string] = []
    let hll: vector[int] = []
    return RedisEntry(key, val, "string", ql, el, hk, hv, sm, zm, zs, st_id, st_f, hll, exp)

fn entry_new_list(key: string, items: vector[string]) -> RedisEntry:
    ql := quicklist_new()
    let mut i: int = 0
    while i < len(items):
        quicklist_rpush(ql, items[i])
        i = i + 1
    let el: vector[string] = []
    let hk: vector[string] = []
    let hv: vector[string] = []
    let sm: vector[string] = []
    let zm: vector[string] = []
    let zs: vector[string] = []
    let st_id: vector[string] = []
    let st_f: vector[string] = []
    let hll: vector[int] = []
    return RedisEntry(key, "", "list", ql, el, hk, hv, sm, zm, zs, st_id, st_f, hll, -1)

fn entry_new_hash(key: string) -> RedisEntry:
    ql := quicklist_new()
    let el: vector[string] = []
    let hk: vector[string] = []
    let hv: vector[string] = []
    let sm: vector[string] = []
    let zm: vector[string] = []
    let zs: vector[string] = []
    let st_id: vector[string] = []
    let st_f: vector[string] = []
    let hll: vector[int] = []
    return RedisEntry(key, "", "hash", ql, el, hk, hv, sm, zm, zs, st_id, st_f, hll, -1)

fn entry_new_set(key: string) -> RedisEntry:
    ql := quicklist_new()
    let el: vector[string] = []
    let hk: vector[string] = []
    let hv: vector[string] = []
    let sm: vector[string] = []
    let zm: vector[string] = []
    let zs: vector[string] = []
    let st_id: vector[string] = []
    let st_f: vector[string] = []
    let hll: vector[int] = []
    return RedisEntry(key, "", "set", ql, el, hk, hv, sm, zm, zs, st_id, st_f, hll, -1)

fn entry_new_zset(key: string) -> RedisEntry:
    ql := quicklist_new()
    let el: vector[string] = []
    let hk: vector[string] = []
    let hv: vector[string] = []
    let sm: vector[string] = []
    let zm: vector[string] = []
    let zs: vector[string] = []
    let st_id: vector[string] = []
    let st_f: vector[string] = []
    let hll: vector[int] = []
    return RedisEntry(key, "", "zset", ql, el, hk, hv, sm, zm, zs, st_id, st_f, hll, -1)

fn entry_new_stream(key: string) -> RedisEntry:
    ql := quicklist_new()
    let el: vector[string] = []
    let hk: vector[string] = []
    let hv: vector[string] = []
    let sm: vector[string] = []
    let zm: vector[string] = []
    let zs: vector[string] = []
    let st_id: vector[string] = []
    let st_f: vector[string] = []
    let hll: vector[int] = []
    return RedisEntry(key, "", "stream", ql, el, hk, hv, sm, zm, zs, st_id, st_f, hll, -1)

fn store_new() -> RedisStore:
    let list: vector[RedisEntry] = []
    let s_list: vector[PubSubSubscription] = []
    let sh_h: vector[string] = []
    let sh_s: vector[string] = []
    d := dict_new(65536)
    m := mutex_create()
    return RedisStore(list, d, m, s_list, 0, 0, 0, "", sh_h, sh_s)

fn store_insert_entry(s: RedisStore, entry: RedisEntry) -> int:
    new_idx := len(s.entries)
    s.entries.push(entry)
    dict_set(s.key_dict, entry.key, new_idx)
    return new_idx

fn store_find_idx(s: RedisStore, key: string) -> int:
    idx := dict_get(s.key_dict, key)
    if idx < 0:
        return -1
    if idx >= len(s.entries):
        return -1
    let e: RedisEntry = s.entries[idx]
    if e.key != key:
        return -1
    if e.expire_at > 0:
        if s.now_tick >= e.expire_at:
            return -2
    return idx

// ------------------------------------------------------------------------------
// String Operations (SET, GET, EXISTS, DEL, INCR, STRLEN, APPEND, SETNX, GETSET, GETRANGE, SETRANGE)
// ------------------------------------------------------------------------------

fn store_set(s: RedisStore, key: string, val: string, ttl_seconds: int) -> void:
    let mut exp: int = -1
    if ttl_seconds > 0:
        exp = s.now_tick + ttl_seconds

    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        e.val = val
        e.val_type = "string"
        e.expire_at = exp
        return

    entry := entry_new_string(key, val, exp)
    store_insert_entry(s, entry)

fn store_get(s: RedisStore, key: string) -> string:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        return e.val
    return ""

fn store_exists(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        return 1
    return 0

fn store_del(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0

    dict_del(s.key_dict, key)
    last_idx := len(s.entries) - 1
    if idx != last_idx:
        let last_e: RedisEntry = s.entries[last_idx]
        s.entries[idx] = last_e
        dict_set(s.key_dict, last_e.key, idx)
    s.entries.pop()
    return 1

fn store_setnx(s: RedisStore, key: string, val: string) -> int:
    if store_exists(s, key) == 1:
        return 0
    store_set(s, key, val, -1)
    return 1

fn store_getset(s: RedisStore, key: string, val: string) -> string:
    old := store_get(s, key)
    store_set(s, key, val, -1)
    return old

fn store_strlen(s: RedisStore, key: string) -> int:
    val := store_get(s, key)
    return len(val)

fn store_append(s: RedisStore, key: string, append_val: string) -> int:
    val := store_get(s, key)
    new_val := val + append_val
    store_set(s, key, new_val, -1)
    return len(new_val)

fn store_getrange(s: RedisStore, key: string, start: int, end: int) -> string:
    val := store_get(s, key)
    sz := len(val)
    if sz == 0:
        return ""
    let mut s_idx: int = start
    let mut e_idx: int = end
    if s_idx < 0:
        s_idx = sz + s_idx
    if e_idx < 0:
        e_idx = sz + e_idx
    if s_idx < 0:
        s_idx = 0
    if e_idx >= sz:
        e_idx = sz - 1
    if s_idx > e_idx:
        return ""
    return substring(val, s_idx, e_idx - s_idx + 1)

fn store_setrange(s: RedisStore, key: string, offset: int, new_val: string) -> int:
    let mut cur: string = store_get(s, key)
    while len(cur) < offset:
        cur = cur + " "
    let mut out: string = ""
    if offset > 0:
        out = substring(cur, 0, offset)
    out = out + new_val
    tail_start := offset + len(new_val)
    if tail_start < len(cur):
        out = out + substring(cur, tail_start, len(cur) - tail_start)
    store_set(s, key, out, -1)
    return len(out)

fn store_incr(s: RedisStore, key: string, delta: int) -> int:
    cur := store_get(s, key)
    let mut num: int = 0
    if len(cur) > 0:
        num = to_int(cur)
    num = num + delta
    store_set(s, key, str(num), -1)
    return num

fn store_type(s: RedisStore, key: string) -> string:
    idx := store_find_idx(s, key)
    if idx < 0:
        return "none"
    let e: RedisEntry = s.entries[idx]
    return e.val_type

// ------------------------------------------------------------------------------
// Bitmaps / Bit Operations (SETBIT, GETBIT, BITCOUNT, BITPOS)
// ------------------------------------------------------------------------------

fn store_setbit(s: RedisStore, key: string, offset: int, val: int) -> int:
    byte_idx := offset / 8
    bit_idx := 7 - (offset % 8)
    let mut cur: string = store_get(s, key)
    
    while len(cur) <= byte_idx:
        cur = cur + chr(0)

    let b: int = byte_at(cur, byte_idx)
    let old_bit: int = (b >> bit_idx) & 1

    let mut new_b: int = 0
    if val == 1:
        new_b = b | (1 << bit_idx)
    else:
        new_b = b & (255 - (1 << bit_idx))

    let mut updated: string = ""
    if byte_idx > 0:
        updated = updated + substring(cur, 0, byte_idx)
    updated = updated + chr(new_b)
    if byte_idx + 1 < len(cur):
        updated = updated + substring(cur, byte_idx + 1, len(cur) - byte_idx - 1)

    store_set(s, key, updated, -1)
    return old_bit

fn store_getbit(s: RedisStore, key: string, offset: int) -> int:
    byte_idx := offset / 8
    bit_idx := 7 - (offset % 8)
    cur := store_get(s, key)
    if byte_idx >= len(cur):
        return 0
    b := byte_at(cur, byte_idx)
    return (b >> bit_idx) & 1

fn store_bitcount(s: RedisStore, key: string, start: int, end: int) -> int:
    cur := store_get(s, key)
    sz := len(cur)
    if sz == 0:
        return 0
    let mut s_idx: int = start
    let mut e_idx: int = end
    if s_idx < 0:
        s_idx = sz + s_idx
    if e_idx < 0:
        e_idx = sz + e_idx
    if s_idx < 0:
        s_idx = 0
    if e_idx >= sz:
        e_idx = sz - 1
    if s_idx > e_idx:
        return 0

    let mut count: int = 0
    let mut i: int = s_idx
    while i <= e_idx:
        b := byte_at(cur, i)
        let mut bit_i: int = 0
        while bit_i < 8:
            if ((b >> bit_i) & 1) == 1:
                count = count + 1
            bit_i = bit_i + 1
        i = i + 1
    return count

// ------------------------------------------------------------------------------
// HyperLogLog (PFADD, PFCOUNT, PFMERGE)
// ------------------------------------------------------------------------------

fn hash_fnv1a(str_val: string) -> int:
    let mut h: int = 2166136261
    let mut i: int = 0
    while i < len(str_val):
        h = (h ^ byte_at(str_val, i)) * 16777619
        if h < 0:
            h = -h
        i = i + 1
    return h

fn store_pfadd(s: RedisStore, key: string, elements: vector[string]) -> int:
    idx := store_find_idx(s, key)
    let mut real_idx: int = idx
    if real_idx < 0:
        ql := quicklist_new()
        let el: vector[string] = []
        let hk: vector[string] = []
        let hv: vector[string] = []
        let sm: vector[string] = []
        let zm: vector[string] = []
        let zs: vector[string] = []
        let st_id: vector[string] = []
        let st_f: vector[string] = []
        let mut hll: vector[int] = []
        let mut reg_i: int = 0
        while reg_i < 16:
            hll.push(0)
            reg_i = reg_i + 1
        entry := RedisEntry(key, "", "hll", ql, el, hk, hv, sm, zm, zs, st_id, st_f, hll, -1)
        real_idx = store_insert_entry(s, entry)

    let e: RedisEntry = s.entries[real_idx]
    if len(e.hll_registers) < 16:
        let mut ri: int = len(e.hll_registers)
        while ri < 16:
            e.hll_registers.push(0)
            ri = ri + 1

    let mut altered: int = 0
    let mut elem_i: int = 0
    while elem_i < len(elements):
        elem := elements[elem_i]
        h := hash_fnv1a(elem)
        bucket := h % 16
        let mut val: int = h / 16
        let mut rank: int = 1
        while val > 0 and (val & 1) == 0 and rank < 32:
            rank = rank + 1
            val = val / 2
        if rank > e.hll_registers[bucket]:
            e.hll_registers[bucket] = rank
            altered = 1
        elem_i = elem_i + 1
    return altered

fn store_pfcount(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    if len(e.hll_registers) == 0:
        return 0
    let mut sum_reg: int = 0
    let mut non_zero: int = 0
    let mut i: int = 0
    while i < len(e.hll_registers):
        r := e.hll_registers[i]
        if r > 0:
            sum_reg = sum_reg + r
            non_zero = non_zero + 1
        i = i + 1
    if non_zero == 0:
        return 0
    let avg_rank: int = sum_reg / non_zero
    let mut est: int = 1
    let mut p: int = 0
    while p < avg_rank:
        est = est * 2
        p = p + 1
    est = (est * non_zero) / 10
    if est < non_zero:
        est = non_zero
    return est

// ------------------------------------------------------------------------------
// Redis Streams (XADD, XLEN, XRANGE, XDEL)
// ------------------------------------------------------------------------------

fn store_xadd(s: RedisStore, key: string, id: string, fields: vector[string]) -> string:
    idx := store_find_idx(s, key)
    let mut real_idx: int = idx
    if real_idx < 0:
        entry := entry_new_stream(key)
        real_idx = store_insert_entry(s, entry)

    let e: RedisEntry = s.entries[real_idx]
    let mut assigned_id: string = id
    if id == "*" or len(id) == 0:
        t_ms := s.now_tick * 1000 + len(e.stream_ids)
        assigned_id = str(t_ms) + "-0"

    e.stream_ids.push(assigned_id)
    let mut field_blob: string = ""
    let mut i: int = 0
    while i < len(fields):
        if i > 0:
            field_blob = field_blob + "\x1f"
        field_blob = field_blob + fields[i]
        i = i + 1
    e.stream_fields.push(field_blob)
    return assigned_id

fn store_xlen(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    let mut count: int = 0
    let mut i: int = 0
    while i < len(e.stream_ids):
        if e.stream_ids[i] != "__DELETED__":
            count = count + 1
        i = i + 1
    return count

fn store_xdel(s: RedisStore, key: string, id: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.stream_ids):
        if e.stream_ids[i] == id:
            e.stream_ids[i] = "__DELETED__"
            return 1
        i = i + 1
    return 0

// ------------------------------------------------------------------------------
// Geospatial Support (GEOADD, GEOPOS)
// ------------------------------------------------------------------------------

fn store_geoadd(s: RedisStore, key: string, lon_str: string, lat_str: string, member: string) -> int:
    lon := to_int(lon_str)
    lat := to_int(lat_str)
    score_val := (lat + 90) * 360 + (lon + 180)
    store_zadd(s, key, str(score_val), member)
    coord_key := key + ":__coords__"
    store_hset(s, coord_key, member, lon_str + "," + lat_str)
    return 1

fn store_geopos(s: RedisStore, key: string, member: string) -> string:
    coord_key := key + ":__coords__"
    return store_hget(s, coord_key, member)

// ------------------------------------------------------------------------------
// Lists (LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LRANGE)
// ------------------------------------------------------------------------------

fn store_lpush(s: RedisStore, key: string, val: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        quicklist_lpush(e.qlist, val)
        return quicklist_len(e.qlist)
    let v: vector[string] = [val]
    entry := entry_new_list(key, v)
    store_insert_entry(s, entry)
    return 1

fn store_rpush(s: RedisStore, key: string, val: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        quicklist_rpush(e.qlist, val)
        return quicklist_len(e.qlist)
    let v: vector[string] = [val]
    entry := entry_new_list(key, v)
    store_insert_entry(s, entry)
    return 1

fn store_lpop(s: RedisStore, key: string) -> string:
    idx := store_find_idx(s, key)
    if idx < 0:
        return ""
    let e: RedisEntry = s.entries[idx]
    return quicklist_lpop(e.qlist)

fn store_rpop(s: RedisStore, key: string) -> string:
    idx := store_find_idx(s, key)
    if idx < 0:
        return ""
    let e: RedisEntry = s.entries[idx]
    return quicklist_rpop(e.qlist)

fn store_llen(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    return quicklist_len(e.qlist)

fn store_lindex(s: RedisStore, key: string, target_idx: int) -> string:
    idx := store_find_idx(s, key)
    if idx < 0:
        return ""
    let e: RedisEntry = s.entries[idx]
    return quicklist_index(e.qlist, target_idx)

fn store_lrange(s: RedisStore, key: string, start: int, stop: int) -> vector[string]:
    idx := store_find_idx(s, key)
    let empty_res: vector[string] = []
    if idx < 0:
        return empty_res
    let e: RedisEntry = s.entries[idx]
    return quicklist_range(e.qlist, start, stop)

// ------------------------------------------------------------------------------
// Hashes (HSET, HGET, HDEL, HEXISTS, HLEN, HGETALL, HKEYS, HVALS)
// ------------------------------------------------------------------------------

fn store_hset(s: RedisStore, key: string, field: string, val: string) -> int:
    idx := store_find_idx(s, key)
    let mut real_idx: int = idx
    if real_idx < 0:
        entry := entry_new_hash(key)
        real_idx = store_insert_entry(s, entry)

    let e: RedisEntry = s.entries[real_idx]
    let mut i: int = 0
    while i < len(e.hash_keys):
        if e.hash_keys[i] == field:
            e.hash_vals[i] = val
            return 0
        i = i + 1

    e.hash_keys.push(field)
    e.hash_vals.push(val)
    return 1

fn store_hget(s: RedisStore, key: string, field: string) -> string:
    idx := store_find_idx(s, key)
    if idx < 0:
        return ""
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.hash_keys):
        if e.hash_keys[i] == field:
            return e.hash_vals[i]
        i = i + 1
    return ""

fn store_hdel(s: RedisStore, key: string, field: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.hash_keys):
        if e.hash_keys[i] == field:
            last_i := len(e.hash_keys) - 1
            if i != last_i:
                e.hash_keys[i] = e.hash_keys[last_i]
                e.hash_vals[i] = e.hash_vals[last_i]
            e.hash_keys.pop()
            e.hash_vals.pop()
            return 1
        i = i + 1
    return 0

fn store_hexists(s: RedisStore, key: string, field: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.hash_keys):
        if e.hash_keys[i] == field:
            return 1
        i = i + 1
    return 0

fn store_hlen(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    return len(e.hash_keys)

fn store_hgetall(s: RedisStore, key: string) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx < 0:
        return res
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.hash_keys):
        res.push(e.hash_keys[i])
        res.push(e.hash_vals[i])
        i = i + 1
    return res

fn store_hkeys(s: RedisStore, key: string) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx < 0:
        return res
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.hash_keys):
        res.push(e.hash_keys[i])
        i = i + 1
    return res

fn store_hvals(s: RedisStore, key: string) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx < 0:
        return res
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.hash_vals):
        res.push(e.hash_vals[i])
        i = i + 1
    return res

// ------------------------------------------------------------------------------
// Sets (SADD, SREM, SMEMBERS, SISMEMBER, SCARD)
// ------------------------------------------------------------------------------

fn store_sadd(s: RedisStore, key: string, member: string) -> int:
    idx := store_find_idx(s, key)
    let mut real_idx: int = idx
    if real_idx < 0:
        entry := entry_new_set(key)
        real_idx = store_insert_entry(s, entry)

    let e: RedisEntry = s.entries[real_idx]
    let mut i: int = 0
    while i < len(e.set_members):
        if e.set_members[i] == member:
            return 0
        i = i + 1

    e.set_members.push(member)
    return 1

fn store_srem(s: RedisStore, key: string, member: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.set_members):
        if e.set_members[i] == member:
            last_i := len(e.set_members) - 1
            if i != last_i:
                e.set_members[i] = e.set_members[last_i]
            e.set_members.pop()
            return 1
        i = i + 1
    return 0

fn store_smembers(s: RedisStore, key: string) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx < 0:
        return res
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.set_members):
        res.push(e.set_members[i])
        i = i + 1
    return res

fn store_sismember(s: RedisStore, key: string, member: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.set_members):
        if e.set_members[i] == member:
            return 1
        i = i + 1
    return 0

fn store_scard(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    return len(e.set_members)

// ------------------------------------------------------------------------------
// Sorted Sets (ZADD, ZRANGE, ZREVRANGE, ZSCORE, ZCARD, ZREM, ZRANK, ZINCRBY)
// ------------------------------------------------------------------------------

fn store_zadd(s: RedisStore, key: string, score: string, member: string) -> int:
    idx := store_find_idx(s, key)
    let mut real_idx: int = idx
    if real_idx < 0:
        entry := entry_new_zset(key)
        real_idx = store_insert_entry(s, entry)

    let e: RedisEntry = s.entries[real_idx]
    let mut i: int = 0
    while i < len(e.zset_members):
        if e.zset_members[i] == member:
            e.zset_scores[i] = score
            return 0
        i = i + 1

    e.zset_members.push(member)
    e.zset_scores.push(score)
    return 1

fn store_zscore(s: RedisStore, key: string, member: string) -> string:
    idx := store_find_idx(s, key)
    if idx < 0:
        return ""
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.zset_members):
        if e.zset_members[i] == member:
            return e.zset_scores[i]
        i = i + 1
    return ""

fn store_zcard(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    return len(e.zset_members)

fn store_zrem(s: RedisStore, key: string, member: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return 0
    let e: RedisEntry = s.entries[idx]
    let mut i: int = 0
    while i < len(e.zset_members):
        if e.zset_members[i] == member:
            last_i := len(e.zset_members) - 1
            if i != last_i:
                e.zset_members[i] = e.zset_members[last_i]
                e.zset_scores[i] = e.zset_scores[last_i]
            e.zset_members.pop()
            e.zset_scores.pop()
            return 1
        i = i + 1
    return 0

fn store_zrank(s: RedisStore, key: string, member: string) -> int:
    idx := store_find_idx(s, key)
    if idx < 0:
        return -1
    let e: RedisEntry = s.entries[idx]
    let target_score: string = store_zscore(s, key, member)
    if len(target_score) == 0:
        return -1

    target_val := to_int(target_score)
    let mut rank: int = 0
    let mut i: int = 0
    while i < len(e.zset_scores):
        if to_int(e.zset_scores[i]) < target_val:
            rank = rank + 1
        i = i + 1
    return rank

fn store_zrange(s: RedisStore, key: string, start: int, stop: int, with_scores: int) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx < 0:
        return res
    let e: RedisEntry = s.entries[idx]
    sz := len(e.zset_members)
    if sz == 0:
        return res

    let mut indices: vector[int] = []
    let mut i: int = 0
    while i < sz:
        indices.push(i)
        i = i + 1

    let mut a: int = 0
    while a < sz:
        let mut b: int = a + 1
        while b < sz:
            idx_a := indices[a]
            idx_b := indices[b]
            sc_a := to_int(e.zset_scores[idx_a])
            sc_b := to_int(e.zset_scores[idx_b])
            if sc_a > sc_b:
                indices[a] = idx_b
                indices[b] = idx_a
            b = b + 1
        a = a + 1

    let mut s_idx: int = start
    let mut e_idx: int = stop
    if s_idx < 0: s_idx = sz + s_idx
    if e_idx < 0: e_idx = sz + e_idx
    if s_idx < 0: s_idx = 0
    if e_idx >= sz: e_idx = sz - 1
    if s_idx > e_idx: return res

    let mut cur: int = s_idx
    while cur <= e_idx:
        actual := indices[cur]
        res.push(e.zset_members[actual])
        if with_scores == 1:
            res.push(e.zset_scores[actual])
        cur = cur + 1
    return res

fn store_zrevrange(s: RedisStore, key: string, start: int, stop: int, with_scores: int) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx < 0:
        return res
    let e: RedisEntry = s.entries[idx]
    sz := len(e.zset_members)
    if sz == 0:
        return res

    let mut indices: vector[int] = []
    let mut i: int = 0
    while i < sz:
        indices.push(i)
        i = i + 1

    let mut a: int = 0
    while a < sz:
        let mut b: int = a + 1
        while b < sz:
            idx_a := indices[a]
            idx_b := indices[b]
            sc_a := to_int(e.zset_scores[idx_a])
            sc_b := to_int(e.zset_scores[idx_b])
            if sc_a < sc_b:
                indices[a] = idx_b
                indices[b] = idx_a
            b = b + 1
        a = a + 1

    let mut s_idx: int = start
    let mut e_idx: int = stop
    if s_idx < 0: s_idx = sz + s_idx
    if e_idx < 0: e_idx = sz + e_idx
    if s_idx < 0: s_idx = 0
    if e_idx >= sz: e_idx = sz - 1
    if s_idx > e_idx: return res

    let mut cur: int = s_idx
    while cur <= e_idx:
        actual := indices[cur]
        res.push(e.zset_members[actual])
        if with_scores == 1:
            res.push(e.zset_scores[actual])
        cur = cur + 1
    return res

fn store_zincrby(s: RedisStore, key: string, delta: string, member: string) -> string:
    cur_sc := store_zscore(s, key, member)
    let mut sc_num: int = 0
    if len(cur_sc) > 0:
        sc_num = to_int(cur_sc)
    sc_num = sc_num + to_int(delta)
    new_sc := str(sc_num)
    store_zadd(s, key, new_sc, member)
    return new_sc

// ------------------------------------------------------------------------------
// Pub / Sub Messaging Engine
// ------------------------------------------------------------------------------

fn store_subscribe(s: RedisStore, ch: string, client_fd: int) -> int:
    let mut i: int = 0
    while i < len(s.subs):
        let sub: PubSubSubscription = s.subs[i]
        if sub.channel == ch and sub.client_fd == client_fd:
            return len(s.subs)
        i = i + 1
    new_sub := PubSubSubscription(ch, client_fd)
    s.subs.push(new_sub)
    return len(s.subs)

fn store_unsubscribe(s: RedisStore, ch: string, client_fd: int) -> int:
    let mut i: int = 0
    while i < len(s.subs):
        let sub: PubSubSubscription = s.subs[i]
        if sub.channel == ch and sub.client_fd == client_fd:
            last_idx := len(s.subs) - 1
            if i != last_idx:
                s.subs[i] = s.subs[last_idx]
            s.subs.pop()
            return len(s.subs)
        i = i + 1
    return len(s.subs)

fn store_unsubscribe_all(s: RedisStore, client_fd: int) -> int:
    let mut i: int = 0
    while i < len(s.subs):
        let sub: PubSubSubscription = s.subs[i]
        if sub.client_fd == client_fd:
            last_idx := len(s.subs) - 1
            if i != last_idx:
                s.subs[i] = s.subs[last_idx]
            s.subs.pop()
        else:
            i = i + 1
    return len(s.subs)

fn store_publish(s: RedisStore, ch: string, message: string) -> int:
    let mut count: int = 0
    let mut i: int = 0
    while i < len(s.subs):
        let sub: PubSubSubscription = s.subs[i]
        if sub.channel == ch:
            count = count + 1
            if sub.client_fd > 0:
                payload := "*3\r\n$7\r\nmessage\r\n$" + str(len(ch)) + "\r\n" + ch + "\r\n$" + str(len(message)) + "\r\n" + message + "\r\n"
                tcp_write(sub.client_fd, payload)
        i = i + 1
    return count

// ------------------------------------------------------------------------------
// Key Management, Scan, and Flushing
// ------------------------------------------------------------------------------

fn store_flushall(s: RedisStore) -> void:
    let empty: vector[RedisEntry] = []
    s.entries = empty
    s.key_dict = dict_new(65536)

fn store_scan(s: RedisStore, cursor: int, pattern: string) -> vector[string]:
    let keys: vector[string] = []
    let mut i: int = cursor
    let sz: int = len(s.entries)
    while i < sz:
        let e: RedisEntry = s.entries[i]
        if len(pattern) == 0 or pattern == "*":
            keys.push(e.key)
        elif string_contains(e.key, pattern) == 1:
            keys.push(e.key)
        i = i + 1
    return keys

fn store_advance_time(s: RedisStore, seconds: int) -> int:
    s.now_tick = s.now_tick + seconds
    let mut expired_count: int = 0
    let mut i: int = 0
    while i < len(s.entries):
        let e: RedisEntry = s.entries[i]
        if e.expire_at > 0 and s.now_tick >= e.expire_at:
            expired_count = expired_count + 1
            dict_del(s.key_dict, e.key)
            last_idx := len(s.entries) - 1
            if i != last_idx:
                let last_e: RedisEntry = s.entries[last_idx]
                s.entries[i] = last_e
                dict_set(s.key_dict, last_e.key, i)
            s.entries.pop()
        else:
            i = i + 1
    return expired_count

// ------------------------------------------------------------------------------
// Persistence Engine: AOF and .clb Snapshots
// ------------------------------------------------------------------------------

fn store_bgrewriteaof(s: RedisStore, filepath: string) -> int:
    let mut aof_buf: string = ""
    let mut i: int = 0
    while i < len(s.entries):
        let e: RedisEntry = s.entries[i]
        if e.val_type == "string":
            aof_buf = aof_buf + "*3\r\n$3\r\nSET\r\n$" + str(len(e.key)) + "\r\n" + e.key + "\r\n$" + str(len(e.val)) + "\r\n" + e.val + "\r\n"
        elif e.val_type == "hash":
            let mut h_i: int = 0
            while h_i < len(e.hash_keys):
                k := e.hash_keys[h_i]
                v := e.hash_vals[h_i]
                aof_buf = aof_buf + "*4\r\n$4\r\nHSET\r\n$" + str(len(e.key)) + "\r\n" + e.key + "\r\n$" + str(len(k)) + "\r\n" + k + "\r\n$" + str(len(v)) + "\r\n" + v + "\r\n"
                h_i = h_i + 1
        elif e.val_type == "set":
            let mut s_i: int = 0
            while s_i < len(e.set_members):
                m := e.set_members[s_i]
                aof_buf = aof_buf + "*3\r\n$4\r\nSADD\r\n$" + str(len(e.key)) + "\r\n" + e.key + "\r\n$" + str(len(m)) + "\r\n" + m + "\r\n"
                s_i = s_i + 1
        elif e.val_type == "zset":
            let mut z_i: int = 0
            while z_i < len(e.zset_members):
                zm := e.zset_members[z_i]
                zs := e.zset_scores[z_i]
                aof_buf = aof_buf + "*4\r\n$4\r\nZADD\r\n$" + str(len(e.key)) + "\r\n" + e.key + "\r\n$" + str(len(zs)) + "\r\n" + zs + "\r\n$" + str(len(zm)) + "\r\n" + zm + "\r\n"
                z_i = z_i + 1
        i = i + 1
    file_write(filepath, aof_buf)
    return 1

fn store_save_rdb(s: RedisStore, filepath: string) -> int:
    doc := clb_new()
    clb_set_int(doc, "__rdb_version__", 3)
    clb_set_int(doc, "__entry_count__", len(s.entries))
    
    let mut i: int = 0
    while i < len(s.entries):
        let e: RedisEntry = s.entries[i]
        pfx := "k_" + str(i) + "_"
        clb_set_string(doc, pfx + "key", e.key)
        clb_set_string(doc, pfx + "type", e.val_type)
        clb_set_string(doc, pfx + "val", e.val)
        clb_set_int(doc, pfx + "exp", e.expire_at)
        
        if e.val_type == "list":
            let lv: vector[string] = quicklist_to_vector(e.qlist)
            clb_set_int(doc, pfx + "l_cnt", len(lv))
            let mut j: int = 0
            while j < len(lv):
                clb_set_string(doc, pfx + "l_" + str(j), lv[j])
                j = j + 1
        elif e.val_type == "hash":
            clb_set_int(doc, pfx + "h_cnt", len(e.hash_keys))
            let mut j: int = 0
            while j < len(e.hash_keys):
                clb_set_string(doc, pfx + "hk_" + str(j), e.hash_keys[j])
                clb_set_string(doc, pfx + "hv_" + str(j), e.hash_vals[j])
                j = j + 1
        elif e.val_type == "set":
            clb_set_int(doc, pfx + "s_cnt", len(e.set_members))
            let mut j: int = 0
            while j < len(e.set_members):
                clb_set_string(doc, pfx + "sm_" + str(j), e.set_members[j])
                j = j + 1
        elif e.val_type == "zset":
            clb_set_int(doc, pfx + "z_cnt", len(e.zset_members))
            let mut j: int = 0
            while j < len(e.zset_members):
                clb_set_string(doc, pfx + "zm_" + str(j), e.zset_members[j])
                clb_set_string(doc, pfx + "zs_" + str(j), e.zset_scores[j])
                j = j + 1
        i = i + 1
    return clb_save(filepath, doc)

fn store_load_rdb(s: RedisStore, filepath: string) -> int:
    doc := clb_load(filepath)
    if clb_has(doc, "__rdb_version__") == 0:
        return 0
        
    store_flushall(s)
    count := clb_get_int(doc, "__entry_count__", 0)
    let mut i: int = 0
    while i < count:
        pfx := "k_" + str(i) + "_"
        key := clb_get_string(doc, pfx + "key", "")
        vtype := clb_get_string(doc, pfx + "type", "string")
        val := clb_get_string(doc, pfx + "val", "")
        exp := clb_get_int(doc, pfx + "exp", -1)
        
        let list_v: vector[string] = []
        let hash_k: vector[string] = []
        let hash_v: vector[string] = []
        let set_m: vector[string] = []
        let zset_m: vector[string] = []
        let zset_s: vector[string] = []
        let stream_ids: vector[string] = []
        let stream_fields: vector[string] = []
        let hll_regs: vector[int] = []
        
        if vtype == "list":
            l_cnt := clb_get_int(doc, pfx + "l_cnt", 0)
            let mut j: int = 0
            while j < l_cnt:
                item := clb_get_string(doc, pfx + "l_" + str(j), "")
                list_v.push(item)
                j = j + 1
        elif vtype == "hash":
            h_cnt := clb_get_int(doc, pfx + "h_cnt", 0)
            let mut j: int = 0
            while j < h_cnt:
                hk := clb_get_string(doc, pfx + "hk_" + str(j), "")
                hv := clb_get_string(doc, pfx + "hv_" + str(j), "")
                hash_k.push(hk)
                hash_v.push(hv)
                j = j + 1
        elif vtype == "set":
            s_cnt := clb_get_int(doc, pfx + "s_cnt", 0)
            let mut j: int = 0
            while j < s_cnt:
                sm := clb_get_string(doc, pfx + "sm_" + str(j), "")
                set_m.push(sm)
                j = j + 1
        elif vtype == "zset":
            z_cnt := clb_get_int(doc, pfx + "z_cnt", 0)
            let mut j: int = 0
            while j < z_cnt:
                zm := clb_get_string(doc, pfx + "zm_" + str(j), "")
                zs := clb_get_string(doc, pfx + "zs_" + str(j), "")
                zset_m.push(zm)
                zset_s.push(zs)
                j = j + 1
                
        if vtype == "list":
            entry := entry_new_list(key, list_v)
            entry.val = val
            entry.expire_at = exp
            store_insert_entry(s, entry)
        else:
            ql_empty := quicklist_new()
            entry := RedisEntry(key, val, vtype, ql_empty, list_v, hash_k, hash_v, set_m, zset_m, zset_s, stream_ids, stream_fields, hll_regs, exp)
            store_insert_entry(s, entry)
        i = i + 1
    return count
