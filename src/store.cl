// ==============================================================================
// Cluster-Redis: High-Performance In-Memory Key-Value Store
// 100% Pure Cluster Language - Zero Foreign Dependencies
// ==============================================================================

import "std/clb.cl"

model RedisEntry:
    key: string
    val: string
    val_type: string
    list_vals: vector[string]
    expire_at: int

model RedisStore:
    entries: vector[RedisEntry]
    now_tick: int

fn store_new() -> RedisStore:
    let list: vector[RedisEntry] = []
    return RedisStore(list, 0)

fn store_find_idx(s: RedisStore, key: string) -> int:
    let mut i: int = 0
    while i < len(s.entries):
        let e: RedisEntry = s.entries[i]
        if e.key == key:
            // Check TTL expiration
            if e.expire_at > 0 and s.now_tick >= e.expire_at:
                return -2 // expired
            return i
        i = i + 1
    return -1 // not found

fn store_set(s: RedisStore, key: string, val: string, ttl_seconds: int) -> void:
    let mut exp: int = -1
    if ttl_seconds > 0:
        exp = s.now_tick + ttl_seconds

    let mut i: int = 0
    while i < len(s.entries):
        let e: RedisEntry = s.entries[i]
        if e.key == key:
            e.val = val
            e.val_type = "string"
            e.expire_at = exp
            let empty_list: vector[string] = []
            e.list_vals = empty_list
            return
        i = i + 1

    let empty_list: vector[string] = []
    entry := RedisEntry(key, val, "string", empty_list, exp)
    s.entries.push(entry)

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
    let mut i: int = 0
    while i < len(s.entries):
        let e: RedisEntry = s.entries[i]
        if e.key == key:
            // Swap with last and pop
            last_idx := len(s.entries) - 1
            if i != last_idx:
                let last_e: RedisEntry = s.entries[last_idx]
                s.entries[i] = last_e
            s.entries.pop()
            return 1
        i = i + 1
    return 0

fn store_incr(s: RedisStore, key: string, by_val: int) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        cur_int := to_int(e.val)
        new_int := cur_int + by_val
        e.val = str(new_int)
        e.val_type = "int"
        return new_int
    else:
        store_set(s, key, str(by_val), -1)
        return by_val

// ------------------------------------------------------------------------------
// List Operations (LPUSH, RPUSH, LPOP, RPOP, LRANGE)
// ------------------------------------------------------------------------------
fn store_lpush(s: RedisStore, key: string, val: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        // Insert at beginning by creating fresh vector
        let new_l: vector[string] = [val]
        let mut j: int = 0
        while j < len(e.list_vals):
            new_l.push(e.list_vals[j])
            j = j + 1
        e.list_vals = new_l
        e.val_type = "list"
        return len(e.list_vals)
    else:
        let new_l: vector[string] = [val]
        entry := RedisEntry(key, "", "list", new_l, -1)
        s.entries.push(entry)
        return 1

fn store_rpush(s: RedisStore, key: string, val: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        e.list_vals.push(val)
        e.val_type = "list"
        return len(e.list_vals)
    else:
        let new_l: vector[string] = [val]
        entry := RedisEntry(key, "", "list", new_l, -1)
        s.entries.push(entry)
        return 1

fn store_lpop(s: RedisStore, key: string) -> string:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        if len(e.list_vals) == 0:
            return ""
        head_val := e.list_vals[0]
        let new_l: vector[string] = []
        let mut j: int = 1
        while j < len(e.list_vals):
            new_l.push(e.list_vals[j])
            j = j + 1
        e.list_vals = new_l
        return head_val
    return ""

fn store_rpop(s: RedisStore, key: string) -> string:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        n := len(e.list_vals)
        if n == 0:
            return ""
        last_val := e.list_vals[n - 1]
        let new_l: vector[string] = []
        let mut j: int = 0
        while j < n - 1:
            new_l.push(e.list_vals[j])
            j = j + 1
        e.list_vals = new_l
        return last_val
    return ""

fn store_lrange(s: RedisStore, key: string, start_i: int, stop_i: int) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx < 0:
        return res
    let e: RedisEntry = s.entries[idx]
    total := len(e.list_vals)
    if total == 0:
        return res

    let mut st: int = start_i
    if st < 0:
        st = total + st
    if st < 0:
        st = 0

    let mut sp: int = stop_i
    if sp < 0:
        sp = total + sp
    if sp >= total:
        sp = total - 1

    let mut k: int = st
    while k <= sp:
        res.push(e.list_vals[k])
        k = k + 1
    return res

fn store_keys(s: RedisStore, pattern: string) -> vector[string]:
    let res: vector[string] = []
    let mut i: int = 0
    while i < len(s.entries):
        let e: RedisEntry = s.entries[i]
        if e.expire_at <= 0 or s.now_tick < e.expire_at:
            if pattern == "*" or pattern == "" or string_contains(e.key, pattern) == 1:
                res.push(e.key)
        i = i + 1
    return res

fn store_flushall(s: RedisStore) -> void:
    let fresh: vector[RedisEntry] = []
    s.entries = fresh

fn store_advance_time(s: RedisStore, seconds: int) -> int:
    s.now_tick = s.now_tick + seconds
    let mut expired_count: int = 0
    let mut i: int = 0
    while i < len(s.entries):
        let e: RedisEntry = s.entries[i]
        if e.expire_at > 0 and s.now_tick >= e.expire_at:
            expired_count = expired_count + 1
            last_idx := len(s.entries) - 1
            if i != last_idx:
                let last_e: RedisEntry = s.entries[last_idx]
                s.entries[i] = last_e
            s.entries.pop()
        else:
            i = i + 1
    return expired_count

// ------------------------------------------------------------------------------
// Persistence Engine: Saves and Loads In-Memory State using .clb (Cluster Binary Bundle)
// ------------------------------------------------------------------------------
fn store_save_rdb(s: RedisStore, filepath: string) -> int:
    doc := clb_new()
    clb_set_int(doc, "__rdb_version__", 1)
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
            clb_set_int(doc, pfx + "l_cnt", len(e.list_vals))
            let mut j: int = 0
            while j < len(e.list_vals):
                clb_set_string(doc, pfx + "l_" + str(j), e.list_vals[j])
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
        if vtype == "list":
            l_cnt := clb_get_int(doc, pfx + "l_cnt", 0)
            let mut j: int = 0
            while j < l_cnt:
                item := clb_get_string(doc, pfx + "l_" + str(j), "")
                list_v.push(item)
                j = j + 1
                
        entry := RedisEntry(key, val, vtype, list_v, exp)
        s.entries.push(entry)
        i = i + 1
    return count
