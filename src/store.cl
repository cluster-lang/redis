// ==============================================================================
// Cluster-Redis: High-Performance In-Memory Key-Value Store
// 100% Pure Cluster Language - Zero Foreign Dependencies
// ==============================================================================

import "std/clb.cl"
import "std/net.cl"

model RedisEntry:
    key: string
    val: string
    val_type: string
    list_vals: vector[string]
    hash_keys: vector[string]
    hash_vals: vector[string]
    set_members: vector[string]
    zset_members: vector[string]
    zset_scores: vector[string]
    expire_at: int

model PubSubSubscription:
    channel: string
    client_fd: int

model RedisStore:
    entries: vector[RedisEntry]
    subs: vector[PubSubSubscription]
    now_tick: int
    total_commands: int

fn entry_new_string(key: string, val: string, exp: int) -> RedisEntry:
    let el: vector[string] = []
    let hk: vector[string] = []
    let hv: vector[string] = []
    let sm: vector[string] = []
    let zm: vector[string] = []
    let zs: vector[string] = []
    return RedisEntry(key, val, "string", el, hk, hv, sm, zm, zs, exp)

fn entry_new_list(key: string, items: vector[string]) -> RedisEntry:
    let hk: vector[string] = []
    let hv: vector[string] = []
    let sm: vector[string] = []
    let zm: vector[string] = []
    let zs: vector[string] = []
    return RedisEntry(key, "", "list", items, hk, hv, sm, zm, zs, -1)

fn entry_new_hash(key: string) -> RedisEntry:
    let el: vector[string] = []
    let hk: vector[string] = []
    let hv: vector[string] = []
    let sm: vector[string] = []
    let zm: vector[string] = []
    let zs: vector[string] = []
    return RedisEntry(key, "", "hash", el, hk, hv, sm, zm, zs, -1)

fn entry_new_set(key: string) -> RedisEntry:
    let el: vector[string] = []
    let hk: vector[string] = []
    let hv: vector[string] = []
    let sm: vector[string] = []
    let zm: vector[string] = []
    let zs: vector[string] = []
    return RedisEntry(key, "", "set", el, hk, hv, sm, zm, zs, -1)

fn entry_new_zset(key: string) -> RedisEntry:
    let el: vector[string] = []
    let hk: vector[string] = []
    let hv: vector[string] = []
    let sm: vector[string] = []
    let zm: vector[string] = []
    let zs: vector[string] = []
    return RedisEntry(key, "", "zset", el, hk, hv, sm, zm, zs, -1)

fn store_new() -> RedisStore:
    let list: vector[RedisEntry] = []
    let s_list: vector[PubSubSubscription] = []
    return RedisStore(list, s_list, 0, 0)

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

// ------------------------------------------------------------------------------
// String Operations (SET, GET, EXISTS, DEL, INCR, STRLEN, APPEND, SETNX, GETSET, GETRANGE, SETRANGE)
// ------------------------------------------------------------------------------

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
            let empty_hk: vector[string] = []
            e.hash_keys = empty_hk
            let empty_hv: vector[string] = []
            e.hash_vals = empty_hv
            let empty_sm: vector[string] = []
            e.set_members = empty_sm
            let empty_zm: vector[string] = []
            e.zset_members = empty_zm
            let empty_zs: vector[string] = []
            e.zset_scores = empty_zs
            return
        i = i + 1

    entry := entry_new_string(key, val, exp)
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
        e.val_type = "string"
        return new_int
    else:
        store_set(s, key, str(by_val), -1)
        return by_val

fn store_strlen(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        return len(e.val)
    return 0

fn store_append(s: RedisStore, key: string, val: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        e.val = e.val + val
        return len(e.val)
    else:
        store_set(s, key, val, -1)
        return len(val)

fn store_setnx(s: RedisStore, key: string, val: string) -> int:
    if store_exists(s, key) == 1:
        return 0
    store_set(s, key, val, -1)
    return 1

fn store_getset(s: RedisStore, key: string, val: string) -> string:
    old := store_get(s, key)
    store_set(s, key, val, -1)
    return old

fn store_getrange(s: RedisStore, key: string, start_i: int, stop_i: int) -> string:
    val := store_get(s, key)
    total := len(val)
    if total == 0:
        return ""
    let mut st: int = start_i
    if st < 0: st = total + st
    if st < 0: st = 0
    let mut sp: int = stop_i
    if sp < 0: sp = total + sp
    if sp >= total: sp = total - 1
    if st > sp:
        return ""
    return substring(val, st, sp - st + 1)

fn store_setrange(s: RedisStore, key: string, offset: int, val: string) -> int:
    cur := store_get(s, key)
    let mut out: string = ""
    if offset > len(cur):
        out = cur
        let mut p: int = len(cur)
        while p < offset:
            out = out + " "
            p = p + 1
        out = out + val
    else:
        out = substring(cur, 0, offset) + val
        rem_start := offset + len(val)
        if rem_start < len(cur):
            out = out + substring(cur, rem_start, len(cur) - rem_start)
    store_set(s, key, out, -1)
    return len(out)

// ------------------------------------------------------------------------------
// List Operations (LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX)
// ------------------------------------------------------------------------------

fn store_lpush(s: RedisStore, key: string, val: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
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
        entry := entry_new_list(key, new_l)
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
        entry := entry_new_list(key, new_l)
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

fn store_llen(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        return len(e.list_vals)
    return 0

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
    if st < 0: st = total + st
    if st < 0: st = 0

    let mut sp: int = stop_i
    if sp < 0: sp = total + sp
    if sp >= total: sp = total - 1

    let mut k: int = st
    while k <= sp:
        res.push(e.list_vals[k])
        k = k + 1
    return res

fn store_lindex(s: RedisStore, key: string, index: int) -> string:
    idx := store_find_idx(s, key)
    if idx < 0:
        return ""
    let e: RedisEntry = s.entries[idx]
    total := len(e.list_vals)
    let mut real_idx: int = index
    if real_idx < 0:
        real_idx = total + real_idx
    if real_idx >= 0 and real_idx < total:
        return e.list_vals[real_idx]
    return ""

// ------------------------------------------------------------------------------
// Hash Operations (HSET, HGET, HDEL, HEXISTS, HLEN, HGETALL, HKEYS, HVALS)
// ------------------------------------------------------------------------------

fn store_hset(s: RedisStore, key: string, field: string, val: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        e.val_type = "hash"
        let mut j: int = 0
        while j < len(e.hash_keys):
            if e.hash_keys[j] == field:
                e.hash_vals[j] = val
                return 0 // field updated
            j = j + 1
        e.hash_keys.push(field)
        e.hash_vals.push(val)
        return 1 // new field added
    else:
        entry := entry_new_hash(key)
        entry.hash_keys.push(field)
        entry.hash_vals.push(val)
        s.entries.push(entry)
        return 1

fn store_hget(s: RedisStore, key: string, field: string) -> string:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.hash_keys):
            if e.hash_keys[j] == field:
                return e.hash_vals[j]
            j = j + 1
    return ""

fn store_hexists(s: RedisStore, key: string, field: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.hash_keys):
            if e.hash_keys[j] == field:
                return 1
            j = j + 1
    return 0

fn store_hdel(s: RedisStore, key: string, field: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.hash_keys):
            if e.hash_keys[j] == field:
                last_idx := len(e.hash_keys) - 1
                if j != last_idx:
                    e.hash_keys[j] = e.hash_keys[last_idx]
                    e.hash_vals[j] = e.hash_vals[last_idx]
                e.hash_keys.pop()
                e.hash_vals.pop()
                return 1
            j = j + 1
    return 0

fn store_hlen(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        return len(e.hash_keys)
    return 0

fn store_hgetall(s: RedisStore, key: string) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.hash_keys):
            res.push(e.hash_keys[j])
            res.push(e.hash_vals[j])
            j = j + 1
    return res

fn store_hkeys(s: RedisStore, key: string) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.hash_keys):
            res.push(e.hash_keys[j])
            j = j + 1
    return res

fn store_hvals(s: RedisStore, key: string) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.hash_vals):
            res.push(e.hash_vals[j])
            j = j + 1
    return res

// ------------------------------------------------------------------------------
// Set Operations (SADD, SREM, SMEMBERS, SISMEMBER, SCARD)
// ------------------------------------------------------------------------------

fn store_sadd(s: RedisStore, key: string, member: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        e.val_type = "set"
        let mut j: int = 0
        while j < len(e.set_members):
            if e.set_members[j] == member:
                return 0 // already exists
            j = j + 1
        e.set_members.push(member)
        return 1
    else:
        entry := entry_new_set(key)
        entry.set_members.push(member)
        s.entries.push(entry)
        return 1

fn store_srem(s: RedisStore, key: string, member: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.set_members):
            if e.set_members[j] == member:
                last_idx := len(e.set_members) - 1
                if j != last_idx:
                    e.set_members[j] = e.set_members[last_idx]
                e.set_members.pop()
                return 1
            j = j + 1
    return 0

fn store_sismember(s: RedisStore, key: string, member: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.set_members):
            if e.set_members[j] == member:
                return 1
            j = j + 1
    return 0

fn store_smembers(s: RedisStore, key: string) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.set_members):
            res.push(e.set_members[j])
            j = j + 1
    return res

fn store_scard(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        return len(e.set_members)
    return 0

// ------------------------------------------------------------------------------
// Sorted Set (ZSet) Operations (ZADD, ZRANGE, ZREVRANGE, ZSCORE, ZCARD, ZREM, ZRANK, ZINCRBY)
// ------------------------------------------------------------------------------

fn store_zadd(s: RedisStore, key: string, score: string, member: string) -> int:
    sc := to_float(score)
    idx := store_find_idx(s, key)
    let mut e: RedisEntry = RedisEntry("", "", "", [], [], [], [], [], [], 0)
    let mut is_new_entry: int = 0

    if idx >= 0:
        e = s.entries[idx]
        e.val_type = "zset"
    else:
        e = entry_new_zset(key)
        is_new_entry = 1

    let mut is_new_member: int = 1
    let mut clean_m: vector[string] = []
    let mut clean_s: vector[string] = []

    // Remove old instance if exists
    let mut j: int = 0
    while j < len(e.zset_members):
        if e.zset_members[j] == member:
            is_new_member = 0
        else:
            clean_m.push(e.zset_members[j])
            clean_s.push(e.zset_scores[j])
        j = j + 1

    // Insert at sorted position
    let mut new_m: vector[string] = []
    let mut new_s: vector[string] = []
    let mut inserted: int = 0
    let mut k: int = 0
    while k < len(clean_m):
        item_sc := to_float(clean_s[k])
        if inserted == 0 and sc < item_sc:
            new_m.push(member)
            new_s.push(score)
            inserted = 1
        new_m.push(clean_m[k])
        new_s.push(clean_s[k])
        k = k + 1

    if inserted == 0:
        new_m.push(member)
        new_s.push(score)

    e.zset_members = new_m
    e.zset_scores = new_s

    if is_new_entry == 1:
        s.entries.push(e)

    return is_new_member

fn store_zrange(s: RedisStore, key: string, start_i: int, stop_i: int, with_scores: int) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx < 0:
        return res
    let e: RedisEntry = s.entries[idx]
    total := len(e.zset_members)
    if total == 0:
        return res

    let mut st: int = start_i
    if st < 0: st = total + st
    if st < 0: st = 0
    let mut sp: int = stop_i
    if sp < 0: sp = total + sp
    if sp >= total: sp = total - 1

    let mut k: int = st
    while k <= sp:
        res.push(e.zset_members[k])
        if with_scores == 1:
            res.push(e.zset_scores[k])
        k = k + 1
    return res

fn store_zrevrange(s: RedisStore, key: string, start_i: int, stop_i: int, with_scores: int) -> vector[string]:
    let res: vector[string] = []
    idx := store_find_idx(s, key)
    if idx < 0:
        return res
    let e: RedisEntry = s.entries[idx]
    total := len(e.zset_members)
    if total == 0:
        return res

    let mut st: int = start_i
    if st < 0: st = total + st
    if st < 0: st = 0
    let mut sp: int = stop_i
    if sp < 0: sp = total + sp
    if sp >= total: sp = total - 1

    let mut k: int = total - 1 - st
    let min_k: int = total - 1 - sp
    while k >= min_k and k >= 0:
        res.push(e.zset_members[k])
        if with_scores == 1:
            res.push(e.zset_scores[k])
        k = k - 1
    return res

fn store_zscore(s: RedisStore, key: string, member: string) -> string:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.zset_members):
            if e.zset_members[j] == member:
                return e.zset_scores[j]
            j = j + 1
    return ""

fn store_zcard(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        return len(e.zset_members)
    return 0

fn store_zrem(s: RedisStore, key: string, member: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.zset_members):
            if e.zset_members[j] == member:
                let new_m: vector[string] = []
                let new_s: vector[string] = []
                let mut k: int = 0
                while k < len(e.zset_members):
                    if k != j:
                        new_m.push(e.zset_members[k])
                        new_s.push(e.zset_scores[k])
                    k = k + 1
                e.zset_members = new_m
                e.zset_scores = new_s
                return 1
            j = j + 1
    return 0

fn store_zrank(s: RedisStore, key: string, member: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        let mut j: int = 0
        while j < len(e.zset_members):
            if e.zset_members[j] == member:
                return j
            j = j + 1
    return -1

fn store_zincrby(s: RedisStore, key: string, increment: string, member: string) -> string:
    cur := store_zscore(s, key, member)
    let mut base_sc: float = 0.0
    if len(cur) > 0:
        base_sc = to_float(cur)
    new_sc := base_sc + to_float(increment)
    new_str := str(new_sc)
    store_zadd(s, key, new_str, member)
    return new_str

// ------------------------------------------------------------------------------
// Pub/Sub Messaging Engine (SUBSCRIBE, PUBLISH, UNSUBSCRIBE)
// ------------------------------------------------------------------------------

fn store_subscribe(s: RedisStore, channel: string, client_fd: int) -> int:
    let mut already: int = 0
    let mut j: int = 0
    while j < len(s.subs):
        let sub: PubSubSubscription = s.subs[j]
        if sub.channel == channel and sub.client_fd == client_fd:
            already = 1
            break
        j = j + 1

    if already == 0:
        s.subs.push(PubSubSubscription(channel, client_fd))

    let mut cnt: int = 0
    let mut k: int = 0
    while k < len(s.subs):
        let sub2: PubSubSubscription = s.subs[k]
        if sub2.client_fd == client_fd:
            cnt = cnt + 1
        k = k + 1
    return cnt

fn store_unsubscribe(s: RedisStore, channel: string, client_fd: int) -> int:
    let new_subs: vector[PubSubSubscription] = []
    let mut j: int = 0
    while j < len(s.subs):
        let sub: PubSubSubscription = s.subs[j]
        if sub.channel != channel or sub.client_fd != client_fd:
            new_subs.push(sub)
        j = j + 1
    s.subs = new_subs

    let mut cnt: int = 0
    let mut k: int = 0
    while k < len(s.subs):
        let sub2: PubSubSubscription = s.subs[k]
        if sub2.client_fd == client_fd:
            cnt = cnt + 1
        k = k + 1
    return cnt

fn store_unsubscribe_all(s: RedisStore, client_fd: int) -> void:
    let new_subs: vector[PubSubSubscription] = []
    let mut j: int = 0
    while j < len(s.subs):
        let sub: PubSubSubscription = s.subs[j]
        if sub.client_fd != client_fd:
            new_subs.push(sub)
        j = j + 1
    s.subs = new_subs

fn store_publish(s: RedisStore, channel: string, message: string) -> int:
    let mut notified: int = 0
    let payload: string = "*3\r\n$7\r\nmessage\r\n$" + str(len(channel)) + "\r\n" + channel + "\r\n$" + str(len(message)) + "\r\n" + message + "\r\n"

    let mut j: int = 0
    while j < len(s.subs):
        let sub: PubSubSubscription = s.subs[j]
        if sub.channel == channel:
            tcp_write(sub.client_fd, payload)
            notified = notified + 1
        j = j + 1
    return notified

// ------------------------------------------------------------------------------
// Key Metadata, Scanning & Expiry (TYPE, EXPIRE, TTL, PERSIST, KEYS, RENAME, SCAN)
// ------------------------------------------------------------------------------

fn store_type(s: RedisStore, key: string) -> string:
    idx := store_find_idx(s, key)
    if idx < 0:
        return "none"
    let e: RedisEntry = s.entries[idx]
    return e.val_type

fn store_expire(s: RedisStore, key: string, seconds: int) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        e.expire_at = s.now_tick + seconds
        return 1
    return 0

fn store_ttl(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx == -1:
        return -2 // not found
    if idx == -2:
        return -2 // expired
    let e: RedisEntry = s.entries[idx]
    if e.expire_at <= 0:
        return -1 // no timeout
    rem := e.expire_at - s.now_tick
    if rem <= 0:
        return -2
    return rem

fn store_persist(s: RedisStore, key: string) -> int:
    idx := store_find_idx(s, key)
    if idx >= 0:
        let e: RedisEntry = s.entries[idx]
        if e.expire_at > 0:
            e.expire_at = -1
            return 1
    return 0

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

fn store_rename(s: RedisStore, old_key: string, new_key: string) -> int:
    idx := store_find_idx(s, old_key)
    if idx < 0:
        return 0
    store_del(s, new_key)
    idx2 := store_find_idx(s, old_key)
    let e: RedisEntry = s.entries[idx2]
    e.key = new_key
    return 1

fn store_renamenx(s: RedisStore, old_key: string, new_key: string) -> int:
    if store_exists(s, old_key) == 0:
        return -1
    if store_exists(s, new_key) == 1:
        return 0
    store_rename(s, old_key, new_key)
    return 1

fn store_randomkey(s: RedisStore) -> string:
    if len(s.entries) == 0:
        return ""
    let e: RedisEntry = s.entries[0]
    return e.key

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
// Persistence Engine: Saves and Loads In-Memory State using .clb
// ------------------------------------------------------------------------------

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
            clb_set_int(doc, pfx + "l_cnt", len(e.list_vals))
            let mut j: int = 0
            while j < len(e.list_vals):
                clb_set_string(doc, pfx + "l_" + str(j), e.list_vals[j])
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
                
        entry := RedisEntry(key, val, vtype, list_v, hash_k, hash_v, set_m, zset_m, zset_s, exp)
        s.entries.push(entry)
        i = i + 1
    return count
