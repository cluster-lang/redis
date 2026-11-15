// ==============================================================================
// Fast O(1) QuickList / Deque for Cluster-Redis Lists (LPUSH, RPUSH, LPOP, RPOP)
// 100% Pure Cluster Language - Zero-copy O(1) Head and Tail operations
// ==============================================================================

model QuickNode:
    val: string
    prev: int
    next: int
    active: int

model QuickList:
    head: int
    tail: int
    nodes: vector[QuickNode]
    free_head: int
    count: int

fn quicklist_new() -> QuickList:
    let n: vector[QuickNode] = []
    return QuickList(-1, -1, n, -1, 0)

fn quicklist_alloc_node(ql: QuickList, val: string, prev: int, next: int) -> int:
    if ql.free_head >= 0:
        idx := ql.free_head
        let fnode: QuickNode = ql.nodes[idx]
        ql.free_head = fnode.next
        fnode.val = val
        fnode.prev = prev
        fnode.next = next
        fnode.active = 1
        return idx
    else:
        idx := len(ql.nodes)
        node := QuickNode(val, prev, next, 1)
        ql.nodes.push(node)
        return idx

fn quicklist_lpush(ql: QuickList, val: string) -> int:
    new_idx := quicklist_alloc_node(ql, val, -1, ql.head)
    if ql.head >= 0:
        let old_head: QuickNode = ql.nodes[ql.head]
        old_head.prev = new_idx
    else:
        ql.tail = new_idx
    ql.head = new_idx
    ql.count = ql.count + 1
    return ql.count

fn quicklist_rpush(ql: QuickList, val: string) -> int:
    new_idx := quicklist_alloc_node(ql, val, ql.tail, -1)
    if ql.tail >= 0:
        let old_tail: QuickNode = ql.nodes[ql.tail]
        old_tail.next = new_idx
    else:
        ql.head = new_idx
    ql.tail = new_idx
    ql.count = ql.count + 1
    return ql.count

fn quicklist_lpop(ql: QuickList) -> string:
    if ql.head < 0:
        return ""
    let hnode: QuickNode = ql.nodes[ql.head]
    val := hnode.val
    hnode.active = 0
    hnode.val = ""
    old_head := ql.head
    ql.head = hnode.next
    if ql.head >= 0:
        let new_h: QuickNode = ql.nodes[ql.head]
        new_h.prev = -1
    else:
        ql.tail = -1
    hnode.next = ql.free_head
    ql.free_head = old_head
    ql.count = ql.count - 1
    return val

fn quicklist_rpop(ql: QuickList) -> string:
    if ql.tail < 0:
        return ""
    let tnode: QuickNode = ql.nodes[ql.tail]
    val := tnode.val
    tnode.active = 0
    tnode.val = ""
    old_tail := ql.tail
    ql.tail = tnode.prev
    if ql.tail >= 0:
        let new_t: QuickNode = ql.nodes[ql.tail]
        new_t.next = -1
    else:
        ql.head = -1
    tnode.next = ql.free_head
    ql.free_head = old_tail
    ql.count = ql.count - 1
    return val

fn quicklist_range(ql: QuickList, start_i: int, stop_i: int) -> vector[string]:
    let res: vector[string] = []
    total := ql.count
    if total == 0:
        return res
    let mut st: int = start_i
    if st < 0: st = total + st
    if st < 0: st = 0
    let mut sp: int = stop_i
    if sp < 0: sp = total + sp
    if sp >= total: sp = total - 1
    if st > sp:
        return res

    let mut cur: int = ql.head
    let mut idx: int = 0
    while cur >= 0:
        let n: QuickNode = ql.nodes[cur]
        if idx >= st:
            if idx <= sp:
                res.push(n.val)
        if idx > sp:
            break
        cur = n.next
        idx = idx + 1
    return res

fn quicklist_index(ql: QuickList, index_i: int) -> string:
    total := ql.count
    if total == 0:
        return ""
    let mut target: int = index_i
    if target < 0: target = total + target
    if target < 0: return ""
    if target >= total: return ""

    let mut cur: int = ql.head
    let mut idx: int = 0
    while cur >= 0:
        let n: QuickNode = ql.nodes[cur]
        if idx == target:
            return n.val
        cur = n.next
        idx = idx + 1
    return ""

fn quicklist_len(ql: QuickList) -> int:
    return ql.count

fn quicklist_to_vector(ql: QuickList) -> vector[string]:
    return quicklist_range(ql, 0, -1)
