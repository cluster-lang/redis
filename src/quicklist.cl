// ==============================================================================
// Module:      Fast O(1) Deque QuickList (quicklist.cl)
// Project:     Cluster Redis (High-Performance In-Memory Key-Value Store)
// Author:      mohammadzaid/(cluster) <zkalamgir@proton.me>
// License:     Cluster Community // License:     MIT License Fair-Source License (CCFL-1.0)
// Description: Bidirectional doubly-linked deque list engine for Redis LPUSH, RPUSH,
//              LPOP, RPOP, and LRANGE operations with intrusive node recycling.
// ==============================================================================

// ------------------------------------------------------------------------------
// Model:   QuickNode
// Purpose: Intrusive doubly-linked deque node storing string values.
// Fields:
//   val:    String payload stored in list entry
//   prev:   Index of previous node (-1 for head)
//   next:   Index of next node (-1 for tail)
//   active: 1 if active, 0 if recycled in free list
// ------------------------------------------------------------------------------
model QuickNode:
    val: string
    prev: int
    next: int
    active: int

// ------------------------------------------------------------------------------
// Model:   QuickList
// Purpose: Main deque header managing head, tail, node arena, and free-list recycling.
// Fields:
//   head:      Index of first node in deque (-1 if empty)
//   tail:      Index of last node in deque (-1 if empty)
//   nodes:     Contiguous allocation pool of QuickNode elements
//   free_head: Head index of free-list recycle chain (-1 if none)
//   count:     Total number of active elements in list
// ------------------------------------------------------------------------------
model QuickList:
    head: int
    tail: int
    nodes: vector[QuickNode]
    free_head: int
    count: int

// ------------------------------------------------------------------------------
// Function: quicklist_new
// Syntax:   fn quicklist_new() -> QuickList
// Purpose:  Instantiates an empty O(1) QuickList deque.
// ------------------------------------------------------------------------------
fn quicklist_new() -> QuickList:
    let n: vector[QuickNode] = []
    return QuickList(-1, -1, n, -1, 0)

// ------------------------------------------------------------------------------
// Function: quicklist_alloc_node
// Syntax:   fn quicklist_alloc_node(ql: QuickList, val: string, prev: int, next: int) -> int
// Purpose:  Allocates a node from free list or appends new node into pool, returning its index.
// ------------------------------------------------------------------------------
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

// ------------------------------------------------------------------------------
// Function: quicklist_lpush
// Syntax:   fn quicklist_lpush(ql: QuickList, val: string) -> int
// Purpose:  Prepends value to head in O(1) time. Returns updated element count.
// ------------------------------------------------------------------------------
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

// ------------------------------------------------------------------------------
// Function: quicklist_rpush
// Syntax:   fn quicklist_rpush(ql: QuickList, val: string) -> int
// Purpose:  Appends value to tail in O(1) time. Returns updated element count.
// ------------------------------------------------------------------------------
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

// ------------------------------------------------------------------------------
// Function: quicklist_lpop
// Syntax:   fn quicklist_lpop(ql: QuickList) -> string
// Purpose:  Pops and returns first element at head in O(1) time, returning "" if empty.
// ------------------------------------------------------------------------------
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

// ------------------------------------------------------------------------------
// Function: quicklist_rpop
// Syntax:   fn quicklist_rpop(ql: QuickList) -> string
// Purpose:  Pops and returns last element at tail in O(1) time, returning "" if empty.
// ------------------------------------------------------------------------------
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

// ------------------------------------------------------------------------------
// Function: quicklist_range
// Syntax:   fn quicklist_range(ql: QuickList, start_i: int, stop_i: int) -> vector[string]
// Purpose:  Extracts subslice of elements matching Redis LRANGE conventions (supports negative indices).
// ------------------------------------------------------------------------------
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

// ------------------------------------------------------------------------------
// Function: quicklist_index
// Syntax:   fn quicklist_index(ql: QuickList, index_i: int) -> string
// Purpose:  Retrieves element at 0-based or negative offset index matching Redis LINDEX.
// ------------------------------------------------------------------------------
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

// ------------------------------------------------------------------------------
// Function: quicklist_len
// Syntax:   fn quicklist_len(ql: QuickList) -> int
// Purpose:  Returns total number of items stored in list.
// ------------------------------------------------------------------------------
fn quicklist_len(ql: QuickList) -> int:
    return ql.count

// ------------------------------------------------------------------------------
// Function: quicklist_to_vector
// Syntax:   fn quicklist_to_vector(ql: QuickList) -> vector[string]
// Purpose:  Serializes all deque elements sequentially into a vector of strings.
// ------------------------------------------------------------------------------
fn quicklist_to_vector(ql: QuickList) -> vector[string]:
    return quicklist_range(ql, 0, -1)
