// ==============================================================================
// Module:      Fast O(1) Hash Table (dict.cl)
// Project:     Cluster Redis (High-Performance In-Memory Key-Value Store)
// Author:      mohammadzaid/(cluster) <zkalamgir@proton.me>
// License:     Cluster Community // License:     MIT License Fair-Source License (CCFL-1.0)
// Description: High-speed O(1) closed-addressing hash table powered by DJB2 hashing
//              and intrusive free-list recycling.
// ==============================================================================

// ------------------------------------------------------------------------------
// Model:   DictNode
// Purpose: Individual key-value index bucket chain node.
// Fields:
//   key:     String key name
//   val_idx: Integer index of value payload in storage array
//   next:    Index of next node in collision chain, or -1 for end
//   active:  1 if node contains live data, 0 if in free list
// ------------------------------------------------------------------------------
model DictNode:
    key: string
    val_idx: int
    next: int
    active: int

// ------------------------------------------------------------------------------
// Model:   Dict
// Purpose: Main hash table structure with dynamic buckets and intrusive free node recycling.
// Fields:
//   buckets:   Bucket array storing index of chain head (-1 for empty)
//   nodes:     Contiguous allocation arena for all hash nodes
//   free_head: Head index of free node recycle pool (-1 for none)
//   count:     Total number of active keys stored in hash table
// ------------------------------------------------------------------------------
model Dict:
    buckets: vector[int]
    nodes: vector[DictNode]
    free_head: int
    count: int

// ------------------------------------------------------------------------------
// Function: dict_hash
// Syntax:   fn dict_hash(s: string) -> int
// Purpose:  Computes 32-bit positive DJB2 hash code for string key.
// ------------------------------------------------------------------------------
fn dict_hash(s: string) -> int:
    let mut h: int = 5381
    let mut i: int = 0
    let sz: int = len(s)
    while i < sz:
        b := byte_at(s, i)
        h = ((h << 5) + h) + b
        i = i + 1
    if h < 0:
        h = 0 - h
    return h

// ------------------------------------------------------------------------------
// Function: dict_new
// Syntax:   fn dict_new(num_buckets: int) -> Dict
// Purpose:  Initializes a new hash table with at least 16 buckets.
// ------------------------------------------------------------------------------
fn dict_new(num_buckets: int) -> Dict:
    let mut b: vector[int] = []
    let mut n: vector[DictNode] = []
    let mut i: int = 0
    let mut cap: int = num_buckets
    if cap < 16:
        cap = 16
    while i < cap:
        b.push(-1)
        i = i + 1
    return Dict(b, n, -1, 0)

// ------------------------------------------------------------------------------
// Function: dict_get
// Syntax:   fn dict_get(d: Dict, key: string) -> int
// Purpose:  Looks up key in hash table. Returns value index or -1 if not found.
// ------------------------------------------------------------------------------
fn dict_get(d: Dict, key: string) -> int:
    sz := len(d.buckets)
    if sz == 0:
        return -1
    h := dict_hash(key) % sz
    let mut cur: int = d.buckets[h]
    while cur >= 0:
        let node: DictNode = d.nodes[cur]
        if node.active == 1:
            if node.key == key:
                return node.val_idx
        cur = node.next
    return -1

// ------------------------------------------------------------------------------
// Function: dict_set
// Syntax:   fn dict_set(d: Dict, key: string, val_idx: int) -> void
// Purpose:  Inserts or updates key with value index, reusing recycled nodes when available.
// ------------------------------------------------------------------------------
fn dict_set(d: Dict, key: string, val_idx: int):
    sz := len(d.buckets)
    if sz == 0:
        return
    h := dict_hash(key) % sz
    let mut cur: int = d.buckets[h]
    
    // Check if key already exists
    while cur >= 0:
        let node: DictNode = d.nodes[cur]
        if node.active == 1:
            if node.key == key:
                node.val_idx = val_idx
                return
        cur = node.next

    // Allocate node (reuse from free list if available)
    let mut node_idx: int = -1
    if d.free_head >= 0:
        node_idx = d.free_head
        let fnode: DictNode = d.nodes[node_idx]
        d.free_head = fnode.next
        fnode.key = key
        fnode.val_idx = val_idx
        fnode.next = d.buckets[h]
        fnode.active = 1
    else:
        node_idx = len(d.nodes)
        new_node := DictNode(key, val_idx, d.buckets[h], 1)
        d.nodes.push(new_node)

    d.buckets[h] = node_idx
    d.count = d.count + 1

// ------------------------------------------------------------------------------
// Function: dict_del
// Syntax:   fn dict_del(d: Dict, key: string) -> int
// Purpose:  Removes key from hash table, linking node to free list. Returns 1 on success, 0 on absent.
// ------------------------------------------------------------------------------
fn dict_del(d: Dict, key: string) -> int:
    sz := len(d.buckets)
    if sz == 0:
        return 0
    h := dict_hash(key) % sz
    let mut prev: int = -1
    let mut cur: int = d.buckets[h]

    while cur >= 0:
        let node: DictNode = d.nodes[cur]
        if node.active == 1:
            if node.key == key:
                // Unlink from bucket chain
                if prev < 0:
                    d.buckets[h] = node.next
                else:
                    let prev_node: DictNode = d.nodes[prev]
                    prev_node.next = node.next

                // Mark inactive and link to free list
                node.active = 0
                node.key = ""
                node.val_idx = -1
                node.next = d.free_head
                d.free_head = cur
                d.count = d.count - 1
                return 1
        prev = cur
        cur = node.next
    return 0

// ------------------------------------------------------------------------------
// Function: dict_size
// Syntax:   fn dict_size(d: Dict) -> int
// Purpose:  Returns total number of live keys in hash table.
// ------------------------------------------------------------------------------
fn dict_size(d: Dict) -> int:
    return d.count
