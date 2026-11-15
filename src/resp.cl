// ==============================================================================
// Cluster-Redis: RESP (REdis Serialization Protocol) Engine
// 100% Pure Cluster Language - Zero Dependencies
// ==============================================================================

fn resp_encode_simple(msg: string) -> string:
    return "+" + msg + "\r\n"

fn resp_encode_error(err: string) -> string:
    return "-ERR " + err + "\r\n"

fn resp_encode_int(val: int) -> string:
    return ":" + str(val) + "\r\n"

fn resp_encode_null() -> string:
    return "$-1\r\n"

fn resp_encode_bulk(val: string) -> string:
    return "$" + str(len(val)) + "\r\n" + val + "\r\n"

fn resp_encode_array(items: vector[string]) -> string:
    let mut out: string = "*" + str(len(items)) + "\r\n"
    let mut i: int = 0
    while i < len(items):
        item := items[i]
        out = out + resp_encode_bulk(item)
        i = i + 1
    return out

// Parses either RESP array format (*3\r\n$3\r\n...) or raw inline text (SET foo bar)
fn resp_parse_command(raw: string) -> vector[string]:
    let args: vector[string] = []
    if len(raw) == 0:
        return args

    c0 := char_at(raw, 0)
    // RESP Array protocol
    if c0 == "*":
        let mut i: int = 1
        // Parse array count
        let mut count_str: string = ""
        while i < len(raw):
            c := char_at(raw, i)
            if c == "\r" or c == "\n":
                if c == "\r" and i + 1 < len(raw) and char_at(raw, i + 1) == "\n":
                    i = i + 1
                i = i + 1
                break
            count_str = count_str + c
            i = i + 1

        num_args := to_int(count_str)
        let mut arg_idx: int = 0
        while arg_idx < num_args and i < len(raw):
            if char_at(raw, i) == "$":
                i = i + 1
                // Read bulk string length
                let mut len_str: string = ""
                while i < len(raw):
                    c := char_at(raw, i)
                    if c == "\r" or c == "\n":
                        if c == "\r" and i + 1 < len(raw) and char_at(raw, i + 1) == "\n":
                            i = i + 1
                        i = i + 1
                        break
                    len_str = len_str + c
                    i = i + 1
                bulk_len := to_int(len_str)
                // Read bulk string content
                arg_val := substring(raw, i, bulk_len)
                args.push(arg_val)
                i = i + bulk_len
                // Consume trailing \r\n
                if i < len(raw) and char_at(raw, i) == "\r":
                    i = i + 1
                if i < len(raw) and char_at(raw, i) == "\n":
                    i = i + 1
            arg_idx = arg_idx + 1
        return args

    // Inline command format (e.g., "SET key val\r\n")
    let mut buf: string = ""
    let mut j: int = 0
    while j < len(raw):
        c := char_at(raw, j)
        if c == " " or c == "\r" or c == "\n":
            if len(buf) > 0:
                args.push(buf)
                buf = ""
        else:
            buf = buf + c
        j = j + 1
    if len(buf) > 0:
        args.push(buf)
    return args
