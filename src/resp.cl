// ==============================================================================
// Cluster-Redis: RESP (REdis Serialization Protocol) Engine
// 100% Pure Cluster Language - Zero Dependencies
// ==============================================================================

model RespCursor:
    pos: int

fn cursor_new(p: int) -> RespCursor:
    return RespCursor(p)

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

fn resp_encode_empty_array() -> string:
    return "*0\r\n"

fn resp_encode_array(items: vector[string]) -> string:
    let mut out: string = "*" + str(len(items)) + "\r\n"
    let mut i: int = 0
    while i < len(items):
        item := items[i]
        out = out + resp_encode_bulk(item)
        i = i + 1
    return out

// Parses next command from raw stream buffer starting at cursor position
fn resp_parse_command_at(raw: string, c: RespCursor) -> vector[string]:
    let args: vector[string] = []
    
    // Skip leading delimiters
    while c.pos < len(raw):
        ch := char_at(raw, c.pos)
        if ch == "\r" or ch == "\n" or ch == " ":
            c.pos = c.pos + 1
        else:
            break

    if c.pos >= len(raw):
        return args

    // RESP Array protocol (*<count>\r\n$<len>\r\n...)
    if char_at(raw, c.pos) == "*":
        c.pos = c.pos + 1
        let mut count_str: string = ""
        while c.pos < len(raw):
            ch := char_at(raw, c.pos)
            c.pos = c.pos + 1
            if ch == "\r" or ch == "\n":
                if ch == "\r" and c.pos < len(raw) and char_at(raw, c.pos) == "\n":
                    c.pos = c.pos + 1
                break
            count_str = count_str + ch

        num_args := to_int(count_str)
        let mut arg_idx: int = 0
        while arg_idx < num_args and c.pos < len(raw):
            // Skip any interstitial whitespace
            while c.pos < len(raw):
                w := char_at(raw, c.pos)
                if w == "\r" or w == "\n" or w == " ":
                    c.pos = c.pos + 1
                else:
                    break
            if c.pos >= len(raw):
                break

            if char_at(raw, c.pos) == "$":
                c.pos = c.pos + 1
                let mut len_str: string = ""
                while c.pos < len(raw):
                    bch := char_at(raw, c.pos)
                    c.pos = c.pos + 1
                    if bch == "\r" or bch == "\n":
                        if bch == "\r" and c.pos < len(raw) and char_at(raw, c.pos) == "\n":
                            c.pos = c.pos + 1
                        break
                    len_str = len_str + bch

                bulk_len := to_int(len_str)
                if bulk_len >= 0:
                    arg_val := substring(raw, c.pos, bulk_len)
                    args.push(arg_val)
                    c.pos = c.pos + bulk_len
                    if c.pos < len(raw) and char_at(raw, c.pos) == "\r":
                        c.pos = c.pos + 1
                    if c.pos < len(raw) and char_at(raw, c.pos) == "\n":
                        c.pos = c.pos + 1
                else:
                    args.push("")
            arg_idx = arg_idx + 1
        return args

    // Inline command format (e.g., "PING\r\n" or "SET foo bar\r\n")
    let mut cur: string = ""
    while c.pos < len(raw):
        ch := char_at(raw, c.pos)
        c.pos = c.pos + 1
        if ch == "\r" or ch == "\n":
            if ch == "\r" and c.pos < len(raw) and char_at(raw, c.pos) == "\n":
                c.pos = c.pos + 1
            if len(cur) > 0:
                args.push(cur)
                cur = ""
            break
        elif ch == " ":
            if len(cur) > 0:
                args.push(cur)
                cur = ""
        else:
            cur = cur + ch
    if len(cur) > 0:
        args.push(cur)
    return args

// Convenience helper to parse a single command string
fn resp_parse_command(raw: string) -> vector[string]:
    c := cursor_new(0)
    return resp_parse_command_at(raw, c)
