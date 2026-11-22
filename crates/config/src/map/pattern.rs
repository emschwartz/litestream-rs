//! Go's `filepath.Match` on Unix (Go 1.24 `path/filepath/match.go`), byte for byte.
//!
//! A pattern is not checked on its own. Go reports `ErrBadPattern` only when matching a
//! particular name reaches the malformed part, so `a*[` is an error against `ab` and a plain
//! mismatch against `b`. The scan in `dir.rs` depends on that: Go's walk fails at the first
//! file whose match errors, and a directory where no file reaches the bad part scans normally.
//! Names are bytes, as Go's strings are; classes and `?` decode one UTF-8 rune, and an invalid
//! byte is U+FFFD, consumed alone.

/// Go's `ErrBadPattern`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BadPattern;

const SEPARATOR: u8 = b'/';
const RUNE_ERROR: char = char::REPLACEMENT_CHARACTER;

/// Go's `filepath.Match(pattern, name)`.
pub(crate) fn matches(pattern: &[u8], name: &[u8]) -> Result<bool, BadPattern> {
    let mut pattern = pattern;
    let mut name = name;
    'chunks: while !pattern.is_empty() {
        let (star, chunk, rest) = scan_chunk(pattern);
        pattern = rest;
        if star && chunk.is_empty() {
            return Ok(!name.contains(&SEPARATOR));
        }
        if let Some(tail) = match_chunk(chunk, name)?
            && (tail.is_empty() || !pattern.is_empty())
        {
            name = tail;
            continue;
        }
        if star {
            let mut skipped = name;
            while let Some((&c, after)) = skipped.split_first() {
                if c == SEPARATOR {
                    break;
                }
                skipped = after;
                if let Some(tail) = match_chunk(chunk, after)? {
                    if pattern.is_empty() && !tail.is_empty() {
                        continue;
                    }
                    name = tail;
                    continue 'chunks;
                }
            }
        }
        return Ok(false);
    }
    Ok(name.is_empty())
}

/// The next run of non-star terms, after any leading stars.
fn scan_chunk(pattern: &[u8]) -> (bool, &[u8], &[u8]) {
    let mut star = false;
    let mut pattern = pattern;
    while let Some(rest) = pattern.strip_prefix(b"*") {
        pattern = rest;
        star = true;
    }
    let mut in_range = false;
    let mut end = 0_usize;
    let mut bytes = pattern.iter();
    while let Some(&c) = bytes.next() {
        match c {
            b'\\' => {
                if bytes.next().is_some() {
                    end = end.saturating_add(1);
                }
            }
            b'[' => in_range = true,
            b']' => in_range = false,
            b'*' if !in_range => break,
            _ => {}
        }
        end = end.saturating_add(1);
    }
    let (chunk, rest) = pattern.split_at_checked(end).unwrap_or((pattern, &[]));
    (star, chunk, rest)
}

/// Whether `chunk` matches the start of `s`, and what of `s` is left. After a mismatch the
/// rest of the chunk is still parsed, so a malformed term after it is still an error.
fn match_chunk<'s>(chunk: &[u8], s: &'s [u8]) -> Result<Option<&'s [u8]>, BadPattern> {
    let mut chunk = chunk;
    let mut s = s;
    let mut failed = false;
    while let Some((&c, after)) = chunk.split_first() {
        if !failed && s.is_empty() {
            failed = true;
        }
        match c {
            b'[' => {
                let mut r = RUNE_ERROR;
                if !failed {
                    let (rune, n) = decode_rune(s);
                    r = rune;
                    s = s.get(n..).unwrap_or_default();
                }
                let (matched, rest) = class(after, r)?;
                chunk = rest;
                if !matched {
                    failed = true;
                }
            }
            b'?' => {
                if let Some((&first, _)) = s.split_first().filter(|_| !failed) {
                    if first == SEPARATOR {
                        failed = true;
                    }
                    let (_, n) = decode_rune(s);
                    s = s.get(n..).unwrap_or_default();
                }
                chunk = after;
            }
            b'\\' => {
                let Some((&literal, rest)) = after.split_first() else {
                    return Err(BadPattern);
                };
                (failed, s) = literal_byte(failed, literal, s);
                chunk = rest;
            }
            literal => {
                (failed, s) = literal_byte(failed, literal, s);
                chunk = after;
            }
        }
    }
    Ok((!failed).then_some(s))
}

fn literal_byte(failed: bool, literal: u8, s: &[u8]) -> (bool, &[u8]) {
    match s.split_first() {
        Some((&first, rest)) if !failed => (first != literal, rest),
        Some(_) | None => (true, s),
    }
}

/// A class after its `[`: whether `r` is in it (negation applied), and the chunk after `]`.
fn class(chunk: &[u8], r: char) -> Result<(bool, &[u8]), BadPattern> {
    let (negated, mut chunk) = match chunk.strip_prefix(b"^") {
        Some(rest) => (true, rest),
        None => (false, chunk),
    };
    let mut matched = false;
    let mut ranges = 0_usize;
    loop {
        if ranges > 0
            && let Some(rest) = chunk.strip_prefix(b"]")
        {
            return Ok((matched != negated, rest));
        }
        let (lo, rest) = get_esc(chunk)?;
        let (hi, rest) = match rest.strip_prefix(b"-") {
            Some(after) => get_esc(after)?,
            None => (lo, rest),
        };
        chunk = rest;
        if lo <= r && r <= hi {
            matched = true;
        }
        ranges = ranges.saturating_add(1);
    }
}

/// Go's `getEsc`: one possibly escaped rune of a class, which must not be its last byte.
fn get_esc(chunk: &[u8]) -> Result<(char, &[u8]), BadPattern> {
    let chunk = match chunk.split_first() {
        None | Some((b'-' | b']', _)) => return Err(BadPattern),
        Some((b'\\', rest)) => rest,
        Some(_) => chunk,
    };
    if chunk.is_empty() {
        return Err(BadPattern);
    }
    let (r, n) = decode_rune(chunk);
    let rest = chunk.get(n..).unwrap_or_default();
    if (r == RUNE_ERROR && n == 1) || rest.is_empty() {
        return Err(BadPattern);
    }
    Ok((r, rest))
}

/// Go's `utf8.DecodeRune`: the first rune and its length; an invalid or empty start is
/// U+FFFD of length 1 (0 for empty input).
fn decode_rune(s: &[u8]) -> (char, usize) {
    let Some(chunk) = s.utf8_chunks().next() else {
        return (RUNE_ERROR, 0);
    };
    match chunk.valid().chars().next() {
        Some(c) => (c, c.len_utf8()),
        None => (RUNE_ERROR, 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pattern: &str, name: &str) -> Result<bool, BadPattern> {
        matches(pattern.as_bytes(), name.as_bytes())
    }

    /// Go's own table, `path/filepath/match_test.go` `matchTests` (Go 1.24), Unix rows, each
    /// checked against Go 1.24's `filepath.Match`.
    #[test]
    fn go_match_table() {
        let bad = Err(BadPattern);
        let rows: &[(&str, &str, Result<bool, BadPattern>)] = &[
            ("abc", "abc", Ok(true)),
            ("*", "abc", Ok(true)),
            ("*c", "abc", Ok(true)),
            ("a*", "a", Ok(true)),
            ("a*", "abc", Ok(true)),
            ("a*", "ab/c", Ok(false)),
            ("a*/b", "abc/b", Ok(true)),
            ("a*/b", "a/c/b", Ok(false)),
            ("a*b*c*d*e*/f", "axbxcxdxe/f", Ok(true)),
            ("a*b*c*d*e*/f", "axbxcxdxexxx/f", Ok(true)),
            ("a*b*c*d*e*/f", "axbxcxdxe/xxx/f", Ok(false)),
            ("a*b*c*d*e*/f", "axbxcxdxexxx/fff", Ok(false)),
            ("a*b?c*x", "abxbbxdbxebxczzx", Ok(true)),
            ("a*b?c*x", "abxbbxdbxebxczzy", Ok(false)),
            ("ab[c]", "abc", Ok(true)),
            ("ab[b-d]", "abc", Ok(true)),
            ("ab[e-g]", "abc", Ok(false)),
            ("ab[^c]", "abc", Ok(false)),
            ("ab[^b-d]", "abc", Ok(false)),
            ("ab[^e-g]", "abc", Ok(true)),
            ("a\\*b", "a*b", Ok(true)),
            ("a\\*b", "ab", Ok(false)),
            ("a?b", "a\u{263a}b", Ok(true)),
            ("a[^a]b", "a\u{263a}b", Ok(true)),
            ("a???b", "a\u{263a}b", Ok(false)),
            ("a[^a][^a][^a]b", "a\u{263a}b", Ok(false)),
            ("[a-\u{3b6}]*", "\u{3b1}", Ok(true)),
            ("*[a-\u{3b6}]", "A", Ok(false)),
            ("a?b", "a/b", Ok(false)),
            ("a*b", "a/b", Ok(false)),
            ("[\\]a]", "]", Ok(true)),
            ("[\\-]", "-", Ok(true)),
            ("[x\\-]", "x", Ok(true)),
            ("[x\\-]", "-", Ok(true)),
            ("[x\\-]", "z", Ok(false)),
            ("[\\-x]", "x", Ok(true)),
            ("[\\-x]", "-", Ok(true)),
            ("[\\-x]", "a", Ok(false)),
            ("[]a]", "]", bad),
            ("[-]", "-", bad),
            ("[x-]", "x", bad),
            ("[x-]", "-", bad),
            ("[x-]", "z", bad),
            ("[-x]", "x", bad),
            ("[-x]", "-", bad),
            ("[-x]", "a", bad),
            ("\\", "a", bad),
            ("[a-b-c]", "a", bad),
            ("[", "a", bad),
            ("[^", "a", bad),
            ("[^bc", "a", bad),
            ("a[", "a", bad),
            ("a[", "ab", bad),
            ("a[", "x", bad),
            ("a/b[", "x", bad),
            ("*x", "xxx", Ok(true)),
        ];
        for (pattern, name, want) in rows {
            assert_eq!(m(pattern, name), *want, "Match({pattern:?}, {name:?})");
        }
    }

    #[test]
    fn a_bad_pattern_is_an_error_only_for_names_that_reach_it() {
        assert_eq!(m("a*[", "b"), Ok(false));
        assert_eq!(m("a*[", "ab"), Err(BadPattern));
        assert_eq!(m("x*", "x["), Ok(true));
    }

    #[test]
    fn invalid_utf8_is_one_replacement_rune() {
        assert_eq!(matches(b"?", b"\xff"), Ok(true));
        assert_eq!(matches(b"??", b"\xff"), Ok(false));
        assert_eq!(matches(b"[\xff]", b"a"), Err(BadPattern));
    }
}
