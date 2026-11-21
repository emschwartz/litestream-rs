//! The one JSON string writer, shared by the log lines, `/healthz` and the `lsr` command output
//! so all three escape the same way.

/// A JSON string literal: quotes, backslash and control characters escaped, everything else
/// as is.
#[must_use]
pub fn string(text: &str) -> String {
    let mut out = String::with_capacity(text.len().saturating_add(2));
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::string;

    #[test]
    fn quotes_backslashes_and_controls_are_escaped() {
        assert_eq!(string("plain"), "\"plain\"");
        assert_eq!(string("a\"b\\c"), r#""a\"b\\c""#);
        assert_eq!(string("l1\nl2\t\r"), r#""l1\nl2\t\r""#);
        assert_eq!(string("\u{1}é"), "\"\\u0001é\"");
    }
}
