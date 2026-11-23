//! The JSON string writer of the log lines, which are written by hand in slog's layout.
//! `/healthz` and `lsr`'s output are built with `serde_json`.

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
