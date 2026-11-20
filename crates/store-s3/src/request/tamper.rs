//! Test-only edits to a built request, for the cases a correct builder never
//! produces: a wrong digest, a header sent without being signed.

use super::HttpRequest;

impl HttpRequest {
    /// Sets (or with `None` removes) a header, leaving the signature as it was.
    #[must_use]
    pub fn tamper_header(mut self, name: &'static str, value: Option<String>) -> Self {
        self.headers.retain(|(have, _)| *have != name);
        if let Some(value) = value {
            self.headers.push((name, value));
        }
        self
    }

    /// Appends a query parameter, leaving the signature as it was.
    #[must_use]
    pub fn tamper_query(mut self, name: &str, value: &str) -> Self {
        self.url.query_pairs_mut().append_pair(name, value);
        self
    }

    /// Replaces the body, keeping the digest the builder promised.
    #[must_use]
    pub fn tamper_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }
}
