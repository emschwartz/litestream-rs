//! Test-only edits to a built request, for the cases a correct builder never
//! produces: a wrong digest, a header sent without being signed.

use super::{HttpRequest, WriteRequest};

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

    /// Sets a query parameter, replacing any of that name, leaving the signature as it was.
    /// Giving `X-Amz-Signature` makes a damaged signature.
    #[must_use]
    pub fn tamper_query(mut self, name: &str, value: &str) -> Self {
        let kept: Vec<(String, String)> = self
            .url
            .query_pairs()
            .filter(|(have, _)| have != name)
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        {
            let mut pairs = self.url.query_pairs_mut();
            pairs.clear();
            pairs.extend_pairs(kept);
            pairs.append_pair(name, value);
        }
        self
    }

    /// Replaces the body, keeping the digest the builder promised.
    #[must_use]
    pub fn tamper_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }
}

impl WriteRequest {
    /// [`HttpRequest::tamper_header`] on the request, keeping its digest.
    #[must_use]
    pub fn tamper_header(self, name: &'static str, value: Option<String>) -> Self {
        Self {
            request: self.request.tamper_header(name, value),
            md5: self.md5,
        }
    }

    /// [`HttpRequest::tamper_body`] on the request, keeping its digest.
    #[must_use]
    pub fn tamper_body(self, body: Vec<u8>) -> Self {
        Self {
            request: self.request.tamper_body(body),
            md5: self.md5,
        }
    }
}
