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

    /// Sets a query parameter, replacing any of that name, leaving the signature as it was.
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

    /// Replaces the signature in the `Authorization` header with `signature`: a damaged one.
    #[must_use]
    pub fn tamper_signature(self, signature: &str) -> Self {
        let damaged = self
            .header("authorization")
            .and_then(|auth| auth.split_once("Signature="))
            .map(|(head, _)| format!("{head}Signature={signature}"));
        self.tamper_header("authorization", damaged)
    }

    /// Replaces the body, keeping the hash the request was signed with.
    #[must_use]
    pub fn tamper_body(mut self, body: Vec<u8>) -> Self {
        self.body = body.into();
        self
    }
}

impl HttpRequest {
    /// The headers its `Authorization` names as signed, for a test that checks every header
    /// sent is one of them.
    #[must_use]
    pub fn signed_headers(&self) -> Vec<&str> {
        self.header("authorization")
            .and_then(|auth| auth.split_once("SignedHeaders="))
            .and_then(|(_, rest)| rest.split(',').next())
            .map(|list| list.split(';').collect())
            .unwrap_or_default()
    }
}
