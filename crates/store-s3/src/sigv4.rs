//! AWS Signature Version 4 in the `Authorization` header, with the body's SHA-256 signed.
//!
//! Go's SDK signs every request this way (`x-amz-content-sha256` holds the hex SHA-256 of the
//! body, and the same value ends the canonical request), so the store refuses a body changed
//! in flight with `XAmzContentSHA256Mismatch`. The steps are the ones AWS documents: a
//! canonical request (method, path as sent, the sorted query, the signed headers, the payload
//! hash), a string to sign over its SHA-256, and an HMAC chain from the secret through the
//! day, region and service. HMAC-SHA-256 (RFC 2104) is written here over the `sha2` crate,
//! which picks the CPU's SHA instructions at run time (SHA-NI on x86-64, the `sha2`
//! extension on aarch64). The tests are AWS's own signature examples and RFC 4231's vectors.

use sha2::{Digest as _, Sha256};
use url::Url;

use crate::response::hex;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// The SHA-256 of a request body: what `x-amz-content-sha256` carries and the signature covers.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PayloadHash([u8; 32]);

impl PayloadHash {
    /// The hash of no body, which every request without one signs.
    pub const EMPTY: Self = Self([
        0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9,
        0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52,
        0xb8, 0x55,
    ]);

    /// The hash of `body`.
    #[must_use]
    pub fn of(body: &[u8]) -> Self {
        Self(Sha256::digest(body).into())
    }

    /// A hash taken in pieces, over a body read again as it is sent (`again.rs`).
    pub(crate) fn from_hasher(hasher: Sha256) -> Self {
        Self(hasher.finalize().into())
    }

    /// The lower-case hex form the header and the canonical request carry.
    #[must_use]
    pub fn to_hex(self) -> String {
        hex(&self.0)
    }
}

/// HMAC-SHA-256 (RFC 2104).
fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut block = [0_u8; 64];
    let hashed: [u8; 32];
    let key = if key.len() > block.len() {
        hashed = Sha256::digest(key).into();
        hashed.as_slice()
    } else {
        key
    };
    for (to, from) in block.iter_mut().zip(key) {
        *to = *from;
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|byte| byte ^ 0x36));
    inner.update(data);
    let mut outer = Sha256::new();
    outer.update(block.map(|byte| byte ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

/// RFC 3986 percent-encoding of all but the unreserved characters, as SigV4 encodes a query.
fn encode(text: &str, out: &mut String) {
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push_str(&hex(&[byte]).to_ascii_uppercase());
        }
    }
}

/// The query string as sent and as signed: each name and value encoded, sorted by name and
/// then value after encoding, as AWS sorts them.
pub(crate) fn canonical_query(query: &[(&str, &str)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(name, value)| {
            let (mut name_out, mut value_out) = (String::new(), String::new());
            encode(name, &mut name_out);
            encode(value, &mut value_out);
            (name_out, value_out)
        })
        .collect();
    // By name, then value: sorting the joined pairs instead would put `a-b=` before `a=`.
    pairs.sort_unstable();
    let mut out = String::new();
    for (name, value) in pairs {
        if !out.is_empty() {
            out.push('&');
        }
        out.push_str(&name);
        out.push('=');
        out.push_str(&value);
    }
    out
}

/// Who signs: the access key, its secret, and the region the signature is scoped to.
pub(crate) struct Key<'a> {
    pub(crate) id: &'a str,
    pub(crate) secret: &'a str,
    pub(crate) region: &'a str,
}

/// The `Authorization` value for a request to `url` (its path and query exactly as sent) at
/// `amz_date` (`YYYYMMDDTHHMMSSZ`). `headers` are every signed header, `host` among them,
/// with lower-case names in name order.
pub(crate) fn authorization(
    key: &Key<'_>,
    amz_date: &str,
    method: &str,
    url: &Url,
    headers: &[(&str, &str)],
    payload: PayloadHash,
) -> String {
    let mut canonical = format!("{method}\n{}\n{}\n", url.path(), url.query().unwrap_or(""));
    let mut signed = String::new();
    for (name, value) in headers {
        canonical.push_str(name);
        canonical.push(':');
        canonical.push_str(&value.split_whitespace().collect::<Vec<_>>().join(" "));
        canonical.push('\n');
        if !signed.is_empty() {
            signed.push(';');
        }
        signed.push_str(name);
    }
    canonical.push('\n');
    canonical.push_str(&signed);
    canonical.push('\n');
    canonical.push_str(&payload.to_hex());
    let day = amz_date.get(..8).unwrap_or(amz_date);
    let scope = format!("{day}/{}/s3/aws4_request", key.region);
    let to_sign = format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let mut signing = hmac(format!("AWS4{}", key.secret).as_bytes(), day.as_bytes());
    for part in [key.region, "s3", "aws4_request"] {
        signing = hmac(&signing, part.as_bytes());
    }
    let signature = hex(&hmac(&signing, to_sign.as_bytes()));
    format!(
        "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
        key.id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: Key<'static> = Key {
        id: "AKIAIOSFODNN7EXAMPLE",
        secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        region: "us-east-1",
    };
    const DATE: &str = "20130524T000000Z";
    const HOST: &str = "examplebucket.s3.amazonaws.com";

    fn signature(auth: &str) -> &str {
        auth.rsplit_once("Signature=").unwrap().1
    }

    /// AWS's "GET Object" example (sig-v4-header-based-auth), with a `Range` header.
    #[test]
    fn aws_get_object_example() {
        let url: Url = "https://examplebucket.s3.amazonaws.com/test.txt"
            .parse()
            .unwrap();
        let empty = PayloadHash::EMPTY.to_hex();
        let headers = [
            ("host", HOST),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", empty.as_str()),
            ("x-amz-date", DATE),
        ];
        let auth = authorization(&KEY, DATE, "GET", &url, &headers, PayloadHash::EMPTY);
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// AWS's "PUT Object" example: the signed payload hash is the body's.
    #[test]
    fn aws_put_object_example() {
        let url: Url = "https://examplebucket.s3.amazonaws.com/test%24file.text"
            .parse()
            .unwrap();
        let body = PayloadHash::of(b"Welcome to Amazon S3.");
        let hash = body.to_hex();
        assert_eq!(
            hash,
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
        );
        let headers = [
            ("date", "Fri, 24 May 2013 00:00:00 GMT"),
            ("host", HOST),
            ("x-amz-content-sha256", hash.as_str()),
            ("x-amz-date", DATE),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        ];
        let auth = authorization(&KEY, DATE, "PUT", &url, &headers, body);
        assert_eq!(
            signature(&auth),
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
        // The same request with another body signs differently.
        let other = authorization(&KEY, DATE, "PUT", &url, &headers, PayloadHash::EMPTY);
        assert_ne!(signature(&other), signature(&auth));
    }

    /// AWS's "GET Bucket (List Objects)" example: the query is sorted and signed.
    #[test]
    fn aws_list_objects_example() {
        let mut url: Url = "https://examplebucket.s3.amazonaws.com/".parse().unwrap();
        let query = canonical_query(&[("prefix", "J"), ("max-keys", "2")]);
        assert_eq!(query, "max-keys=2&prefix=J");
        url.set_query(Some(&query));
        let empty = PayloadHash::EMPTY.to_hex();
        let headers = [
            ("host", HOST),
            ("x-amz-content-sha256", empty.as_str()),
            ("x-amz-date", DATE),
        ];
        let auth = authorization(&KEY, DATE, "GET", &url, &headers, PayloadHash::EMPTY);
        assert_eq!(
            signature(&auth),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    #[test]
    fn the_empty_hash_is_the_hash_of_no_body() {
        assert_eq!(PayloadHash::of(b""), PayloadHash::EMPTY);
    }

    #[test]
    fn query_encoding_is_rfc_3986() {
        assert_eq!(
            canonical_query(&[("b", "x y/+~"), ("a", ""), ("a-b_c.d", "é")]),
            "a=&a-b_c.d=%C3%A9&b=x%20y%2F%2B~"
        );
    }

    /// RFC 4231 test cases 1, 2, 4, 6 and 7: a short key, a key below the block size, a
    /// block-sized key and two keys longer than a block (hashed first).
    #[test]
    fn hmac_matches_rfc_4231() {
        let cases: [(Vec<u8>, &[u8], &str); 5] = [
            (
                vec![0x0b; 20],
                b"Hi There",
                "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
            ),
            (
                b"Jefe".to_vec(),
                b"what do ya want for nothing?",
                "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
            ),
            (
                (1..=25).collect(),
                &[0xcd; 50],
                "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b",
            ),
            (
                vec![0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First",
                "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54",
            ),
            (
                vec![0xaa; 131],
                b"This is a test using a larger than block-size key and a larger than block-size data. The key needs to be hashed before being used by the HMAC algorithm.",
                "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2",
            ),
        ];
        for (key, data, want) in cases {
            assert_eq!(hex(&hmac(&key, data)), want);
        }
        // A key of exactly one block is used as it is, not hashed.
        assert_ne!(
            hex(&hmac(&[0xaa; 64], b"x")),
            hex(&hmac(&Sha256::digest([0xaa; 64]), b"x"))
        );
    }
}
