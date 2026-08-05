// SPDX-License-Identifier: Elastic-2.0
//! An S3-compatible [`ObjectStore`] — ADR-0011 D8's transport, with no new
//! dependencies.
//!
//! The obvious move was the AWS SDK, and the user had approved it. It was
//! not taken, because the bill was out of proportion to the need: the SDK
//! brings tokio and ~100 transitive crates into a fully synchronous binary
//! to speak four requests — PUT, GET, ListObjectsV2, and nothing else.
//! Everything those four need is already in the tree: `ring` for the SigV4
//! HMAC chain (it signs every token digest in the fleet), `flint-tls` for
//! the connection, and the operating system's own CA bundle for trust.
//! ~400 lines here versus a dependency tree on the backup credential path;
//! the flip-audit surface stays what it was.
//!
//! Path-style addressing (`endpoint/bucket/key`), deliberately: D8's target
//! is "any S3-compatible endpoint, not AWS S3 specifically", and path-style
//! is the form MinIO and friends accept without wildcard DNS.
//!
//! One connection per request (`Connection: close`). Backup moves a small
//! number of large objects; connection reuse would buy latency on the many-
//! small-object workload this is not.

use flint_backup::store::ObjectStore;
use std::io::{self, Read, Write};

/// Everything needed to address one set under one bucket.
pub struct S3Store {
    /// `host[:port]` of the endpoint, e.g. `s3.us-east-1.amazonaws.com`.
    endpoint: String,
    bucket: String,
    /// Key prefix inside the bucket ("" = bucket root). The store's keys
    /// are relative to this, exactly as LocalDir's are relative to its root.
    prefix: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    tls: Option<std::sync::Arc<flint_tls::ClientConfig>>,
}

impl S3Store {
    /// Build from an `s3://bucket/prefix` spec plus the standard AWS
    /// environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional
    /// `AWS_SESSION_TOKEN`, `AWS_REGION`), `FLINT_S3_ENDPOINT` for
    /// non-AWS endpoints, and `FLINT_S3_CA` for a trust bundle when the
    /// well-known system paths don't hold one.
    ///
    /// Credentials come from the environment and nowhere else: the seat is
    /// the one process that holds them (ADR-0011 D8), and taking them as
    /// flags would put them in `ps` output on every host.
    pub fn from_spec(spec: &str) -> io::Result<Self> {
        let rest = spec
            .strip_prefix("s3://")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "not an s3:// spec"))?;
        let (bucket, prefix) = match rest.split_once('/') {
            Some((b, p)) => (b.to_string(), p.trim_matches('/').to_string()),
            None => (rest.to_string(), String::new()),
        };
        if bucket.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "s3:// spec names no bucket",
            ));
        }
        let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let region = env("AWS_REGION")
            .or_else(|| env("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|| "us-east-1".into());
        let endpoint =
            env("FLINT_S3_ENDPOINT").unwrap_or_else(|| format!("s3.{region}.amazonaws.com"));
        let access_key = env("AWS_ACCESS_KEY_ID").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "AWS_ACCESS_KEY_ID is not set")
        })?;
        let secret_key = env("AWS_SECRET_ACCESS_KEY").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "AWS_SECRET_ACCESS_KEY is not set",
            )
        })?;
        // Plain http is permitted ONLY when asked for by endpoint scheme —
        // a MinIO on a lab network — never inferred.
        let (endpoint, tls) = if let Some(h) = endpoint.strip_prefix("http://") {
            (h.to_string(), None)
        } else {
            let endpoint = endpoint
                .strip_prefix("https://")
                .unwrap_or(&endpoint)
                .to_string();
            let ca = env("FLINT_S3_CA")
                .or_else(|| {
                    // The OS's own trust store, wherever this OS keeps it.
                    ["/etc/ssl/cert.pem", "/etc/pki/tls/certs/ca-bundle.crt"]
                        .iter()
                        .find(|p| std::path::Path::new(p).exists())
                        .map(|p| p.to_string())
                })
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "no CA bundle found; set FLINT_S3_CA to a PEM bundle",
                    )
                })?;
            (endpoint, Some(flint_tls::edge_client_config(&ca)?))
        };
        Ok(Self {
            endpoint,
            bucket,
            prefix,
            region,
            access_key,
            secret_key,
            session_token: env("AWS_SESSION_TOKEN"),
            tls,
        })
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{key}", self.prefix)
        }
    }

    fn host(&self) -> &str {
        &self.endpoint
    }

    fn connect(&self) -> io::Result<flint_tls::Stream> {
        let addr = if self.endpoint.contains(':') {
            self.endpoint.clone()
        } else if self.tls.is_some() {
            format!("{}:443", self.endpoint)
        } else {
            format!("{}:80", self.endpoint)
        };
        let s = flint_tls::connect_edge(&addr, &self.tls)?;
        s.set_read_timeout(Some(std::time::Duration::from_secs(60)))?;
        s.set_write_timeout(Some(std::time::Duration::from_secs(60)))?;
        Ok(s)
    }

    /// One signed request. `payload_sha256` is the hex hash of the body the
    /// caller will send; the body itself is streamed by `send_body`.
    fn request(
        &self,
        method: &str,
        uri_path: &str,
        query: &[(String, String)],
        payload_sha256: &str,
        content_length: Option<u64>,
        send_body: &mut dyn FnMut(&mut flint_tls::Stream) -> io::Result<()>,
    ) -> io::Result<Response> {
        let amz_date = now_stamp();
        let scope_date = &amz_date[..8];

        let canonical_query = canonical_query(query);
        let mut signed_headers = vec![
            ("host".to_string(), self.host().to_string()),
            ("x-amz-content-sha256".to_string(), payload_sha256.into()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(t) = &self.session_token {
            signed_headers.push(("x-amz-security-token".to_string(), t.clone()));
        }
        signed_headers.sort();
        let header_list: String = signed_headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers: String = signed_headers
            .iter()
            .map(|(k, v)| format!("{k}:{}\n", v.trim()))
            .collect();
        let canonical_request = format!(
            "{method}\n{uri_path}\n{canonical_query}\n{canonical_headers}\n{header_list}\n{payload_sha256}"
        );
        let scope = format!("{scope_date}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            flint_tls::sha256_hex(canonical_request.as_bytes())
        );
        let signature = hex(&sign(
            &signing_key(&self.secret_key, scope_date, &self.region, "s3"),
            string_to_sign.as_bytes(),
        ));
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={header_list}, Signature={signature}",
            self.access_key
        );

        let mut s = self.connect()?;
        let mut head = String::new();
        let target = if canonical_query.is_empty() {
            uri_path.to_string()
        } else {
            format!("{uri_path}?{canonical_query}")
        };
        head.push_str(&format!("{method} {target} HTTP/1.1\r\n"));
        for (k, v) in &signed_headers {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        head.push_str(&format!("authorization: {auth}\r\n"));
        if let Some(n) = content_length {
            head.push_str(&format!("content-length: {n}\r\n"));
        }
        head.push_str("connection: close\r\n\r\n");
        s.write_all(head.as_bytes())?;
        send_body(&mut s)?;
        read_response(s)
    }

    fn simple(&self, method: &str, key: &str, query: &[(String, String)]) -> io::Result<Response> {
        let path = format!(
            "/{}/{}",
            uri_encode(&self.bucket, false),
            uri_encode(&self.full_key(key), false)
        );
        self.request(method, &path, query, EMPTY_SHA256, None, &mut |_| Ok(()))
    }
}

/// SHA-256 of the empty payload, the constant every bodyless request signs.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

impl ObjectStore for S3Store {
    fn open(&self, key: &str) -> io::Result<Box<dyn Read>> {
        let resp = self.simple("GET", key, &[])?;
        match resp.status {
            200 => Ok(Box::new(resp.into_body())),
            404 => Err(io::Error::new(io::ErrorKind::NotFound, key.to_string())),
            s => Err(io::Error::other(format!("GET {key}: HTTP {s}"))),
        }
    }

    fn put_file(&self, key: &str, from: &std::path::Path) -> io::Result<()> {
        // The payload hash is part of the SIGNATURE, so the file is read
        // twice: once to hash, once to send. Two sequential passes over a
        // page-warm file beat holding an SST of unbounded size in memory,
        // and an unsigned-payload upload would waive exactly the integrity
        // property this store exists to keep.
        let sha = flint_tls::sha256_stream_hex(&mut std::fs::File::open(from)?)?;
        let len = std::fs::metadata(from)?.len();
        let path = format!(
            "/{}/{}",
            uri_encode(&self.bucket, false),
            uri_encode(&self.full_key(key), false)
        );
        let resp = self.request("PUT", &path, &[], &sha, Some(len), &mut |s| {
            let mut f = std::fs::File::open(from)?;
            let mut buf = vec![0u8; 256 * 1024];
            loop {
                let n = f.read(&mut buf)?;
                if n == 0 {
                    return Ok(());
                }
                s.write_all(&buf[..n])?;
            }
        })?;
        match resp.status {
            200 => Ok(()),
            s => Err(io::Error::other(format!(
                "PUT {key}: HTTP {s}: {}",
                resp.body_text()
            ))),
        }
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let full = self.full_key(prefix);
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), full.clone()),
            ];
            if let Some(t) = &token {
                q.push(("continuation-token".to_string(), t.clone()));
            }
            let path = format!("/{}", uri_encode(&self.bucket, false));
            let resp = self.request("GET", &path, &q, EMPTY_SHA256, None, &mut |_| Ok(()))?;
            if resp.status != 200 {
                let status = resp.status;
                return Err(io::Error::other(format!(
                    "ListObjectsV2: HTTP {status}: {}",
                    resp.body_text()
                )));
            }
            let xml = resp.body_text();
            for key in xml_values(&xml, "Key") {
                // Report keys relative to the store's prefix, as LocalDir
                // does — callers must not learn which store they are on.
                let rel = if self.prefix.is_empty() {
                    key
                } else {
                    match key.strip_prefix(&format!("{}/", self.prefix)) {
                        Some(r) => r.to_string(),
                        None => continue,
                    }
                };
                out.push(rel);
            }
            if xml_values(&xml, "IsTruncated").first().map(String::as_str) == Some("true") {
                token = xml_values(&xml, "NextContinuationToken").into_iter().next();
                if token.is_none() {
                    return Err(io::Error::other("truncated listing without a token"));
                }
            } else {
                break;
            }
        }
        out.sort();
        Ok(out)
    }

    fn read(&self, key: &str) -> io::Result<Vec<u8>> {
        let mut r = self.open(key)?;
        let mut v = Vec::new();
        r.read_to_end(&mut v)?;
        Ok(v)
    }

    fn write(&self, key: &str, bytes: &[u8]) -> io::Result<()> {
        let sha = flint_tls::sha256_hex(bytes);
        let path = format!(
            "/{}/{}",
            uri_encode(&self.bucket, false),
            uri_encode(&self.full_key(key), false)
        );
        let resp = self.request(
            "PUT",
            &path,
            &[],
            &sha,
            Some(bytes.len() as u64),
            &mut |s| s.write_all(bytes),
        )?;
        match resp.status {
            200 => Ok(()),
            s => Err(io::Error::other(format!(
                "PUT {key}: HTTP {s}: {}",
                resp.body_text()
            ))),
        }
    }
}

// ---------- SigV4 primitives ----------

fn sign(key: &[u8], data: &[u8]) -> Vec<u8> {
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    ring::hmac::sign(&k, data).as_ref().to_vec()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The AWS4 key-derivation chain. Pinned by a unit test against the
/// worked example in AWS's own signature documentation, because a signer
/// that is subtly wrong fails as `SignatureDoesNotMatch` with no hint of
/// which of the four steps disagreed.
fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k = sign(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k = sign(&k, region.as_bytes());
    let k = sign(&k, service.as_bytes());
    sign(&k, b"aws4_request")
}

/// RFC 3986 unreserved-set encoding, the SigV4 flavor: `/` is preserved in
/// paths and encoded in query values.
fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn canonical_query(params: &[(String, String)]) -> String {
    let mut enc: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
        .collect();
    enc.sort();
    enc.join("&")
}

// ---------- a UTC clock without a chrono dependency ----------

/// A Unix timestamp as SigV4's `YYYYMMDDTHHMMSSZ`, via civil-from-days
/// (Howard Hinnant's algorithm) — how a timestamp becomes a calendar date
/// without pulling a date crate onto the credential path. A function of its
/// input so the test can pin it against a real calendar; the signing path
/// calls it with now.
fn amz_stamp(secs: i64) -> String {
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

fn now_stamp() -> String {
    amz_stamp(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
    )
}

// ---------- minimal HTTP/1.1 response handling ----------

struct Response {
    status: u16,
    content_length: Option<u64>,
    chunked: bool,
    stream: flint_tls::Stream,
    /// Bytes read past the header while finding its end.
    buffered: Vec<u8>,
}

fn read_response(mut s: flint_tls::Stream) -> io::Result<Response> {
    let mut buf = Vec::with_capacity(2048);
    let mut chunk = [0u8; 2048];
    let header_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        let n = s.read(&mut chunk)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "closed before headers",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad status line"))?;
    let mut content_length = None;
    let mut chunked = false;
    for line in head.lines().skip(1) {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.eq_ignore_ascii_case("content-length") {
            content_length = v.trim().parse().ok();
        } else if k.eq_ignore_ascii_case("transfer-encoding")
            && v.trim().eq_ignore_ascii_case("chunked")
        {
            chunked = true;
        }
    }
    Ok(Response {
        status,
        content_length,
        chunked,
        stream: s,
        buffered: buf[header_end..].to_vec(),
    })
}

impl Response {
    fn into_body(self) -> BodyReader {
        BodyReader {
            remaining: self.content_length,
            chunked: self.chunked,
            chunk_left: 0,
            chunk_done: false,
            buffered: self.buffered,
            pos: 0,
            stream: self.stream,
        }
    }

    fn body_text(self) -> String {
        let mut r = self.into_body();
        let mut v = Vec::new();
        let _ = r.read_to_end(&mut v);
        String::from_utf8_lossy(&v).into_owned()
    }
}

/// Streams a response body: `Content-Length`-bounded, chunked, or (with
/// `Connection: close`) until EOF.
struct BodyReader {
    remaining: Option<u64>,
    chunked: bool,
    chunk_left: u64,
    chunk_done: bool,
    buffered: Vec<u8>,
    pos: usize,
    stream: flint_tls::Stream,
}

impl BodyReader {
    fn raw_read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos < self.buffered.len() {
            let n = (self.buffered.len() - self.pos).min(out.len());
            out[..n].copy_from_slice(&self.buffered[self.pos..self.pos + n]);
            self.pos += n;
            return Ok(n);
        }
        self.stream.read(out)
    }

    fn raw_read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        let mut off = 0;
        while off < out.len() {
            let n = self.raw_read(&mut out[off..])?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "body cut"));
            }
            off += n;
        }
        Ok(())
    }

    fn raw_read_line(&mut self) -> io::Result<String> {
        let mut line = Vec::new();
        let mut b = [0u8; 1];
        loop {
            self.raw_read_exact(&mut b)?;
            line.push(b[0]);
            if line.ends_with(b"\r\n") {
                line.truncate(line.len() - 2);
                return Ok(String::from_utf8_lossy(&line).into_owned());
            }
        }
    }
}

impl Read for BodyReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.chunked {
            if self.chunk_done {
                return Ok(0);
            }
            if self.chunk_left == 0 {
                let size_line = self.raw_read_line()?;
                let size = u64::from_str_radix(size_line.split(';').next().unwrap_or(""), 16)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad chunk size"))?;
                if size == 0 {
                    let _ = self.raw_read_line();
                    self.chunk_done = true;
                    return Ok(0);
                }
                self.chunk_left = size;
            }
            let want = (self.chunk_left.min(out.len() as u64)) as usize;
            let n = self.raw_read(&mut out[..want])?;
            self.chunk_left -= n as u64;
            if self.chunk_left == 0 {
                let mut crlf = [0u8; 2];
                self.raw_read_exact(&mut crlf)?;
            }
            return Ok(n);
        }
        match self.remaining {
            Some(0) => Ok(0),
            Some(rem) => {
                let want = (rem.min(out.len() as u64)) as usize;
                let n = self.raw_read(&mut out[..want])?;
                self.remaining = Some(rem - n as u64);
                Ok(n)
            }
            None => self.raw_read(out),
        }
    }
}

/// Every text between `<tag>` and `</tag>`, in order. Enough XML for
/// ListObjectsV2, whose schema is flat where we read it; a real parser for
/// a four-element vocabulary would be the dependency this module exists to
/// avoid.
fn xml_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else { break };
        out.push(xml_unescape(&after[..j]));
        rest = &after[j + close.len()..];
    }
    out
}

fn xml_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worked example from AWS's "deriving the signing key"
    /// documentation — the one place a known-good signature can be checked
    /// without a network. A signer that is subtly wrong produces only
    /// `SignatureDoesNotMatch`, so this vector is the difference between a
    /// broken signer diagnosed here and one diagnosed against a live
    /// endpoint at midnight.
    #[test]
    fn signing_key_matches_the_aws_worked_example() {
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex(&key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    #[test]
    fn canonical_query_sorts_and_encodes() {
        let q = canonical_query(&[
            ("prefix".into(), "sets/backup 1/".into()),
            ("list-type".into(), "2".into()),
        ]);
        assert_eq!(q, "list-type=2&prefix=sets%2Fbackup%201%2F");
    }

    #[test]
    fn uri_encoding_preserves_path_slashes_only() {
        assert_eq!(uri_encode("pairs/0/a b.sst", false), "pairs/0/a%20b.sst");
        assert_eq!(uri_encode("pairs/0", true), "pairs%2F0");
    }

    #[test]
    fn the_civil_calendar_is_right_where_it_matters() {
        // Fixed conversions checked against python's datetime, including a
        // leap day: the signature scope is derived from this date, and a
        // signer a day off at a UTC boundary fails only at midnight — the
        // worst time to learn your calendar arithmetic is wrong. (The first
        // version of this test hand-computed the third vector, got it wrong
        // by ten minutes, and also re-implemented the algorithm inline —
        // the two classic ways a test verifies nothing, in one test.)
        for (secs, want) in [
            (0i64, "19700101T000000Z"),
            (951_782_400, "20000229T000000Z"),
            (1_754_000_000, "20250731T221320Z"),
        ] {
            assert_eq!(amz_stamp(secs), want);
        }
    }

    #[test]
    fn list_xml_extraction_handles_escapes_and_order() {
        let xml = "<ListBucketResult><Contents><Key>a/b&amp;c</Key></Contents>\
                   <Contents><Key>a/d</Key></Contents>\
                   <IsTruncated>false</IsTruncated></ListBucketResult>";
        assert_eq!(xml_values(xml, "Key"), ["a/b&c", "a/d"]);
        assert_eq!(xml_values(xml, "IsTruncated"), ["false"]);
    }
}
