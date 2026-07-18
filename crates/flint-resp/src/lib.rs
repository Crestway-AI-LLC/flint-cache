// SPDX-License-Identifier: Elastic-2.0
//! RESP2 wire protocol: encoding and incremental decoding.
//!
//! Scope (v0): RESP2 only — every Redis client speaks it. RESP3 is a later,
//! additive concern. The decoder is incremental: it consumes from a byte
//! slice and reports `NeedMore` on partial frames, so the server can read
//! from sockets without framing assumptions.

/// A RESP2 value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// `+OK\r\n`
    Simple(String),
    /// `-ERR message\r\n`
    Error(String),
    /// `:42\r\n`
    Integer(i64),
    /// `$5\r\nhello\r\n`; `None` is the null bulk string `$-1\r\n`.
    Bulk(Option<Vec<u8>>),
    /// `*2\r\n...`; `None` is the null array `*-1\r\n`.
    Array(Option<Vec<Value>>),
}

/// Decoding outcome for a single frame.
#[derive(Debug, PartialEq, Eq)]
pub enum Decoded {
    /// A complete value and the number of input bytes it consumed.
    Complete(Value, usize),
    /// The input ends mid-frame; read more bytes and retry.
    NeedMore,
}

/// Errors for malformed frames (protocol violations, not partial input).
#[derive(Debug, PartialEq, Eq)]
pub enum ProtocolError {
    UnknownType(u8),
    BadInteger,
    BadLength,
    MissingCrlf,
    /// Nesting deeper than the decoder permits.
    TooDeep,
}

const MAX_DEPTH: usize = 32;

/// Largest accepted bulk-string payload (Redis `proto-max-bulk-len`).
/// Declared lengths are rejected at header-parse time, BEFORE any payload
/// arrives — otherwise a 5-byte `$4294967296\r\n` header commits the
/// server to buffering 4GB from that connection.
pub const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Largest accepted array element count (Redis caps multibulk at 1M).
pub const MAX_ARRAY_LEN: usize = 1024 * 1024;

pub fn encode(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Simple(s) => {
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Error(s) => {
            out.push(b'-');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Integer(i) => {
            out.push(b':');
            out.extend_from_slice(i.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
        Value::Bulk(Some(data)) => {
            out.push(b'$');
            out.extend_from_slice(data.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
        }
        Value::Array(None) => out.extend_from_slice(b"*-1\r\n"),
        Value::Array(Some(items)) => {
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode(item, out);
            }
        }
    }
}

/// Decode one frame from the front of `input`.
pub fn decode(input: &[u8]) -> Result<Decoded, ProtocolError> {
    decode_at(input, 0)
}

fn decode_at(input: &[u8], depth: usize) -> Result<Decoded, ProtocolError> {
    if depth > MAX_DEPTH {
        return Err(ProtocolError::TooDeep);
    }
    let Some(&type_byte) = input.first() else {
        return Ok(Decoded::NeedMore);
    };
    match type_byte {
        b'+' | b'-' | b':' => {
            let Some(line_end) = find_crlf(&input[1..]) else {
                return Ok(Decoded::NeedMore);
            };
            let line = &input[1..1 + line_end];
            let consumed = 1 + line_end + 2;
            let value = match type_byte {
                b'+' => Value::Simple(to_string(line)?),
                b'-' => Value::Error(to_string(line)?),
                _ => Value::Integer(parse_int(line)?),
            };
            Ok(Decoded::Complete(value, consumed))
        }
        b'$' => {
            let Some(line_end) = find_crlf(&input[1..]) else {
                return Ok(Decoded::NeedMore);
            };
            let len = parse_int(&input[1..1 + line_end])?;
            let header = 1 + line_end + 2;
            if len == -1 {
                return Ok(Decoded::Complete(Value::Bulk(None), header));
            }
            let len = usize::try_from(len).map_err(|_| ProtocolError::BadLength)?;
            if len > MAX_BULK_LEN {
                return Err(ProtocolError::BadLength);
            }
            let total = header + len + 2;
            if input.len() < total {
                return Ok(Decoded::NeedMore);
            }
            if &input[header + len..total] != b"\r\n" {
                return Err(ProtocolError::MissingCrlf);
            }
            let data = input[header..header + len].to_vec();
            Ok(Decoded::Complete(Value::Bulk(Some(data)), total))
        }
        b'*' => {
            let Some(line_end) = find_crlf(&input[1..]) else {
                return Ok(Decoded::NeedMore);
            };
            let len = parse_int(&input[1..1 + line_end])?;
            let mut offset = 1 + line_end + 2;
            if len == -1 {
                return Ok(Decoded::Complete(Value::Array(None), offset));
            }
            let len = usize::try_from(len).map_err(|_| ProtocolError::BadLength)?;
            if len > MAX_ARRAY_LEN {
                return Err(ProtocolError::BadLength);
            }
            let mut items = Vec::with_capacity(len.min(1024));
            for _ in 0..len {
                match decode_at(&input[offset..], depth + 1)? {
                    Decoded::Complete(value, used) => {
                        items.push(value);
                        offset += used;
                    }
                    Decoded::NeedMore => return Ok(Decoded::NeedMore),
                }
            }
            Ok(Decoded::Complete(Value::Array(Some(items)), offset))
        }
        other => Err(ProtocolError::UnknownType(other)),
    }
}

fn find_crlf(input: &[u8]) -> Option<usize> {
    input.windows(2).position(|w| w == b"\r\n")
}

fn to_string(line: &[u8]) -> Result<String, ProtocolError> {
    String::from_utf8(line.to_vec()).map_err(|_| ProtocolError::BadInteger)
}

fn parse_int(line: &[u8]) -> Result<i64, ProtocolError> {
    let s = std::str::from_utf8(line).map_err(|_| ProtocolError::BadInteger)?;
    s.parse().map_err(|_| ProtocolError::BadInteger)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(value: Value) {
        let mut buf = Vec::new();
        encode(&value, &mut buf);
        match decode(&buf) {
            Ok(Decoded::Complete(decoded, consumed)) => {
                assert_eq!(decoded, value);
                assert_eq!(consumed, buf.len());
            }
            other => panic!("roundtrip failed: {other:?}"),
        }
    }

    #[test]
    fn roundtrips() {
        roundtrip(Value::Simple("OK".into()));
        roundtrip(Value::Error("ERR unknown command".into()));
        roundtrip(Value::Integer(-42));
        roundtrip(Value::Bulk(None));
        roundtrip(Value::Bulk(Some(b"hello\r\nworld".to_vec())));
        roundtrip(Value::Array(None));
        roundtrip(Value::Array(Some(vec![
            Value::Bulk(Some(b"SET".to_vec())),
            Value::Bulk(Some(b"key".to_vec())),
            Value::Bulk(Some(b"value".to_vec())),
            Value::Array(Some(vec![Value::Integer(1)])),
        ])));
    }

    #[test]
    fn partial_frames_need_more() {
        let mut buf = Vec::new();
        encode(
            &Value::Array(Some(vec![
                Value::Bulk(Some(b"GET".to_vec())),
                Value::Bulk(Some(b"k".to_vec())),
            ])),
            &mut buf,
        );
        for cut in 0..buf.len() {
            assert_eq!(
                decode(&buf[..cut]),
                Ok(Decoded::NeedMore),
                "prefix of {cut} bytes should be incomplete"
            );
        }
    }

    #[test]
    fn pipelined_frames_report_exact_consumption() {
        let mut buf = Vec::new();
        encode(&Value::Simple("OK".into()), &mut buf);
        let first_len = buf.len();
        encode(&Value::Integer(7), &mut buf);
        let Ok(Decoded::Complete(v, used)) = decode(&buf) else {
            panic!("first frame should decode");
        };
        assert_eq!(v, Value::Simple("OK".into()));
        assert_eq!(used, first_len);
        let Ok(Decoded::Complete(v2, _)) = decode(&buf[used..]) else {
            panic!("second frame should decode");
        };
        assert_eq!(v2, Value::Integer(7));
    }

    #[test]
    fn malformed_input_errors() {
        assert_eq!(decode(b"?bogus\r\n"), Err(ProtocolError::UnknownType(b'?')));
        assert_eq!(decode(b":notanum\r\n"), Err(ProtocolError::BadInteger));
        assert_eq!(decode(b"$-2\r\n"), Err(ProtocolError::BadLength));
        assert_eq!(decode(b"$3\r\nabcXY"), Err(ProtocolError::MissingCrlf));
    }

    /// The caps must fire on the HEADER alone: waiting for payload bytes
    /// would defeat their purpose (bounding what a connection can make the
    /// server buffer).
    #[test]
    fn oversized_declared_lengths_are_rejected_from_the_header() {
        // 4GB bulk declaration, zero payload bytes sent.
        assert_eq!(decode(b"$4294967296\r\n"), Err(ProtocolError::BadLength));
        // One past the cap fails; the cap itself parses (NeedMore: header
        // accepted, awaiting payload).
        let over = format!("${}\r\n", MAX_BULK_LEN + 1);
        assert_eq!(decode(over.as_bytes()), Err(ProtocolError::BadLength));
        let at = format!("${MAX_BULK_LEN}\r\n");
        assert_eq!(decode(at.as_bytes()), Ok(Decoded::NeedMore));
        // Same for array element counts.
        let over = format!("*{}\r\n", MAX_ARRAY_LEN + 1);
        assert_eq!(decode(over.as_bytes()), Err(ProtocolError::BadLength));
        let at = format!("*{MAX_ARRAY_LEN}\r\n");
        assert_eq!(decode(at.as_bytes()), Ok(Decoded::NeedMore));
    }

    #[test]
    fn deep_nesting_is_rejected() {
        let mut buf = Vec::new();
        for _ in 0..64 {
            buf.extend_from_slice(b"*1\r\n");
        }
        buf.extend_from_slice(b":1\r\n");
        assert_eq!(decode(&buf), Err(ProtocolError::TooDeep));
    }
}
