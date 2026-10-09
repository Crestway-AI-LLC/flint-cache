// SPDX-License-Identifier: Elastic-2.0
//! Redis's glob, over bytes: what `SCAN … MATCH`, `KEYS`, `HSCAN`, `SSCAN`
//! and `ZSCAN` filter with, and what a pattern subscription matches channels
//! by (ADR-0052 D5). One definition, because the two had drifted (BUG-0244).

/// Redis's glob (`stringmatchlen`), over bytes: `*`, `?`, `[...]` with `^`
/// and ranges, and `\` to take the next byte literally. `*` backtracks to
/// its last position only, so a pattern costs time linear in the channel
/// times the pattern, whatever it holds.
pub fn glob_match(pattern: &[u8], s: &[u8]) -> bool {
    let (mut p, mut i) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while i < s.len() {
        if p < pattern.len() {
            match pattern[p] {
                b'*' => {
                    while p < pattern.len() && pattern[p] == b'*' {
                        p += 1;
                    }
                    if p == pattern.len() {
                        return true;
                    }
                    star = Some((p, i));
                    continue;
                }
                b'?' => {
                    p += 1;
                    i += 1;
                    continue;
                }
                b'[' => {
                    let (matched, next) = class(pattern, p, s[i]);
                    if matched {
                        p = next;
                        i += 1;
                        continue;
                    }
                }
                b'\\' if p + 1 < pattern.len() => {
                    if pattern[p + 1] == s[i] {
                        p += 2;
                        i += 1;
                        continue;
                    }
                }
                c => {
                    if c == s[i] {
                        p += 1;
                        i += 1;
                        continue;
                    }
                }
            }
        }
        match star {
            Some((sp, si)) => {
                p = sp;
                i = si + 1;
                star = Some((sp, si + 1));
            }
            None => return false,
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

/// Match `c` against the class opening at `pattern[open]`, `[`. Answers
/// whether it matched and where the pattern continues. As in Redis, a class
/// with no closing `]` runs to the end of the pattern.
fn class(pattern: &[u8], open: usize, c: u8) -> (bool, usize) {
    let mut p = open + 1;
    let negate = pattern.get(p) == Some(&b'^');
    if negate {
        p += 1;
    }
    let mut matched = false;
    while p < pattern.len() && pattern[p] != b']' {
        if pattern[p] == b'\\' && p + 1 < pattern.len() {
            p += 1;
            matched |= pattern[p] == c;
        } else if p + 2 < pattern.len() && pattern[p + 1] == b'-' {
            let (lo, hi) = if pattern[p] <= pattern[p + 2] {
                (pattern[p], pattern[p + 2])
            } else {
                (pattern[p + 2], pattern[p])
            };
            matched |= (lo..=hi).contains(&c);
            p += 2;
        } else {
            matched |= pattern[p] == c;
        }
        p += 1;
    }
    // Past the `]`, or at the end of an unterminated class.
    let next = (p + 1).min(pattern.len());
    (matched != negate, next)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Redis's own `stringmatchlen` cases, and the traps in them.
    #[test]
    fn the_glob_is_redis_glob() {
        let yes: &[(&[u8], &[u8])] = &[
            (b"*", b""),
            (b"*", b"anything"),
            (b"h?llo", b"hello"),
            (b"h*llo", b"hllo"),
            (b"h*llo", b"heeeello"),
            (b"h[ae]llo", b"hallo"),
            (b"h[^e]llo", b"hallo"),
            (b"h[a-b]llo", b"hbllo"),
            (b"h[b-a]llo", b"hallo"),
            (b"h\\*llo", b"h*llo"),
            (b"/0.celery.pidbox", b"/0.celery.pidbox"),
            (b"news.*", b"news.art.figurative"),
            (b"*a*b*c*", b"xaxbxcx"),
            (b"[\\]]", b"]"),
        ];
        for (p, s) in yes {
            assert!(glob_match(p, s), "{:?} should match {:?}", p, s);
        }
        let no: &[(&[u8], &[u8])] = &[
            (b"h?llo", b"hllo"),
            (b"h[ae]llo", b"hillo"),
            (b"h[^e]llo", b"hello"),
            (b"h\\*llo", b"hello"),
            (b"news.*", b"new"),
            (b"a*b", b"acbd"),
            (b"", b"x"),
            // An unterminated class matches nothing, even itself.
            (b"a[", b"a["),
        ];
        for (p, s) in no {
            assert!(!glob_match(p, s), "{:?} should not match {:?}", p, s);
        }
        // Linear, not exponential: this would never finish by recursion.
        let pat = b"*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b".as_slice();
        assert!(!glob_match(pat, &[b'a'; 4096]));
    }

    /// BUG-0244: an unterminated class runs to the end of the pattern, as
    /// Redis's does; the matcher `SCAN` used answered no to each of these.
    /// From 2.4 million (pattern, key) pairs drawn at random and judged by
    /// Valkey 9.1's `KEYS`, on which this matcher and Valkey agree on every
    /// one.
    #[test]
    fn an_unterminated_class_runs_to_the_end_as_redis_reads_it() {
        let yes: &[(&[u8], &[u8])] = &[
            (b"?[b", b"ab"),
            (b"?[b", b"]b"),
            (b"?[b", b"*b"),
            (b"[**?a*", b"*"),
            (b"[**?a*", b"?"),
            (b"*^[-z[-z", b"-^b"),
            (b"*^[-z[-z", b"]^c"),
        ];
        for (p, s) in yes {
            assert!(glob_match(p, s), "{:?} should match {:?}", p, s);
        }
    }
}
