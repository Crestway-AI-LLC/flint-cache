// SPDX-License-Identifier: Elastic-2.0
//! Unicode's full case folding, as RediSearch folds a `SORTABLE` value with
//! ICU's `fold_string` (ADR-0063 amendment 1): `CaseFolding.txt`'s C and F
//! mappings, of Unicode 17.0.0, the version RediSearch's `icu_casemap` 2.2.0
//! carries. `table.rs` is generated from the file by
//! `tools/casefold_table.py`; the file and its licence (`COPYING`, Unicode
//! V3) are vendored beside it.

// The table as the script writes it.
#[rustfmt::skip]
mod table;

/// `s` case-folded: each character replaced by its full folding, `ß` by
/// `ss` and a final `ς` by `σ`.
pub fn fold(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii() {
            out.push(c.to_ascii_lowercase());
            continue;
        }
        match table::FOLDS.binary_search_by_key(&c, |&(from, _)| from) {
            Ok(i) => out.push_str(table::FOLDS[i].1),
            Err(_) => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::fold;
    use std::collections::HashMap;

    /// The C and F mappings of the vendored `CaseFolding.txt`, read here
    /// apart from the script that generated the table.
    fn file() -> HashMap<char, String> {
        let text = include_str!("CaseFolding.txt");
        assert!(
            text.starts_with("# CaseFolding-17.0.0.txt\n"),
            "the vendored file is Unicode 17.0.0's"
        );
        let ch = |h: &str| {
            u32::from_str_radix(h, 16)
                .ok()
                .and_then(char::from_u32)
                .expect("a code point")
        };
        let mut folds = HashMap::new();
        for line in text.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let f: Vec<&str> = line.split("; ").collect();
            if f[1] == "C" || f[1] == "F" {
                folds.insert(ch(f[0]), f[2].split(' ').map(ch).collect());
            }
        }
        folds
    }

    /// Every character folds as `CaseFolding.txt` says: as its C or F line
    /// gives, or to itself where it has neither.
    #[test]
    fn every_character_folds_as_case_folding_txt_says() {
        let want = file();
        assert_eq!(super::table::FOLDS.len(), want.len(), "one entry a line");
        assert!(want.len() > 1_400, "the whole file was read");
        let mut n = 0;
        for c in (0..=0x10_FFFF).filter_map(char::from_u32) {
            let to = want.get(&c).cloned().unwrap_or_else(|| c.to_string());
            assert_eq!(fold(&c.to_string()), to, "U+{:04X}", c as u32);
            n += 1;
        }
        assert_eq!(n, 0x11_0000 - 0x800, "every scalar value");
    }

    /// Where full folding differs from Rust's lower case (ADR-0063
    /// amendment 1, fold (ii)): `ß`, the final sigma, the dotted capital I,
    /// Cherokee.
    #[test]
    fn folding_is_not_lower_case() {
        assert_eq!(fold("Straße"), "strasse");
        assert_eq!(fold("ΟΔΟΣ οδος"), "οδοσ οδοσ");
        assert_eq!(fold("İ"), "i\u{307}");
        assert_eq!(fold("\u{13F8}\u{AB70}"), "\u{13F0}\u{13A0}");
        assert_eq!(fold("Banana ZEBRA apple"), "banana zebra apple");
    }
}
