// SPDX-License-Identifier: Elastic-2.0
//! Snowball's English stemmer, as RediSearch stems (ADR-0063): the Rust that
//! Snowball v2.1.0's own compiler generates from its `english.sbl`, the
//! version RediSearch's `deps/snowball` pins. Snowball's English changed in
//! 2025, so a newer one stems some words differently. The generated code and
//! its runtime are vendored beside this file, under Snowball's BSD licence
//! (`COPYING`); `english-voc.txt` and `english-output.txt` are
//! `snowball-data`'s English vocabulary and its expected stems (2837d237,
//! unchanged from 2006 through v2.1.0, `COPYING-data`).

// Snowball's code as it wrote it: not reformatted, and not held to this
// workspace's lints, which it was not written for.
#[rustfmt::skip]
#[allow(clippy::all, clippy::unwrap_used, dead_code, unused, rustdoc::all)]
mod among;
#[rustfmt::skip]
#[allow(clippy::all, clippy::unwrap_used, dead_code, unused, rustdoc::all)]
mod english_stemmer;
#[rustfmt::skip]
#[allow(clippy::all, clippy::unwrap_used, dead_code, unused, rustdoc::all)]
mod snowball_env;

pub use among::Among;
pub use snowball_env::SnowballEnv;

/// `word`'s English stem, as `libstemmer`'s `english` gives it. `word` is
/// already lowercased, as RediSearch lowercases a word before stemming it.
pub fn english(word: &str) -> String {
    let mut env = SnowballEnv::create(word);
    english_stemmer::stem(&mut env);
    env.get_current().into_owned()
}

#[cfg(test)]
mod tests {
    /// Every word of Snowball's English vocabulary stems to the stem its
    /// expected output gives, as v2.1.0's own `make check` holds it.
    #[test]
    fn every_english_word_stems_as_snowball_v2_1_0_stems_it() {
        let words = include_str!("english-voc.txt").lines();
        let stems = include_str!("english-output.txt").lines();
        let mut n = 0;
        for (w, want) in words.zip(stems) {
            assert_eq!(super::english(w), want, "the stem of {w:?}");
            n += 1;
        }
        assert_eq!(n, 29_417, "the whole vocabulary was read");
    }
}
