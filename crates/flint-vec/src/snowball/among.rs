// SPDX-License-Identifier: BSD-3-Clause
// Vendored from Snowball v2.1.0 (snowballstem/snowball, tag v2.1.0,
// commit 4764395431c8, the commit RediSearch's deps/snowball pins);
// licence and copyright in COPYING beside this file. Changed only in its
// `use` paths, from Rust 2015's `snowball::` to `super::`. ADR-0063.
use super::SnowballEnv;

pub struct Among<T: 'static>(pub &'static str,
                             pub i32,
                             pub i32,
                             pub Option<&'static (dyn Fn(&mut SnowballEnv, &mut T) -> bool + Sync)>);
