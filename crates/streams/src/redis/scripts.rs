//! The Lua scripts, byte for byte what the SDKs sent before the store moved into Core.
//!
//! Identical bytes give the same SHA-1, so a server that cached a script for an SDK's provider
//! serves Core from the same cache entry, and both sides keep one behavior. `keep.lua` is the
//! prelude of every script that writes a log.

use redis::Script;
use std::sync::LazyLock;

pub(crate) const APPEND: &str = concat!(
    include_str!("../../lua/keep.lua"),
    include_str!("../../lua/append.lua")
);

pub(crate) static APPEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(APPEND));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_scripts_are_the_ones_the_python_sdk_sends() {
        assert_eq!(
            APPEND_SCRIPT.get_hash(),
            "7944489663d477b49e3f4f947933e15de3269718"
        );
    }
}
