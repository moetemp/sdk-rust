//! The Lua scripts, on the stored format the SDKs used before the store moved into Core.
//!
//! Redis doesn't roll a script back, so the append script runs every step that can fail before
//! its first record lands. `keep.lua` is the prelude of every script that writes a log.

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
            "f941565517f6f874c75ede57534b46e811f2c6fe"
        );
    }
}
