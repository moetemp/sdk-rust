//! The Lua scripts, on the stored format the SDKs used before the store moved into Core.
//!
//! The append script is the one the SDKs sent, with one more check, the topic's close flag, so a
//! batch can't land after the topic's close returned (DD-16). It also reports the topic's arm, so
//! a producer notifies only while a reader sleeps (DD-118). Redis doesn't roll a script back, so
//! it runs every step that can fail before its first record lands. `keep.lua` is the prelude of
//! every script that writes a log.

use redis::Script;
use std::sync::LazyLock;

pub(crate) const APPEND: &str = concat!(
    include_str!("../../lua/keep.lua"),
    include_str!("../../lua/append.lua")
);

pub(crate) static APPEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(APPEND));
pub(crate) static ARM_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../../lua/arm.lua")));
pub(crate) static DISARM_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../../lua/disarm.lua")));
// Marks the topic closed and keeps its first close's outcome in one step, so two closes can't both
// find no outcome and the second overwrite the first.
pub(crate) static CLOSE_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../../lua/close.lua")));

#[cfg(test)]
mod tests {
    use super::*;

    /// The close check the append script adds to the SDKs' one.
    const TOPIC_CHECK: &str = " or redis.call('HGET', KEYS[2], 'closed')";
    /// The arm the append script adds to each of its answers.
    const APPEND_ARM: &str = ", redis.call('HGET', KEYS[2], 'arm') or '0'";

    #[test]
    fn the_append_script_is_pinned() {
        assert_eq!(
            APPEND_SCRIPT.get_hash(),
            "0eca2d1cf90ab743ca84471387b1494768599832"
        );
        assert_eq!(APPEND.matches(TOPIC_CHECK).count(), 1);
        assert_eq!(APPEND.matches(APPEND_ARM).count(), 2);
    }
}
