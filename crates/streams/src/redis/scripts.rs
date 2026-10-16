//! The Lua scripts, on the stored format the SDKs used before the store moved into Core.
//!
//! The append script is the one the SDKs sent, with one more check, the topic's close flag, so a
//! batch can't land after the topic's close returned (DD-16), and its trim keeps what
//! positions-only readers hold. Redis doesn't roll a script back, so it runs every step that can
//! fail before its first record lands. `keep.lua` is the prelude of every script that writes a
//! log, and `floor.lua` of every script that reads or writes the holds.

use redis::Script;
use std::sync::LazyLock;

pub(crate) const APPEND: &str = concat!(
    include_str!("../../lua/floor.lua"),
    include_str!("../../lua/keep.lua"),
    include_str!("../../lua/append.lua")
);

pub(crate) static APPEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(APPEND));
// Marks the topic closed and keeps its first close's outcome in one step, so two closes can't both
// find no outcome and the second overwrite the first.
pub(crate) static CLOSE_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../../lua/close.lua")));
pub(crate) static HOLD_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(concat!(
        include_str!("../../lua/floor.lua"),
        include_str!("../../lua/hold.lua")
    ))
});
pub(crate) static RELEASE_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(concat!(
        include_str!("../../lua/floor.lua"),
        include_str!("../../lua/release.lua")
    ))
});

#[cfg(test)]
mod tests {
    use super::*;

    /// The close check the append script adds to the SDKs' one.
    const TOPIC_CHECK: &str = " or redis.call('HGET', KEYS[2], 'closed')";

    /// What the floor adds to the append script, each with what it replaces.
    const FLOOR_EDITS: [(&str, &str); 8] = [
        (
            "local function trim(log, meta, retention, floors)",
            "local function trim(log, meta, retention)",
        ),
        (
            "  local life\n  floor, life = held(floors, floor, retention)\n",
            "",
        ),
        ("  return floor_ms, life\n", "  return floor_ms\n"),
        (
            "local function keep(log, meta, added, last, grace, floor_ms, life)",
            "local function keep(log, meta, added, last, retention, grace, floor_ms)",
        ),
        (
            "redis.call('PEXPIRE', log, life)",
            "redis.call('PEXPIRE', log, retention)",
        ),
        (
            "redis.call('PEXPIRE', meta, life + grace)",
            "redis.call('PEXPIRE', meta, retention + grace)",
        ),
        (
            "local floor_ms, life = trim(KEYS[1], KEYS[2], retention, KEYS[4])",
            "local floor_ms = trim(KEYS[1], KEYS[2], retention)",
        ),
        (
            "keep(KEYS[1], KEYS[2], #ARGV - 5, last, grace, floor_ms, life)",
            "keep(KEYS[1], KEYS[2], #ARGV - 5, last, retention, grace, floor_ms)",
        ),
    ];

    #[test]
    fn the_append_script_is_the_one_before_the_floor_with_the_floor_edits_only() {
        assert_eq!(
            APPEND_SCRIPT.get_hash(),
            "ad509b51833fb21b5bdc61a87cba7dbdf8f15233"
        );
        let mut before_floor = APPEND
            .strip_prefix(include_str!("../../lua/floor.lua"))
            .unwrap()
            .to_string();
        for (added, was) in FLOOR_EDITS {
            assert_eq!(before_floor.matches(added).count(), 1, "{added}");
            before_floor = before_floor.replacen(added, was, 1);
        }
        assert_eq!(
            Script::new(&before_floor).get_hash(),
            "806e20d928e761eff5467386c0ffe78372e48bec"
        );
        assert_eq!(before_floor.matches(TOPIC_CHECK).count(), 1);
    }
}
