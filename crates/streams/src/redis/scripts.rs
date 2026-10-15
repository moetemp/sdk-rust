//! The Lua scripts, on the stored format the SDKs used before the store moved into Core.
//!
//! The append script is the one the SDKs sent, with one more check, the topic's close flag, so a
//! batch can't land after the topic's close returned (DD-16), and its trim keeps what
//! positions-only readers hold. `keep.lua` is the prelude of every script that writes a log, and
//! `floor.lua` of every script that reads or writes the holds.

use redis::Script;
use std::sync::LazyLock;

pub(crate) const APPEND: &str = concat!(
    include_str!("../../lua/floor.lua"),
    include_str!("../../lua/keep.lua"),
    include_str!("../../lua/append.lua")
);

pub(crate) static APPEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(APPEND));

/// The line that names the key of a Workflow Activity's topic set, which only its appends pass.
/// It is the key after the notification stream when the store keeps a notification log.
const TOPICS_KEY: &str = "local topics = KEYS[5]\n";

/// What a store with a notification log writes after the batch, in place of the append's last
/// answer: an entry in the chain's notification stream (`KEYS[5]`), and the stream's length, so
/// Core rings the doorbell when the entry is the stream's only one. A repeat writes no entry.
const LOGGED_ANSWER: &str = "redis.call('XADD', KEYS[5], 'MAXLEN', '~', 10000, '*', \
'k', KEYS[1], 'p', last)
return {first, last, redis.call('XLEN', KEYS[5])}
";

/// [APPEND] with its last answer replaced by [LOGGED_ANSWER], and the topic set as `KEYS[6]`.
pub(crate) static APPEND_LOGGED: LazyLock<String> = LazyLock::new(|| {
    let (body, last) = APPEND
        .trim_end()
        .rsplit_once('\n')
        .expect("the append script has a last line");
    assert_eq!(last, "return {first, last}");
    assert_eq!(body.matches(TOPICS_KEY).count(), 1);
    let body = body.replacen(TOPICS_KEY, "local topics = KEYS[6]\n", 1);
    format!("{body}\n{LOGGED_ANSWER}")
});

pub(crate) static APPEND_LOGGED_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(&APPEND_LOGGED));
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

    /// What the append script adds for a Workflow Activity's topic set.
    const TOPIC_SET: &str = "local topics = KEYS[5]
if topics then
  if sequence == 1 then
    redis.call('SADD', topics, KEYS[1])
  end
  redis.call('PEXPIRE', topics, retention + grace)
end
";

    /// What the floor adds to the append script, each with what it replaces.
    const FLOOR_EDITS: [(&str, &str); 5] = [
        (
            "local function keep(log, meta, added, last, retention, grace, floors)",
            "local function keep(log, meta, added, last, retention, grace)",
        ),
        (
            "  local life\n  floor, life = held(floors, floor, retention)\n",
            "",
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
            "keep(KEYS[1], KEYS[2], #ARGV - 5, last, retention, grace, KEYS[4])",
            "keep(KEYS[1], KEYS[2], #ARGV - 5, last, retention, grace)",
        ),
    ];

    #[test]
    fn the_logged_append_differs_in_its_topic_set_key_and_last_answer_only() {
        assert_eq!(
            APPEND_LOGGED
                .replacen(LOGGED_ANSWER, "return {first, last}\n", 1)
                .replacen("local topics = KEYS[6]\n", TOPICS_KEY, 1),
            APPEND
        );
        assert_eq!(APPEND_LOGGED.matches("XADD', KEYS[5]").count(), 1);
        assert_eq!(
            APPEND_LOGGED_SCRIPT.get_hash(),
            "418b9d100738aa50bc1ee5c1b7e61e62c8f9c434"
        );
    }

    #[test]
    fn the_append_script_matches_the_sdks_with_the_topic_check_floor_and_topic_set_only() {
        assert_eq!(
            APPEND_SCRIPT.get_hash(),
            "eec20388c2d5655389344177b25a640e1ba3f625"
        );
        let mut before_floor = APPEND
            .strip_prefix(include_str!("../../lua/floor.lua"))
            .unwrap()
            .to_string();
        assert_eq!(before_floor.matches(TOPIC_SET).count(), 1);
        before_floor = before_floor.replacen(TOPIC_SET, "", 1);
        for (added, was) in FLOOR_EDITS {
            assert_eq!(before_floor.matches(added).count(), 1, "{added}");
            before_floor = before_floor.replacen(added, was, 1);
        }
        assert_eq!(
            Script::new(&before_floor).get_hash(),
            "96d2eb81ddf241103a1ed5495a015285d7bd37f0"
        );
        assert_eq!(before_floor.matches(TOPIC_CHECK).count(), 1);
        let sdk_script = before_floor.replacen(TOPIC_CHECK, "", 1);
        assert_eq!(
            Script::new(&sdk_script).get_hash(),
            "7944489663d477b49e3f4f947933e15de3269718"
        );
    }
}
