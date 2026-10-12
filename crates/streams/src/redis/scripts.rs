//! The Lua scripts, on the stored format the SDKs used before the store moved into Core.
//!
//! Stage is byte for byte what the SDKs sent, so a server that cached it for an SDK's provider
//! serves Core from the same cache entry. The append script also checks the topic's close flag,
//! which D1b's lacks, so a batch can't land after the topic's close returned (DD-96). The append
//! and promote scripts also report each topic's arm, so a producer notifies only while a reader
//! sleeps (DD-118). `keep.lua` is the prelude of every script that writes a log.

use redis::Script;
use std::sync::LazyLock;

pub(crate) const APPEND: &str = concat!(
    include_str!("../../lua/keep.lua"),
    include_str!("../../lua/append.lua")
);
pub(crate) const PROMOTE: &str = concat!(
    include_str!("../../lua/keep.lua"),
    include_str!("../../lua/promote.lua")
);
pub(crate) const STAGE: &str = include_str!("../../lua/stage.lua");

pub(crate) static APPEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(APPEND));
pub(crate) static PROMOTE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(PROMOTE));
pub(crate) static ARM_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../../lua/arm.lua")));
pub(crate) static DISARM_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../../lua/disarm.lua")));

#[cfg(test)]
mod tests {
    use super::*;

    /// The close check the append script adds to D1b's.
    const TOPIC_CHECK: &str = " or redis.call('HGET', KEYS[2], 'closed')";
    /// The arm the append script adds to each of its answers.
    const APPEND_ARM: &str = ", redis.call('HGET', KEYS[2], 'arm') or '0'";
    /// The arms the promote script answers with instead of the record count alone.
    const PROMOTE_ARMS: &str = "local reply = {#items / 2}
for i = 4, #ARGV do
  reply[#reply + 1] = redis.call('HGET', KEYS[2 * (i - 3) + 2], 'arm') or '0'
end
return reply
";

    #[test]
    fn the_append_script_is_d1bs_with_the_topic_check_and_the_arm_only() {
        assert_eq!(
            APPEND_SCRIPT.get_hash(),
            "8545a1ddb455599193a343d231501deecf170914"
        );
        assert_eq!(APPEND.matches(TOPIC_CHECK).count(), 1);
        assert_eq!(APPEND.matches(APPEND_ARM).count(), 2);
        let d1b = APPEND.replacen(TOPIC_CHECK, "", 1).replace(APPEND_ARM, "");
        assert_eq!(
            Script::new(&d1b).get_hash(),
            "7944489663d477b49e3f4f947933e15de3269718"
        );
    }

    #[test]
    fn the_promote_script_is_the_python_sdks_with_the_arms_only() {
        assert_eq!(
            PROMOTE_SCRIPT.get_hash(),
            "87e0c1d8eb7e837df00e09c4b096387346722b51"
        );
        assert_eq!(PROMOTE.matches(PROMOTE_ARMS).count(), 1);
        let python = PROMOTE.replacen(PROMOTE_ARMS, "return #items / 2\n", 1);
        assert_eq!(
            Script::new(&python).get_hash(),
            "35a31f033f98bd6b321945a6b32f2f0fded1b22a"
        );
    }

    #[test]
    fn the_stage_script_is_the_one_the_python_sdk_sends() {
        assert_eq!(
            Script::new(STAGE).get_hash(),
            "e641e35793f1cad850988bfbd105d4787573f122"
        );
    }
}
