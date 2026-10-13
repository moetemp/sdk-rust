//! The Lua scripts, on the stored format the SDKs used before the store moved into Core.
//!
//! Stage and promote are byte for byte what the SDKs sent, so a server that cached them for an
//! SDK's provider serves Core from the same cache entry. The append script also checks the
//! topic's close flag, which D1b's lacks, so a batch can't land after the topic's close returned
//! (DD-96). `keep.lua` is the prelude of every script that writes a log.

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

#[cfg(test)]
mod tests {
    use super::*;

    /// The close check the append script adds to D1b's.
    const TOPIC_CHECK: &str = " or redis.call('HGET', KEYS[2], 'closed')";

    #[test]
    fn the_append_script_is_d1bs_with_the_topic_check_only() {
        assert_eq!(
            APPEND_SCRIPT.get_hash(),
            "96d2eb81ddf241103a1ed5495a015285d7bd37f0"
        );
        assert_eq!(APPEND.matches(TOPIC_CHECK).count(), 1);
        let d1b = APPEND.replacen(TOPIC_CHECK, "", 1);
        assert_eq!(
            Script::new(&d1b).get_hash(),
            "7944489663d477b49e3f4f947933e15de3269718"
        );
    }

    #[test]
    fn the_other_scripts_are_the_ones_the_python_sdk_sends() {
        assert_eq!(
            PROMOTE_SCRIPT.get_hash(),
            "35a31f033f98bd6b321945a6b32f2f0fded1b22a"
        );
        assert_eq!(
            Script::new(STAGE).get_hash(),
            "e641e35793f1cad850988bfbd105d4787573f122"
        );
    }
}
