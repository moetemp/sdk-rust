//! The Lua scripts, on the stored format the SDKs used before the store moved into Core.
//!
//! The append script is the one the SDKs sent, with one more check, the topic's close flag, so a
//! batch can't land after the topic's close returned (DD-16). `keep.lua` is the prelude of every
//! script that writes a log.

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

    /// The close check the append script adds to the SDKs' one.
    const TOPIC_CHECK: &str = " or redis.call('HGET', KEYS[2], 'closed')";

    #[test]
    fn the_append_script_matches_the_sdks_with_the_topic_check_only() {
        assert_eq!(
            APPEND_SCRIPT.get_hash(),
            "96d2eb81ddf241103a1ed5495a015285d7bd37f0"
        );
        assert_eq!(APPEND.matches(TOPIC_CHECK).count(), 1);
        let sdk_script = APPEND.replacen(TOPIC_CHECK, "", 1);
        assert_eq!(
            Script::new(&sdk_script).get_hash(),
            "7944489663d477b49e3f4f947933e15de3269718"
        );
    }
}
