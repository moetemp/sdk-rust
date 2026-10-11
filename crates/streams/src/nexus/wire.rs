//! The N0a contract's models in their JSON transfer form.
//!
//! Inputs reject unknown members, so a typo fails at the endpoint. Outputs skip absent members, as
//! the generated callers expect. Bytes cross as padded standard base64, and an integer may cross
//! as a whole float, as the generated models allow.

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

/// Bytes that cross as a base64 string.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Base64(pub(crate) Vec<u8>);

impl Serialize for Base64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for Base64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        STANDARD
            .decode(text.as_bytes())
            .map(Base64)
            .map_err(|_| D::Error::custom(format!("must be base64-encoded, got {text:?}")))
    }
}

/// An integer as the contract's JSON carries it, where `1.0` counts and `1.5` doesn't.
fn spec_integer<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    let number = serde_json::Number::deserialize(deserializer)?;
    if let Some(value) = number.as_i64() {
        return Ok(value);
    }
    match number.as_f64() {
        Some(value) if value.fract() == 0.0 && value.abs() < 2f64.powi(53) => Ok(value as i64),
        _ => Err(D::Error::custom(format!("expected integer, got {number}"))),
    }
}

fn optional_spec_integer<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<i64>, D::Error> {
    Option::<serde_json::Value>::deserialize(deserializer)?
        .map(|value| spec_integer(value).map_err(D::Error::custom))
        .transpose()
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamRef {
    pub(crate) kind: String,
    pub(crate) workflow_id: String,
    #[serde(default)]
    pub(crate) run_id: Option<String>,
    pub(crate) topic: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppendInput {
    pub(crate) stream: StreamRef,
    pub(crate) producer_id: String,
    #[serde(deserialize_with = "spec_integer")]
    pub(crate) attempt: i64,
    #[serde(deserialize_with = "spec_integer")]
    pub(crate) sequence: i64,
    #[serde(default)]
    pub(crate) payloads: Option<Vec<Base64>>,
    #[serde(default)]
    pub(crate) finish: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct AppendOutput {
    pub(crate) cursor: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadInput {
    pub(crate) stream: StreamRef,
    #[serde(default)]
    pub(crate) after_token: Option<String>,
    #[serde(default, deserialize_with = "optional_spec_integer")]
    pub(crate) max_records: Option<i64>,
    #[serde(default, deserialize_with = "optional_spec_integer")]
    pub(crate) wait_ms: Option<i64>,
    #[serde(default)]
    pub(crate) latest_only: Option<bool>,
    #[serde(default)]
    pub(crate) state: Option<Base64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ReadOutput {
    pub(crate) records: Vec<RecordWire>,
    pub(crate) next_token: String,
    pub(crate) done: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) state: Option<Base64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct RecordWire {
    pub(crate) token: String,
    pub(crate) record: Base64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stale: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) superseded: Option<Supersession>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Supersession {
    pub(crate) producer_id: String,
    pub(crate) previous_attempt: i64,
    pub(crate) attempt: i64,
}

/// The bytes `size` record bytes take in a read answer, as base64.
pub(crate) fn crossing_size(size: usize) -> usize {
    4 * size.div_ceil(3)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_integer_may_cross_as_a_whole_float() {
        let input: AppendInput = serde_json::from_str(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","topic":"t"},
                "producer_id":"p","attempt":1.0,"sequence":2,"payloads":["AQI="]}"#,
        )
        .unwrap();
        assert_eq!((input.attempt, input.sequence), (1, 2));
        assert_eq!(input.payloads.unwrap()[0].0, [1, 2]);
        let error = serde_json::from_str::<AppendInput>(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","topic":"t"},
                "producer_id":"p","attempt":1.5,"sequence":2}"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("expected integer"), "{error}");
    }

    #[test]
    fn an_input_refuses_an_unknown_member_and_bad_base64() {
        let unknown = serde_json::from_str::<ReadInput>(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","topic":"t"},"wait":5}"#,
        )
        .unwrap_err();
        assert!(unknown.to_string().contains("unknown field"), "{unknown}");
        let bad = serde_json::from_str::<ReadInput>(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","topic":"t"},"state":"!!"}"#,
        )
        .unwrap_err();
        assert!(bad.to_string().contains("base64"), "{bad}");
        // Null is what a generated caller sends for an absent optional member.
        let null: ReadInput = serde_json::from_str(
            r#"{"stream":{"kind":"workflow","workflow_id":"wf","run_id":null,"topic":"t"},
                "after_token":null,"max_records":null}"#,
        )
        .unwrap();
        assert_eq!(null.stream.run_id, None);
        assert_eq!(null.max_records, None);
    }

    #[test]
    fn an_output_leaves_absent_members_out() {
        let wire = RecordWire {
            token: "c".to_string(),
            record: Base64(vec![1]),
            error: None,
            stale: None,
            superseded: None,
        };
        assert_eq!(
            serde_json::to_string(&wire).unwrap(),
            r#"{"token":"c","record":"AQ=="}"#
        );
    }

    #[test]
    fn a_record_crosses_at_four_thirds_of_its_bytes() {
        assert_eq!(crossing_size(0), 0);
        assert_eq!(crossing_size(1), 4);
        assert_eq!(crossing_size(3), 4);
        assert_eq!(crossing_size(4), 8);
    }
}
