//! A presence channel member, parsed from the client's `channel_data` JSON.

use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct PresenceMember {
    pub user_id: String,
    pub user_info: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelDataError {
    NotJson,
    MissingUserId,
}

/// Parse `channel_data` (e.g. `{"user_id":10,"user_info":{...}}`). `user_id` is
/// required and normalized to a string (Pusher allows numeric ids); `user_info`
/// is optional and defaults to `null`.
pub fn parse_channel_data(raw: &str) -> Result<PresenceMember, ChannelDataError> {
    let v: Value = serde_json::from_str(raw).map_err(|_| ChannelDataError::NotJson)?;
    let user_id = match v.get("user_id") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => return Err(ChannelDataError::MissingUserId),
    };
    let user_info = v.get("user_info").cloned().unwrap_or(Value::Null);
    Ok(PresenceMember { user_id, user_info })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_string_user_id_and_info() {
        let m = parse_channel_data(r#"{"user_id":"u1","user_info":{"name":"Ann"}}"#).unwrap();
        assert_eq!(m.user_id, "u1");
        assert_eq!(m.user_info, json!({"name":"Ann"}));
    }

    #[test]
    fn normalizes_numeric_user_id_to_string() {
        let m = parse_channel_data(r#"{"user_id":10}"#).unwrap();
        assert_eq!(m.user_id, "10");
        assert_eq!(m.user_info, Value::Null);
    }

    #[test]
    fn rejects_missing_user_id() {
        assert_eq!(
            parse_channel_data(r#"{"user_info":{}}"#),
            Err(ChannelDataError::MissingUserId)
        );
    }

    #[test]
    fn rejects_non_json() {
        assert_eq!(
            parse_channel_data("not json"),
            Err(ChannelDataError::NotJson)
        );
    }
}
