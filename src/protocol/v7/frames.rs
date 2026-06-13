//! v7 wire (de)serialization. Every `data` is double-encoded EXCEPT pusher:error.

use crate::protocol::event::ServerEvent;
use serde_json::json;

pub fn encode(event: &ServerEvent) -> String {
    match event {
        ServerEvent::ConnectionEstablished {
            socket_id,
            activity_timeout,
        } => {
            let data =
                json!({ "socket_id": socket_id.as_str(), "activity_timeout": activity_timeout })
                    .to_string();
            json!({ "event": "pusher:connection_established", "data": data }).to_string()
        }
        ServerEvent::Pong => json!({ "event": "pusher:pong", "data": {} }).to_string(),
        ServerEvent::SubscriptionSucceeded { channel, presence } => {
            let data = match presence {
                None => String::new(),
                Some(p) => {
                    json!({ "presence": { "ids": p.ids, "hash": p.hash, "count": p.count } })
                        .to_string()
                }
            };
            json!({ "event": "pusher_internal:subscription_succeeded", "channel": channel, "data": data })
                .to_string()
        }
        ServerEvent::SubscriptionCount { channel, count } => {
            let data = json!({ "subscription_count": count }).to_string();
            json!({ "event": "pusher_internal:subscription_count", "channel": channel, "data": data })
                .to_string()
        }
        ServerEvent::Error(e) => {
            json!({ "event": "pusher:error", "data": { "code": e.code, "message": e.message } })
                .to_string()
        }
        ServerEvent::ChannelEvent {
            channel,
            event,
            data,
        } => json!({ "event": event, "channel": channel, "data": data }).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::error::PusherError;
    use crate::protocol::event::ServerEvent;
    use crate::protocol::socket_id::SocketId;
    use serde_json::Value;

    fn parse(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn connection_established_double_encodes_data() {
        let id = SocketId::generate();
        let out = parse(&encode(&ServerEvent::ConnectionEstablished {
            socket_id: id.clone(),
            activity_timeout: 120,
        }));
        assert_eq!(out["event"], "pusher:connection_established");
        let data = parse(out["data"].as_str().expect("data is a stringified JSON"));
        assert_eq!(data["socket_id"], id.as_str());
        assert_eq!(data["activity_timeout"], 120);
    }

    #[test]
    fn pong_frame() {
        let out = parse(&encode(&ServerEvent::Pong));
        assert_eq!(out["event"], "pusher:pong");
        assert!(out["data"].is_object());
    }

    #[test]
    fn public_subscription_succeeded_has_empty_string_data() {
        let out = parse(&encode(&ServerEvent::SubscriptionSucceeded {
            channel: "c".into(),
            presence: None,
        }));
        assert_eq!(out["event"], "pusher_internal:subscription_succeeded");
        assert_eq!(out["channel"], "c");
        assert_eq!(out["data"], ""); // empty string per spec
    }

    #[test]
    fn subscription_count_double_encodes() {
        let out = parse(&encode(&ServerEvent::SubscriptionCount {
            channel: "c".into(),
            count: 2,
        }));
        assert_eq!(out["event"], "pusher_internal:subscription_count");
        let data = parse(out["data"].as_str().unwrap());
        assert_eq!(data["subscription_count"], 2);
    }

    #[test]
    fn error_data_is_object_not_string() {
        let out = parse(&encode(&ServerEvent::Error(PusherError::app_not_found())));
        assert_eq!(out["event"], "pusher:error");
        assert!(
            out["data"].is_object(),
            "error data must be an object, not stringified"
        );
        assert_eq!(out["data"]["code"], 4001);
    }
}
