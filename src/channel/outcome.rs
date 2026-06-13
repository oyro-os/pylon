//! Values returned by channel-state mutations and queries. Produced by the
//! registry, returned by the `Adapter` trait — so they live in a neutral module.

use crate::presence::member::PresenceMember;
use crate::protocol::event::PresencePayload;

#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeOutcome {
    pub subscription_count: usize,
    pub presence: Option<PresenceJoin>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PresenceJoin {
    pub first_for_user: bool,
    pub roster: PresencePayload,
    pub member: PresenceMember,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnsubscribeOutcome {
    pub subscription_count: usize,
    pub presence: Option<PresenceLeave>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PresenceLeave {
    pub last_for_user: bool,
    pub user_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelSummary {
    pub name: String,
    pub occupied: bool,
    pub subscription_count: usize,
    pub user_count: Option<usize>,
}
