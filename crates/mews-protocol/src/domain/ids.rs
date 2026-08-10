use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// These types are persisted and sent over the wire. Development-state schema
// changes are intentionally breaking: reset local MEWS state instead of
// accepting legacy representations.

macro_rules! id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new() -> Self {
                Self(format!("{}{}", $prefix, Uuid::now_v7()))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl FromStr for $name {
            type Err = &'static str;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let uuid = value.strip_prefix($prefix).ok_or("invalid ID prefix")?;
                Uuid::parse_str(uuid).map_err(|_| "invalid ID")?;
                Ok(Self(value.to_owned()))
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

id!(InstallationId, "ins_");
id!(HostId, "hst_");
id!(AgentId, "agt_");
id!(SessionId, "ses_");
id!(MessageId, "msg_");
id!(RunId, "run_");
id!(InvitationId, "inv_");
id!(RequestId, "req_");
id!(ConsumerId, "con_");
id!(EventId, "evt_");
