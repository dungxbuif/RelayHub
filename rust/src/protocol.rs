use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const VERSION: u16 = 1;
pub const SUBPROTOCOL: &str = "relayhub.v1";
pub const MAX_BODY_BYTES: usize = 1_048_576;
pub const DEFAULT_SYNC_TIMEOUT_SECONDS: u8 = 15;
pub const MIN_SYNC_TIMEOUT_SECONDS: u8 = 1;
pub const MAX_SYNC_TIMEOUT_SECONDS: u8 = 30;
pub const RETENTION_SECONDS: u64 = 7 * 24 * 60 * 60;
pub const MAX_OUTSTANDING_DELIVERIES: u32 = 10_000;
pub const MAX_OUTSTANDING_PAYLOAD_BYTES: u64 = 100 * 1024 * 1024;
pub const LEASE_SECONDS: u64 = 60;
pub const HEARTBEAT_SECONDS: u64 = 20;
pub const MAX_ATTEMPT_RUNTIME_SECONDS: u64 = 300;
pub const MAX_ATTEMPTS: u8 = 5;
pub const RETRY_DELAYS_SECONDS: [u64; 4] = [1, 5, 30, 120];
pub const PRESENCE_HEARTBEAT_SECONDS: u64 = 20;
pub const PRESENCE_EXPIRY_SECONDS: u64 = 60;
pub const MAX_IN_FLIGHT: u16 = 16;
pub const SETUP_TOKEN_TTL_SECONDS: u64 = 24 * 60 * 60;

/// Ordered header pairs retain duplicates such as Set-Cookie.
pub type Headers = Vec<(String, String)>;

/// Server-authenticated context; socket frames cannot supply this authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Principal {
    SystemAdmin {
        user_id: Uuid,
    },
    WorkspaceOwner {
        user_id: Uuid,
        workspace_id: Uuid,
    },
    Application {
        workspace_id: Uuid,
        app_id: Uuid,
        key_id: Uuid,
    },
    Agent {
        workspace_id: Uuid,
        app_id: Uuid,
        agent_id: Uuid,
        key_id: Uuid,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventInput {
    pub endpoint_id: Uuid,
    pub event_type: String,
    pub method: String,
    pub path: String,
    pub headers: Headers,
    #[serde(with = "body_base64")]
    pub body: Vec<u8>,
}

impl EventInput {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_body(&self.body)?;
        if self.method.parse::<axum::http::Method>().is_err()
            || !self.path.starts_with('/')
            || self.path.parse::<axum::http::Uri>().is_err()
            || !valid_percent_escapes(&self.path)
        {
            return Err(ProtocolError::InvalidFrame);
        }
        validate_headers(&self.headers)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Accepted {
    pub event_id: Uuid,
    pub delivery_id: Uuid,
    pub status: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum EndpointPolicy {
    Async,
    Sync {
        timeout_seconds: u8,
        offline: OfflinePolicy,
        timeout: TimeoutPolicy,
    },
}

impl EndpointPolicy {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if let Self::Sync {
            timeout_seconds, ..
        } = self
            && !(MIN_SYNC_TIMEOUT_SECONDS..=MAX_SYNC_TIMEOUT_SECONDS).contains(timeout_seconds)
        {
            return Err(ProtocolError::InvalidFrame);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OfflinePolicy {
    Queue,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeoutPolicy {
    Continue,
    ExpirePending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryLease {
    pub delivery_id: Uuid,
    pub event_id: Uuid,
    pub attempt_token: Uuid,
    pub generation: u32,
    pub attempt: u8,
}

impl DeliveryLease {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.generation == 0 || !(1..=MAX_ATTEMPTS).contains(&self.attempt) {
            return Err(ProtocolError::InvalidFrame);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Headers,
    #[serde(with = "body_base64")]
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_body(&self.body)?;
        if !(100..=599).contains(&self.status) || self.status == 101 {
            return Err(ProtocolError::InvalidFrame);
        }
        validate_headers(&self.headers)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Frame {
    Hello {
        version: u16,
        request_id: Uuid,
        agent_id: Uuid,
        max_in_flight: u16,
    },
    Delivery {
        version: u16,
        request_id: Uuid,
        lease: DeliveryLease,
        input: EventInput,
    },
    Ack {
        version: u16,
        request_id: Uuid,
        event_id: Uuid,
        delivery_id: Uuid,
        attempt_token: Uuid,
    },
    Response {
        version: u16,
        request_id: Uuid,
        event_id: Uuid,
        delivery_id: Uuid,
        attempt_token: Uuid,
        response: HttpResponse,
    },
    Realtime {
        version: u16,
        request_id: Uuid,
        channel: String,
        #[serde(with = "body_base64")]
        body: Vec<u8>,
    },
    Heartbeat {
        version: u16,
        request_id: Uuid,
        #[serde(deserialize_with = "required_option")]
        lease: Option<DeliveryLease>,
    },
    /// Server-to-agent admission cutoff; socket close signals completion, without an ACK.
    Drain {
        version: u16,
        request_id: Uuid,
        deadline_ms: u32,
    },
}

impl Frame {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let version = match self {
            Self::Hello { version, .. }
            | Self::Delivery { version, .. }
            | Self::Ack { version, .. }
            | Self::Response { version, .. }
            | Self::Realtime { version, .. }
            | Self::Heartbeat { version, .. }
            | Self::Drain { version, .. } => *version,
        };
        if version != VERSION {
            return Err(ProtocolError::UnsupportedVersion);
        }
        match self {
            Self::Hello { max_in_flight, .. } if !(1..=MAX_IN_FLIGHT).contains(max_in_flight) => {
                Err(ProtocolError::InvalidFrame)
            }
            Self::Drain { deadline_ms, .. } if !(1..=30_000).contains(deadline_ms) => {
                Err(ProtocolError::InvalidFrame)
            }
            Self::Delivery { lease, input, .. } => {
                lease.validate()?;
                input.validate()
            }
            Self::Response { response, .. } => response.validate(),
            Self::Realtime { body, .. } => validate_body(body),
            Self::Heartbeat {
                lease: Some(lease), ..
            } => lease.validate(),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProtocolError {
    UnsupportedVersion,
    BodyTooLarge,
    InvalidFrame,
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::UnsupportedVersion => "unsupported_version",
            Self::BodyTooLarge => "body_too_large",
            Self::InvalidFrame => "invalid_frame",
        })
    }
}

impl std::error::Error for ProtocolError {}

pub fn parse_frame(bytes: &[u8]) -> Result<Frame, ProtocolError> {
    #[derive(Deserialize)]
    struct Version {
        version: u64,
    }
    let version: Version =
        serde_json::from_slice(bytes).map_err(|_| ProtocolError::InvalidFrame)?;
    if version.version != u64::from(VERSION) {
        return Err(ProtocolError::UnsupportedVersion);
    }
    let frame: Frame = serde_json::from_slice(bytes).map_err(|_| ProtocolError::InvalidFrame)?;
    frame.validate()?;
    Ok(frame)
}

fn validate_body(body: &[u8]) -> Result<(), ProtocolError> {
    if body.len() > MAX_BODY_BYTES {
        return Err(ProtocolError::BodyTooLarge);
    }
    Ok(())
}

fn validate_headers(headers: &Headers) -> Result<(), ProtocolError> {
    for (name, value) in headers {
        if axum::http::HeaderName::from_bytes(name.as_bytes()).is_err()
            || axum::http::HeaderValue::from_str(value).is_err()
        {
            return Err(ProtocolError::InvalidFrame);
        }
    }
    Ok(())
}

fn valid_percent_escapes(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.iter().enumerate().any(|(index, byte)| {
        *byte == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit())
    })
}

fn required_option<'de, D>(deserializer: D) -> Result<Option<DeliveryLease>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<DeliveryLease>::deserialize(deserializer)
}

mod body_base64 {
    use super::*;

    pub fn serialize<S: serde::Serializer>(body: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(body))
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(encoded).map_err(serde::de::Error::custom)
    }
}
