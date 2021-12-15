use crate::error;
use base64;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EncodedMessage {
    pub(crate) data: String,
    pub(crate) attributes: Option<HashMap<String, String>>,
    #[serde(skip_serializing)]
    pub(crate) publish_time: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) message_id: Option<String>,
}

pub trait FromPubSubMessage
where
    Self: std::marker::Sized,
{
    fn from(message: EncodedMessage) -> Result<Self, error::Error>;
}

impl EncodedMessage {
    pub fn decode(&self) -> Result<Vec<u8>, base64::DecodeError> {
        base64::decode(&self.data)
    }

    pub fn new<T: serde::Serialize>(data: &T) -> Self {
        let json = serde_json::to_string(data).unwrap();
        let data = base64::encode(&json);
        EncodedMessage {
            data,
            publish_time: None,
            message_id: None,
            attributes: None,
        }
    }
}

#[derive(Deserialize, Debug)]
pub(crate) struct Message {
    #[serde(alias = "ackId")]
    pub(crate) ack_id: String,
    pub(crate) message: EncodedMessage,
}

#[derive(Debug)]
pub struct RawMessage {
    pub ack_id: String,
    pub attributes: Option<HashMap<String, String>>,
    pub publish_time: Option<String>,
    pub message_id: Option<String>,
    pub data: String,
}

impl From<Message> for RawMessage {
    fn from(msg: Message) -> Self {
        RawMessage {
            ack_id: msg.ack_id,
            attributes: msg.message.attributes,
            publish_time: msg.message.publish_time,
            message_id: msg.message.message_id,
            data: msg.message.data,
        }
    }
}
