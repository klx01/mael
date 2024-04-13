use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

/*
todo: if possible, rewrite the messages in a way that would reduce the amount of cloning
 */

#[derive(Serialize, Deserialize)]
pub struct Message<T> {
    #[serde(flatten)]
    pub meta: MessageMeta,
    pub body: T,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageMeta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<usize>,
    pub src: String,
    pub dest: String,
}

#[derive(Debug, Deserialize)]
pub struct InitMessage {
    pub msg_id: usize,
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "init_ok")]
pub struct InitOkMessage {
    pub in_reply_to: usize,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "error")]
pub struct ErrorMessage {
    pub in_reply_to: usize,
    pub code: ErrorCode,
    pub text: String,
}

#[derive(Debug, Serialize_repr, Deserialize_repr, Copy, Clone)]
#[repr(u16)]
pub enum ErrorCode {
    Timeout = 0,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    PreconditionFailed = 22,
    TxnConflict = 30,
    Unexpected = 1001,
}

#[derive(Debug, Deserialize)]
pub struct AnyResponseMessage<T> {
    pub in_reply_to: usize,
    #[serde(flatten)]
    pub data: T,
}
#[derive(Debug, Deserialize)]
pub struct ErrorMessageResponse {
    pub code: ErrorCode,
    pub text: String,
}
