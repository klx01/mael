use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use serde_json::Value;
use crate::id_generator::MessageIdGenerator;
use crate::messages::{ErrorCode, ErrorMessageResponse, Message, MessageMeta};
use crate::rpc_handler::{RpcError, RpcHandler};
use crate::util::convert_json_value;

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "read")]
struct KVReadMessage {
    msg_id: usize,
    key: String,
}

#[derive(Debug, Deserialize)]
struct KVReadOkMessage {
    value: Value,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "cas")]
struct KVCasMessage {
    msg_id: usize,
    key: String,
    from: Value,
    to: Value,
    create_if_not_exists: bool,
}

#[derive(Debug, Deserialize)]
struct KVCasOkMessage {}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum KVResponseMessage {
    ReadOk(KVReadOkMessage),
    CasOk(KVCasOkMessage),
    Error(ErrorMessageResponse),
}

pub enum KVReadError {
    KeyDoesNotExist,
    Other,
}

const KV_NODE_NAME: &'static str = "lin-kv";

pub struct LinKV {
    id: Arc<MessageIdGenerator>,
    node_id: String,
    rpc: RpcHandler<KVResponseMessage>,
    timeout: u64,
}
impl LinKV {
    pub fn new(id: Arc<MessageIdGenerator>, node_id: String, timeout: u64) -> Self {
        Self {
            id: id,
            node_id: node_id,
            rpc: Default::default(),
            timeout: timeout,
        }
    }
    pub async fn cas(&self, key: String, from: Value, to: Value, create_if_not_exists: bool) -> bool {
        let msg_id = self.id.next();
        let message = Message {
            meta: MessageMeta {
                id: None,
                src: self.node_id.clone(),
                dest: KV_NODE_NAME.to_string(),
            },
            body: KVCasMessage {
                msg_id,
                key,
                from,
                to,
                create_if_not_exists,
            },
        };
        let response = self.rpc.call_rpc(message, msg_id, self.timeout).await;
        let Ok(KVResponseMessage::CasOk(_)) = response else {
            Self::log_error(response);
            return false;
        };
        return true;
    }
    pub async fn cas_typed<T>(&self, key: String, from: T, to: T, create_if_not_exists: bool) -> bool
        where Value: From<T>
    {
        self.cas(key, Value::from(from), Value::from(to), create_if_not_exists).await
    }
    pub async fn cas_hash_map<T: Serialize>(&self, key: String, from: String, to: HashMap<String, T>, create_if_not_exists: bool) -> bool {
        let to = serde_json::to_string(&to).expect("failed to encode value");
        self.cas_typed(key, from, to, create_if_not_exists).await
    }
    pub async fn read(&self, key: String) -> Result<Value, KVReadError> {
        let msg_id = self.id.next();
        let message = Message {
            meta: MessageMeta {
                id: None,
                src: self.node_id.clone(),
                dest: KV_NODE_NAME.to_string(),
            },
            body: KVReadMessage {
                msg_id,
                key,
            },
        };
        let response = self.rpc.call_rpc(message, msg_id, self.timeout).await;
        match response {
            Ok(KVResponseMessage::ReadOk(response)) => Ok(response.value),
            Ok(KVResponseMessage::Error(err)) => match err.code {
                ErrorCode::KeyDoesNotExist => Err(KVReadError::KeyDoesNotExist),
                _ => {
                    eprintln!("received error response: {err:?}");
                    Err(KVReadError::Other)
                }
            },
            _ => { 
                Self::log_error(response);
                Err(KVReadError::Other)
            },
        }
    }
    pub async fn read_typed<T: DeserializeOwned>(&self, key: String) -> Result<T, KVReadError> {
        let result = self.read(key).await?;
        convert_json_value(result).ok_or(KVReadError::Other)
    }
    pub async fn read_hash_map_for_cas<V: DeserializeOwned>(&self, key: String) -> Option<(HashMap<String, V>, String)> {
        /*
        we need to CAS the values,
        so we need to be sure that the old value that we send in CAS request is exactly the same the stored one.
        hashmaps don't guarantee the order of elements,
        so if we use read_typed<HashMap>, then we would not be able to make a correct CAS request.
        so we read and store the value as String, and not as HashMap
         */
        let result = self.read_typed::<String>(key).await;
        match result {
            Ok(encoded) => {
                let decoded = serde_json::from_str::<HashMap<String, V>>(&encoded);
                match decoded {
                    Ok(offsets) => Some((offsets, encoded)),
                    Err(err) => {
                        eprintln!("failed to decode the value {encoded} {err}");
                        None
                    }
                }
            },
            Err(KVReadError::KeyDoesNotExist) => Some((HashMap::new(), String::new())),
            _ => None,
        }
    }
    fn log_error(response: Result<KVResponseMessage, RpcError>) {
        match response {
            Ok(KVResponseMessage::Error(err)) => {
                eprintln!("received error response: {err:?}");
            },
            Ok(message) => {
                eprintln!("received unexpected response: {message:?}");
            },
            Err(err) => match err {
                RpcError::SenderDropped => {
                    eprintln!("unexpected error, sender dropped");
                },
                RpcError::Timeout => {
                    eprintln!("request timed out");
                },
            },
        }
    }
    pub fn init_response(&self, in_reply_to: usize, message: KVResponseMessage) {
        self.rpc.init_response(in_reply_to, message)
    }
}
