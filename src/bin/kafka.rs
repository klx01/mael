use std::cmp;
use std::collections::HashMap;
use std::sync::RwLock;
use serde::{Deserialize, Serialize};
use mael::{MessageIdGenerator, MessageMeta, output_reply, InitMessage, ErrorMessage, ErrorCode, DefaultInitService, default_init_and_async_loop, AsyncService};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InputMessage {
    Send(SendMessage),
    Poll(PollMessage),
    CommitOffsets(CommitOffsetsMessage),
    ListCommittedOffsets(ListCommittedOffsetsMessage),
    /*ReadOk(KVReadOkMessage),
    WriteOk(KVWriteOkMessage),
    CasOk(KVCasOkMessage),
    Error(ErrorMessage),*/
}

#[derive(Debug, Deserialize)]
struct SendMessage {
    msg_id: usize,
    key: String,
    msg: usize,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "send_ok")]
struct SendOkMessage {
    msg_id: usize,
    in_reply_to: usize,
    offset: usize,
}

#[derive(Debug, Deserialize)]
struct PollMessage {
    msg_id: usize,
    offsets: HashMap<String, usize>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "poll_ok")]
struct PollOkMessage {
    msg_id: usize,
    in_reply_to: usize,
    msgs: HashMap<String, Vec<(usize, usize)>>
}

#[derive(Debug, Deserialize)]
struct CommitOffsetsMessage {
    msg_id: usize,
    offsets: HashMap<String, usize>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "commit_offsets_ok")]
struct CommitOffsetsOkMessage {
    msg_id: usize,
    in_reply_to: usize,
}

#[derive(Debug, Deserialize)]
struct ListCommittedOffsetsMessage {
    msg_id: usize,
    keys: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "list_committed_offsets_ok")]
struct ListCommittedOffsetsOkMessage {
    msg_id: usize,
    in_reply_to: usize,
    offsets: HashMap<String, usize>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "write")]
struct KVWriteMessage {
    msg_id: usize,
    key: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct KVWriteOkMessage {
    msg_id: usize,
    in_reply_to: usize,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "read")]
struct KVReadMessage {
    msg_id: usize,
    key: String,
}

#[derive(Debug, Deserialize)]
struct KVReadOkMessage {
    msg_id: usize,
    in_reply_to: usize,
    value: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "cas")]
struct KVCasMessage {
    msg_id: usize,
    key: String,
    from: String,
    to: String,
    create_if_not_exists: bool,
}

#[derive(Debug, Deserialize)]
struct KVCasOkMessage {
    msg_id: usize,
    in_reply_to: usize,
}

const POLL_MAX_LEN: usize = 50;
const KV_NODE_NAME: &'static str = "lin-kv";

struct KafkaService {
    id: MessageIdGenerator,
    node_id: String,
    messages: RwLock<HashMap<String, Vec<usize>>>,
    committed_offsets: RwLock<HashMap<String, usize>>,
}
impl DefaultInitService for KafkaService {
    fn new(init_message: InitMessage) -> Self {
        Self {
            id: Default::default(),
            node_id: init_message.node_id,
            messages: Default::default(),
            committed_offsets: Default::default(),
        }
    }
}
impl KafkaService {
    fn copy_lengths(&self) -> HashMap<String, usize> {
        let lock = self.messages.read().expect("got a poisoned lock, cant really handle it");
        let result = lock
            .iter()
            .map(|(key, val)| (key.clone(), val.len()))
            .collect();
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        result
    }
}
impl AsyncService<InputMessage> for KafkaService {
    async fn process_message(&self, message: InputMessage, meta: MessageMeta) {
        match message {
            InputMessage::Send(message) => {
                // todo: correctly handle retries
                // todo: maybe try to not lock the whole hashmap for this?
                let mut lock = self.messages.write().expect("got a poisoned lock, cant really handle it");
                let log = lock.entry(message.key).or_insert(vec![]);
                let offset = log.len();
                log.push(message.msg);
                drop(lock);

                let output = SendOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                    offset,
                };
                output_reply(output, meta);
            }
            InputMessage::Poll(message) => {
                let lock = self.messages.read().expect("got a poisoned lock, cant really handle it");
                let mut response = HashMap::new();
                for (key, offset) in message.offsets {
                    let log = lock.get(&key);
                    let Some(log) = log else {
                        continue;
                    };
                    if offset >= log.len() {
                        response.insert(key, vec![]);
                        continue;
                    }
                    let last_index = cmp::min(offset + POLL_MAX_LEN, log.len());
                    let messages = log[offset..last_index]
                        .iter()
                        .copied()
                        .enumerate()
                        .map(|(index, val)| (index + offset, val))
                        .collect::<Vec<_>>();
                    response.insert(key, messages);
                }
                drop(lock);

                let output = PollOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                    msgs: response,
                };
                output_reply(output, meta);
            }
            InputMessage::CommitOffsets(message) => {
                let key_lengths = self.copy_lengths();
                for (key, offset) in message.offsets.iter() {
                    let length = key_lengths.get(key);
                    let Some(length) = length else {
                        let error = ErrorMessage {
                            in_reply_to: message.msg_id,
                            code: ErrorCode::KeyDoesNotExist,
                            text: format!("Key {key} was not initialised, can not commit"),
                        };
                        output_reply(error, meta);
                        return;
                    };
                    if *offset >= *length {
                        let error = ErrorMessage {
                            in_reply_to: message.msg_id,
                            code: ErrorCode::PreconditionFailed,
                            text: format!("Offset {offset} does not exist for key {key}, can not commit"),
                        };
                        output_reply(error, meta);
                        return;
                    }
                }

                let mut lock = self.committed_offsets.write().expect("got a poisoned lock, cant really handle it");
                for (key, offset) in message.offsets {
                    let committed = lock.entry(key).or_insert(0);
                    if offset >= *committed {
                        *committed = offset;
                    }
                }
                drop(lock);

                let output = CommitOffsetsOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                };
                output_reply(output, meta);
            }
            InputMessage::ListCommittedOffsets(message) => {
                let mut response = HashMap::new();
                let lock = self.committed_offsets.read().expect("got a poisoned lock, cant really handle it");
                for key in message.keys {
                    let offset = lock.get(&key);
                    let Some(offset) = offset else {
                        // the task description mentions that keys that don't exist can be omitted
                        continue;
                    };
                    response.insert(key, *offset);
                }
                drop(lock);

                let output = ListCommittedOffsetsOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                    offsets: response,
                };
                output_reply(output, meta);
            }
        }
    }

    async fn on_timeout(&self) {
        // empty
    }
}

#[tokio::main]
async fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w kafka --bin ./target/release/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 10
    // cargo build --release && ./maelstrom/maelstrom test -w kafka --bin ./target/release/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
    // cargo build --release && ./maelstrom/maelstrom test -w kafka --bin ./target/release/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
    /*
{"id":3,"src":"c3","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":100}}
{"id":4,"src":"c3","dest":"n1","body":{"type":"poll","msg_id":101,"offsets":{"k1":0}}}
{"id":5,"src":"c3","dest":"n1","body":{"type":"send","msg_id":102,"key":"k1","msg":222}}
{"id":5,"src":"c3","dest":"n1","body":{"type":"send","msg_id":102,"key":"k1","msg":222}}
{"id":6,"src":"c3","dest":"n1","body":{"type":"send","msg_id":103,"key":"k1","msg":333}}
{"id":7,"src":"c3","dest":"n1","body":{"type":"poll","msg_id":104,"offsets":{"k1":0}}}
{"id":8,"src":"c3","dest":"n1","body":{"type":"poll","msg_id":105,"offsets":{"k1":1}}}
{"id":9,"src":"c3","dest":"n1","body":{"type":"poll","msg_id":106,"offsets":{"k1":2}}}
{"id":10,"src":"c3","dest":"n1","body":{"type":"list_committed_offsets","msg_id":107,"keys":["k1"]}}
{"id":11,"src":"c3","dest":"n1","body":{"type":"commit_offsets","msg_id":108,"offsets":{"k1":1}}}
{"id":12,"src":"c3","dest":"n1","body":{"type":"commit_offsets","msg_id":109,"offsets":{"k1":2}}}
{"id":13,"src":"c3","dest":"n1","body":{"type":"list_committed_offsets","msg_id":110,"keys":["k1"]}}

     */
    default_init_and_async_loop::<KafkaService, InputMessage>(None).await;
}
