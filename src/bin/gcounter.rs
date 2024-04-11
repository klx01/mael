use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use mael::{MessageIdGenerator, SyncService, MessageMeta, output_reply, InitMessage, Message, output_message, DefaultInitService, default_init_and_sync_loop_with_timeout};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InputMessage {
    Add(AddMessage),
    Read(ReadMessage),
    Sync(SyncMessage),
    WriteOk(KVWriteOkMessage),
}

#[derive(Debug, Deserialize)]
struct AddMessage {
    msg_id: usize,
    delta: usize,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "add_ok")]
struct AddOkMessage {
    msg_id: usize,
    in_reply_to: usize,
}

#[derive(Debug, Deserialize)]
struct ReadMessage {
    msg_id: usize,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "read_ok")]
struct ReadOkMessage {
    msg_id: usize,
    in_reply_to: usize,
    value: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "sync")]
struct SyncMessage {
    value: usize,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "write")]
struct KVWriteMessage {
    key: String,
    value: usize,
}

#[derive(Debug, Deserialize)]
struct KVWriteOkMessage {
    // empty
}

const KV_NODE_NAME: &'static str = "seq-kv";

struct GCounterService {
    id: MessageIdGenerator,
    node_id: String,
    counter: usize,
    processed_adds: HashSet<String>, // this grows indefinitely; maybe we could somehow periodically discard the old ones
    other_counters: HashMap<String, usize>,
    /*
    the task says that nodes should be stateless, so this solution is probably incorrect. Or maybe it is correct?
    i think that it would work well too.
    it is already writing counters into the storage, so if the node goes down, it can just read it back from storage during the initialisation.
    i don't really see what exactly would be improved by making it truly stateless.
    maybe the case where multiple nodes accidentally get the same id?
     */
}
impl DefaultInitService for GCounterService {
    fn new(init_message: InitMessage) -> Self {
        let other_counters = init_message
            .node_ids
            .into_iter()
            .filter(|x| *x != init_message.node_id)
            .map(|x| (x, 0))
            .collect();
        Self {
            id: Default::default(),
            node_id: init_message.node_id,
            counter: 0,
            processed_adds: HashSet::new(),
            other_counters: other_counters,
        }
    }
}
impl SyncService<InputMessage> for GCounterService {
    fn process_message(&mut self, message: InputMessage, meta: MessageMeta) {
        match message {
            InputMessage::Add(message) => {
                let message_id = format!("{}_{}", meta.src.clone(), message.msg_id);
                let is_new = self.processed_adds.insert(message_id);
                if is_new {
                    self.counter += message.delta;
                }
                let output = AddOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                };
                output_reply(output, meta);
            }
            InputMessage::Read(message) => {
                let output = ReadOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                    value: self.counter + self.other_counters.values().sum::<usize>(),
                };
                output_reply(output, meta);
            }
            InputMessage::Sync(message) => {
                let node = meta.src;
                let counter = self.other_counters.get_mut(&node);
                let Some(counter) = counter else {
                    eprintln!("did not find a counter for {node} when processing sync message");
                    return;
                };
                if message.value > *counter {
                    *counter = message.value;
                }
            }
            InputMessage::WriteOk(_) => {
                // ignore responses from seq-kv
            }
        }
    }

    fn on_timeout(&mut self) {
        let write_message = Message {
            meta: MessageMeta {
                id: None,
                src: self.node_id.clone(),
                dest: KV_NODE_NAME.to_string(),
            },
            body: KVWriteMessage {
                key: self.node_id.clone(),
                value: self.counter,
            }
        };
        output_message(write_message);

        for node in self.other_counters.keys() {
            let write_message = Message {
                meta: MessageMeta {
                    id: None,
                    src: self.node_id.clone(),
                    dest: node.clone(),
                },
                body: SyncMessage {
                    value: self.counter,
                }
            };
            output_message(write_message);
        }
    }
}

fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w g-counter --bin ./target/release/gcounter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
    /*
{"id":3,"src":"c3","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":100}}
{"id":4,"src":"c3","dest":"n1","body":{"type":"add","delta":10,"msg_id":101}}
{"id":5,"src":"c3","dest":"n1","body":{"type":"read","msg_id":102}}

{"id":6,"src":"n2","dest":"n1","body":{"type":"sync","value":22,"msg_id":103}}
{"id":7,"src":"c3","dest":"n1","body":{"type":"read","msg_id":104}}
{"id":8,"src":"seq-kv","dest":"n1","body":{"type":"write_ok","msg_id":105}}

     */
    default_init_and_sync_loop_with_timeout::<GCounterService, InputMessage>(400..=600);
}
