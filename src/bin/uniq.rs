use serde::{Deserialize, Serialize};
use mael::{MessageIdGenerator, AsyncService, MessageMeta, output_reply, InitMessage, get_stub_timeout, DefaultInitService, default_init_and_async_loop};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "generate")]
struct GenerateMessage {
    msg_id: usize,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "generate_ok")]
struct GenerateOkMessage {
    msg_id: usize,
    in_reply_to: usize,
    id: String,
}

struct UniqService {
    id: MessageIdGenerator,
    node_id: String,
}
impl DefaultInitService for UniqService {
    fn new(init_message: InitMessage) -> Self {
        Self { id: Default::default(), node_id: init_message.node_id }
    }
}
impl AsyncService<GenerateMessage> for UniqService {
    fn process_message(&self, message: GenerateMessage, meta: MessageMeta) {
        let msg_id = self.id.next();
        let output = GenerateOkMessage {
            msg_id: msg_id,
            in_reply_to: message.msg_id,
            id: format!("{}_{msg_id}", self.node_id),
        };
        output_reply(output, meta);
    }

    fn on_timeout(&self) {
        // empty
    }
}

#[tokio::main]
async fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w unique-ids --bin ./target/release/uniq --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
    /*
{"id":3,"src":"c3","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":1}}
{"src": "c1", "dest": "n1", "body": {"type": "generate", "msg_id": 2}}

     */
    default_init_and_async_loop::<UniqService, GenerateMessage>(get_stub_timeout()).await
}
