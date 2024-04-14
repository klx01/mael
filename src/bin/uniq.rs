use std::sync::Arc;
use serde::{Deserialize, Serialize};
use mael::async_service::{AsyncService, default_init_and_async_loop};
use mael::id_generator::MessageIdGenerator;
use mael::init::DefaultInitService;
use mael::messages::{InitMessage, MessageMeta};
use mael::output::output_reply;

struct UniqService {
    id: MessageIdGenerator,
    node_id: String,
}
impl AsyncService<GenerateMessage> for UniqService {
    async fn process_message(arc_self: Arc<Self>, message: GenerateMessage, meta: MessageMeta) {
        let msg_id = arc_self.id.next();
        let output = GenerateOkMessage {
            msg_id: msg_id,
            in_reply_to: message.msg_id,
            id: format!("{}_{msg_id}", arc_self.node_id),
        };
        output_reply(output, meta);
    }

    async fn on_timeout(_arc_self: Arc<Self>) {
        // empty
    }
}
impl DefaultInitService for UniqService {
    fn new(init_message: InitMessage) -> Self {
        Self { id: Default::default(), node_id: init_message.node_id }
    }
}

#[tokio::main]
async fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w unique-ids --bin ./target/release/uniq --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
    /*
{"id":3,"src":"c3","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":1}}
{"src": "c1", "dest": "n1", "body": {"type": "generate", "msg_id": 2}}

     */
    default_init_and_async_loop::<UniqService, GenerateMessage>(None).await
}

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
