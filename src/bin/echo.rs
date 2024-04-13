use std::sync::Arc;
use serde::{Deserialize, Serialize};
use mael::async_service::{AsyncService, default_init_and_async_loop};
use mael::id_generator::MessageIdGenerator;
use mael::init::DefaultInitService;
use mael::messages::{InitMessage, MessageMeta};
use mael::output::output_reply;

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "echo")]
struct EchoMessage {
    msg_id: usize,
    echo: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "echo_ok")]
struct EchoOkMessage {
    msg_id: usize,
    in_reply_to: usize,
    echo: String,
}

struct EchoService {
    id: MessageIdGenerator,
}
impl DefaultInitService for EchoService {
    fn new(_: InitMessage) -> Self {
        Self { id: Default::default() }
    }
}
impl AsyncService<EchoMessage> for EchoService {
    async fn process_message(&self, message: EchoMessage, meta: MessageMeta) {
        let output = EchoOkMessage {
            msg_id: self.id.next(),
            in_reply_to: message.msg_id,
            echo: message.echo,
        };
        output_reply(output, meta);
    }

    async fn on_timeout(_arc_self: Arc<Self>) {
        // empty
    }
}

#[tokio::main]
async fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w echo --bin ./target/release/echo --node-count 1 --time-limit 10
    /*
{"id":3,"src":"c3","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":1}}
{"src": "c1", "dest": "n1", "body": {"type": "echo", "msg_id": 2, "echo": "Please echo 35"}}

     */
    default_init_and_async_loop::<EchoService, EchoMessage>(None).await
}
