use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use mael::sync_service::{SyncService, default_init_and_sync_loop};
use mael::id_generator::MessageIdGenerator;
use mael::init::DefaultInitService;
use mael::messages::{ErrorCode, ErrorMessage, InitMessage, MessageMeta};
use mael::output::output_reply;

#[derive(Debug, Deserialize)]
struct TxnMessage {
    msg_id: usize,
    txn: Vec<Statement>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Statement(char, usize, Option<usize>);

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "txn_ok")]
struct TxnOkMessage {
    msg_id: usize,
    in_reply_to: usize,
    txn: Vec<Statement>,
}

struct TxnService {
    id: MessageIdGenerator,
    node_id: String,
    storage: HashMap<usize, usize>,
}
impl DefaultInitService for TxnService {
    fn new(init_message: InitMessage) -> Self {
        Self {
            id: Default::default(),
            node_id: init_message.node_id,
            storage: Default::default(),
        }
    }
}
impl SyncService<TxnMessage> for TxnService {
    fn process_message(&mut self, mut message: TxnMessage, meta: MessageMeta) {
        for stmt in message.txn.iter_mut() {
            let Statement(action, key, value) = stmt;
            match action {
                'r' => {
                    *value = self.storage.get(key).copied();
                },
                'w' => match value { 
                    Some(value) => {
                        self.storage.insert(*key, *value);
                    },
                    None => {
                        let error = ErrorMessage {
                            in_reply_to: message.msg_id,
                            code: ErrorCode::MalformedRequest,
                            text: "empty write value".to_string(),
                        };
                        output_reply(error, meta);
                        return;
                    },
                },
                _ => {
                    let error = ErrorMessage {
                        in_reply_to: message.msg_id,
                        code: ErrorCode::MalformedRequest,
                        text: format!("invalid action {action}"),
                    };
                    output_reply(error, meta);
                    return;
                },
            }
        }
        let output = TxnOkMessage {
            msg_id: self.id.next(),
            in_reply_to: message.msg_id,
            txn: message.txn,
        };
        output_reply(output, meta);
    }

    fn on_timeout(&mut self) {
        
    }
}

fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w txn-rw-register --bin ./target/release/txn --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
    /*
{"id":3,"src":"c3","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":100}}
{"id":4,"src":"c3","dest":"n1","body":{"type":"txn","msg_id":101,"txn":[["r", 1, null], ["w", 1, 6], ["w", 2, 9]]}}

     */
    default_init_and_sync_loop::<TxnService, TxnMessage>(None);
}
