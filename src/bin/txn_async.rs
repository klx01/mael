use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};
use mael::async_service::{AsyncService, default_init_and_async_loop};
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
    storage: Mutex<HashMap<usize, usize>>,
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
impl AsyncService<TxnMessage> for TxnService {
    async fn process_message(arc_self: Arc<Self>, mut message: TxnMessage, meta: MessageMeta) {
        // todo: try to implement this without having a global mutex for the whole transaction
        let mut storage = arc_self.storage.lock().expect("got a poisoned lock, cant really handle it");
        for stmt in message.txn.iter_mut() {
            let Statement(action, key, value) = stmt;
            match action {
                'r' => {
                    *value = storage.get(key).copied();
                },
                'w' => match value {
                    Some(value) => {
                        storage.insert(*key, *value);
                    },
                    None => {
                        drop(storage);
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
                    drop(storage);
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
        drop(storage);
        
        let output = TxnOkMessage {
            msg_id: arc_self.id.next(),
            in_reply_to: message.msg_id,
            txn: message.txn,
        };
        output_reply(output, meta);
    }

    async fn on_timeout(arc_self: Arc<Self>) {
        // empty
    }
}

#[tokio::main]
async fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w txn-rw-register --bin ./target/release/txn_async --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
    // cargo build --release && ./maelstrom/maelstrom test -w txn-rw-register --bin ./target/release/txn_async --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
    // cargo build --release && ./maelstrom/maelstrom test -w txn-rw-register --bin ./target/release/txn_async --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition
    
    /*
    todo:
        6c passes even with this implementation. This is probably not intended, but i'm not sure what is intended
        looks like it is not even required to replicate the writes between nodes, and it is not required for nodes to eventually end up in the same state  
     */

    /*
{"id":3,"src":"c3","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":100}}
{"id":4,"src":"c3","dest":"n1","body":{"type":"txn","msg_id":101,"txn":[["r", 1, null], ["w", 1, 6], ["w", 2, 9]]}}

     */
    default_init_and_async_loop::<TxnService, TxnMessage>(None).await;
}
