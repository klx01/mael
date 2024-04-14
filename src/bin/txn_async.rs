use std::collections::HashMap;
use std::sync::{Arc, RwLock};
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

enum StatementError {
    InvalidAction,
    WriteNone,
}

struct TxnService {
    id: MessageIdGenerator,
    node_id: String,
    storage: RwLock<HashMap<usize, usize>>,
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
impl TxnService {
    async fn process_statement(&self, stmt: &Statement, changes: &mut HashMap<usize, usize>) -> Result<Statement, StatementError> {
        let Statement(action, key, value) = stmt;
        match action {
            'r' => {
                if let Some(val) = changes.get(key).copied() {
                    Ok(Statement(*action, *key, Some(val)))
                } else {
                    Ok(Statement(*action, *key, self.read_value(key)))
                }
            },
            'w' => match value {
                Some(value) => {
                    changes.insert(*key, *value);
                    Ok(Statement(*action, *key, Some(*value)))
                },
                None => Err(StatementError::WriteNone),
            },
            _ => Err(StatementError::InvalidAction),
        }
    }
    fn read_value(&self, key: &usize) -> Option<usize> {
        let lock = self.storage.read().expect("got a poisoned lock, cant really handle it");
        let val = lock.get(key).copied();
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        val
    }
    fn flush_writes(&self, changes: HashMap<usize, usize>) {
        let mut lock = self.storage.write().expect("got a poisoned lock, cant really handle it");
        for (key, value) in changes {
            lock.insert(key, value);
        }
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
    }
    fn handle_error(stmt: &Statement, error: StatementError, msg_id: usize, meta: MessageMeta) {
        match error {
            StatementError::InvalidAction => {
                let error = ErrorMessage {
                    in_reply_to: msg_id,
                    code: ErrorCode::MalformedRequest,
                    text: format!("invalid action {}", stmt.0),
                };
                output_reply(error, meta);
            },
            StatementError::WriteNone => {
                let error = ErrorMessage {
                    in_reply_to: msg_id,
                    code: ErrorCode::MalformedRequest,
                    text: "empty write value".to_string(),
                };
                output_reply(error, meta);
            },
        }
    }
}
impl AsyncService<TxnMessage> for TxnService {
    async fn process_message(arc_self: Arc<Self>, message: TxnMessage, meta: MessageMeta) {
        // try to process each statement in the transaction separately from each other, as if they are issued one by one and not as a batch
        let mut response = vec![];
        let mut changes = HashMap::new();

        for stmt in message.txn {
            let stmt_response = arc_self.process_statement(&stmt, &mut changes).await;
            match stmt_response {
                Ok(resp) => response.push(resp),
                Err(err) => {
                    Self::handle_error(&stmt, err, message.msg_id, meta);
                    return;
                }
            }
        }
        arc_self.flush_writes(changes);

        let output = TxnOkMessage {
            msg_id: arc_self.id.next(),
            in_reply_to: message.msg_id,
            txn: response,
        };
        output_reply(output, meta);
    }

    async fn on_timeout(_arc_self: Arc<Self>) {
        // empty
    }
}

#[tokio::main]
async fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w txn-rw-register --bin ./target/release/txn_async --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
    // cargo build --release && ./maelstrom/maelstrom test -w txn-rw-register --bin ./target/release/txn_async --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
    // cargo build --release && ./maelstrom/maelstrom test -w txn-rw-register --bin ./target/release/txn_async --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition

    /*
{"id":3,"src":"c3","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":100}}
{"id":4,"src":"c3","dest":"n1","body":{"type":"txn","msg_id":101,"txn":[["r", 1, null], ["w", 1, 6], ["w", 2, 9]]}}

     */
    default_init_and_async_loop::<TxnService, TxnMessage>(None).await;
}
