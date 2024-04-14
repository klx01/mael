use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use mael::async_service::{AsyncService, default_init_and_async_loop};
use mael::id_generator::MessageIdGenerator;
use mael::init::DefaultInitService;
use mael::messages::{ErrorCode, ErrorMessage, InitMessage, MessageMeta};
use mael::output::output_reply;

enum StatementError {
    InvalidAction(char),
    WriteNone,
    LockWaitTimeout,
}

struct TxnService {
    id: MessageIdGenerator,
    node_id: String,
    storage: RwLock<HashMap<usize, RwLock<Option<usize>>>>,
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
                    Self::handle_error(err, message.msg_id, meta);
                    return;
                }
            }
        }
        let flush_result = arc_self.flush_writes(changes).await;
        if let Err(err) = flush_result {
            Self::handle_error(err, message.msg_id, meta);
            return;
        }

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
impl TxnService {
    async fn process_statement(&self, stmt: &Statement, changes: &mut HashMap<usize, usize>) -> Result<Statement, StatementError> {
        let Statement(action, key, value) = stmt;
        match action {
            'r' => {
                if let Some(val) = changes.get(key).copied() {
                    Ok(Statement(*action, *key, Some(val)))
                } else {
                    Ok(Statement(*action, *key, self.read_value(key).await))
                }
            },
            'w' => match value {
                Some(value) => {
                    changes.insert(*key, *value);
                    Ok(Statement(*action, *key, Some(*value)))
                },
                None => Err(StatementError::WriteNone),
            },
            _ => Err(StatementError::InvalidAction(*action)),
        }
    }
    async fn read_value(&self, key: &usize) -> Option<usize> {
        let storage_guard = self.storage.read().await;
        let value_lock = storage_guard.get(key);

        let Some(value_lock) = value_lock else {
            drop(storage_guard);
            return None;
        };
        let value_guard = value_lock.read().await;
        let value = *value_guard;
        drop(value_guard);
        drop(storage_guard); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop

        value
    }
    async fn flush_writes(&self, changes: HashMap<usize, usize>) -> Result<(), StatementError> {
        if changes.len() == 0 {
            return Ok(());
        }
        /*
        most of the complexity comes from trying to achieve this:
            1) don't take a global write lock for all writes
            2) if 2 concurrent transactions are writing into the same set of keys at the same time, we should see results only of one of them.
                we should not see values from the 1st one in one part of keys, and values from 2nd one for another part of keys
            3) given both of the above, deadlocks are possible. In case of a deadlock, one tx should be rolled back completely
         */
        self.init_new_keys(self.get_new_keys(&changes).await).await;

        let storage_guard = self.storage.read().await;
        let mut value_guards = HashMap::new();
        for key in changes.keys() {
            let value_lock = storage_guard.get(key).expect("values should be present for all keys, the call to init_new_keys() above should take care of that");
            let value_guard_future = value_lock.write();
            let value_guard = tokio::time::timeout(
                Duration::from_millis(50),
                value_guard_future,
            ).await;
            let Ok(value_guard) = value_guard else {
                drop(value_guard);
                drop(value_guards);
                drop(storage_guard);
                return Err(StatementError::LockWaitTimeout);
            };
            value_guards.insert(*key, value_guard);
        }
        for (key, value) in changes {
            let value_guard = value_guards.get_mut(&key).unwrap();
            **value_guard = Some(value);
        }

        drop(value_guards); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        drop(storage_guard);
        Ok(())
    }
    async fn get_new_keys(&self, changes: &HashMap<usize, usize>) -> Vec<usize> {
        let storage_guard = self.storage.read().await;
        let res = changes
            .keys()
            .filter(|key| !storage_guard.contains_key(*key))
            .copied()
            .collect();
        drop(storage_guard); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        res
    }
    async fn init_new_keys(&self, keys: Vec<usize>) {
        if keys.len() == 0 {
            return;
        }
        let mut storage_guard = self.storage.write().await;
        for key in keys {
            // if the key was inserted between collecting the list and taking the write lock, we need to make sure to not overwrite it yet, because current tx can be rolled back
            storage_guard.entry(key).or_insert(RwLock::new(None));
        }
        drop(storage_guard); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
    }
    fn handle_error(error: StatementError, msg_id: usize, meta: MessageMeta) {
        match error {
            StatementError::InvalidAction(action) => {
                let error = ErrorMessage {
                    in_reply_to: msg_id,
                    code: ErrorCode::MalformedRequest,
                    text: format!("invalid action {action}"),
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
            StatementError::LockWaitTimeout => {
                let error = ErrorMessage {
                    in_reply_to: msg_id,
                    code: ErrorCode::TxnConflict,
                    text: "lock wait timeout".to_string(),
                };
                output_reply(error, meta);
            },
        }
    }
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
