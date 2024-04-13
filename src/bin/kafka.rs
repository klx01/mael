use std::cmp;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use mael::async_service::{AsyncService, default_init_and_async_loop};
use mael::id_generator::MessageIdGenerator;
use mael::init::DefaultInitService;
use mael::keyvalue::{KVResponseMessage, LinKV};
use mael::messages::{AnyResponseMessage, ErrorCode, ErrorMessage, InitMessage, MessageMeta};
use mael::output::output_reply;

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InputMessage {
    Send(SendMessage),
    Poll(PollMessage),
    CommitOffsets(CommitOffsetsMessage),
    ListCommittedOffsets(ListCommittedOffsetsMessage),
    #[serde(untagged)]
    Response(AnyResponseMessage<KVResponseMessage>),
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

const POLL_MAX_LEN: usize = 50;
const REQUEST_TIMEOUT_MILLI: u64 = 1500;

struct KafkaService {
    id: Arc<MessageIdGenerator>,
    node_id: String,
    messages: RwLock<HashMap<String, Vec<usize>>>,
    kv: LinKV,
}
impl DefaultInitService for KafkaService {
    fn new(init_message: InitMessage) -> Self {
        let id = Arc::new(MessageIdGenerator::new());
        let node_id = init_message.node_id;
        let kv = LinKV::new(id.clone(), node_id.clone(), REQUEST_TIMEOUT_MILLI);
        Self {
            id: id,
            node_id: node_id,
            messages: Default::default(),
            kv: kv,
        }
    }
}
impl KafkaService {
    fn respond_timeout(reply_to_id: usize, meta: MessageMeta) {
        let error = ErrorMessage {
            in_reply_to: reply_to_id,
            code: ErrorCode::Timeout,
            text: "".to_string(),
        };
        output_reply(error, meta);
    }
    async fn await_with_timeout<T, F: Future<Output=T>>(future: F, reply_to_id: usize, meta: &MessageMeta) -> Option<T> {
        let result = tokio::time::timeout(
            Duration::from_millis(REQUEST_TIMEOUT_MILLI),
            future,
        ).await;

        match result {
            Ok(result) => Some(result),
            Err(_) => {
                Self::respond_timeout(reply_to_id, meta.clone());
                None
            }
        }
    }

    fn get_store_key_messages(key: &String) -> String {
        format!("msg_{key}")
    }
    async fn append_message_to_key(&self, key: &String, message: usize) -> usize {
        loop {
            let mut messages_in_key = self.copy_messages_in_key(&key);
            messages_in_key.push(message);
            let store_success = self.kv.cas_typed(
                Self::get_store_key_messages(key),
                messages_in_key[..messages_in_key.len() - 1].to_vec(),
                messages_in_key.clone(),
                true,
            ).await;
            if store_success {
                let offset = messages_in_key.len() - 1;
                let set_success = self.set_messages_for_key(key, messages_in_key);
                /*
                it should be impossible for new vec to be smaller than old one.
                the only way for the vec to grow is to CAS, 
                so if we were able to CAS, it means that no one else was able to do it  
                 */
                assert!(set_success, "this should never fail");
                return offset;
            }
            while !self.refresh_messages_in_key(&key).await {}
        }
    }
    fn copy_messages_in_key(&self, key: &String) -> Vec<usize> {
        let mut lock = self.messages.read().expect("got a poisoned lock, cant really handle it");
        if !lock.contains_key(key) {
            drop(lock);
            self.init_key(key.clone());
            lock = self.messages.read().expect("got a poisoned lock, cant really handle it");
        }
        let messages = lock.get(key).expect("did not get messages after init ????");
        let copy = messages.clone();
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        copy
    }
    fn init_key(&self, key: String) {
        let mut lock = self.messages.write().expect("got a poisoned lock, cant really handle it");
        lock.entry(key).or_insert(vec![]);
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
    }
    async fn refresh_messages_in_key(&self, key: &String) -> bool {
        let messages = self.kv.read_typed(Self::get_store_key_messages(key)).await;
        let Ok(messages) = messages else {
            return false;
        };
        /*
        this set can fail, if we have inserted into vec between the calls to read and set in this method
        but it also means that our vec is already fresh
         */
        let _ = self.set_messages_for_key(key, messages);
        true
    }
    async fn refresh_messages_in_key_safe(&self, key: &String) -> bool {
        let has_key = self.has_key(key);
        if !has_key {
            self.init_key(key.clone());
        }
        self.refresh_messages_in_key(key).await
    }
    fn set_messages_for_key(&self, key: &String, new_messages: Vec<usize>) -> bool {
        let new_len = new_messages.len();
        let mut lock = self.messages.write().expect("got a poisoned lock, cant really handle it");
        let messages = lock.get_mut(key).expect("called set messages for a key that does not exist");
        if new_len < messages.len() {
            drop(lock);
            return false;
        }
        *messages = new_messages;
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        true
    }

    fn copy_lengths(&self) -> HashMap<String, usize> {
        let lock = self.messages.read().expect("got a poisoned lock, cant really handle it");
        let result = lock
            .iter()
            .map(|(key, val)| (key.clone(), val.len()))
            .collect();
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        result
    }
    fn get_store_key_committed() -> String {
        "committed".to_string()
    }
    async fn commit_offsets(&self, new_offsets: HashMap<String, usize>) {
        loop {
            /*
            we need to CAS the values,
            so we need to be sure that the old value that we send in CAS request is exactly the same the stored one.
            hashmaps don't guarantee the order of elements,
            so if we use read_typed<HashMap>, then we would not be able to make a correct CAS request.
            so we read and store the value as String, and not as HashMap
             */
            let read_result = self.read_committed_offsets().await;
            let Some((mut offsets, old_value)) = read_result else {
                continue;
            };
            for (key, offset) in new_offsets.iter() {
                let committed = offsets.entry(key.clone()).or_insert(0);
                if *offset >= *committed {
                    *committed = *offset;
                }
            }
            let save_success = self.kv.cas_hash_map(Self::get_store_key_committed(), old_value, offsets, true).await;
            if save_success {
                return;
            }
        }
    }
    async fn read_committed_offsets(&self) -> Option<(HashMap<String, usize>, String)> {
        self.kv.read_hash_map_for_cas(Self::get_store_key_committed()).await
    }

    fn get_store_key_keys() -> String {
        "keys".to_string()
    }
    async fn sync_keys_list(&self) -> Option<Vec<String>> {
        let read_result = self.kv.read_hash_map_for_cas::<usize>(Self::get_store_key_keys()).await;
        let Some((stored_key_lengths, old_value)) = read_result else {
            return None;
        };
        let mut node_key_lengths = self.copy_lengths();
        let mut to_sync_keys = vec![];
        let mut need_save = node_key_lengths
            .keys()
            .find(|k| !stored_key_lengths.contains_key(*k))
            .is_some();
        for (key, stored_length) in stored_key_lengths {
            let node_length = node_key_lengths.get(&key).copied().unwrap_or(0);
            if stored_length > node_length {
                to_sync_keys.push(key.clone());
                node_key_lengths.insert(key, stored_length);
            } else if stored_length < node_length {
                need_save = true;
            }
        }
         
        if need_save {
            let save_success = self.kv.cas_hash_map(Self::get_store_key_keys(), old_value, node_key_lengths, true).await;
            if !save_success {
                return None;
            }   
        }
        Some(to_sync_keys)
    }
    fn has_key(&self, key: &String) -> bool {
        let lock = self.messages.read().expect("got a poisoned lock, cant really handle it");
        let has_key = lock.contains_key(key);
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        has_key
    }
}
impl AsyncService<InputMessage> for KafkaService {
    async fn process_message(&self, message: InputMessage, meta: MessageMeta) {
        match message {
            InputMessage::Send(message) => {
                let offset = Self::await_with_timeout(
                    self.append_message_to_key(&message.key, message.msg),
                    message.msg_id,
                    &meta,
                ).await;
                let Some(offset) = offset else {
                    return;
                };
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

                let result = Self::await_with_timeout(
                    self.commit_offsets(message.offsets),
                    message.msg_id,
                    &meta,
                ).await;
                let Some(_) = result else {
                    return;
                };

                let output = CommitOffsetsOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                };
                output_reply(output, meta);
            }
            InputMessage::ListCommittedOffsets(message) => {
                let offsets = Self::await_with_timeout(
                    self.read_committed_offsets(),
                    message.msg_id,
                    &meta,
                ).await;
                let Some(offsets) = offsets else {
                    return;
                };
                let Some((offsets, _)) = offsets else {
                    let error = ErrorMessage {
                        in_reply_to: message.msg_id,
                        code: ErrorCode::Unexpected,
                        text: "Unexpected error when reading from the storage".to_string(),
                    };
                    output_reply(error, meta);
                    return;
                };

                let mut response = HashMap::new();
                for key in message.keys {
                    let offset = offsets.get(&key);
                    let Some(offset) = offset else {
                        // the task description mentions that keys that don't exist can be omitted
                        continue;
                    };
                    response.insert(key, *offset);
                }

                let output = ListCommittedOffsetsOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                    offsets: response,
                };
                output_reply(output, meta);
            }
            InputMessage::Response(reply) => {
                self.kv.init_response(reply.in_reply_to, reply.data);
            }
        }
    }

    async fn on_timeout(arc_self: Arc<Self>) {
        let keys_to_update = arc_self.sync_keys_list().await;
        let Some(keys_to_update) = keys_to_update else {
            return;
        };
        let mut join_set = JoinSet::new();
        for key in keys_to_update {
            let arc_self = arc_self.clone();
            join_set.spawn(async move {
                arc_self.refresh_messages_in_key_safe(&key).await;
            });
        }
        while let Some(res) = join_set.join_next().await {
            if let Err(err) = res {
                eprintln!("error joining task {err}");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w kafka --bin ./target/release/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 10
    // cargo build --release && ./maelstrom/maelstrom test -w kafka --bin ./target/release/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 10
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
    default_init_and_async_loop::<KafkaService, InputMessage>(Some(400..=600)).await;
}
