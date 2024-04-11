use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use mael::{MessageIdGenerator, AsyncService, MessageMeta, output_reply, InitMessage, Message, output_message, get_init_message, async_loop_with_timeout, wait_until_message};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InputMessage {
    Broadcast(BroadcastMessage),
    Read(ReadMessage),
    Sync(SyncMessage),
    SyncOk(SyncOkMessage),
}

#[derive(Debug, Deserialize)]
struct BroadcastMessage {
    msg_id: usize,
    message: usize,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "broadcast_ok")]
struct BroadcastOkMessage {
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
    messages: HashSet<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "topology")]
struct TopologyMessage {
    msg_id: usize,
    topology: HashMap<String, Vec<String>>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "topology_ok")]
struct TopologyOkMessage {
    msg_id: usize,
    in_reply_to: usize,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "sync")]
struct SyncMessage {
    msg_id: usize,
    messages: HashSet<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "sync_ok")]
struct SyncOkMessage {
    msg_id: usize,
    in_reply_to: usize,
    messages: HashSet<usize>,
}

struct BroadcastService {
    id: MessageIdGenerator,
    node_id: String,
    messages: RwLock<HashSet<usize>>,
    neighbours: RwLock<HashMap<String, HashSet<usize>>>,
    /*
    messages and neighbours grow indefinitely.
    there is nothing we can do about messages, due to the nature of the task,
    but neighbours can be compacted.
    we can periodically extract messages that are known to all neighbours into a one separate set.
    and in can also be compacted together with current node's messages
     */
}
impl BroadcastService {
    fn new(id: MessageIdGenerator, init_message: InitMessage, topology_message: TopologyMessage) -> Self {
        let node_id = init_message.node_id;
        let neighbours = Self::extract_neighbours(topology_message, &node_id);
        Self {
            id: id,
            node_id: node_id,
            messages: RwLock::new(HashSet::new()),
            neighbours: RwLock::new(neighbours),
        }
    }
    fn extract_neighbours(mut topology_message: TopologyMessage, current_id: &String) -> HashMap<String, HashSet<usize>> {
        let neighbours = topology_message.topology.remove(current_id).expect("no neighbours for current node in the topology message");
        let mut neighbours_messages = HashMap::new();
        for neighbour in neighbours {
            neighbours_messages.insert(neighbour, HashSet::new());
        }
        neighbours_messages
    }
    fn copy_messages(&self) -> HashSet<usize> {
        /*
        i wonder if it's possible to refactor some usages to remove cloning? does not seem like it is at the moment
         */
        let lock = self.messages.read().expect("got a poisoned lock, cant really handle it");
        let copy = lock.clone();
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        copy
    }
    fn copy_neighbours(&self) -> HashMap<String, HashSet<usize>> {
        /*
        i wonder if it's possible to refactor some usages to remove cloning? does not seem like it is at the moment
         */
        let lock = self.neighbours.read().expect("got a poisoned lock, cant really handle it");
        let copy = lock.clone();
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        copy
    }
    fn add_to_known_for_neighbour(&self, neighbour: &String, messages: &HashSet<usize>) -> bool {
        let mut lock = self.neighbours.write().expect("got a poisoned lock, cant really handle it");
        let Some(confirmed) = lock.get_mut(neighbour) else {
            drop(lock);
            eprintln!("got a message from {neighbour} which is not in the neighbours list");
            return false;
        };
        confirmed.extend(messages);
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        true
    }
}
impl AsyncService<InputMessage> for BroadcastService {
    fn process_message(&self, message: InputMessage, meta: MessageMeta) {
        match message {
            InputMessage::Broadcast(message) => {
                let mut lock = self.messages.write().expect("got a poisoned lock, cant really handle it");
                lock.insert(message.message);
                drop(lock);

                let output = BroadcastOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                };
                output_reply(output, meta);
            },
            InputMessage::Read(message) => {
                let messages = self.copy_messages();
                let output = ReadOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                    messages: messages,
                };
                output_reply(output, meta);
            },
            InputMessage::Sync(message) => {
                let mut lock = self.messages.write().expect("got a poisoned lock, cant really handle it");
                lock.extend(&message.messages);
                drop(lock);

                self.add_to_known_for_neighbour(&meta.src, &message.messages);
                let output = SyncOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                    messages: message.messages,
                };
                output_reply(output, meta);
            }
            InputMessage::SyncOk(message) => {
                if !self.add_to_known_for_neighbour(&meta.src, &message.messages) {
                    panic!("got a sync ok response from an unknown neighbour");
                }
            }
        }
    }

    fn on_timeout(&self) {
        let neighbours = self.copy_neighbours();
        let messages = self.copy_messages();
        for (neighbour, confirmed_messages) in neighbours.iter() {
            /*
            i would want to move the whole body of this loop into the async spawn
            but i can't, because it references self.
            i could extract those references outside, but that would mean incrementing the ids when we don't send messages
            every 100ms by the amount of neighbours. And i don't really like that.
            or maybe refactor it in a way that could use the Arc?
            so i've only left the output inside the spawn. Is there even any point in that?
            not sure. But it can block, so i guess there might be some point
             */
            let to_send_messages = messages.difference(confirmed_messages).copied().collect::<HashSet<_>>();
            if to_send_messages.len() == 0 {
                continue;
            }
            let message = Message {
                meta: MessageMeta {
                    id: None,
                    src: self.node_id.clone(),
                    dest: neighbour.clone(),
                },
                body: SyncMessage {
                    msg_id: self.id.next(),
                    messages: to_send_messages,
                },
            };
            tokio::spawn(async move {
                output_message(message);
            });
        }
    }
}

fn get_topology_message(id: &MessageIdGenerator) -> Option<TopologyMessage> {
    let Message {meta, body} = wait_until_message::<TopologyMessage>()?;
    let reply = TopologyOkMessage { msg_id: id.next(), in_reply_to: body.msg_id };
    output_reply(reply, meta);
    Some(body)
}

#[tokio::main]
async fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 1 --time-limit 20 --rate 10
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 5 --time-limit 20 --rate 10
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
    /*
{"id":3,"src":"c3","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":1}}
{"src": "c1", "dest": "n1", "body": {"type": "broadcast", "msg_id": 2, "message": 2000}}
{"src": "c1", "dest": "n1", "body": {"type": "read", "msg_id": 3}}
{"src": "c1", "dest": "n1", "body": {"type": "topology", "msg_id": 4, "topology": {"n1": ["n2", "n3"], "n2": ["n4"]}}}

{"src": "n2", "dest": "n1", "body": {"type": "sync_ok", "msg_id": 6, "in_reply_to": 5, "messages": [2000]}}
{"src": "n3", "dest": "n1", "body": {"type": "sync_ok", "msg_id": 6, "in_reply_to": 5, "messages": [2000]}}

     */
    let Some(init_message) = get_init_message() else {
        return;
    };
    let id_generator = MessageIdGenerator::default();
    let Some(topology_message) = get_topology_message(&id_generator) else {
        return;
    };
    let service = BroadcastService::new(id_generator, init_message, topology_message);

    async_loop_with_timeout(Arc::new(service), 100..=150).await;
}
