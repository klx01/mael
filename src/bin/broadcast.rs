use std::collections::{HashMap, HashSet};
use std::sync::{RwLock};
use serde::{Deserialize, Serialize};
use mael::{MessageIdGenerator, AsyncService, MessageMeta, output_reply, async_loop, InitMessage, Message, output_message};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InputMessage {
    Broadcast(BroadcastMessage),
    Read(ReadMessage),
    Topology(TopologyMessage),
    Sync(SyncMessageReceive),
    SyncOk(SyncOkMessage),
}

#[derive(Debug, Deserialize, Serialize)]
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


#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "sync")]
struct SyncMessageSend {
    msg_id: usize,
    messages: HashSet<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "sync")]
struct SyncMessageReceive {
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
    neighbours: RwLock<Option<HashMap<String, HashSet<usize>>>>,
}
impl BroadcastService {
    fn copy_messages(&self) -> HashSet<usize> {
        /*
        i wonder if it's possible to refactor some usages to remove cloning? does not seem like it is at the moment
         */
        let lock = self.messages.read().expect("got a poisoned lock, cant really handle it");
        let copy = lock.clone();
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        copy
    }
    fn copy_neighbours(&self) -> Option<HashMap<String, HashSet<usize>>> {
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
        let Some(neighbours) = lock.as_mut() else {
            drop(lock);
            eprintln!("empty neighbours list, seems like we've got the message before topology was set");
            return false;
        };
        let Some(confirmed) = neighbours.get_mut(neighbour) else {
            drop(lock);
            eprintln!("got a message from {neighbour} which is not in the neighbours list");
            return false;
        };
        confirmed.extend(messages);
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        true
    }
    fn set_topology(&self, message: &mut TopologyMessage) -> bool {
        let neighbours = match message.topology.remove(&self.node_id) {
            Some(neighbours) => neighbours,
            None => {
                eprintln!("no neighbours for current node in the topology message");
                return false;
            }
        };
        let mut neighbours_messages = HashMap::new();
        for neighbour in neighbours {
            neighbours_messages.insert(neighbour, HashSet::new());
        }

        let mut lock = self.neighbours.write().expect("got a poisoned lock, cant really handle it");
        if lock.as_ref().is_some() {
            drop(lock);
            eprintln!("tried to reinitialize neighbours, ignoring");
            return false;
        }
        lock.replace(neighbours_messages);
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        true
    }
}
impl AsyncService<InputMessage> for BroadcastService {
    fn new(init_message: InitMessage) -> Self {
        Self {
            id: Default::default(),
            node_id: init_message.node_id,
            messages: RwLock::new(HashSet::new()),
            neighbours: RwLock::new(None),
        }
    }

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
            InputMessage::Topology(mut message) => {
                self.set_topology(&mut message);
                let output = TopologyOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
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
        let Some(neighbours) = neighbours else {
            eprintln!("sync called before we got topology");
            return;
        };
        let messages = self.copy_messages();
        for (neighbour, confirmed_messages) in neighbours.iter() {
            /*
            i would want to move the whole body of this loop into the async spawn
            but i can't, because it references self.
            i could extract those references outside, but that would mean incrementing the ids when we don't send messages
            every 100ms by the amount of neighbours. And i don't really like that.
            so i've only left the output inside the spawn. Is there even any point in that?
            not sure. But it can block, so i guess there might be some point
             */
            let to_send_messages = messages.difference(confirmed_messages).map(|x| *x).collect::<HashSet<_>>();
            if to_send_messages.len() == 0 {
                continue;
            }
            let message = Message {
                meta: MessageMeta {
                    id: None,
                    src: self.node_id.clone(),
                    dest: neighbour.clone(),
                },
                body: SyncMessageSend {
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
    async_loop::<BroadcastService, InputMessage>(100).await;
}
