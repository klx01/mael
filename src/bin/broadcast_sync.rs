use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use mael::{MessageIdGenerator, SyncService, MessageMeta, output_reply, sync_loop, InitMessage, Message, output_message};

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
struct ReadOkMessage<'a> {
    msg_id: usize,
    in_reply_to: usize,
    messages: &'a HashSet<usize>,
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
    messages: HashSet<usize>,
    neighbours: Option<HashMap<String, HashSet<usize>>>,
}
impl BroadcastService {
    fn add_to_known_for_neighbour(&mut self, neighbour: &String, messages: &HashSet<usize>) -> bool {
        let Some(neighbours) = self.neighbours.as_mut() else {
            eprintln!("empty neighbours list, seems like we've got the message before topology was set");
            return false;
        };
        let Some(confirmed) = neighbours.get_mut(neighbour) else {
            eprintln!("got a message from {neighbour} which is not in the neighbours list");
            return false;
        };
        confirmed.extend(messages);
        true
    }
    fn set_topology(&mut self, message: &mut TopologyMessage) -> bool {
        if self.neighbours.is_some() {
            eprintln!("tried to reinitialize neighbours, ignoring");
            return false;
        }
        
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
        self.neighbours = Some(neighbours_messages);
        true
    }
}
impl SyncService<InputMessage> for BroadcastService {
    fn new(init_message: InitMessage) -> Self {
        Self {
            id: Default::default(),
            node_id: init_message.node_id,
            messages: HashSet::new(),
            neighbours: None,
        }
    }

    fn process_message(&mut self, message: InputMessage, meta: MessageMeta) {
        match message {
            InputMessage::Broadcast(message) => {
                self.messages.insert(message.message);
                let output = BroadcastOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                };
                output_reply(output, meta);
            },
            InputMessage::Read(message) => {
                let output = ReadOkMessage {
                    msg_id: self.id.next(),
                    in_reply_to: message.msg_id,
                    messages: &self.messages,
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
                self.messages.extend(&message.messages);
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

    fn on_timeout(&mut self) {
        let Some(neighbours) = self.neighbours.as_ref() else {
            eprintln!("sync called before we got topology");
            return;
        };
        for (neighbour, confirmed_messages) in neighbours.iter() {
            let to_send_messages = self.messages.difference(confirmed_messages).map(|x| *x).collect::<HashSet<_>>();
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
            output_message(message);
        }
    }
}

fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast_sync --node-count 1 --time-limit 20 --rate 10
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast_sync --node-count 5 --time-limit 20 --rate 10
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast_sync --node-count 5 --time-limit 20 --rate 10 --nemesis partition
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast_sync --node-count 25 --time-limit 20 --rate 100 --latency 100
    /*
{"id":3,"src":"c3","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":1}}
{"src": "c1", "dest": "n1", "body": {"type": "broadcast", "msg_id": 2, "message": 2000}}
{"src": "c1", "dest": "n1", "body": {"type": "read", "msg_id": 3}}
{"src": "c1", "dest": "n1", "body": {"type": "topology", "msg_id": 4, "topology": {"n1": ["n2", "n3"], "n2": ["n4"]}}}

{"src": "n2", "dest": "n1", "body": {"type": "sync_ok", "msg_id": 6, "in_reply_to": 5, "messages": [2000]}}
{"src": "n3", "dest": "n1", "body": {"type": "sync_ok", "msg_id": 6, "in_reply_to": 5, "messages": [2000]}}

     */
    sync_loop::<BroadcastService, InputMessage>(100);
}
