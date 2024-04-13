use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use mael::sync_service::{sync_loop, SyncService};
use mael::id_generator::MessageIdGenerator;
use mael::init::{get_init_message, wait_until_message};
use mael::messages::{InitMessage, Message, MessageMeta};
use mael::output::{output_message, output_reply};

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
struct ReadOkMessage<'a> {
    msg_id: usize,
    in_reply_to: usize,
    messages: &'a HashSet<usize>,
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
    messages: HashSet<usize>,
    neighbours: HashMap<String, HashSet<usize>>,
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
            messages: HashSet::new(),
            neighbours: neighbours,
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
    fn add_to_known_for_neighbour(&mut self, neighbour: &String, messages: &HashSet<usize>) -> bool {
        let Some(confirmed) = self.neighbours.get_mut(neighbour) else {
            eprintln!("got a message from {neighbour} which is not in the neighbours list");
            return false;
        };
        confirmed.extend(messages);
        true
    }
}
impl SyncService<InputMessage> for BroadcastService {
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
        for (neighbour, confirmed_messages) in self.neighbours.iter() {
            let to_send_messages = self.messages.difference(confirmed_messages).copied().collect::<HashSet<_>>();
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
            output_message(message);
        }
    }
}

fn get_topology_message(id: &MessageIdGenerator) -> Option<TopologyMessage> {
    let Message {meta, body} = wait_until_message::<TopologyMessage>()?;
    let reply = TopologyOkMessage { msg_id: id.next(), in_reply_to: body.msg_id };
    output_reply(reply, meta);
    Some(body)
}

fn main() {
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast_sync --node-count 1 --time-limit 20 --rate 10
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast_sync --node-count 5 --time-limit 20 --rate 10
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast_sync --node-count 5 --time-limit 20 --rate 10 --nemesis partition
    // cargo build --release && ./maelstrom/maelstrom test -w broadcast --bin ./target/release/broadcast_sync --node-count 25 --time-limit 20 --rate 100 --latency 100
    /*
{"id":3,"src":"c3","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":1}}
{"src": "c1", "dest": "n1", "body": {"type": "topology", "msg_id": 4, "topology": {"n1": ["n2", "n3"], "n2": ["n4"]}}}
{"src": "c1", "dest": "n1", "body": {"type": "broadcast", "msg_id": 2, "message": 2000}}
{"src": "c1", "dest": "n1", "body": {"type": "read", "msg_id": 3}}

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
    let mut service = BroadcastService::new(id_generator, init_message, topology_message);

    sync_loop(&mut service, Some(100..=150));
}
