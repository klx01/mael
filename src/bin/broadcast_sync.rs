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
struct TopologyMessage {
    msg_id: usize,
    #[allow(dead_code)]
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
}
impl BroadcastService {
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
    fn new(init_message: InitMessage) -> Self {
        let node_ids = init_message.node_ids;
        let other_nodes_count = node_ids.len() - 1;
        let nodes_to_notify_count = if other_nodes_count > 0 { (other_nodes_count / 2) + 1} else { 0 };
        let current_pos = node_ids.iter().position(|x| *x == init_message.node_id).unwrap();
        let copy = node_ids.clone(); // this is unpleasant, but i'm not sure how else to do it
        /*
        we are taking the next nodes_to_notify_count nodes in order.
        at first i was just taking any nodes_to_notify_count nodes randomly, but in that case it is possible for a node to be no one's neighbour.
        and this way we can guarantee that all nodes are someone's neighbours.
        i would like the nodes that we take to be shuffled, and collecting them into a hashmap kind of takes care of that
         */
        let neighbours = node_ids.into_iter()
            .chain(copy.into_iter())
            .skip(current_pos + 1)
            .take(nodes_to_notify_count)
            .map(|node| (node, HashSet::new()))
            .collect::<HashMap<_, _>>();
        Self {
            id: Default::default(),
            node_id: init_message.node_id,
            messages: HashSet::new(),
            neighbours: neighbours,
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
            InputMessage::Topology(message) => {
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
        for (neighbour, confirmed_messages) in self.neighbours.iter() {
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
                body: SyncMessage {
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
    sync_loop::<BroadcastService, InputMessage>(400, 600);
}
