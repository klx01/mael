use std::{io, thread, time};
use std::sync::mpsc;
use std::time::Duration;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Message<T> {
    #[serde(flatten)]
    pub meta: MessageMeta,
    pub body: T,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageMeta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<usize>,
    pub src: String,
    pub dest: String,
}

#[derive(Debug, Deserialize)]
pub struct InitMessage {
    msg_id: usize,
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename = "init_ok")]
struct InitOkMessage {
    in_reply_to: usize,
}

pub struct MessageIdGenerator {
    id: usize,
}
impl MessageIdGenerator {
    pub fn new() -> Self {
        Self {id: 0}
    }
    pub fn next(&mut self) -> usize {
        self.id += 1;
        self.id
    }
}
impl Default for MessageIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

pub trait Service<M> {
    fn new(init_message: InitMessage) -> Self;
    fn process_message(&mut self, message: M, meta: MessageMeta);
    fn on_timeout(&mut self) {}
}

pub fn output_reply<T: Serialize>(body: T, input_meta: MessageMeta) {
    let message = Message {
        meta: MessageMeta {
            id: None,
            src: input_meta.dest,
            dest: input_meta.src,
        },
        body,
    };
    output_message(message);
}

pub fn output_message<B: Serialize>(message: Message<B>) {
    let message = serde_json::to_string(&message).expect("failed to serialize to json");
    println!("{message}");
}

pub fn main_loop<T: Service<M>, M: for<'a> Deserialize<'a>>(timeout: u128) {
    let Some(Message {meta, body}) = get_init_message() else {
        return;
    };
    output_reply(InitOkMessage{ in_reply_to: body.msg_id }, meta);

    let mut service = T::new(body);

    let (tx, rx) = mpsc::channel();
    let stdin = io::stdin();
    let input_thread = thread::spawn(move || {
        for line in stdin.lines() {
            tx.send(line).unwrap()
        }
    });

    let mut last_timeout_ts = time::Instant::now();
    if timeout > 0 {
        loop {
            let now = time::Instant::now();
            let time_spent = (now - last_timeout_ts).as_millis();
            if time_spent > timeout {
                service.on_timeout();
                last_timeout_ts = now;
            } else {
                let message = rx.recv_timeout(Duration::from_millis((timeout - time_spent) as u64));
                let message = match message {
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Ok(message) => message,
                };
                match parse_line(message) {
                    Some(message) => service.process_message(message.body, message.meta),
                    None => (),
                }
            }
        }
        service.on_timeout();
    } else {
        for message in rx {
            match parse_line(message) {
                Some(message) => service.process_message(message.body, message.meta),
                None => (),
            }
        }
    }
    let thread_result = input_thread.join();
    if let Err(_) = thread_result {
        eprintln!("input thread error");
    }
}

fn get_init_message() -> Option<Message<InitMessage>> {
    let stdin = io::stdin();
    for line in stdin.lines() {
        match parse_line(line) {
            Some(message) => return Some(message),
            None => (),
        }
    }
    None
}

fn parse_line<M: for<'a> Deserialize<'a>>(line: Result<String, io::Error>) -> Option<Message<M>> {
    let Ok(line) = line else {
        eprintln!("error reading init from stdin: {}", line.err().unwrap());
        return None;
    };
    let message = serde_json::from_str::<Message<M>>(&line);
    let Ok(message) = message else {
        eprintln!("error decoding message: {} {line}", message.err().unwrap());
        return None;
    };
    return Some(message);
}
