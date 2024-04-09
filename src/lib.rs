use std::{io, thread, time};
use std::sync::{Arc, mpsc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
use tokio::select;

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
    id: AtomicUsize,
}
impl MessageIdGenerator {
    pub fn new() -> Self {
        Self {id: AtomicUsize::new(0)}
    }
    pub fn next(&self) -> usize {
        self.id.fetch_add(1, Ordering::AcqRel)
    }
}
impl Default for MessageIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

pub trait SyncService<M>{
    fn new(init_message: InitMessage) -> Self;
    fn process_message(&mut self, message: M, meta: MessageMeta);
    fn on_timeout(&mut self) {}
}
pub trait AsyncService<M>: Sync + Send + 'static {
    fn new(init_message: InitMessage) -> Self;
    fn process_message(&self, message: M, meta: MessageMeta);
    fn on_timeout(&self) {}
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

static OUT_MUTEX: Mutex<()> = Mutex::new(());
pub fn output_message<B: Serialize>(message: Message<B>) {
    /*
    should i refactor the output to be async? not sure, seems fine this way
     */
    let message = serde_json::to_string(&message).expect("failed to serialize to json");
    let guard = OUT_MUTEX.lock().expect("got a poisoned lock, cant really handle it");
    println!("{message}");
    drop(guard); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop 
}

pub async fn async_loop<T: AsyncService<M>, M: for<'a> Deserialize<'a>>(timeout: u64) {
    let Some(Message {meta, body}) = get_init_message() else {
        return;
    };
    output_reply(InitOkMessage{ in_reply_to: body.msg_id }, meta);

    // is there any better way to handle services that don't need timeouts?
    let timeout = if timeout > 0 { timeout } else { 1000000 };
    let service = Arc::new(T::new(body));

    let tokio_stdin = tokio::io::stdin();
    let reader = tokio::io::BufReader::new(tokio_stdin);
    let mut lines = reader.lines();
    let mut interval = tokio::time::interval(Duration::from_millis(timeout));
    loop {
        select! {
            line = lines.next_line() => {
                let Ok(line) = line else {
                    eprintln!("error reading init from stdin: {}", line.err().unwrap());
                    continue;
                };
                let Some(line) = line else {
                    break;
                };
                let service = service.clone();
                tokio::spawn(async move {
                    let message = serde_json::from_str::<Message<M>>(&line);
                    match message {
                        Ok(message) => service.process_message(message.body, message.meta),
                        Err(err) => eprintln!("error decoding message: {err} {line}"),
                    };
                });
            }
            _ = interval.tick() => {
                let service = service.clone();
                tokio::spawn(async move {
                    service.on_timeout();
                });
            }
        }
    }
}

pub fn sync_loop<T: SyncService<M>, M: for<'a> Deserialize<'a>>(timeout: u128) {
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
