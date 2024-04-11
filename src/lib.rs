use std::{io, thread, time};
use std::ops::RangeInclusive;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use tokio::io::AsyncBufReadExt;
use tokio::select;
use tokio::time::Instant;

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

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "error")]
pub struct ErrorMessage {
    pub in_reply_to: usize,
    pub code: ErrorCode,
    pub text: String,
}

#[derive(Debug, Serialize_repr, Deserialize_repr, Copy, Clone)]
#[repr(u8)]
pub enum ErrorCode {
    Timeout = 0,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    PreconditionFailed = 22,
    TxnConflict = 30,
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
    fn process_message(&mut self, message: M, meta: MessageMeta);
    fn on_timeout(&mut self);
}
pub trait AsyncService<M>: Sync + Send + 'static {
    fn process_message(&self, message: M, meta: MessageMeta);
    fn on_timeout(&self);
}
pub trait DefaultInitService {
    fn new(init_message: InitMessage) -> Self;
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

pub async fn async_loop_with_timeout<T: AsyncService<M>, M: for<'a> Deserialize<'a>>(service: Arc<T>, timeout_range: RangeInclusive<u64>) {
    assert!(!timeout_range.is_empty());
    let tokio_stdin = tokio::io::stdin();
    let reader = tokio::io::BufReader::new(tokio_stdin);
    let mut lines = reader.lines();
    let timeout = rand::thread_rng().gen_range(timeout_range.clone());
    let stub_duration = Duration::from_millis(1000);
    let mut interval = tokio::time::interval_at(Instant::now() + Duration::from_millis(timeout), stub_duration);
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
                let timeout = rand::thread_rng().gen_range(timeout_range.clone());
                interval = tokio::time::interval_at(Instant::now() + Duration::from_millis(timeout), stub_duration);
            }
        }
    }
}

pub fn sync_loop_with_timeout<T: SyncService<M>, M: for<'a> Deserialize<'a>>(service: &mut T, timeout_range: RangeInclusive<u128>) {
    assert!(!timeout_range.is_empty());
    let (tx, rx) = mpsc::channel();
    let stdin = io::stdin();
    let input_thread = thread::spawn(move || {
        for line in stdin.lines() {
            tx.send(line).unwrap()
        }
    });

    let mut timeout = rand::thread_rng().gen_range(timeout_range.clone());
    let mut last_timeout_ts = time::Instant::now();
    loop {
        let now = time::Instant::now();
        let time_spent = (now - last_timeout_ts).as_millis();
        if time_spent > timeout {
            service.on_timeout();
            last_timeout_ts = now;
            timeout = rand::thread_rng().gen_range(timeout_range.clone());
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
    let thread_result = input_thread.join();
    if let Err(_) = thread_result {
        eprintln!("input thread error");
    }
}

pub fn sync_loop_simple<T: SyncService<M>, M: for<'a> Deserialize<'a>>(service: &mut T) {
    let stdin = io::stdin();
    for line in stdin.lines() {
        match parse_line(line) {
            Some(message) => service.process_message(message.body, message.meta),
            None => (),
        }
    }
}

pub fn default_init_service<T: DefaultInitService>() -> Option<T> {
    let init_message = get_init_message()?;
    let service = T::new(init_message);
    Some(service)
}

pub async fn default_init_and_async_loop<T: AsyncService<M> + DefaultInitService, M: for<'a> Deserialize<'a>>(timeout: RangeInclusive<u64>) {
    let Some(service) = default_init_service::<T>() else {
        return;
    };
    let service_arc = Arc::new(service);
    async_loop_with_timeout(service_arc, timeout).await
}

pub fn default_init_and_sync_loop_with_timeout<T: SyncService<M> + DefaultInitService, M: for<'a> Deserialize<'a>>(timeout: RangeInclusive<u128>) {
    let Some(mut service) = default_init_service::<T>() else {
        return;
    };
    sync_loop_with_timeout(&mut service, timeout);
}

pub fn default_init_and_sync_loop_simple<T: SyncService<M> + DefaultInitService, M: for<'a> Deserialize<'a>>() {
    let Some(mut service) = default_init_service::<T>() else {
        return;
    };
    sync_loop_simple(&mut service);
}

pub fn get_init_message() -> Option<InitMessage> {
    let Message {meta, body} = wait_until_message::<InitMessage>()?;
    /*
    need to correctly handle the situation where init_ok message was lost, and init request was retried
    same for topology request in the broadcast challenge 
     */
    output_reply(InitOkMessage{ in_reply_to: body.msg_id }, meta);
    Some(body)
}

pub fn wait_until_message<M: for<'a> Deserialize<'a>>() -> Option<Message<M>> {
    let stdin = io::stdin();
    for line in stdin.lines() {
        match parse_line::<M>(line) {
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

pub fn get_stub_timeout() -> RangeInclusive<u64> {
    100000..=100000
}
