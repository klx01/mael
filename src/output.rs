use std::sync::Mutex;
use serde::Serialize;
use crate::messages::{Message, MessageMeta};

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