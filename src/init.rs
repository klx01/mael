use std::io;
use serde::Deserialize;
use crate::messages::{InitMessage, Message, InitOkMessage};
use crate::output::output_reply;
use crate::util::parse_line;

pub trait DefaultInitService {
    fn new(init_message: InitMessage) -> Self;
}

pub fn default_init_service<T: DefaultInitService>() -> Option<T> {
    let init_message = get_init_message()?;
    let service = T::new(init_message);
    Some(service)
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