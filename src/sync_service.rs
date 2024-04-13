use std::{io, thread, time};
use std::ops::RangeInclusive;
use std::sync::mpsc;
use std::time::Duration;
use rand::Rng;
use serde::Deserialize;
use crate::init::{default_init_service, DefaultInitService};
use crate::messages::MessageMeta;
use crate::util::parse_line;

pub trait SyncService<M>{
    fn process_message(&mut self, message: M, meta: MessageMeta);
    fn on_timeout(&mut self);
}

pub fn sync_loop<T: SyncService<M>, M: for<'a> Deserialize<'a>>(service: &mut T, timeout_range_milli: Option<RangeInclusive<u128>>) {
    let (tx, rx) = mpsc::channel();
    let stdin = io::stdin();
    let input_thread = thread::spawn(move || {
        for line in stdin.lines() {
            tx.send(line).unwrap()
        }
    });

    if let Some(timeout_range) = timeout_range_milli {
        assert!(!timeout_range.is_empty());
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
    } else {
        for message in rx {
            match parse_line(message) {
                Some(message) => service.process_message(message.body, message.meta),
                None => (),
            }
        }
    }
}

pub fn default_init_and_sync_loop<T: SyncService<M> + DefaultInitService, M: for<'a> Deserialize<'a>>(timeout_range_milli: Option<RangeInclusive<u128>>) {
    let Some(mut service) = default_init_service::<T>() else {
        return;
    };
    sync_loop(&mut service, timeout_range_milli);
}
