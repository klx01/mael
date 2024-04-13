use std::ops::RangeInclusive;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use rand::Rng;
use serde::Deserialize;
use tokio::io::AsyncBufReadExt;
use tokio::task::JoinSet;
use tokio::time::Instant;
use crate::init::{default_init_service, DefaultInitService};
use crate::messages::{Message, MessageMeta};

pub trait AsyncService<M>: Sync + Send + 'static {
    fn process_message(&self, message: M, meta: MessageMeta) -> impl std::future::Future<Output = ()> + Send;
    fn on_timeout(arc_self: Arc<Self>) -> impl std::future::Future<Output = ()> + Send;
}

pub async fn async_loop<T: AsyncService<M>, M: for<'a> Deserialize<'a> + Send>(service: Arc<T>, timeout_range_milli: Option<RangeInclusive<u64>>) {
    let is_finished = Arc::new(RwLock::new(false));

    let mut join_set = JoinSet::new();
    if let Some(timeout_range) = timeout_range_milli {
        assert!(!timeout_range.is_empty());
        let service = service.clone();
        let is_finished = is_finished.clone();
        join_set.spawn(async move {
            let stub_duration = Duration::from_millis(1000);
            loop {
                let timeout = rand::thread_rng().gen_range(timeout_range.clone());
                let mut interval = tokio::time::interval_at(Instant::now() + Duration::from_millis(timeout), stub_duration);

                T::on_timeout(service.clone()).await;

                interval.tick().await;

                let lock = is_finished.read().expect("got a poisoned lock, cant really handle it");
                let is_finished = *lock;
                drop(lock);
                if is_finished {
                    break;
                }
            }
        });
    }

    let tokio_stdin = tokio::io::stdin();
    let reader = tokio::io::BufReader::new(tokio_stdin);
    let mut lines = reader.lines();
    join_set.spawn(async move {
        loop {
            let line = lines.next_line().await;
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
                    Ok(message) => service.process_message(message.body, message.meta).await,
                    Err(err) => eprintln!("error decoding message: {err} {line}"),
                };
            });
        }
        let mut lock = is_finished.write().expect("got a poisoned lock, cant really handle it");
        *lock = true;
        drop(lock);
    });

    while let Some(res) = join_set.join_next().await {
        if let Err(err) = res {
            eprintln!("error joining task {err}");
        }
    }
}

pub async fn default_init_and_async_loop<T: AsyncService<M> + DefaultInitService, M: for<'a> Deserialize<'a> + Send>(timeout_range_milli: Option<RangeInclusive<u64>>) {
    let Some(service) = default_init_service::<T>() else {
        return;
    };
    let service_arc = Arc::new(service);
    async_loop(service_arc, timeout_range_milli).await
}