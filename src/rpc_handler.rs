use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Receiver, Sender};
use crate::messages::Message;
use crate::output::output_message;

pub enum RpcError {
    SenderDropped,
    Timeout,
}
pub struct RpcHandler<Resp> {
    pending: Mutex<HashMap<usize, Sender<Resp>>>,
}
impl<Resp> RpcHandler<Resp> {
    fn new() -> Self {
        Self { pending: Default::default() }
    }
    pub async fn call_rpc<Req: Serialize>(&self, message: Message<Req>, msg_id: usize, timeout_milli: u64) -> Result<Resp, RpcError> {
        output_message(message);
        self.get_response(msg_id, timeout_milli).await
    }
    fn init_receiver(&self, msg_id: usize) -> Receiver<Resp> {
        let (tx, rx) = oneshot::channel();
        let mut lock = self.pending.lock().expect("got a poisoned lock, cant really handle it");
        lock.insert(msg_id, tx);
        drop(lock); // dropping lock guards explicitly to be future-proof. i.e. if i want to add something to the end of the function, i would need make a conscious decision for adding it before or after drop
        rx
    } 
    async fn get_response(&self, msg_id: usize, timeout_milli: u64) -> Result<Resp, RpcError> {
        let rx = self.init_receiver(msg_id);
        let response = tokio::time::timeout(
            Duration::from_millis(timeout_milli),
            rx
        ).await;
        let Ok(response) = response else {
            self.remove_from_pending(msg_id);
            return Err(RpcError::Timeout);
        };
        let Ok(response) = response else {
            self.remove_from_pending(msg_id);
            eprintln!("sender was dropped before we received the message! {msg_id}");
            return Err(RpcError::SenderDropped);
        };
        Ok(response)
    }
    fn remove_from_pending(&self, msg_id: usize) -> Option<Sender<Resp>> {
        let mut lock = self.pending.lock().expect("got a poisoned lock, cant really handle it");
        let result = lock.remove(&msg_id);
        drop(lock);
        result
    }
    pub fn init_response(&self, in_reply_to: usize, message: Resp) {
        // todo: this method is weird, need to think of a better API
        let tx = self.remove_from_pending(in_reply_to);
        let Some(tx) = tx else {
            // can happen if response was duplicated
            return;
        };
        let _ = tx.send(message);
        // fail can happen if response came after timeout
    }
}
impl<M> Default for RpcHandler<M> {
    fn default() -> Self {
        Self::new()
    }
}
