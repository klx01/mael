use std::sync::atomic::{AtomicUsize, Ordering};

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