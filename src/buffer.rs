use crate::entry::LogEntry;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct Buffer {
    inner: Arc<Mutex<Vec<LogEntry>>>,
}

impl Buffer {
    // create a new buffer
    pub fn new() -> Self {
        let inner = Arc::new(Mutex::new(Vec::new()));
        Self { inner }
    }

    // add a log to buffer
    pub fn add(&self, entry: LogEntry) -> bool {
        let mut buffer = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        let notify = buffer.is_empty();
        buffer.push(entry);
        notify
    }

    // add many logs to buffer
    pub fn bulk_add(&self, entry: Vec<LogEntry>) -> bool {
        let mut buffer = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        let notify = buffer.is_empty();
        buffer.extend(entry.into_iter());
        notify
    }

    // get all items and empty the buffer
    pub fn drain(&self) -> Vec<LogEntry> {
        let mut data = Vec::new();
        // Open new scope for locking the queue
        {
            let mut buffer = match self.inner.lock() {
                Ok(g) => g,
                Err(e) => e.into_inner(),
            };
            // If there is data, process it
            if !buffer.is_empty() {
                std::mem::swap(&mut *buffer, &mut data);
            }
        }
        data
    }
}
