use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct LockInner {
    // Interface to signal writer to stop here
    can_write: AtomicBool,
    // confirmation from writer that it has stopped
    is_writing: AtomicBool,
}

impl LockInner {
    pub fn new() -> Self {
        Self {
            can_write: AtomicBool::new(true),
            is_writing: AtomicBool::new(true),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Lock {
    inner: Arc<LockInner>,
}

// todo: change ordering to ensure proper synchronization between threads
impl Lock {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(LockInner::new()),
        }
    }

    // writer to check if it can write
    pub fn can_write(&self) -> bool {
        self.inner.can_write.load(Ordering::Relaxed)
    }

    // request from interface to writer to stop
    pub fn request_to_stop(&self) {
        self.inner.can_write.store(false, Ordering::Relaxed);
    }

    // response from writer that it is stopping
    pub fn stop(&self) {
        if self.inner.can_write.load(Ordering::Relaxed) {
            panic!("Method `request_to_stop` to stop shall be called before calling `stop`");
        }
        self.inner.is_writing.store(false, Ordering::Relaxed);
    }

    // check if writer has stopped
    pub fn has_stopped(&self) -> bool {
        if self.inner.can_write.load(Ordering::Relaxed) {
            panic!("The lock has not been request to stop");
        }
        self.inner.is_writing.load(Ordering::Relaxed)
    }

    // start write again
    // this is more of a reset method
    pub fn start(&self) {
        self.inner.can_write.store(true, Ordering::Relaxed);
        self.inner.is_writing.store(true, Ordering::Relaxed);
    }
}
