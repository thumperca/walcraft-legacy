use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

struct Wal {
    location: Arc<String>,
    buffer: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Wal {
    pub fn new(location: &str, capacity: usize) -> Self {
        todo!()
    }

    pub fn write<T>(entry: T)
    where
        T: Serialize + for<'a> Deserialize<'a>,
    {
        todo!()
    }

    pub fn batch_write<T>(entry: &[T])
    where
        T: Serialize + for<'a> Deserialize<'a>,
    {
        todo!()
    }

    pub fn read<T>() -> Vec<T>
    where
        T: Serialize + for<'a> Deserialize<'a>,
    {
        todo!()
    }

    pub fn empty() {
        todo!()
    }
}
