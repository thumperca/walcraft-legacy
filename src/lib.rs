mod writer;

use self::writer::WalWriter;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
enum WalError {
    Capacity(String),
    File(String),
}

struct Wal {
    location: Arc<PathBuf>,
    buffer: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Wal {
    pub fn new(location: &str, capacity: usize) -> Result<Self, WalError> {
        if capacity < 100 {
            return Err(WalError::Capacity(
                "Capacity should be at least 100".to_string(),
            ));
        }
        let writer = WalWriter::new(location, capacity);
        todo!()
    }

    /// Write a log
    pub fn write<T>(&self, entry: T)
    where
        T: Serialize + for<'a> Deserialize<'a>,
    {
        // Serializing entry to binary
        let encoded = match bincode::serialize(&entry) {
            Ok(d) => d,
            Err(_) => {
                return;
            }
        };
        // obtain lock on the buffer
        let mut lock = match self.buffer.lock() {
            Ok(l) => l,
            Err(e) => e.into_inner(),
        };
        // add entry to buffer
        lock.push(encoded);
    }

    /// Batch write many logs
    pub fn batch_write<T>(&self, entries: &[T])
    where
        T: Serialize + for<'a> Deserialize<'a>,
    {
        // serialize to binary
        let mut data = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Ok(d) = bincode::serialize(&entry) {
                data.push(d);
            }
        }
        if data.is_empty() {
            return;
        }
        // obtain lock on the buffer
        let mut lock = match self.buffer.lock() {
            Ok(l) => l,
            Err(e) => e.into_inner(),
        };
        // add to buffer
        lock.extend(data.into_iter());
    }

    /// Read all written logs
    pub fn read<T>() -> Vec<T>
    where
        T: Serialize + for<'a> Deserialize<'a>,
    {
        todo!()
    }

    /// Clean or empty all existing logs
    pub fn empty() {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Debug)]
    struct Item {
        id: u8,
    }

    #[test]
    fn it_works() {
        let item = Item { id: 1 };
        let wal = Wal::new("./tmp/", 100).unwrap();
        wal.write(item);
    }
}
