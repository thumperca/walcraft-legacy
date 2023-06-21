mod writer;

use self::writer::WalWriter;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};

#[derive(Debug)]
pub enum WalError {
    Capacity(String),
    File(String),
}

#[derive(Clone)]
pub struct Wal {
    // location of WAL files
    location: Arc<PathBuf>,
    // Shared buffer to communicate with [WalWriter]
    buffer: Arc<Mutex<Vec<Vec<u8>>>>,
    // A channel to alert [WalWriter] of new logs
    sender: Sender<()>,
}

impl Wal {
    pub fn new(location: &str, capacity: usize) -> Result<Self, WalError> {
        if capacity < 100 {
            return Err(WalError::Capacity(
                "Capacity should be at least 100".to_string(),
            ));
        }
        let location = PathBuf::from(location);
        let (tx, rx) = mpsc::channel();
        let writer = WalWriter::new(location.clone(), capacity, rx)?;
        let buffer = writer.buffer();
        std::thread::spawn(move || writer.run());
        Ok(Self {
            location: Arc::new(location),
            buffer,
            sender: tx,
        })
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
        let mut notify = false;
        // new scope for obtaining lock
        {
            // obtain lock on the buffer
            let mut lock = match self.buffer.lock() {
                Ok(l) => l,
                Err(e) => e.into_inner(),
            };
            // check if WalWriter needs to be notified or not
            if lock.is_empty() {
                notify = true;
            }
            // add entry to buffer
            lock.push(encoded);
        }
        // notify WalWriter
        if notify {
            let _ = self.sender.send(());
        }
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
        let mut notify = false;
        // new scope for obtaining lock
        {
            // obtain lock on the buffer
            let mut lock = match self.buffer.lock() {
                Ok(l) => l,
                Err(e) => e.into_inner(),
            };
            // check if WalWriter needs to be notified or not
            if lock.is_empty() {
                notify = true;
            }
            // add to buffer
            lock.extend(data.into_iter());
        }
        // notify WalWriter
        if notify {
            let _ = self.sender.send(());
        }
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
    use std::path::Path;
    use std::time::Duration;

    #[derive(Serialize, Deserialize, Debug)]
    struct Item {
        id: usize,
    }

    fn clear_storage() {
        let mut paths = Vec::new();
        paths.push("./tmp/meta".to_string());
        for i in 1..6 {
            paths.push(format!("./tmp/wal_{}", i));
        }
        for path in paths {
            if Path::new(&path).exists() {
                std::fs::remove_file(path).expect("Failed to delete old file");
            }
        }
    }

    #[test]
    fn simple_write() {
        clear_storage();
        let wal = Wal::new("./tmp/", 10_000).unwrap();
        for i in 0..1000 {
            let item = Item { id: i };
            wal.write(item);
        }
        // allow some time for WalWriter to work
        std::thread::sleep(Duration::from_secs(2));
        // check that log file exists
        let metadata = std::fs::metadata("./tmp/wal_1").expect("Failed to read file");
        assert!(metadata.len() > 5000); // at least 5KB of data is added
    }

    #[test]
    fn multiple_files() {
        // clear existing files
        clear_storage();
        // create a new wal object
        let wal = Wal::new("./tmp/", 100).unwrap();
        // This shall be dumped to first file
        let dump = (1..=30)
            .into_iter()
            .map(|i| Item { id: i })
            .collect::<Vec<_>>();
        wal.batch_write(&dump);
        // This shall be dumped to second file
        let dump = (40..=45)
            .into_iter()
            .map(|i| Item { id: i })
            .collect::<Vec<_>>();
        wal.batch_write(&dump);
        // allow some time for WalWriter to work
        std::thread::sleep(Duration::from_secs(2));
        // check that log file exists
        let metadata1 = std::fs::metadata("./tmp/wal_1").expect("Failed to read file1");
        assert!(metadata1.len() > 200); // at least 200 bytes of data is added
        let metadata2 = std::fs::metadata("./tmp/wal_2").expect("Failed to read file2");
        assert!(metadata2.len() > 10 && metadata2.len() < 100); // more than 10 bytes of data
    }
}
