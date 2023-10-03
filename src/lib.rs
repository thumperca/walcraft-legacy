mod buffer;
mod entry;
mod lock;
mod reader;
mod writer;

use self::buffer::Buffer;
use self::entry::LogEntry;
use self::lock::LockManager;
use self::reader::WalReader;
use self::writer::{WalWriter, WalWriterProps};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{sleep, Thread};
use std::time::Duration;

#[derive(Debug)]
pub enum WalError {
    Capacity(String),
    File(String),
    Serialization(String),
}

/// A Write Ahead Log (WAL) solution for concurrent operations
///
/// # How?
/// This library gives atomic guarantees in concurrent environments and high performance/throughput
/// by using in-memory buffer and leveraging append-only logs. The library spawns a dedicated log
/// writing thread that is parked when read operations happen.
///
/// The logs are split across multiple files. The older files are deleted to preserve the capacity constraints.
///
///
/// # Usage
/// ```
/// use serde::{Deserialize, Serialize};
/// use walcraft::Wal;
///
/// // Log to write
/// #[derive(Serialize, Deserialize, Clone)]
/// struct Log {
///     id: usize,
///     value: f64
/// }
/// let log = Log {id: 1, value: 5.6234};
///
/// // initiate wal and add a log
/// let wal = Wal::new("./tmp/", 500).unwrap(); // 500MB of log capacity
/// wal.write(log); // write a log
///
/// // write a log in another thread
/// let wal2 = wal.clone();
/// std::thread::spawn(move || {
///     let log = Log{id: 2, value: 0.45};
///     wal2.write(log);
/// });
///
/// // keep writing logs in current thread
/// let log = Log{id: 3, value: 123.59};
/// wal.write(log);
/// ```
///
#[derive(Clone)]
pub struct Wal<T>
where
    T: Serialize + for<'a> Deserialize<'a>,
{
    // location of WAL files
    location: PathBuf,
    // capacity of data
    capacity: usize, // todo: remove this field as this shall be managed by the writer
    // Shared buffer to communicate with [WalWriter]
    buffer: Buffer,
    // A channel to alert [WalWriter] of new logs
    sender: Sender<()>,
    // Lock manager to switch between read and write mode for file IO
    lock: LockManager,
    // Handle to write thread.. needed to unpark the thread when going from read to write mode
    writer: Thread,
    // State for whether we are in read mode or write mode.. true here means read mode
    read_lock: Arc<Mutex<()>>,
    // Phantom ownership of generic to avoid usage of complex lifetimes
    phantom: PhantomData<T>,
}

impl<T> Wal<T>
where
    T: Serialize + for<'a> Deserialize<'a>,
{
    /// Create a new WAL instance
    ///
    /// # Arguments
    /// - `location`: The location on storage where to store WAL files
    /// - `capacity`: The size of WAL on storage in MBs
    ///
    /// # Examples
    /// The code below creates a WAL at location `/tmp/` for 2GB
    /// ```rust,ignore
    /// use walcraft::Wal;
    /// let wal = Wal::new("./tmp/", 2_000);
    /// ```
    ///
    pub fn new(location: &str, capacity: usize) -> Result<Self, WalError> {
        if capacity < 100 {
            return Err(WalError::Capacity(
                "Capacity should be at least 100".to_string(),
            ));
        }
        let location = PathBuf::from(location);
        let (tx, rx) = mpsc::channel();
        let buffer = Buffer::new();
        let lock = LockManager::new();

        // start writer thread
        let props = WalWriterProps {
            buffer: buffer.clone(),
            location: location.clone(),
            receiver: rx,
            lock: lock.clone(),
            capacity,
        };
        let writer = WalWriter::new(props)?;
        let writer = std::thread::spawn(move || writer.run()).thread().clone();

        // return WAL handle
        Ok(Self {
            location,
            buffer,
            capacity,
            writer,
            sender: tx,
            lock,
            read_lock: Arc::new(Mutex::new(())),
            phantom: Default::default(),
        })
    }

    /// Write an item to log
    ///
    /// # Example
    /// ```
    /// use serde::{Deserialize, Serialize};
    /// use walcraft::Wal;
    ///
    /// // Log to write
    /// #[derive(Serialize, Deserialize)]
    /// struct Log {
    ///     id: usize,
    ///     value: f64
    /// }
    /// let log1 = Log {id: 12, value: 5.6234};
    /// let log2 = Log {id: 13, value: 0.3484};
    ///
    /// // create wal and add a log
    /// let wal = Wal::new("./tmp/", 500).unwrap();
    /// wal.write(log1);
    /// wal.write(log2);
    /// ```
    ///
    pub fn write(&self, entry: T) {
        // Serializing entry to binary
        let entry = match LogEntry::new(entry) {
            None => return,
            Some(e) => e,
        };
        // add log to buffer
        let notify = self.buffer.add(entry);
        // notify writer thread
        if notify {
            let _ = self.sender.send(());
        }
    }

    /// Batch write many logs in a single step
    ///
    /// # Example
    /// ```
    /// use serde::{Deserialize, Serialize};
    /// use walcraft::Wal;
    ///
    /// // Log to write
    /// #[derive(Serialize, Deserialize)]
    /// struct Log {
    ///     id: usize,
    ///     value: f64
    /// }
    /// let log1 = Log {id: 12, value: 5.6234};
    /// let log2 = Log {id: 13, value: 0.3484};
    /// let logs = vec![log1, log2];
    ///
    /// // create wal and add a log
    /// let wal = Wal::new("./tmp/", 500).unwrap();
    /// wal.batch_write(logs);
    /// ```
    ///
    pub fn batch_write(&self, entries: Vec<T>) {
        // serialize to binary
        let mut data = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(d) = LogEntry::new(entry) {
                data.push(d);
            }
        }
        if data.is_empty() {
            return;
        }
        // add logs to buffer
        let notify = self.buffer.bulk_add(data);
        // notify writer thread
        if notify {
            let _ = self.sender.send(());
        }
    }

    /// Read all written logs
    //  ToDo: update this method as below and add an `iter()` method
    //  1. This an also be changed to read last 'x' amount of logs
    //     such as wal.read(10_000) read last 10k entries
    //     The files shall be read in the reverse order of what they are written
    //     This will best preserve the last 'x' logs
    //     Also some logs shall come from the buffer as well?
    //  2. Add iter() method that will provide an iterator over all items in array
    //     `for item in wal.iter() {}`
    //
    pub fn read(&self) -> Result<Vec<T>, WalError> {
        loop {
            // acquire read lock
            let _ = self.read_lock.lock();

            // park writer thread
            self.lock.request_to_stop();
            while !self.lock.has_stopped() {
                sleep(Duration::from_millis(1));
            }

            // read data
            let reader = WalReader::new(self.location.clone());
            let buffer = reader.read()?;
            let mut data = Vec::with_capacity(buffer.len());
            for item in buffer {
                if let Some(d) = item.to_original() {
                    data.push(d);
                }
            }
            if data.len() > self.capacity {
                let cutoff = data.len() - self.capacity;
                data = data.split_off(cutoff);
            }

            // start writer thread
            self.lock.start();
            self.writer.unpark();

            // return data
            break Ok(data);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::Duration;

    #[derive(Serialize, Deserialize, Debug)]
    struct Item {
        id: u16,
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
        sleep(Duration::from_secs(2));
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
        wal.batch_write(dump);
        sleep(Duration::from_millis(100));
        // This shall be dumped to second file
        let dump = (40..=45)
            .into_iter()
            .map(|i| Item { id: i })
            .collect::<Vec<_>>();
        wal.batch_write(dump);
        // allow some time for WalWriter to work
        sleep(Duration::from_secs(2));
        // check that log file exists
        let metadata1 = std::fs::metadata("./tmp/wal_1").expect("Failed to read file1");
        assert!(metadata1.len() > 10); // at least 200 bytes of data is added
        let metadata2 = std::fs::metadata("./tmp/wal_2").expect("Failed to read file2");
        assert!(metadata2.len() > 10); // more than 10 bytes of data
    }

    #[test]
    fn read_after_write() {
        clear_storage();
        // create a new wal object
        let wal = Wal::new("./tmp/", 1000).unwrap();
        // This shall be dumped to first file
        let dump = (1..=1234)
            .into_iter()
            .map(|i| Item { id: i })
            .collect::<Vec<_>>();
        wal.batch_write(dump);
        sleep(Duration::from_secs(2));
        let data = wal.read();
        assert!(data.is_ok());
        let data = data.unwrap();
        assert_eq!(data.len(), 1000);
        assert_eq!(data.last().unwrap().id, 1234);
    }
}
