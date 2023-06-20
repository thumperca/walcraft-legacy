use crate::WalError;
use std::fmt::format;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, LockResult, Mutex};

pub(crate) struct WalWriter {
    // shared buffer
    buffer: Arc<Mutex<Vec<Vec<u8>>>>,
    // Location where files are stored
    location: PathBuf,
    // Notifier from Wal interface about new log addition
    receiver: Receiver<()>,
    // Handle to current file
    file: File,
    // storage capacity per file
    capacity_per_file: usize,
    // storage capacity filled in the current file
    filled: usize,
    // file sequence number for the current file
    pointer: u8,
}

impl WalWriter {
    pub fn new(
        location: PathBuf,
        capacity: usize,
        receiver: Receiver<()>,
    ) -> Result<Self, WalError> {
        let pointer = 1u8;
        let (file, filled) = Self::set_pointer(location.clone(), pointer)?;
        let wal = Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
            location,
            receiver,
            file,
            capacity_per_file: capacity / 5,
            filled,
            pointer,
        };
        Ok(wal)
    }

    pub fn buffer(&self) -> Arc<Mutex<Vec<Vec<u8>>>> {
        self.buffer.clone()
    }

    pub fn run(self) {
        loop {
            // Wait for the notification of new logs
            self.receiver.recv().unwrap();
            // Open new scope for locking the queue
            {
                let buffer = match self.buffer.lock() {
                    Ok(g) => g,
                    Err(e) => e.into_inner(),
                };
                // If there is data, process it
                if !buffer.is_empty() {
                    println!("process data {}", buffer.len());
                }
            }
        }
    }

    fn set_pointer(location: PathBuf, pointer: u8) -> Result<(File, usize), WalError> {
        // write pointer to meta file
        Self::write_pointer(location.clone(), pointer)?;
        // open and return pointer WAL file
        Self::open_file(location, pointer).map(|file| (file, 0))
    }

    fn write_pointer(mut location: PathBuf, pointer: u8) -> Result<(), WalError> {
        location.push("meta");
        // create a new file for writing logs
        let mut file = match File::create(location) {
            Ok(f) => f,
            Err(_) => {
                return Err(WalError::File("Failed to create pointer file".to_string()));
            }
        };
        // write current pointer
        let text = pointer.to_string();
        if let Err(_) = file.write_all(text.as_bytes()) {
            return Err(WalError::File(
                "Failed to write to pointer file".to_string(),
            ));
        }
        Ok(())
    }

    fn open_file(mut location: PathBuf, pointer: u8) -> Result<File, WalError> {
        let file_name = format!("wal_{}", pointer);
        location.push(file_name);
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(&location)
            .map_err(|_| WalError::File("Failed to open log file".to_string()))
    }
}
