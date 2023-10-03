use crate::buffer::Buffer;
use crate::lock::LockManager;
use crate::WalError;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::thread::sleep;
use std::time::Duration;

// Arguments or properties needed to create a [WalWriter] instance
pub(crate) struct WalWriterProps {
    pub buffer: Buffer,
    pub location: PathBuf,
    pub receiver: Receiver<()>,
    pub lock: LockManager,
    pub capacity: usize,
}

// Writer responsible for saving logs on secondary storage
// The WalWriter takes logs from the buffer and appends them to log files
pub(crate) struct WalWriter {
    // shared buffer
    buffer: Buffer,
    // Location where files are stored
    location: PathBuf,
    // Notifier from Wal interface about new log addition
    receiver: Receiver<()>,
    // Handle to current file
    file: File,
    // Lock manager to switch between read and write mode for file IO
    lock: LockManager,
    // storage capacity per file
    capacity_per_file: usize,
    // storage capacity filled in the current file
    filled: usize,
    // file sequence number for the current file
    pointer: u8,
}

impl WalWriter {
    pub fn new(props: WalWriterProps) -> Result<Self, WalError> {
        let pointer = 1u8;
        let (file, filled) = Self::set_pointer(props.location.clone(), pointer, false)?;
        Ok(Self {
            buffer: props.buffer,
            location: props.location,
            receiver: props.receiver,
            file,
            lock: props.lock,
            capacity_per_file: props.capacity / 4,
            filled,
            pointer,
        })
    }

    pub fn run(mut self) {
        loop {
            // spin lock, until allowed to write
            if !self.lock.can_write() {
                sleep(Duration::from_millis(10));
                continue;
            }
            // Wait for the notification of new logs
            let _d = self.receiver.recv();
            let data = self.buffer.drain();
            if data.is_empty() {
                continue;
            }
            dbg!(data.len());
            self.filled += data.len();
            for item in data {
                let _ = self.file.write_all(&item.to_vec());
            }
            // let _ = self.file.sync_all(); // todo: make this toggleable
            if self.filled >= self.capacity_per_file {
                self.next_file();
            }
            //
            if !self.lock.can_write() {
                self.lock.stop();
            }
        }
    }

    fn next_file(&mut self) {
        // calculate next pointer
        let mut next_pointer = self.pointer + 1;
        if next_pointer > 5 {
            next_pointer = 1;
        }
        // Disk IO for the new pointer & file
        let file = match Self::set_pointer(self.location.clone(), next_pointer, true) {
            Ok((file, _)) => file,
            Err(_) => {
                return;
            }
        };
        // update state
        self.file = file;
        self.pointer = next_pointer;
        self.filled = 0;
    }

    fn set_pointer(
        location: PathBuf,
        pointer: u8,
        delete: bool,
    ) -> Result<(File, usize), WalError> {
        // write pointer to meta file
        Self::write_pointer(location.clone(), pointer)?;
        // open and return pointer WAL file
        Self::open_file(location, pointer, delete).map(|file| (file, 0))
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

    fn open_file(mut location: PathBuf, pointer: u8, delete: bool) -> Result<File, WalError> {
        let file_name = format!("wal_{}", pointer);
        location.push(file_name);
        if delete {
            if let Err(_) = File::create(location.clone()) {
                return Err(WalError::File("Failed to clear old log file".to_string()));
            }
        }
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(&location)
            .map_err(|_| WalError::File("Failed to open log file".to_string()))
    }
}
